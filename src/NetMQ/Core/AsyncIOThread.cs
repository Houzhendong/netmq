/*
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2009 iMatix Corporation
    Copyright (c) 2007-2015 Other contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#if NET

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NetMQ.Core.Utils;

namespace NetMQ.Core
{
    /// <summary>
    /// The <see cref="IMailbox"/> used by <see cref="AsyncIOThread"/>: a
    /// <see cref="Channel{T}"/>-backed queue with a single reader (the owning
    /// <see cref="AsyncIOThread"/>'s own loop task) and an arbitrary number of
    /// writers (any thread sending it a <see cref="Command"/>).
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="IOThreadMailbox"/>, this mailbox is not polled - there is no
    /// Proactor/Signaler backing it. The owning loop drains it directly through
    /// <see cref="Reader"/>, awaiting <see cref="ChannelReader{T}.WaitToReadAsync"/>
    /// when nothing is pending, so a write is all that's needed to wake the loop up.
    /// </remarks>
    internal sealed class AsyncIOThreadMailbox : IMailbox
    {
        private readonly Channel<Command> m_channel = Channel.CreateUnbounded<Command>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        /// <summary>
        /// The read side, drained directly by the owning <see cref="AsyncIOThread"/>'s
        /// loop task.
        /// </summary>
        public ChannelReader<Command> Reader => m_channel.Reader;

        public void Send(Command command)
        {
            // The channel is unbounded, so writing never blocks; it only fails if the
            // reader side has already been completed (Close()), which we allow to be
            // silently dropped, same as sending to a mailbox that's already shutting down.
            m_channel.Writer.TryWrite(command);
        }

        /// <exception cref="NotSupportedException">
        /// Always. Nothing on the async engine's path polls a mailbox synchronously -
        /// see <see cref="Reader"/>.
        /// </exception>
        public bool TryRecv(int timeout, out Command command)
        {
            throw new NotSupportedException($"{nameof(AsyncIOThreadMailbox)} is drained asynchronously; see {nameof(Reader)}.");
        }

        public void Close()
        {
            m_channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// The async engine's I/O thread: a single async "actor" loop instead of the
    /// classic engine's Proactor (IOCP) poller. It is registered into <see cref="Ctx"/>'s
    /// I/O-thread slots in exactly the same way as <see cref="IOThread"/>, so
    /// <see cref="Ctx.ChooseIOThread"/>/affinity and the rest of the actor-model
    /// plumbing (<see cref="Own"/>, <see cref="SessionBase"/>, ...) work against it
    /// unmodified, through the shared <see cref="IIOThread"/> interface.
    /// </summary>
    /// <remarks>
    /// <para>
    /// INVARIANT: everything owned by this thread - sessions, pipe callbacks, engines -
    /// must run only on <see cref="RunLoopAsync"/>'s own loop iteration, never
    /// concurrently on the thread pool. Two things enforce that:
    /// </para>
    /// <list type="bullet">
    /// <item>Mailbox commands and posted work (<see cref="Post"/>) are only ever
    /// executed from inside the loop.</item>
    /// <item>While the loop is running, <see cref="SynchronizationContext.Current"/> is
    /// set to a <see cref="LoopSynchronizationContext"/> that marshals every posted
    /// continuation back through <see cref="Post"/>, so an `await` of, say, a socket
    /// operation resumes back on this same loop instead of on an arbitrary thread-pool
    /// thread.</item>
    /// </list>
    /// <para>
    /// Timers are implemented in-loop with a min-heap keyed by due-time, instead of
    /// relying on a Proactor: there is no Proactor, no AsyncIO, and no Signaler
    /// anywhere on this path. A timer or a piece of work becoming due is simply
    /// something that can wake the loop's wait early; a mailbox write (from any thread)
    /// is what normally does the waking.
    /// </para>
    /// </remarks>
    internal sealed class AsyncIOThread : ZObject, IMailboxEvent, IIOThread
    {
        private readonly AsyncIOThreadMailbox m_mailbox;

        /// <summary>
        /// The "explicit work-queue" side of continuation affinity: callers (from any
        /// thread) can schedule a callback to run on this thread's own loop via
        /// <see cref="Post"/>. <see cref="LoopSynchronizationContext"/> is built on top
        /// of this same queue.
        /// </summary>
        private readonly Channel<Action> m_workQueue = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        private readonly LoopSynchronizationContext m_synchronizationContext;

        /// <summary>
        /// Min-heap of pending timers, ordered by due-time (in <see cref="Clock.NowMs"/>
        /// units). Only ever touched from within the loop: <see cref="AddTimer"/> and
        /// <see cref="CancelTimer"/>, like the rest of <see cref="IIOThread"/>, are only
        /// invoked by code that is itself already running inside this thread's loop -
        /// exactly the same single-threaded discipline the classic engine relies on for
        /// its own <see cref="PollerBase"/> timer wheel.
        /// </summary>
        private readonly PriorityQueue<TimerEntry, long> m_timerHeap = new();

        /// <summary>
        /// The subset of <see cref="m_timerHeap"/>'s entries that haven't fired or been
        /// cancelled yet, used to resolve <see cref="CancelTimer"/> by sink/id.
        /// </summary>
        private readonly List<TimerEntry> m_activeTimers = new();

        private volatile bool m_stopping;
        private int m_load;
        private Task? m_loopTask;

#if DEBUG
        private readonly string m_name;
#endif

        /// <summary>
        /// Create a new AsyncIOThread object within the given context (Ctx) and thread.
        /// </summary>
        /// <param name="ctx">the Ctx (context) for this thread to live within</param>
        /// <param name="threadId">the integer thread-id for this new AsyncIOThread</param>
        public AsyncIOThread(Ctx ctx, int threadId)
            : base(ctx, threadId)
        {
            m_mailbox = new AsyncIOThreadMailbox();
            m_synchronizationContext = new LoopSynchronizationContext(this);

#if DEBUG
            m_name = "async-iothread-" + threadId;
#endif
        }

        public IMailbox Mailbox => m_mailbox;

        public int Load => Volatile.Read(ref m_load);

        public int Tid => ThreadId;

        /// <summary>
        /// Hook for the async engine's socket-registration code (which is out of scope
        /// for the actor plumbing implemented here) to keep <see cref="Load"/>
        /// meaningful for <see cref="Ctx.ChooseIOThread"/>'s load-balancing once sockets
        /// are actually attached to this thread. Safe to call from any thread.
        /// </summary>
        internal void AdjustLoad(int amount)
        {
            Interlocked.Add(ref m_load, amount);
        }

        /// <summary>
        /// Schedule <paramref name="action"/> to run on this thread's own loop - never
        /// concurrently with anything else this thread owns. Safe to call from any
        /// thread, including from within the loop itself.
        /// </summary>
        public void Post(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            bool written = m_workQueue.Writer.TryWrite(action);
            Debug.Assert(written);
        }

        public void Start()
        {
            // A dedicated, long-running task rather than a pooled one, so this behaves
            // like IOThread's own dedicated worker thread instead of borrowing (and
            // potentially starving) the thread pool.
            m_loopTask = Task.Factory.StartNew(
                RunLoopAsync,
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }

        public void Stop()
        {
            SendStop();
        }

        public void Destroy()
        {
            m_mailbox.Close();
            m_workQueue.Writer.TryComplete();

            try
            {
                // Ctx.Terminate always calls Stop() on every IIOThread before
                // Destroy()ing it (see Ctx.Terminate), so the loop should already be
                // unwinding by the time we get here; this timeout only guards against
                // it being wedged, it isn't meant to gate normal shutdown latency.
                if (m_loopTask != null && !m_loopTask.Wait(TimeSpan.FromSeconds(10)))
                    Debug.Fail("AsyncIOThread loop did not terminate within the shutdown timeout.");
            }
            catch
            {
                // Mirrors Proactor.Destroy: shutdown races surfaced here aren't
                // actionable and must not stop the rest of Ctx.Terminate from tearing
                // down the other threads/resources.
            }
        }

        public void AddTimer(long timeout, IProactorEvents sink, int id)
        {
            long expiration = Clock.NowMs() + timeout;
            var entry = new TimerEntry(sink, id);
            m_timerHeap.Enqueue(entry, expiration);
            m_activeTimers.Add(entry);
        }

        public void CancelTimer(IProactorEvents sink, int id)
        {
            // Manual loop rather than List<T>.Find(predicate) - this runs on the hot
            // per-message heartbeat-timer-cancel path (see AsyncStreamEngine's receive
            // loop), and a lambda capturing id/sink would allocate a fresh closure on
            // every call. RemoveAt(index) also avoids Remove(entry)'s own redundant
            // IndexOf scan.
            for (int i = 0; i < m_activeTimers.Count; i++)
            {
                var entry = m_activeTimers[i];
                if (entry.Id == id && ReferenceEquals(entry.Sink, sink))
                {
                    entry.Cancelled = true;
                    m_activeTimers.RemoveAt(i);
                    return;
                }
            }

            Debug.Fail("CancelTimer: no matching active timer found.");
        }

        protected override void ProcessStop()
        {
            m_stopping = true;
        }

        /// <summary>
        /// Not used: unlike the classic engine's poller-driven <see cref="IOThreadMailbox"/>,
        /// this thread's loop drains its mailbox directly via
        /// <see cref="AsyncIOThreadMailbox.Reader"/> rather than being woken through
        /// <see cref="IMailboxEvent.Ready"/>. Implemented only so this class satisfies
        /// <see cref="IMailboxEvent"/> for parity with <see cref="IOThread"/>'s
        /// callback-driven model.
        /// </summary>
        public void Ready()
        {
        }

        /// <summary>
        /// The actor loop: drains mailbox commands, posted work, and due timers,
        /// interleaved one-at-a-time, until stopped - then goes back to sleep (racing the
        /// mailbox and work-queue channels against the next timer's due time) whenever
        /// there's nothing left to do.
        /// </summary>
        /// <remarks>
        /// Mailbox commands and posted work are drained one item at a time, alternating,
        /// rather than fully draining the mailbox before ever looking at posted work.
        /// This matters for a specific ordering hazard: processing one mailbox command
        /// (e.g. a pipe's ActivateRead, which releases a send loop's parked semaphore)
        /// can itself enqueue a posted-work continuation (the awaiter resuming via
        /// <see cref="LoopSynchronizationContext.Post"/>) that needs to run - typically to
        /// pull and start sending a message the socket just wrote - before a *later*
        /// mailbox command (e.g. that same session's Term, sent moments later by the
        /// user disposing the socket) flips the pipe to its terminated state and makes
        /// that message unreachable. Draining the whole mailbox first would let Term
        /// silently discard an already-written, not-yet-picked-up message even though it
        /// was queued before the corresponding wakeup had a chance to run. Interleaving
        /// gives every reaction to a command a turn before the next distinct command is
        /// processed, without changing overall throughput (both queues still get fully
        /// drained each outer iteration).
        /// </remarks>
        private async Task RunLoopAsync()
        {
            var previousContext = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(m_synchronizationContext);
            try
            {
                var mailboxReader = m_mailbox.Reader;
                var workReader = m_workQueue.Reader;

                while (!m_stopping)
                {
                    // SynchronizationContext.Current is thread-static, but this loop's own
                    // await below (WaitForWorkAsync().ConfigureAwait(false), deliberately not
                    // capturing context - see its call site) can resume this method on a
                    // different thread-pool thread than the one running the previous iteration.
                    // Without re-asserting it here on every iteration, any command handler or
                    // posted action that kicks off fire-and-forget async work of its own (e.g.
                    // AsyncStreamEngine.RunAsync -> SendLoopAsync/ReceiveLoopAsync,
                    // AsyncTcpConnector.StartConnectingAsync) would capture whatever ambient
                    // context that new thread happens to have (typically null) instead of
                    // m_synchronizationContext, so its own awaits would resume directly on the
                    // thread pool instead of being marshalled back through Post/m_workQueue -
                    // breaking the single-threaded-actor invariant and letting it run
                    // concurrently with this loop's own command processing.
                    SynchronizationContext.SetSynchronizationContext(m_synchronizationContext);

                    bool didWork = false;

                    while (!m_stopping)
                    {
                        bool progressed = false;

                        if (mailboxReader.TryRead(out var command))
                        {
                            didWork = true;
                            progressed = true;
                            Assumes.NotNull(command.Destination);

                            // Safety net: a command handler (ProcessPlug/ProcessTerm/etc.)
                            // throwing must never tear down this loop task. An unobserved
                            // fault here would silently kill the actor loop (everything
                            // still queued behind it - other Own objects' Term commands,
                            // timers, posted continuations - would never run again) which,
                            // combined with a subsequent unobserved-task-exception, is what
                            // was crashing the test host across fixture teardown. Log and
                            // keep going instead of letting the exception unwind out of
                            // RunLoopAsync.
                            try
                            {
                                command.Destination.ProcessCommand(command);
                            }
                            catch (Exception ex)
                            {
                                LogLoopException(ex, "processing a mailbox command");
                            }
                        }

                        if (m_stopping)
                            break;

                        // Give anything the command above just posted (e.g. a send loop's
                        // wakeup continuation) a turn before the next mailbox command -
                        // see the class remarks for why this ordering matters.
                        if (workReader.TryRead(out var action))
                        {
                            didWork = true;
                            progressed = true;

                            // Same safety net as above, for posted continuations (async
                            // continuations resumed via LoopSynchronizationContext.Post, e.g. an
                            // accept/connect loop's post-await code).
                            try
                            {
                                action();
                            }
                            catch (Exception ex)
                            {
                                LogLoopException(ex, "running posted work");
                            }
                        }

                        if (!progressed)
                            break;
                    }

                    if (m_stopping)
                        break;

                    // Same safety net as the mailbox-command/posted-work dispatch above: a
                    // sink's TimerEvent throwing must never tear down this loop task.
                    try
                    {
                        if (FireDueTimers())
                            didWork = true;
                    }
                    catch (Exception ex)
                    {
                        LogLoopException(ex, "firing a due timer");
                    }

                    if (didWork)
                        continue;

                    await WaitForWorkAsync(mailboxReader, workReader).ConfigureAwait(false);
                }
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previousContext);
            }
        }

        /// <summary>
        /// Suspends the loop until there's something to do: a mailbox command, posted
        /// work, or the next timer becoming due - whichever comes first.
        /// </summary>
        private async ValueTask WaitForWorkAsync(ChannelReader<Command> mailboxReader, ChannelReader<Action> workReader)
        {
            var mailboxWait = mailboxReader.WaitToReadAsync().AsTask();
            var workWait = workReader.WaitToReadAsync().AsTask();

            long delayMs = NextTimerDelayMs();
            if (delayMs < 0)
            {
                await Task.WhenAny(mailboxWait, workWait).ConfigureAwait(false);
            }
            else
            {
                var delayTask = Task.Delay((int)Math.Min(delayMs, int.MaxValue));
                await Task.WhenAny(mailboxWait, workWait, delayTask).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Routes an exception that escaped a mailbox command or a posted continuation to a log
        /// instead of letting it unwind out of <see cref="RunLoopAsync"/>. Never rethrows: this is
        /// the actor loop's last line of defense, not another throw site.
        /// </summary>
        private void LogLoopException(Exception ex, string what)
        {
            Debug.WriteLine($"AsyncIOThread: unhandled exception while {what}: {ex}");
        }

        /// <summary>
        /// Fires every timer that is currently due, in due-time order.
        /// </summary>
        /// <returns>true if at least one timer fired.</returns>
        private bool FireDueTimers()
        {
            bool fired = false;
            long now = Clock.NowMs();

            while (m_timerHeap.TryPeek(out TimerEntry? entry, out long due) && due <= now)
            {
                m_timerHeap.Dequeue();
                m_activeTimers.Remove(entry);

                if (entry.Cancelled)
                    continue;

                fired = true;
                entry.Sink.TimerEvent(entry.Id);
            }

            return fired;
        }

        /// <summary>
        /// The number of milliseconds until the next timer is due, 0 if one is already
        /// due, or -1 (infinite) if there are no timers pending.
        /// </summary>
        private long NextTimerDelayMs()
        {
            if (!m_timerHeap.TryPeek(out _, out long due))
                return -1;

            long delay = due - Clock.NowMs();
            return delay < 0 ? 0 : delay;
        }

#if DEBUG
        public override string ToString()
        {
            return base.ToString() + "[" + m_name + "]";
        }
#endif

        private sealed class TimerEntry
        {
            public TimerEntry(IProactorEvents sink, int id)
            {
                Sink = sink;
                Id = id;
            }

            public IProactorEvents Sink { get; }
            public int Id { get; }
            public bool Cancelled { get; set; }
        }

        /// <summary>
        /// Marshals every posted continuation back onto the owning <see cref="AsyncIOThread"/>'s
        /// loop via <see cref="AsyncIOThread.Post"/>, so that code awaiting something
        /// while running on the loop resumes back on the loop rather than on an
        /// arbitrary thread-pool thread.
        /// </summary>
        private sealed class LoopSynchronizationContext : SynchronizationContext
        {
            private readonly AsyncIOThread m_owner;

            public LoopSynchronizationContext(AsyncIOThread owner)
            {
                m_owner = owner;
            }

            public override void Post(SendOrPostCallback d, object? state)
            {
                m_owner.Post(() => d(state));
            }

            /// <exception cref="NotSupportedException">
            /// Always. Synchronous dispatch would either deadlock (called re-entrantly
            /// from the loop thread itself) or require blocking the caller until the
            /// loop gets around to it; nothing in this engine needs that - only
            /// <see cref="Post"/> is used, for awaiter continuations.
            /// </exception>
            public override void Send(SendOrPostCallback d, object? state)
            {
                throw new NotSupportedException(
                    $"{nameof(LoopSynchronizationContext)} does not support synchronous Send; use Post so continuations always run on the loop.");
            }

            public override SynchronizationContext CreateCopy() => this;
        }
    }
}

#endif
