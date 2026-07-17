#if NET

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetMQ.Core;
using Xunit;

namespace NetMQ.Tests
{
    /// <summary>
    /// Unit tests for the new async engine's actor thread (<see cref="AsyncIOThread"/>),
    /// exercised directly - independent of sockets/transports, which are out of scope here.
    /// </summary>
    public class AsyncIOThreadTests : IClassFixture<CleanupAfterFixture>
    {
        /// <summary>
        /// Records every id passed to <see cref="TimerEvent"/>, in the order they fired.
        /// </summary>
        private sealed class RecordingTimerSink : IProactorEvents
        {
            private readonly object m_sync = new object();
            private readonly List<int> m_firedIds = new List<int>();

            public IReadOnlyList<int> FiredIds
            {
                get
                {
                    lock (m_sync)
                        return m_firedIds.ToList();
                }
            }

            public void TimerEvent(int id)
            {
                lock (m_sync)
                    m_firedIds.Add(id);
            }

            public void InCompleted(SocketError socketError, int bytesTransferred) => throw new NotSupportedException();

            public void OutCompleted(SocketError socketError, int bytesTransferred) => throw new NotSupportedException();
        }

        /// <summary>
        /// A minimal <see cref="ZObject"/> whose <see cref="ProcessActivateRead"/> override
        /// lets a test observe that a command sent through an <see cref="AsyncIOThread"/>'s
        /// mailbox was actually dispatched.
        /// </summary>
        private sealed class TestZObject : ZObject
        {
            private int m_activateReadCount;

            public TestZObject(Ctx ctx, int threadId)
                : base(ctx, threadId)
            {
            }

            public int ActivateReadCount => Volatile.Read(ref m_activateReadCount);

            protected override void ProcessActivateRead()
            {
                Interlocked.Increment(ref m_activateReadCount);
            }
        }

        private static bool SpinWaitUntil(Func<bool> condition, TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                if (condition())
                    return true;

                Thread.Sleep(5);
            }

            return condition();
        }

        /// <summary>
        /// Sends <paramref name="thread"/> a Stop command directly through its own
        /// mailbox. In production, <see cref="AsyncIOThread.Stop"/> routes Stop through
        /// <see cref="Ctx.SendCommand"/>, which requires the thread to already be
        /// registered in the context's slot table - as <see cref="Ctx.CreateSocket"/>
        /// does for real threads. These tests construct an <see cref="AsyncIOThread"/>
        /// directly, bypassing that registration, so this exercises the same
        /// mailbox -> loop -> ProcessStop path without needing a fully wired-up Ctx.
        /// </summary>
        private static void StopViaMailbox(AsyncIOThread thread)
        {
            thread.Mailbox.Send(new Command(thread, CommandType.Stop));
        }

        [Fact]
        public void PostedWorkRunsSeriallyOnTheLoop()
        {
            var thread = new AsyncIOThread(new Ctx(), 2);
            thread.Start();
            try
            {
                const int count = 500;

                var sync = new object();
                var order = new List<int>();
                var done = new ManualResetEventSlim(false);
                int? loopThreadId = null;
                bool sameThreadEveryTime = true;

                for (int i = 0; i < count; i++)
                {
                    int captured = i;
                    thread.Post(() =>
                    {
                        int currentThreadId = Thread.CurrentThread.ManagedThreadId;

                        lock (sync)
                        {
                            if (loopThreadId == null)
                                loopThreadId = currentThreadId;
                            else if (loopThreadId != currentThreadId)
                                sameThreadEveryTime = false;

                            order.Add(captured);

                            if (order.Count == count)
                                done.Set();
                        }
                    });
                }

                Assert.True(done.Wait(TimeSpan.FromSeconds(10)), "Posted work never finished running.");
                Assert.True(sameThreadEveryTime, "Posted work did not all run on the same (loop) thread.");
                Assert.Equal(Enumerable.Range(0, count), order);
            }
            finally
            {
                StopViaMailbox(thread);
                thread.Destroy();
            }
        }

        [Fact]
        public void MailboxCommandsAreProcessed()
        {
            var ctx = new Ctx();
            var thread = new AsyncIOThread(ctx, 2);
            thread.Start();
            try
            {
                var target = new TestZObject(ctx, 2);

                thread.Mailbox.Send(new Command(target, CommandType.ActivateRead));
                thread.Mailbox.Send(new Command(target, CommandType.ActivateRead));

                Assert.True(
                    SpinWaitUntil(() => target.ActivateReadCount == 2, TimeSpan.FromSeconds(10)),
                    "Commands sent through the mailbox were never dispatched.");
            }
            finally
            {
                StopViaMailbox(thread);
                thread.Destroy();
            }
        }

        [Fact]
        public void TimersFireInDueOrderRatherThanInsertionOrder()
        {
            var thread = new AsyncIOThread(new Ctx(), 2);
            thread.Start();
            try
            {
                var sink = new RecordingTimerSink();
                var scheduled = new ManualResetEventSlim(false);

                // AddTimer/CancelTimer are only safe to call from within the loop (see
                // AsyncIOThread's remarks), so schedule the timers via Post, exactly as
                // owned objects (sessions/engines) would from their own command processing.
                thread.Post(() =>
                {
                    // Deliberately added out of due-order, to prove that due-time - not
                    // insertion order - determines firing order.
                    thread.AddTimer(150, sink, 3);
                    thread.AddTimer(10, sink, 1);
                    thread.AddTimer(60, sink, 2);
                    scheduled.Set();
                });

                Assert.True(scheduled.Wait(TimeSpan.FromSeconds(10)));
                Assert.True(
                    SpinWaitUntil(() => sink.FiredIds.Count == 3, TimeSpan.FromSeconds(10)),
                    "Not all timers fired.");

                Assert.Equal(new[] { 1, 2, 3 }, sink.FiredIds);
            }
            finally
            {
                StopViaMailbox(thread);
                thread.Destroy();
            }
        }

        [Fact]
        public void CancelledTimerNeverFires()
        {
            var thread = new AsyncIOThread(new Ctx(), 2);
            thread.Start();
            try
            {
                var sink = new RecordingTimerSink();
                var scheduled = new ManualResetEventSlim(false);

                thread.Post(() =>
                {
                    thread.AddTimer(20, sink, 1);
                    thread.CancelTimer(sink, 1);

                    // A second, un-cancelled timer gives us something to wait on that
                    // proves the loop kept running past the point where the cancelled
                    // timer would otherwise have fired.
                    thread.AddTimer(60, sink, 2);
                    scheduled.Set();
                });

                Assert.True(scheduled.Wait(TimeSpan.FromSeconds(10)));
                Assert.True(
                    SpinWaitUntil(() => sink.FiredIds.Count == 1, TimeSpan.FromSeconds(10)),
                    "The un-cancelled timer never fired.");

                // Give a cancelled-but-buggy timer a further chance to fire before we
                // assert it never did.
                Thread.Sleep(100);

                Assert.Equal(new[] { 2 }, sink.FiredIds);
            }
            finally
            {
                StopViaMailbox(thread);
                thread.Destroy();
            }
        }

        [Fact]
        public void StopAndDestroyShutDownCleanly()
        {
            var thread = new AsyncIOThread(new Ctx(), 2);
            thread.Start();

            StopViaMailbox(thread);

            var sw = Stopwatch.StartNew();
            thread.Destroy();

            // Destroy() blocks until the loop task actually exits, so a prompt return
            // proves the loop unwound rather than being abandoned/leaked.
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), "Destroy() took too long to return.");
        }

        [Fact]
        public async Task FlagOnContextCreatesAndCleansUpWithoutHanging()
        {
            NetMQConfig.Cleanup();
            NetMQConfig.UseAsyncEngine = true;
            try
            {
                // Merely creating the context (no sockets needed) is enough to prove the
                // flag is threaded through to Ctx without throwing.
                Assert.True(NetMQConfig.Context.UseAsyncEngine);

                // Cleanup() must return promptly rather than hanging - run it on a
                // separate thread and bound the wait so a regression that deadlocks
                // fails this test instead of hanging the whole run.
                await Task.Run(() => NetMQConfig.Cleanup()).WaitAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                NetMQConfig.UseAsyncEngine = false;
                NetMQConfig.Cleanup();
            }
        }
    }
}

#endif
