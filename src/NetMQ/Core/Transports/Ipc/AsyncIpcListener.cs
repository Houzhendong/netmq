/*
    Copyright (c) 2011 250bpm s.r.o.
    Copyright (c) 2011-2015 Other contributors as noted in the AUTHORS file

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
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetMQ.Core.Transports;

namespace NetMQ.Core.Transports.Ipc
{
    /// <summary>
    /// The async engine's ipc:// listener: mirrors <see cref="IpcListener"/>'s lifecycle and its
    /// exclusive .lock-file / stale-socket-file semantics verbatim, but listens on a real
    /// <see cref="AddressFamily.Unix"/> BCL <see cref="Socket"/> (via
    /// <see cref="UnixDomainSocketEndPoint"/>) and accepts with
    /// <see cref="Socket.AcceptAsync(CancellationToken)"/> instead of the classic engine's
    /// Proactor, running its accept loop on the owning <c>AsyncIOThread</c>'s loop.
    /// </summary>
    internal sealed class AsyncIpcListener : Own
    {
        /// <summary>
        /// Address to listen on. Reused verbatim from the classic ipc:// transport: resolves the
        /// wildcard "*" to a unique temporary path and tracks the filesystem path to bind to.
        /// </summary>
        private readonly IpcAddress m_address;

        /// <summary>
        /// Socket the listener belongs to.
        /// </summary>
        private readonly SocketBase m_socket;

        /// <summary>
        /// Underlying listening socket.
        /// </summary>
        private Socket? m_handle;

        /// <summary>
        /// String representation of endpoint to bind to.
        /// </summary>
        private string? m_endpoint;

        /// <summary>
        /// An exclusive advisory lock on a sibling "&lt;path&gt;.lock" file, held for the lifetime
        /// of a successful bind - see <see cref="IpcListener"/>'s field of the same name for the
        /// full rationale, reused verbatim here.
        /// </summary>
        private System.IO.FileStream? m_lockStream;

        /// <summary>
        /// Cancels the accept loop on termination.
        /// </summary>
        private CancellationTokenSource? m_acceptCts;

        /// <summary>
        /// Create a new AsyncIpcListener on the given I/O thread and socket.
        /// </summary>
        /// <param name="ioThread">the I/O thread for this to live within</param>
        /// <param name="socket">a SocketBase to listen on</param>
        /// <param name="options">socket-related Options</param>
        public AsyncIpcListener(IIOThread ioThread, SocketBase socket, Options options)
            : base(ioThread, options)
        {
            m_address = new IpcAddress();
            m_handle = null;
            m_socket = socket;
        }

        /// <summary>
        /// Release any contained resources (here - does nothing).
        /// </summary>
        public override void Destroy()
        {
            Debug.Assert(m_handle == null);
        }

        protected override void ProcessPlug()
        {
            Assumes.NotNull(m_handle);

            m_acceptCts = new CancellationTokenSource();

            // Fire-and-forget: ProcessPlug runs on the owning AsyncIOThread's loop, which has its
            // SynchronizationContext set, so every `await` inside AcceptLoopAsync resumes back on
            // that same loop rather than on the thread pool.
            _ = AcceptLoopAsync(m_acceptCts.Token);
        }

        /// <summary>
        /// Process a termination request.
        /// </summary>
        /// <param name="linger">a time (in milliseconds) for this to linger before actually going away. -1 means infinite.</param>
        protected override void ProcessTerm(int linger)
        {
            m_acceptCts?.Cancel();
            Close();
            base.ProcessTerm(linger);
        }

        /// <summary>
        /// Set address to listen on. Ported from <see cref="IpcListener.SetAddress"/> verbatim
        /// except for the socket type: a real AF_UNIX BCL <see cref="Socket"/> bound via
        /// <see cref="UnixDomainSocketEndPoint"/> instead of AsyncIO's AsyncSocket/UnixEndPoint.
        /// </summary>
        /// <param name="addr">a string denoting the address to set this to</param>
        public void SetAddress(string addr)
        {
            m_address.Resolve(addr, false);

            Assumes.NotNull(m_address.Path);

            // Acquire an exclusive advisory lock on a sibling lock-file before touching anything
            // at the bind path - see IpcListener.SetAddress for the full rationale (reused
            // verbatim here). The lock is released either in Close() (below), or here if bind
            // subsequently fails.
            string lockPath = m_address.Path + ".lock";

            try
            {
                m_lockStream = new System.IO.FileStream(lockPath, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None);
            }
            catch (Exception ex) when (ex is System.IO.IOException || ex is UnauthorizedAccessException)
            {
                throw new AddressAlreadyInUseException($"Cannot bind address ( {m_address} ) - already in use.");
            }

            // Delete a stale socket file left over from a previous run, if any (libzmq unlinks the
            // file unconditionally before binding). Safe now that we hold the lock above.
            try
            {
                System.IO.File.Delete(m_address.Path);
            }
            catch
            {
            }

            try
            {
                m_handle = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);

                m_handle.Bind(new UnixDomainSocketEndPoint(m_address.Path));
                m_handle.Listen(m_options.Backlog);

                m_endpoint = m_address.ToString();

                m_socket.EventListening(m_endpoint, m_handle);
            }
            catch (SocketException ex)
            {
                Close();
                throw NetMQException.Create(ex);
            }
        }

        /// <summary>
        /// Accept loop: repeatedly awaits an incoming connection, wires it up to a new session +
        /// async engine, and loops - until cancelled from <see cref="ProcessTerm"/>.
        /// </summary>
        private async Task AcceptLoopAsync(CancellationToken token)
        {
            Assumes.NotNull(m_handle);
            Assumes.NotNull(m_endpoint);

            while (!token.IsCancellationRequested)
            {
                Socket accepted;
                try
                {
                    accepted = await m_handle.AcceptAsync(token).ConfigureAwait(true);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException ex)
                {
                    if (token.IsCancellationRequested)
                        break;

                    switch (ex.SocketErrorCode)
                    {
                        case SocketError.ConnectionReset:
                        case SocketError.NoBufferSpaceAvailable:
                        case SocketError.TooManyOpenSockets:
                            m_socket.EventAcceptFailed(m_endpoint, ex.SocketErrorCode.ToErrorCode());
                            continue;
                        default:
                            // Mirrors IpcListener.InCompleted's default case (an unrecoverable
                            // accept failure) - report it and stop accepting rather than spin.
                            m_socket.EventAcceptFailed(m_endpoint, NetMQException.Create(ex).ErrorCode);
                            return;
                    }
                }

                // The accept can complete successfully in the same loop iteration that
                // ProcessTerm already ran in (ProcessTerm cancels the token and Close()s
                // m_handle, but doesn't - and can't - reach into an AcceptAsync that had already
                // returned before the cancellation was observed here). Re-check before touching
                // m_handle/m_socket or creating a session/engine against a terminated actor tree;
                // otherwise this races Ctx teardown across fixture boundaries.
                if (token.IsCancellationRequested)
                {
                    accepted.Dispose();
                    break;
                }

                // Unix Domain Sockets have no equivalent of TCP NoDelay or keep-alive, so there is
                // nothing to configure on the accepted socket before creating the engine.
                var engine = AsyncEngineFactory.Create(accepted, m_options, m_endpoint);

                // Choose I/O thread to run the session on. Given that we are already
                // running in an I/O thread, there must be at least one available.
                IIOThread? childIoThread = ChooseIOThread(m_options.Affinity);
                Assumes.NotNull(childIoThread);

                // Create and launch a session object.
                SessionBase session = SessionBase.Create(childIoThread, false, m_socket, m_options, new Address(NetMQ.Core.Address.IpcProtocol, m_address.Path ?? string.Empty));
                session.IncSeqnum();
                LaunchChild(session);

                SendAttach(session, engine, false);

                m_socket.EventAccepted(m_endpoint, accepted);
            }
        }

        /// <summary>
        /// Close the listening socket and remove the socket file (the listener owns the file).
        /// Ported from <see cref="IpcListener.Close"/> verbatim.
        /// </summary>
        private void Close()
        {
            if (m_handle != null)
            {
                try
                {
                    Socket closedHandle = m_handle;
                    m_handle.Dispose();

                    if (m_endpoint is not null)
                        m_socket.EventClosed(m_endpoint, closedHandle);
                }
                catch (SocketException ex)
                {
                    if (m_endpoint is not null)
                        m_socket.EventCloseFailed(m_endpoint, ex.SocketErrorCode.ToErrorCode());
                }

                m_handle = null;
            }

            // Best-effort removal of the socket file we created when binding, done while we still
            // hold the lock file acquired in SetAddress.
            if (m_address.Path is not null)
            {
                try
                {
                    System.IO.File.Delete(m_address.Path);
                }
                catch
                {
                }
            }

            // Now release the lock, allowing a waiting rebind to proceed, and clean up the lock
            // file itself (best-effort; a benign race here just means the next bind recreates it).
            if (m_lockStream is not null)
            {
                m_lockStream.Dispose();
                m_lockStream = null;

                if (m_address.Path is not null)
                {
                    try
                    {
                        System.IO.File.Delete(m_address.Path + ".lock");
                    }
                    catch
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Get the bound address for use with wildcards.
        /// </summary>
        public string Address => m_address.ToString();

        /// <summary>
        /// The ipc transport has no port; this is always zero.
        /// </summary>
        public int Port => 0;
    }
}

#endif
