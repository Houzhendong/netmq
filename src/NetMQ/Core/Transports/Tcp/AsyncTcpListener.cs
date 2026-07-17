/*
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2010 iMatix Corporation
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
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NetMQ.Core.Transports;

namespace NetMQ.Core.Transports.Tcp
{
    /// <summary>
    /// The async engine's tcp:// listener: mirrors <see cref="TcpListener"/>'s lifecycle
    /// (ProcessPlug/ProcessTerm, monitor events, ephemeral-port readback) but accepts connections
    /// with the BCL <see cref="Socket"/>'s <see cref="Socket.AcceptAsync(CancellationToken)"/>
    /// instead of the classic engine's Proactor, running its accept loop on the owning
    /// <see cref="AsyncIOThread"/>'s loop (continuations return there via the loop's
    /// <see cref="SynchronizationContext"/>, never on an arbitrary thread-pool thread).
    /// </summary>
    internal sealed class AsyncTcpListener : Own
    {
        private const SocketOptionName IPv6Only = (SocketOptionName)27;

        /// <summary>
        /// Address to listen on.
        /// </summary>
        private readonly TcpAddress m_address;

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
        /// The port that was bound on.
        /// </summary>
        private int m_port;

        /// <summary>
        /// Cancels the accept loop on termination.
        /// </summary>
        private CancellationTokenSource? m_acceptCts;

        /// <summary>
        /// Create a new AsyncTcpListener on the given I/O thread and socket.
        /// </summary>
        /// <param name="ioThread">the I/O thread for this to live within</param>
        /// <param name="socket">a SocketBase to listen on</param>
        /// <param name="options">socket-related Options</param>
        public AsyncTcpListener(IIOThread ioThread, SocketBase socket, Options options)
            : base(ioThread, options)
        {
            m_address = new TcpAddress();
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

            // Fire-and-forget: the loop that runs this ProcessPlug already has the owning
            // AsyncIOThread's SynchronizationContext set, so every `await` inside
            // AcceptLoopAsync resumes back on that same loop rather than on the thread pool -
            // see AsyncIOThread's class remarks.
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
        /// Set address to listen on.
        /// </summary>
        /// <param name="addr">a string denoting the address to set this to</param>
        public void SetAddress(string addr)
        {
            m_address.Resolve(addr, m_options.IPv4Only);

            Assumes.NotNull(m_address.Address);

            try
            {
                m_handle = new Socket(m_address.Address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                if (!m_options.IPv4Only && m_address.Address.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    try
                    {
                        // This is not supported on old windows operating systems and might throw exception
                        m_handle.SetSocketOption(SocketOptionLevel.IPv6, IPv6Only, 0);
                    }
                    catch
                    {
                    }
                }

                // This command is failing on linux
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    m_handle.ExclusiveAddressUse = false;

                m_handle.Bind(m_address.Address);
                m_handle.Listen(m_options.Backlog);

                // Copy the port number after binding in case we requested a system-allocated port number (TCP port zero)
                m_address.Address.Port = ((IPEndPoint)m_handle.LocalEndPoint!).Port;
                m_endpoint = m_address.ToString();

                m_socket.EventListening(m_endpoint, m_handle);

                m_port = ((IPEndPoint)m_handle.LocalEndPoint!).Port;
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
                // Disable TIME_WAIT tcp state, mirroring TcpListener.Accept (applied to the
                // listening handle before each accept).
                if (m_options.DisableTimeWait)
                {
                    try
                    {
                        m_handle.LingerState = new LingerOption(true, 0);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                }

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
                            // Mirrors TcpListener.InCompleted's default case (an unrecoverable
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

                ConfigureAcceptedSocket(accepted);

                var engine = AsyncEngineFactory.Create(accepted, m_options, m_endpoint);

                // Choose I/O thread to run the session on. Given that we are already
                // running in an I/O thread, there must be at least one available.
                IIOThread? childIoThread = ChooseIOThread(m_options.Affinity);
                Assumes.NotNull(childIoThread);

                // Create and launch a session object.
                SessionBase session = SessionBase.Create(childIoThread, false, m_socket, m_options, new Address(m_handle.LocalEndPoint!));
                session.IncSeqnum();
                LaunchChild(session);

                SendAttach(session, engine, false);

                m_socket.EventAccepted(m_endpoint, accepted);
            }
        }

        /// <summary>
        /// Apply NoDelay + keep-alive options to a freshly accepted socket, ported from
        /// <see cref="TcpListener.InCompleted"/>'s #if NET branch.
        /// </summary>
        private void ConfigureAcceptedSocket(Socket socket)
        {
            socket.NoDelay = true;

            if (m_options.TcpKeepalive != -1)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, m_options.TcpKeepalive);

                if (m_options.TcpKeepaliveIdle != -1)
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, m_options.TcpKeepaliveIdle / 1000);
                if (m_options.TcpKeepaliveIntvl != -1)
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, m_options.TcpKeepaliveIntvl / 1000);
                if (m_options.TcpKeepaliveCnt != -1)
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, m_options.TcpKeepaliveCnt);
            }
        }

        /// <summary>
        /// Close the listening socket.
        /// </summary>
        private void Close()
        {
            if (m_handle == null)
                return;

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

        /// <summary>
        /// Get the bound address for use with wildcards.
        /// </summary>
        public string Address => m_address.ToString();

        /// <summary>
        /// Get the port-number to listen on.
        /// </summary>
        public int Port => m_port;
    }
}

#endif
