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
    /// The async engine's ipc:// connector: mirrors <see cref="IpcConnector"/>'s lifecycle
    /// (ProcessPlug/ProcessTerm, reconnect backoff, monitor events) but connects a real
    /// <see cref="AddressFamily.Unix"/> BCL <see cref="Socket"/> (via
    /// <see cref="UnixDomainSocketEndPoint"/>) using
    /// <c>ConnectAsync(EndPoint, CancellationToken)</c> instead of the classic
    /// engine's Proactor.
    /// </summary>
    /// <remarks>
    /// If 'delay' is true connector first waits for a while, then starts connection process.
    /// </remarks>
    internal sealed class AsyncIpcConnector : Own, IProactorEvents
    {
        /// <summary>
        /// ID of the timer used to delay the reconnection. Value is 1.
        /// </summary>
        private const int ReconnectTimerId = 1;

        private readonly IOObject m_ioObject;

        /// <summary>
        /// Address to connect to. Owned by session_base_t.
        /// </summary>
        private readonly Address m_addr;

        /// <summary>
        /// The underlying BCL socket, while a connect attempt is in flight.
        /// </summary>
        private Socket? m_s;

        /// <summary>
        /// Cancels an in-flight connect attempt on termination.
        /// </summary>
        private CancellationTokenSource? m_connectCts;

        /// <summary>
        /// If true, connector is waiting a while before trying to connect.
        /// </summary>
        private readonly bool m_delayedStart;

        /// <summary>
        /// True if a timer has been started.
        /// </summary>
        private bool m_timerStarted;

        /// <summary>
        /// Reference to the session we belong to.
        /// </summary>
        private readonly SessionBase m_session;

        /// <summary>
        /// Current reconnect-interval. This gets updated for back-off strategy.
        /// </summary>
        private int m_currentReconnectIvl;

        /// <summary>
        /// String representation of endpoint to connect to.
        /// </summary>
        private readonly string m_endpoint;

        /// <summary>
        /// Socket.
        /// </summary>
        private readonly SocketBase m_socket;

        /// <summary>
        /// Create a new AsyncIpcConnector object.
        /// </summary>
        /// <param name="ioThread">the I/O-thread for this AsyncIpcConnector to live on.</param>
        /// <param name="session">the session that will contain this</param>
        /// <param name="options">Options that define this new AsyncIpcConnector</param>
        /// <param name="addr">the Address for this Ipc to connect to</param>
        /// <param name="delayedStart">this boolean flag dictates whether to wait before trying to connect</param>
        public AsyncIpcConnector(IIOThread ioThread, SessionBase session, Options options, Address addr, bool delayedStart)
            : base(ioThread, options)
        {
            m_ioObject = new IOObject(ioThread);
            m_addr = addr;
            m_s = null;
            m_delayedStart = delayedStart;
            m_timerStarted = false;
            m_session = session;
            m_currentReconnectIvl = m_options.ReconnectIvl;

            Assumes.NotNull(m_addr);
            m_endpoint = m_addr.ToString();
            m_socket = session.Socket;
        }

        /// <summary>
        /// This does nothing.
        /// </summary>
        public override void Destroy()
        {
            Debug.Assert(!m_timerStarted);
            Debug.Assert(m_s == null);
        }

        /// <summary>
        /// Begin connecting. If a delayed-start was specified - then the reconnect-timer is set, otherwise this starts immediately.
        /// </summary>
        protected override void ProcessPlug()
        {
            m_ioObject.SetHandler(this);
            if (m_delayedStart)
                AddReconnectTimer();
            else
            {
                // Fire-and-forget: ProcessPlug runs on the owning AsyncIOThread's loop, which has
                // its SynchronizationContext set, so every `await` inside StartConnectingAsync
                // resumes back on that same loop rather than on the thread pool.
                _ = StartConnectingAsync();
            }
        }

        /// <summary>
        /// Process a termination request.
        /// This cancels the reconnect-timer, cancels any in-flight connect, and closes the socket.
        /// </summary>
        /// <param name="linger">a time (in milliseconds) for this to linger before actually going away. -1 means infinite.</param>
        protected override void ProcessTerm(int linger)
        {
            if (m_timerStarted)
            {
                m_ioObject.CancelTimer(ReconnectTimerId);
                m_timerStarted = false;
            }

            m_connectCts?.Cancel();

            if (m_s != null)
                Close();

            base.ProcessTerm(linger);
        }

        /// <summary>
        /// Internal function to start the actual connection establishment.
        /// </summary>
        private async Task StartConnectingAsync()
        {
            Debug.Assert(m_s == null);

            // Create the socket.
            Socket socket;
            try
            {
                socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            }
            catch (SocketException)
            {
                AddReconnectTimer();
                return;
            }

            m_s = socket;
            m_connectCts = new CancellationTokenSource();

            // Connect to the remote peer.
            ValueTask connectTask;
            try
            {
                var ipcAddress = (IpcAddress)m_addr.Resolved!;
                Assumes.NotNull(ipcAddress.Path);
                connectTask = socket.ConnectAsync(new UnixDomainSocketEndPoint(ipcAddress.Path), m_connectCts.Token);
            }
            catch (SocketException ex)
            {
                HandleConnectFailure(ex.SocketErrorCode);
                return;
            }

            try
            {
                m_socket.EventConnectDelayed(m_endpoint, ErrorCode.InProgress);
            }
            // TerminatingException can occur in the above call to EventConnectDelayed via
            // MonitorEvent.Write if corresponding PairSocket has been sent Term command
            catch (TerminatingException)
            {
            }

            try
            {
                await connectTask.ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
                // Terminated mid-connect; ProcessTerm already cancelled and will Close().
                return;
            }
            catch (SocketException ex)
            {
                HandleConnectFailure(ex.SocketErrorCode);
                return;
            }

            // The connect can complete successfully in the same loop iteration that
            // ProcessTerm already ran in: ProcessTerm cancels m_connectCts and Close()s (i.e.
            // Dispose()s) m_s, but can't reach into a ConnectAsync that had already returned
            // before the cancellation was observed here. When that happens, `socket` above *is*
            // m_s and has already been disposed by ProcessTerm's Close() - don't touch it again,
            // and don't create an engine/session against a terminated actor tree.
            if (m_connectCts.IsCancellationRequested)
                return;

            HandleConnectSuccess(socket);
        }

        /// <summary>
        /// Handle a failed (or cancelled-then-observed) connect attempt: close the socket and,
        /// unless reconnection is disabled, schedule a retry.
        /// </summary>
        private void HandleConnectFailure(SocketError socketError)
        {
            Close();

            // Try again to connect after a time.
            if (m_options.ReconnectIvl >= 0)
                AddReconnectTimer();
        }

        /// <summary>
        /// Handle a successful connect: create the engine and attach it to the session. Unix
        /// Domain Sockets have no equivalent of TCP NoDelay or keep-alive, so there is nothing to
        /// configure on the socket first (mirrors <see cref="IpcConnector.OutCompleted"/>).
        /// </summary>
        private void HandleConnectSuccess(Socket socket)
        {
            var engine = AsyncEngineFactory.Create(socket, m_options, m_endpoint);

            m_socket.EventConnected(m_endpoint, socket);

            m_s = null;

            // Attach the engine to the corresponding session object.
            SendAttach(m_session, engine);

            // Shut the connector down.
            Terminate();
        }

        /// <summary>
        /// This is called when the reconnect timer expires - to start trying to connect.
        /// </summary>
        /// <param name="id">The timer-id. This is not used.</param>
        public void TimerEvent(int id)
        {
            m_timerStarted = false;
            _ = StartConnectingAsync();
        }

        /// <summary>
        /// This method would be called when a message receive operation has been completed via a
        /// Proactor - not applicable here since there is no Proactor on the async engine's path.
        /// </summary>
        void IProactorEvents.InCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not driven by a Proactor here (see <see cref="HandleConnectSuccess"/>/
        /// <see cref="HandleConnectFailure"/> instead), only implemented to satisfy
        /// <see cref="IProactorEvents"/> for <see cref="IOObject.SetHandler"/>.
        /// </summary>
        void IProactorEvents.OutCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Internal function to add a reconnect timer.
        /// </summary>
        private void AddReconnectTimer()
        {
            int rcIvl = GetNewReconnectIvl();
            m_ioObject.AddTimer(rcIvl, ReconnectTimerId);
            m_socket.EventConnectRetried(m_endpoint, rcIvl);
            m_timerStarted = true;
        }

        /// <summary>
        /// Internal function to return a reconnect back-off delay.
        /// Will modify the current_reconnect_ivl used for next call.
        /// Returns the currently used interval.
        /// </summary>
        private int GetNewReconnectIvl()
        {
            // The new interval is the current interval + random value.
            int thisInterval = m_currentReconnectIvl + new Random().Next(0, m_options.ReconnectIvl);

            // Only change the current reconnect interval if the maximum reconnect
            // interval was set and if it's larger than the reconnect interval.
            if (m_options.ReconnectIvlMax > 0 &&
                m_options.ReconnectIvlMax > m_options.ReconnectIvl)
            {
                // Calculate the next interval
                m_currentReconnectIvl = m_currentReconnectIvl * 2;
                if (m_currentReconnectIvl >= m_options.ReconnectIvlMax)
                {
                    m_currentReconnectIvl = m_options.ReconnectIvlMax;
                }
            }
            return thisInterval;
        }

        /// <summary>
        /// Close the connecting socket.
        /// </summary>
        private void Close()
        {
            Assumes.NotNull(m_s);
            try
            {
                Socket closedHandle = m_s;
                m_s.Dispose();

                m_socket.EventClosed(m_endpoint, closedHandle);

                m_s = null;
            }
            catch (SocketException ex)
            {
                m_socket.EventCloseFailed(m_endpoint, ex.SocketErrorCode.ToErrorCode());
                m_s = null;
            }
        }
    }
}

#endif
