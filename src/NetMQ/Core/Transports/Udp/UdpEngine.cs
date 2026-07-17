using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace NetMQ.Core.Transports.Udp
{
    /// <summary>
    /// UDP transport engine for connectionless datagram messaging.
    /// Supports all socket patterns (Radio/Dish, Pub/Sub, Req/Rep, Push/Pull, Pair, etc.) over UDP.
    /// </summary>
    /// <remarks>
    /// UDP datagram wire format:
    ///   [1 byte: num_frames]
    ///   for each frame:
    ///     [2 bytes: frame_len (big-endian)]
    ///     [frame_len bytes: frame_data]
    /// </remarks>
    internal sealed class UdpEngine : IEngine, IProactorEvents
    {
        private const int ReceiveTimerId = 1;
        private const int MaxUdpDatagram = 65535;

        private readonly Options m_options;
        private readonly Address m_addr;
        private readonly bool m_bind;

        private Socket? m_socket;
        private SessionBase? m_session;
        private IOObject? m_ioObject;

        private bool m_plugged;
        private bool m_terminated;
        private bool m_sendActive;

        private enum ReceiveState
        {
            Active,
            Stuck
        }

        private ReceiveState m_receiveState;

        /// <summary>
        /// Pending datagram that could not be fully pushed because the pipe was full.
        /// </summary>
        private byte[]? m_pendingDatagram;
        private int m_pendingFrameIndex;

        /// <summary>
        /// Last remote endpoint we received a datagram from (bind mode).
        /// Used for sending replies back in bidirectional patterns (REQ/REP, PAIR, etc.).
        /// </summary>
        private EndPoint? m_lastRemoteEndpoint;

        /// <summary>
        /// Reusable receive buffer.
        /// </summary>
        private readonly byte[] m_receiveBuffer = new byte[MaxUdpDatagram];

        public UdpEngine(Options options, Address addr, bool bind)
        {
            m_options = options;
            m_addr = addr;
            m_bind = bind;
            m_receiveState = ReceiveState.Active;
        }

        public void Plug(IIOThread ioThread, SessionBase session)
        {
            m_session = session;
            m_ioObject = new IOObject(null);
            m_ioObject.SetHandler(this);
            m_ioObject.Plug(ioThread);

            var udpAddr = (UdpAddress)m_addr.Resolved!;
            Assumes.NotNull(udpAddr.Address);

            m_socket = new Socket(
                udpAddr.Address.AddressFamily,
                SocketType.Dgram,
                ProtocolType.Udp)
            {
                Blocking = false
            };

            if (m_bind)
            {
                m_socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                m_socket.Bind(udpAddr.Address);
            }
            else
            {
                m_socket.Connect(udpAddr.Address);
            }

            m_plugged = true;
            m_sendActive = true;

            // Push an identity frame if required (Router/Rep sockets expect this
            // as the first message in the pipe, normally provided by the ZMTP handshake).
            if (m_options.RecvIdentity)
            {
                var identity = new Msg();
                identity.InitEmpty(); // empty identity = auto-generate
                m_session.PushMsg(ref identity);
                m_session.Flush();
            }

            // Start timer to poll for incoming datagrams on the IO thread
            m_ioObject.AddTimer(1, ReceiveTimerId);

            // Prime the pipe by attempting to pull messages.
            // This sets the session pipe's m_inActive to false (when the pipe is empty),
            // enabling future ActivateRead notifications to reach engine.ActivateOut().
            // Without this, the pipe's ProcessActivateRead guards skip the callback
            // because m_inActive starts as true. TCP (StreamEngine.Activate → BeginSending)
            // and PGM (PgmSender.Plug → session.PullMsg) do the same.
            SendPendingMessages();
        }

        public void Terminate()
        {
            m_terminated = true;
            m_plugged = false;

            if (m_ioObject != null)
            {
                m_ioObject.CancelTimer(ReceiveTimerId);
                m_ioObject.Unplug();
            }

            try { m_socket?.Close(); }
            catch { }

            m_socket?.Dispose();
            m_socket = null;
        }

        /// <summary>
        /// Called by the session when the pipe has space for more incoming messages.
        /// </summary>
        public void ActivateIn()
        {
            if (m_receiveState == ReceiveState.Stuck)
            {
                m_receiveState = ReceiveState.Active;

                // Try to push the pending datagram first
                if (m_pendingDatagram != null)
                {
                    if (!PushDatagram(m_pendingDatagram, m_pendingFrameIndex))
                    {
                        m_receiveState = ReceiveState.Stuck;
                        return;
                    }
                    m_pendingDatagram = null;
                    m_pendingFrameIndex = 0;
                }

                // Continue polling
                PollReceive();
            }
        }

        /// <summary>
        /// Called by the session when there are messages to send.
        /// </summary>
        public void ActivateOut()
        {
            Assumes.NotNull(m_session);

            m_sendActive = true;
            SendPendingMessages();
        }

        /// <summary>
        /// Pull messages from the session, collect all frames of each message,
        /// and send as a single UDP datagram.
        /// </summary>
        /// <remarks>
        /// Generic wire format (supports both Radio/Dish and Pub/Sub multipart):
        ///   [1 byte: num_frames]
        ///   for each frame:
        ///     [2 bytes: frame_len (big-endian)]
        ///     [frame_len bytes: frame_data]
        /// </remarks>
        private void SendPendingMessages()
        {
            Assumes.NotNull(m_session);
            Assumes.NotNull(m_socket);

            while (m_sendActive)
            {
                // Collect all frames of one message
                var frames = new System.Collections.Generic.List<byte[]>();
                bool gotMessage = false;

                while (true)
                {
                    var msg = new Msg();
                    msg.InitEmpty();

                    var result = m_session.PullMsg(ref msg);
                    if (result != PullMsgResult.Ok)
                    {
                        if (msg.IsInitialised)
                            msg.Close();

                        if (frames.Count == 0)
                        {
                            // No message available at all
                            m_sendActive = false;
                        }
                        // else: partial message pulled (shouldn't happen normally)
                        break;
                    }

                    // Skip ZMTP command messages (subscriptions).
                    // UDP uses broadcast-style delivery; subscriptions are not sent over the wire.
                    if (msg.HasCommand)
                    {
                        msg.Close();
                        continue;
                    }

                    bool hasMore = msg.HasMore;

                    // Copy frame data
                    byte[] frameData = new byte[msg.Size];
                    if (msg.Size > 0)
                        ((ReadOnlySpan<byte>)msg).CopyTo(frameData);

                    frames.Add(frameData);
                    msg.Close();

                    if (!hasMore)
                    {
                        gotMessage = true;
                        break;
                    }
                }

                if (gotMessage && frames.Count > 0)
                    SendDatagram(frames);
                else if (!m_sendActive)
                    break;
            }
        }

        private void SendDatagram(System.Collections.Generic.List<byte[]> frames)
        {
            Assumes.NotNull(m_socket);

            // Calculate total datagram size
            int totalLen = 1; // num_frames byte
            foreach (var frame in frames)
                totalLen += 2 + frame.Length; // 2-byte length + data

            if (totalLen > MaxUdpDatagram)
                return; // Too large, drop silently

            byte[] datagram = new byte[totalLen];
            datagram[0] = (byte)frames.Count;

            int offset = 1;
            foreach (var frame in frames)
            {
                // Big-endian frame length
                datagram[offset] = (byte)(frame.Length >> 8);
                datagram[offset + 1] = (byte)(frame.Length & 0xFF);
                offset += 2;

                if (frame.Length > 0)
                {
                    Buffer.BlockCopy(frame, 0, datagram, offset, frame.Length);
                    offset += frame.Length;
                }
            }

            try
            {
                if (m_bind)
                {
                    // In bind mode, send to the last remote endpoint we received from.
                    // This enables bidirectional patterns (REQ/REP, PAIR, etc.).
                    // Falls back to the configured address for unidirectional patterns (PUB).
                    EndPoint? target = m_lastRemoteEndpoint;
                    if (target == null)
                    {
                        var udpAddr = (UdpAddress)m_addr.Resolved!;
                        Assumes.NotNull(udpAddr.Address);
                        target = udpAddr.Address;
                    }
                    m_socket.SendTo(datagram, 0, totalLen, SocketFlags.None, target);
                }
                else
                {
                    m_socket.Send(datagram, 0, totalLen, SocketFlags.None);
                }
            }
            catch (SocketException)
            {
                // UDP is best-effort, silently drop on send failure
            }
            catch (ObjectDisposedException)
            {
                // Socket was closed
            }
        }

        /// <summary>
        /// Poll for incoming UDP datagrams using non-blocking receive.
        /// Called on the IO thread via timer.
        /// </summary>
        private void PollReceive()
        {
            Assumes.NotNull(m_session);

            if (m_socket == null || m_terminated)
                return;

            EndPoint remoteEp = new IPEndPoint(IPAddress.Any, 0);

            // Process all available datagrams
            while (m_receiveState == ReceiveState.Active)
            {
                try
                {
                    if (!m_socket.Poll(0, SelectMode.SelectRead))
                        break;

                    int received;
                    if (m_bind)
                    {
                        received = m_socket.ReceiveFrom(m_receiveBuffer, ref remoteEp);
                        // Track remote endpoint for sending replies back
                        m_lastRemoteEndpoint = remoteEp;
                    }
                    else
                    {
                        received = m_socket.Receive(m_receiveBuffer);
                    }

                    if (received > 0)
                    {
                        byte[] datagram = new byte[received];
                        Buffer.BlockCopy(m_receiveBuffer, 0, datagram, 0, received);

                        if (!PushDatagram(datagram, 0))
                        {
                            m_pendingDatagram = datagram;
                            m_pendingFrameIndex = 0;
                            m_receiveState = ReceiveState.Stuck;
                            m_session.Flush();
                            return;
                        }
                    }
                }
                catch (SocketException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
            }

            m_session.Flush();
        }

        /// <summary>
        /// Push a single UDP datagram to the session as multipart frames.
        /// </summary>
        /// <param name="datagram">the raw UDP datagram bytes</param>
        /// <param name="startFrameIndex">index of the first frame to push (for resuming after stuck)</param>
        /// <returns>true if fully pushed, false if pipe is full</returns>
        private bool PushDatagram(byte[] datagram, int startFrameIndex)
        {
            Assumes.NotNull(m_session);

            if (datagram.Length < 1)
                return true; // Skip invalid datagrams

            int numFrames = datagram[0];
            if (numFrames == 0)
                return true;

            int offset = 1;

            // Skip frames that were already pushed
            for (int i = 0; i < startFrameIndex && offset + 2 <= datagram.Length; i++)
            {
                int frameLen = (datagram[offset] << 8) | datagram[offset + 1];
                offset += 2 + frameLen;
            }

            // Push remaining frames
            for (int i = startFrameIndex; i < numFrames; i++)
            {
                if (offset + 2 > datagram.Length)
                    return true; // Malformed

                int frameLen = (datagram[offset] << 8) | datagram[offset + 1];
                offset += 2;

                if (offset + frameLen > datagram.Length)
                    return true; // Malformed

                var msg = new Msg();
                msg.InitPool(frameLen);
                if (frameLen > 0)
                    msg.Put(datagram, offset, 0, frameLen);
                offset += frameLen;

                // Set More flag on all frames except the last
                if (i < numFrames - 1)
                    msg.SetFlags(MsgFlags.More);

                var result = m_session.PushMsg(ref msg);
                if (result != PushMsgResult.Ok)
                {
                    msg.Close();
                    m_pendingFrameIndex = i;
                    return false;
                }
            }

            return true;
        }

        #region IProactorEvents

        public void InCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotSupportedException();
        }

        public void OutCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotSupportedException();
        }

        public void TimerEvent(int id)
        {
            if (id != ReceiveTimerId || m_terminated)
                return;

            if (m_receiveState == ReceiveState.Active)
                PollReceive();

            // Reschedule timer
            if (!m_terminated && m_plugged)
                m_ioObject!.AddTimer(1, ReceiveTimerId);
        }

        #endregion
    }
}
