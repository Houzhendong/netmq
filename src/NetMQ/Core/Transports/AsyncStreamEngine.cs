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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ.Core.Mechanisms;
using NetMQ.Core.Utils;

namespace NetMQ.Core.Transports
{
    /// <summary>
    /// The async engine's ZMTP stream engine. Ports <see cref="StreamEngine"/>'s handshake
    /// CONTROL FLOW - greeting exchange, version negotiation (including the ZMTP1/2
    /// downgrade), and mechanism selection/handshake-command exchange - onto the BCL
    /// <see cref="Socket"/>, driven with plain async/await instead of a Proactor-fed state
    /// machine, and runs on <see cref="AsyncIOThread"/>'s loop rather than <c>IOThread</c>'s
    /// poller.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Covers the handshake (greeting, version negotiation/downgrade, mechanism selection)
    /// and the data-plane loops (<see cref="SendLoopAsync"/>/<see cref="ReceiveLoopAsync"/>,
    /// with V3 and legacy (ZMTP1/2) variants), heartbeats, and termination.
    /// </para>
    /// <para>
    /// Unlike <see cref="StreamEngine"/>, this engine never registers the socket with a
    /// Proactor (there is none on this path) - <see cref="IOObject"/> is only used for its
    /// <see cref="IOObject.AddTimer"/>/<see cref="IOObject.CancelTimer"/> forwarding to
    /// <see cref="IIOThread"/>, exactly like <c>AsyncTcpConnector</c>/<c>AsyncTcpListener</c>
    /// already do.
    /// </para>
    /// </remarks>
    internal sealed class AsyncStreamEngine : IEngine, IProactorEvents
    {
        private const int HeartbeatIntervalTimerId = 1;
        private const int HeartbeatTimeoutTimerId = 2;
        private const int HeartbeatTtlTimerId = 3;
        private const int HandshakeTimeoutTimerId = 4;

        /// <summary>
        /// libzmq's built-in <c>ZMQ_HANDSHAKE_IVL</c> default. NetMQ's <see cref="Options"/>
        /// has no settable handshake-timeout option (<see cref="StreamEngine"/> doesn't
        /// enforce one either), so this is a fixed fallback rather than something read off
        /// <see cref="m_options"/> - see the "handshake timeout" decision in the PR
        /// description for the reasoning.
        /// </summary>
        private const int DefaultHandshakeTimeoutMs = 30000;

        private static readonly byte[] NullMechanismBytes =
        {
            (byte) 'N', (byte) 'U', (byte) 'L', (byte) 'L', 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        private static readonly byte[] PlainMechanismBytes =
        {
            (byte) 'P', (byte) 'L', (byte) 'A', (byte) 'I', (byte) 'N', 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        private static readonly byte[] CurveMechanismBytes =
        {
            (byte) 'C', (byte) 'U', (byte) 'R', (byte) 'V', (byte) 'E', 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        // Size of the (non-V3) greeting: preamble (10 bytes) + version (1 byte) + socket
        // type (1 byte) - matches StreamEngine.GreetingSize.
        private const int GreetingSize = 12;
        private const int PreambleSize = 10;
        private const int GreetingSizeV3 = 64;
        private const int VersionPos = 10;

        private enum EngineState
        {
            Handshaking,
            Active,
            Error
        }

        /// <summary>
        /// What the data-plane loops (task 2) need to drive after the handshake completes.
        /// </summary>
        internal enum DowngradeMode
        {
            /// <summary>ZMTP 3.x: mechanism handshake completed, <see cref="m_decoder"/> (a
            /// <see cref="V2SpanDecoder"/>) and <see cref="V2SpanEncoder"/> drive the wire.</summary>
            None,

            /// <summary>Peer speaks unversioned ZMTP/1.0. Task 2 drives the wire via a small
            /// adapter over <see cref="V1Encoder"/>/<see cref="V1Decoder"/> (see remarks on
            /// <see cref="m_greetingLeftover"/>) rather than the V2 span codecs.</summary>
            V1,

            /// <summary>Peer speaks ZMTP/2.0 (versioned, but no mechanism greeting). Same
            /// framing as <see cref="None"/> at the wire level, but no mechanism/heartbeats -
            /// task 2 goes straight to <see cref="SessionBase"/>'s routing-id message pair,
            /// exactly like <see cref="StreamEngine"/>'s <c>RoutingIdMsg</c>/<c>ProcessRoutingIdMsg</c>.</summary>
            V2,

            /// <summary>Raw socket (<see cref="Options.RawSocket"/>): no ZMTP at all, task 2
            /// drives <see cref="RawEncoder"/>/<see cref="RawDecoder"/> directly.</summary>
            Raw
        }

        private readonly Socket m_socket;
        private readonly Options m_options;
        private readonly string m_endpoint;

        private AsyncIOThread? m_ioThread;
        private IOObject? m_ioObject;
        private SessionBase? m_session;
        private SocketBase? m_socketBase;

        private bool m_plugged;
        private EngineState m_state;
        private CancellationTokenSource? m_cts;

        private bool m_hasHandshakeTimer;
        private bool m_hasHeartbeatTimer;
        private bool m_hasHeartbeatTimeoutTimer;
        private bool m_hasHeartbeatTtlTimer;
        private int m_heartbeatTimeout;

        private Mechanism? m_mechanism;

        internal DowngradeMode m_downgradeMode = DowngradeMode.None;

        /// <summary>
        /// Set (mirroring <see cref="StreamEngine"/>'s <c>m_subscriptionRequired</c>) when a
        /// downgraded Pub/Xpub socket needs a phony subscription message injected right after
        /// the peer's routing-id message, for interop with peers that don't forward theirs.
        /// </summary>
        internal bool m_subscriptionRequired;

        /// <summary>
        /// Preserved for task 2's ZMTP1/2 adapter: the raw greeting bytes already read off the
        /// wire (up to and including whatever told us to downgrade), which logically come
        /// before anything else the peer sends.
        /// </summary>
        internal byte[]? m_greetingLeftover;

        /// <summary>
        /// The ZMTP3 span decoder, reused unchanged from the handshake-command exchange into
        /// the data plane: framing does not change when the mechanism goes Ready, so whatever
        /// partial frame state (and buffered-but-unconsumed bytes, via <see cref="m_recvBuffer"/>)
        /// existed at that point is exactly what task 2's receive loop should keep consuming.
        /// Only populated when <see cref="m_downgradeMode"/> is <see cref="DowngradeMode.None"/>.
        /// </summary>
        internal V2SpanDecoder? m_decoder;

        private byte[] m_recvBuffer = new byte[Config.InBatchSize];
        private int m_recvStart;
        private int m_recvEnd;

        private readonly SemaphoreSlim m_outSignal = new(0, int.MaxValue);
        private readonly SemaphoreSlim m_inSignal = new(0, int.MaxValue);

        /// <summary>
        /// Upper bound on the send loop's pinned coalescing batch buffer - flushed to the
        /// socket once the accumulated small frames reach (within one frame's worth of) this
        /// size, mirroring the spirit of <see cref="Config.OutBatchSize"/> without being tied
        /// to it (that constant is sized for the legacy Proactor buffer, not this batch).
        /// </summary>
        private const int SendBatchCapacity = 64 * 1024;

        /// <summary>Set by <see cref="TimerEvent"/> (<see cref="HeartbeatIntervalTimerId"/>)
        /// to ask the send loop to produce a PING the next time it looks for work - mirrors
        /// <see cref="StreamEngine"/>'s <c>m_nextMsg = ProducePingMessage</c> hand-off, just
        /// via a flag instead of swapping a delegate. None mode only.</summary>
        private bool m_pingRequested;

        /// <summary>Set by <see cref="ProcessPingMessage"/> once <see cref="m_pongMsg"/> is
        /// ready to go out - mirrors <see cref="StreamEngine"/>'s
        /// <c>m_nextMsg = ProducePongMessage</c>. None mode only.</summary>
        private bool m_pongRequested;

        private Msg m_pongMsg;

        /// <summary>
        /// Reused by <see cref="SendGatherAsync"/> across every large-message send on this
        /// connection to avoid a per-call List/backing-array allocation on the one path
        /// that's otherwise zero-copy. Safe to reuse: <see cref="SendGatherAsync"/> has a
        /// single call site, inside <see cref="V3SendLoopAsync"/>'s sequential per-connection
        /// send loop, so there is no concurrent/reentrant use of this field.
        /// </summary>
        private readonly List<ArraySegment<byte>> m_gatherSegments = new(2);

        public AsyncStreamEngine(Socket socket, Options options, string endpoint)
        {
            m_socket = socket ?? throw new ArgumentNullException(nameof(socket));
            m_options = options;
            m_endpoint = endpoint;

            if (m_options.SendBuffer != 0)
                m_socket.SendBufferSize = m_options.SendBuffer;
            if (m_options.ReceiveBuffer != 0)
                m_socket.ReceiveBufferSize = m_options.ReceiveBuffer;

            if (m_options.HeartbeatInterval > 0)
            {
                m_heartbeatTimeout = m_options.HeartbeatTimeout;
                if (m_heartbeatTimeout == -1)
                    m_heartbeatTimeout = m_options.HeartbeatInterval;
            }
        }

        public void Plug(IIOThread ioThread, SessionBase session)
        {
            Debug.Assert(!m_plugged);
            m_plugged = true;

            m_ioThread = ioThread as AsyncIOThread
                ?? throw new InvalidOperationException(
                    $"{nameof(AsyncStreamEngine)} requires an {nameof(AsyncIOThread)} - it only runs behind {nameof(NetMQConfig.UseAsyncEngine)}.");

            Debug.Assert(m_session == null);
            m_session = session ?? throw new ArgumentNullException(nameof(session));
            m_socketBase = session.Socket;

            // No AddSocket/RemoveSocket here (unlike StreamEngine's IOObject usage) - there is
            // no Proactor on this path, so IOObject is only used for its AddTimer/CancelTimer
            // forwarding to IIOThread.
            m_ioObject = new IOObject(m_ioThread);
            m_ioObject.SetHandler(this);

            m_cts = new CancellationTokenSource();
            m_state = EngineState.Handshaking;

            // Plug() already runs on the AsyncIOThread's own loop (it's invoked while draining
            // a mailbox command), but we go through Post anyway to keep every re-entry onto the
            // loop uniform with how timers/mailbox commands dispatch into engine code.
            m_ioThread.Post(() => _ = RunAsync());
        }

        public void Terminate()
        {
            Unplug();
            Destroy();
        }

        public void ActivateIn()
        {
            m_inSignal.Release();
        }

        public void ActivateOut()
        {
            m_outSignal.Release();
        }

        /// <summary>
        /// Never invoked: this engine has no Proactor registration to drive it (see the class
        /// remarks), so nothing calls this. Implemented only to satisfy <see cref="IProactorEvents"/>
        /// for <see cref="IOObject.SetHandler"/> - mirrors <c>AsyncTcpConnector</c>'s identical stub.
        /// </summary>
        void IProactorEvents.InCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Never invoked - see <see cref="IProactorEvents.InCompleted"/>.
        /// </summary>
        void IProactorEvents.OutCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotImplementedException();
        }

        public void TimerEvent(int id)
        {
            if (id == HandshakeTimeoutTimerId)
            {
                m_hasHandshakeTimer = false;
                // Error() -> Unplug() already cancels m_cts to abort whatever
                // SendAsync/ReceiveAsync HandshakeAsync is currently awaiting (the
                // OperationCanceledException it raises is caught by RunAsync and turned into
                // a no-op Error() call, guarded by the m_session == null check above), and
                // Error() -> Destroy() disposes m_cts right after. A second Cancel() here
                // would run against that already-disposed CancellationTokenSource and throw
                // ObjectDisposedException, so don't call it again.
                Error();
            }
            else if (id == HeartbeatIntervalTimerId)
            {
                // Re-arm first (mirrors StreamEngine.TimerEvent), then ask the send loop to
                // produce the actual PING the next time it looks for work - ordinary pipe
                // activity doesn't wake it, so we release m_outSignal explicitly.
                if (m_state != EngineState.Error)
                {
                    m_ioObject!.AddTimer(m_options.HeartbeatInterval, HeartbeatIntervalTimerId);
                    m_pingRequested = true;
                    m_outSignal.Release();
                }
            }
            else if (id == HeartbeatTtlTimerId)
            {
                m_hasHeartbeatTtlTimer = false;
                Error();
            }
            else if (id == HeartbeatTimeoutTimerId)
            {
                m_hasHeartbeatTimeoutTimer = false;
                Error();
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(id));
            }
        }

        private void Unplug()
        {
            Debug.Assert(m_plugged);
            m_plugged = false;

            m_cts?.Cancel();

            if (m_hasHandshakeTimer)
            {
                m_ioObject!.CancelTimer(HandshakeTimeoutTimerId);
                m_hasHandshakeTimer = false;
            }

            if (m_hasHeartbeatTtlTimer)
            {
                m_ioObject!.CancelTimer(HeartbeatTtlTimerId);
                m_hasHeartbeatTtlTimer = false;
            }

            if (m_hasHeartbeatTimeoutTimer)
            {
                m_ioObject!.CancelTimer(HeartbeatTimeoutTimerId);
                m_hasHeartbeatTimeoutTimer = false;
            }

            if (m_hasHeartbeatTimer)
            {
                m_ioObject!.CancelTimer(HeartbeatIntervalTimerId);
                m_hasHeartbeatTimer = false;
            }

            m_ioObject?.Unplug();
            m_session = null;
        }

        private void Destroy()
        {
            Debug.Assert(!m_plugged);

            m_mechanism?.Dispose();
            m_cts?.Dispose();

            try
            {
                m_socket.Dispose();
            }
            catch (SocketException)
            {
            }
        }

        /// <summary>
        /// Mirrors <see cref="StreamEngine"/>'s <c>Error()</c>: report disconnection to the
        /// monitor, flush/detach the session, then unplug and destroy. Guarded against
        /// re-entrancy (unlike the classic engine's single-threaded state machine, several
        /// different awaits in the handshake path can independently observe a failure and
        /// call this).
        /// </summary>
        private void Error()
        {
            if (m_state == EngineState.Error)
                return;

            // Guards against a real race, not just re-entrancy: Terminate() can run
            // concurrently with an in-flight SendLoopAsync/ReceiveLoopAsync await that has no
            // CancellationToken to observe it (e.g. SendGatherAsync's IList<ArraySegment<byte>>
            // overload of Socket.SendAsync). Terminate() -> Unplug() sets m_state has already
            // been left untouched (only Error() itself sets it to Error) but nulls m_session
            // before Destroy() disposes the socket; the disposal then faults that pending
            // await, whose catch block calls Error() here. Without this guard we'd
            // NullReferenceException on m_session! below instead of quietly no-op'ing - Terminate
            // already ran Unplug()/Destroy() for us.
            if (m_session == null)
                return;

            Debug.Assert(m_session != null);
            m_state = EngineState.Error;

            // Matches StreamEngine.Error()'s actual runtime behavior: m_state is already
            // Error by the time this is evaluated, so "handshaked" reduces to whether a
            // mechanism was ever created/still handshaking, not whether the engine itself
            // was still in its Handshaking state - keeping the two engines' disconnect-
            // notification behavior identical for early handshake failures.
            m_socketBase!.EventDisconnected(m_endpoint, m_socket);
            m_session!.Flush();
            m_session!.Detach(m_state != EngineState.Handshaking && (m_mechanism is null || m_mechanism!.Status != MechanismStatus.Handshaking));

            Unplug();
            Destroy();
        }

        private void AddHandshakeTimer()
        {
            m_ioObject!.AddTimer(DefaultHandshakeTimeoutMs, HandshakeTimeoutTimerId);
            m_hasHandshakeTimer = true;
        }

        private void CancelHandshakeTimer()
        {
            if (m_hasHandshakeTimer)
            {
                m_ioObject!.CancelTimer(HandshakeTimeoutTimerId);
                m_hasHandshakeTimer = false;
            }
        }

        /// <summary>
        /// Drives the handshake to completion (or failure) and then flips the engine into the
        /// "ready for data-plane" state, starting the (currently stubbed) send/receive loops.
        /// </summary>
        private async Task RunAsync()
        {
            AddHandshakeTimer();

            try
            {
                // Deliberately NOT .ConfigureAwait(false): HandshakeAsync (and
                // MechanismHandshakeLoopAsync beneath it) calls Error() directly on a mismatched/
                // unsupported mechanism or a malformed handshake command, and Error()/Unplug()
                // touch IIOThread's shared, non-thread-safe timer heap plus m_state/m_session -
                // only safe to touch from the loop thread. HandshakeAsync's own awaits must
                // therefore also resume on the loop (see its class remarks), exactly like the
                // data-plane loops rely on for every one of their own awaits. Losing this let a
                // ConfigureAwait(false)-resumed continuation call Error() from an arbitrary
                // thread-pool thread concurrently with the loop thread's own timer bookkeeping
                // (AddTimer/CancelTimer/FireDueTimers), corrupting AsyncIOThread's timer heap.
                await HandshakeAsync(m_cts!.Token);

                // HandshakeAsync may have already called Error() itself (e.g. an unsupported
                // mechanism or a malformed handshake command) and returned normally rather than
                // throwing - don't fall through to Active in that case.
                if (m_state == EngineState.Error)
                    return;

                CancelHandshakeTimer();
                m_state = EngineState.Active;

                // Data-plane loops land in task 2; see the class remarks for what state is
                // already set up for them (m_decoder/m_recvBuffer/m_downgradeMode/m_mechanism).
                _ = SendLoopAsync();
                _ = ReceiveLoopAsync();
            }
            catch (OperationCanceledException) when (m_cts!.IsCancellationRequested)
            {
                // Terminate()/the handshake timer already cancelled us; Unplug/Destroy either
                // already ran (Terminate) or Error() already ran (handshake timeout) before
                // cancellation was requested.
            }
            catch (Exception)
            {
                Error();
            }
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s handshake control flow: send our greeting
        /// signature, decide (as bytes trickle in) whether the peer is unversioned ZMTP/1.0 or
        /// speaks a versioned greeting, negotiate major/minor version (including the ZMTP1/2
        /// "downgrade" branch), and - for ZMTP3+ - select and run the security mechanism to
        /// completion.
        /// </summary>
        private async Task HandshakeAsync(CancellationToken ct)
        {
            if (m_options.RawSocket)
            {
                // Raw sockets skip ZMTP entirely - task 2 drives RawEncoder/RawDecoder
                // directly over the socket, same as StreamEngine's State.Closed/Action.Start
                // RawSocket branch.
                m_downgradeMode = DowngradeMode.Raw;
                return;
            }

            var greeting = new byte[GreetingSizeV3];
            int greetingBytesRead = 0;

            // ---- send our own greeting signature: 0xFF + 8-byte (Identity size + 1) + 0x7F ----
            var outBuf = new ByteArraySegment(new byte[GreetingSizeV3]);
            int outSize = 0;
            outBuf[outSize++] = 0xff;
            outBuf.PutLong(m_options.Endian, (long)m_options.IdentitySize + 1, outSize);
            outSize += 8;
            outBuf[outSize++] = 0x7f;
            await SendAllAsync((byte[])outBuf, outBuf.Offset, outSize, ct);

            // ---- receive the peer's preamble, re-checking after every partial read (not just
            // after a full 10-byte read) - an unversioned ZMTP/1.0 peer's greeting can be
            // shorter than 10 bytes and we must not block waiting for bytes that will never
            // arrive. Mirrors StreamEngine's HandshakeState.ReceivingGreeting exactly. ----
            bool versioned;
            while (true)
            {
                int n = await ReceiveSomeAsync(greeting, greetingBytesRead, PreambleSize - greetingBytesRead, ct);
                greetingBytesRead += n;

                if (greeting[0] != 0xff || (greetingBytesRead == PreambleSize && (greeting[9] & 0x01) == 0))
                {
                    versioned = false;
                    break;
                }

                if (greetingBytesRead >= PreambleSize)
                {
                    versioned = true;
                    break;
                }
            }

            if (!versioned)
            {
                // Peer speaks unversioned ZMTP/1.0. We've already sent our (v1-compatible)
                // header above, so there's nothing left to send here - task 2's downgrade
                // adapter picks up from m_greetingLeftover.
                m_downgradeMode = DowngradeMode.V1;

                if (m_options.SocketType == ZmqSocketType.Pub || m_options.SocketType == ZmqSocketType.Xpub)
                    m_subscriptionRequired = true;

                m_greetingLeftover = new byte[greetingBytesRead];
                Array.Copy(greeting, m_greetingLeftover, greetingBytesRead);
                return;
            }

            // ---- versioned: send our major version, then read the peer's ----
            outSize = 0;
            outBuf[outSize++] = 3; // Protocol version
            await SendAllAsync((byte[])outBuf, outBuf.Offset, outSize, ct);

            await ReceiveExactAsync(greeting, greetingBytesRead, VersionPos + 1 - greetingBytesRead, ct);
            greetingBytesRead = VersionPos + 1;

            if (greeting[VersionPos] == 0 || greeting[VersionPos] == 1)
            {
                // ZMTP/1.0-with-signature or ZMTP/2.0: send our socket type, read theirs, done -
                // no mechanism handshake for these (matches StreamEngine's SendingSocketType /
                // ReceivingResfOfV2Greeting -> Activate() directly).
                outSize = 0;
                outBuf[outSize++] = (byte)m_options.SocketType;
                await SendAllAsync((byte[])outBuf, outBuf.Offset, outSize, ct);

                await ReceiveExactAsync(greeting, greetingBytesRead, GreetingSize - greetingBytesRead, ct);
                greetingBytesRead = GreetingSize;

                m_downgradeMode = greeting[VersionPos] == 0 ? DowngradeMode.V1 : DowngradeMode.V2;
                m_greetingLeftover = Array.Empty<byte>();
                return;
            }

            // ---- ZMTP 3.x: send the rest of our 64-byte greeting (minor version + mechanism
            // name, zero-filled as-server/filler - StreamEngine sends this last 32-byte block
            // as all-zero too, verbatim) ----
            outSize = 0;
            outBuf[outSize++] = 0; // Minor version

            switch (m_options.Mechanism)
            {
                case MechanismType.Null:
                    outBuf.PutBytes(NullMechanismBytes, outSize);
                    outSize += 20;
                    break;
                case MechanismType.Plain:
                    outBuf.PutBytes(PlainMechanismBytes, outSize);
                    outSize += 20;
                    break;
                case MechanismType.Curve:
                    outBuf.PutBytes(CurveMechanismBytes, outSize);
                    outSize += 20;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            outBuf.Fill(0, outSize, 32);
            outSize += 32;

            await SendAllAsync((byte[])outBuf, outBuf.Offset, outSize, ct);

            await ReceiveExactAsync(greeting, greetingBytesRead, GreetingSizeV3 - greetingBytesRead, ct);
            greetingBytesRead = GreetingSizeV3;

            if (m_options.Mechanism == MechanismType.Null && ByteArrayUtility.AreEqual(greeting, 12, NullMechanismBytes, 0, 20))
            {
                m_mechanism = new NullMechanism(m_session!, m_options);
            }
            else if (m_options.Mechanism == MechanismType.Plain && ByteArrayUtility.AreEqual(greeting, 12, PlainMechanismBytes, 0, 20))
            {
                Error(); // Not yet supported - exactly like StreamEngine.
                return;
            }
            else if (m_options.Mechanism == MechanismType.Curve && ByteArrayUtility.AreEqual(greeting, 12, CurveMechanismBytes, 0, 20))
            {
                m_mechanism = m_options.AsServer
                    ? new CurveServerMechanism(m_session!, m_options)
                    : new CurveClientMechanism(m_session!, m_options);
            }
            else
            {
                Error(); // Unsupported/mismatched mechanism.
                return;
            }

            m_downgradeMode = DowngradeMode.None;
            m_decoder = new V2SpanDecoder(m_options.MaxMessageSize, m_options.Endian);

            await MechanismHandshakeLoopAsync(ct);
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>NextHandshakeCommand</c>/<c>ProcessHandshakeCommand</c>
        /// delegate pair into an explicit loop: drain every command the mechanism currently
        /// wants to send, then read and process exactly one incoming command, repeating until
        /// the mechanism reports <see cref="MechanismStatus.Ready"/> or
        /// <see cref="MechanismStatus.Error"/>.
        /// </summary>
        private async Task MechanismHandshakeLoopAsync(CancellationToken ct)
        {
            while (m_mechanism!.Status == MechanismStatus.Handshaking)
            {
                while (true)
                {
                    Msg outgoing = new Msg();
                    var pullResult = m_mechanism!.NextHandshakeCommand(ref outgoing);

                    if (pullResult == PullMsgResult.Error)
                    {
                        Error();
                        return;
                    }

                    if (pullResult != PullMsgResult.Ok)
                        break;

                    outgoing.SetFlags(MsgFlags.Command);
                    await SendHandshakeCommandAsync(outgoing, ct);
                    outgoing.Close();

                    if (m_mechanism!.Status != MechanismStatus.Handshaking)
                        break;
                }

                if (m_mechanism!.Status != MechanismStatus.Handshaking)
                    break;

                Msg incoming = new Msg();
                incoming.InitEmpty();
                var received = await ReceiveHandshakeCommandAsync(incoming, ct);

                var pushResult = m_mechanism!.ProcessHandshakeCommand(ref received);
                if (pushResult == PushMsgResult.Error)
                {
                    Error();
                    return;
                }
            }

            if (m_mechanism!.Status == MechanismStatus.Error)
            {
                Error();
                return;
            }

            MechanismReady();
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>MechanismReady</c>: schedule the heartbeat
        /// timer and push the peer's routing id (from the mechanism's handshake metadata) into
        /// the session, then hand off to the data plane.
        /// </summary>
        private void MechanismReady()
        {
            if (m_options.HeartbeatInterval > 0)
            {
                m_ioObject!.AddTimer(m_options.HeartbeatInterval, HeartbeatIntervalTimerId);
                m_hasHeartbeatTimer = true;
            }

            if (m_options.RecvIdentity)
            {
                Msg identity = new Msg();
                byte[]? peerIdentity = m_mechanism!.PeerIdentity;
                if (peerIdentity is null)
                    identity.InitEmpty();
                else
                {
                    identity.InitPool(peerIdentity.Length);
                    identity.Put(peerIdentity, 0, peerIdentity.Length);
                }

                var pushResult = m_session!.PushMsg(ref identity);
                if (pushResult == PushMsgResult.Full)
                {
                    // Pipe is shutting down under us - same bail-out as StreamEngine's
                    // MechanismReady; task 2's loops will observe the termination.
                    return;
                }

                m_session!.Flush();
            }
        }

        // ---- low-level socket helpers -------------------------------------------------

        private async Task SendAllAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            int sent = 0;
            while (sent < count)
            {
                int n = await m_socket.SendAsync(new ReadOnlyMemory<byte>(buffer, offset + sent, count - sent), SocketFlags.None, ct)
                    .ConfigureAwait(false);
                if (n <= 0)
                    throw new SocketException((int)SocketError.ConnectionReset);
                sent += n;
            }
        }

        private async Task<int> ReceiveSomeAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            int n = await m_socket.ReceiveAsync(new Memory<byte>(buffer, offset, count), SocketFlags.None, ct).ConfigureAwait(false);
            if (n == 0)
                throw new SocketException((int)SocketError.ConnectionReset); // peer closed mid-handshake
            return n;
        }

        private async Task ReceiveExactAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            int read = 0;
            while (read < count)
            {
                int n = await ReceiveSomeAsync(buffer, offset + read, count - read, ct).ConfigureAwait(false);
                read += n;
            }
        }

        /// <summary>
        /// Encodes and sends a single handshake command via <see cref="V2SpanEncoder"/>.
        /// Handshake commands (READY/HELLO/WELCOME/INITIATE/...) are always well under
        /// <see cref="V2SpanEncoder.LargeMessageThreshold"/>, so a single coalesced frame
        /// always suffices - no need for the batch/gather-write machinery the data plane uses.
        /// </summary>
        private async Task SendHandshakeCommandAsync(Msg msg, CancellationToken ct)
        {
            var encoder = new V2SpanEncoder(m_options.Endian);
            var buffer = new byte[V2SpanEncoder.GetHeaderLength(msg.Size) + msg.Size];

            // TryEncodeFrame only reads msg (Size/HasMore/HasCommand/Slice()) - it never
            // mutates it - so passing by value here (required: async methods can't take ref
            // parameters) is equivalent to the ref-passing StreamEngine callers use.
            bool ok = encoder.TryEncodeFrame(ref msg, buffer, out int written, out bool bodyTooLarge);
            Debug.Assert(ok && !bodyTooLarge, "Handshake commands must fit within the coalesced-frame threshold.");

            await SendAllAsync(buffer, 0, written, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads and decodes a single handshake command via <see cref="m_decoder"/>, topping
        /// up <see cref="m_recvBuffer"/> from the socket as needed. Any bytes read but not
        /// consumed by this frame are left in <see cref="m_recvBuffer"/> (between
        /// <see cref="m_recvStart"/> and <see cref="m_recvEnd"/>) for the next call - or, once
        /// the mechanism goes Ready, for task 2's receive loop to keep consuming, since the
        /// wire framing doesn't change at that point.
        /// </summary>
        private async Task<Msg> ReceiveHandshakeCommandAsync(Msg msg, CancellationToken ct)
        {
            while (true)
            {
                if (m_recvEnd > m_recvStart)
                {
                    var sequence = new ReadOnlySequence<byte>(new ReadOnlyMemory<byte>(m_recvBuffer, m_recvStart, m_recvEnd - m_recvStart));
                    int available = m_recvEnd - m_recvStart;

                    var result = m_decoder!.TryDecodeFrame(ref sequence, ref msg);
                    m_recvStart += available - (int)sequence.Length;

                    if (result == SpanDecodeResult.MessageReady)
                        return msg;

                    if (result == SpanDecodeResult.Error)
                        throw new InvalidOperationException("Malformed ZMTP frame during handshake.");
                }

                await FillRecvBufferAsync(ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reads more bytes from the socket into <see cref="m_recvBuffer"/>, compacting
        /// (or growing) it first if there's no room left at the tail.
        /// </summary>
        private async Task FillRecvBufferAsync(CancellationToken ct)
        {
            if (m_recvEnd == m_recvBuffer.Length)
            {
                if (m_recvStart > 0)
                {
                    Array.Copy(m_recvBuffer, m_recvStart, m_recvBuffer, 0, m_recvEnd - m_recvStart);
                    m_recvEnd -= m_recvStart;
                    m_recvStart = 0;
                }
                else
                {
                    Array.Resize(ref m_recvBuffer, m_recvBuffer.Length * 2);
                }
            }

            int n = await ReceiveSomeAsync(m_recvBuffer, m_recvEnd, m_recvBuffer.Length - m_recvEnd, ct).ConfigureAwait(false);
            m_recvEnd += n;
        }

        // ---- data plane ---------------------------------------------------------------

        /// <summary>
        /// Drains <see cref="SessionBase.PullMsg"/>, encodes via <see cref="V2SpanEncoder"/>
        /// (or the downgrade-mode equivalent) into a pinned batch buffer, and gather-writes
        /// large bodies directly - parking on <see cref="m_outSignal"/> when the pipe is
        /// empty.
        /// </summary>
        /// <remarks>
        /// Deliberately does NOT use <c>.ConfigureAwait(false)</c> anywhere in this method (or
        /// the other data-plane loop methods below) - unlike the handshake path. This loop runs
        /// for the engine's entire active lifetime, concurrently with the receive loop and with
        /// <see cref="TimerEvent"/>/<see cref="Terminate"/> (both of which run directly on
        /// <see cref="AsyncIOThread"/>'s loop thread and touch shared, not-thread-safe state
        /// such as the timer heap). Every <c>await</c> here captures the ambient
        /// <c>LoopSynchronizationContext</c> (established for the lifetime of the loop thread),
        /// so every line of this method - including calls into <see cref="IOObject.AddTimer"/>/
        /// <see cref="IOObject.CancelTimer"/> - keeps running on that single thread, preserving
        /// the single-threaded-actor invariant regardless of the fact that the low-level helpers
        /// it calls (<see cref="SendAllAsync"/> etc.) use <c>.ConfigureAwait(false)</c> internally
        /// for their own, purely-local socket awaits.
        /// </remarks>
        private async Task SendLoopAsync()
        {
            var ct = m_cts!.Token;
            try
            {
                switch (m_downgradeMode)
                {
                    case DowngradeMode.Raw:
                    {
                        using var encoder = new RawEncoder(Config.OutBatchSize, m_options.Endian);
                        await LegacySendLoopAsync(encoder, sendRoutingId: false, ct);
                        break;
                    }
                    case DowngradeMode.V1:
                    {
                        using var encoder = new V1Encoder(Config.OutBatchSize, m_options.Endian);
                        await LegacySendLoopAsync(encoder, sendRoutingId: true, ct);
                        break;
                    }
                    default:
                        await V3SendLoopAsync(ct);
                        break;
                }
            }
            catch (OperationCanceledException) when (m_cts!.IsCancellationRequested)
            {
                // Terminate() cancelled us - Unplug/Destroy already ran there.
            }
            catch when (m_state == EngineState.Error)
            {
                // Some other in-flight failure already called Error() (e.g. the receive loop
                // lost the race) - nothing more to do.
            }
            catch (Exception)
            {
                Error();
            }
        }

        /// <summary>
        /// Reads off the socket into <see cref="m_recvBuffer"/>, decodes via
        /// <see cref="m_decoder"/> (or the downgrade-mode equivalent), and pushes into
        /// <see cref="SessionBase.PushMsg"/> - parking on <see cref="m_inSignal"/> when the
        /// pipe reports full. See <see cref="SendLoopAsync"/>'s remarks for why this method also
        /// avoids <c>.ConfigureAwait(false)</c> throughout.
        /// </summary>
        private async Task ReceiveLoopAsync()
        {
            var ct = m_cts!.Token;
            try
            {
                switch (m_downgradeMode)
                {
                    case DowngradeMode.Raw:
                    {
                        var decoder = new RawDecoder(Config.InBatchSize, m_options.MaxMessageSize, m_options.Endian);
                        await LegacyReceiveLoopAsync(decoder, processRoutingId: false, ct);
                        break;
                    }
                    case DowngradeMode.V1:
                    {
                        var decoder = new V1Decoder(Config.InBatchSize, m_options.MaxMessageSize, m_options.Endian);
                        await LegacyReceiveLoopAsync(decoder, processRoutingId: true, ct);
                        break;
                    }
                    default:
                        await V3ReceiveLoopAsync(ct);
                        break;
                }
            }
            catch (OperationCanceledException) when (m_cts!.IsCancellationRequested)
            {
            }
            catch when (m_state == EngineState.Error)
            {
            }
            catch (Exception)
            {
                Error();
            }
        }

        /// <summary>
        /// Send loop for <see cref="DowngradeMode.None"/> (full ZMTP3 mechanism) and
        /// <see cref="DowngradeMode.V2"/> (same wire framing, no mechanism/heartbeats).
        /// </summary>
        private async Task V3SendLoopAsync(CancellationToken ct)
        {
            var encoder = new V2SpanEncoder(m_options.Endian);
            byte[] batch = GC.AllocateArray<byte>(SendBatchCapacity, pinned: true);
            int batchLen = 0;
            var header = new byte[V2SpanEncoder.MaxHeaderSize];

            bool sendRoutingId = m_downgradeMode == DowngradeMode.V2;
            bool isNone = m_downgradeMode == DowngradeMode.None;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var msg = new Msg();
                PullMsgResult pullResult;

                if (isNone && m_pingRequested)
                {
                    m_pingRequested = false;
                    pullResult = ProducePingMessage(ref msg);
                }
                else if (isNone && m_pongRequested)
                {
                    m_pongRequested = false;
                    msg.Move(ref m_pongMsg);
                    pullResult = m_mechanism!.Encode(ref msg);
                }
                else if (sendRoutingId)
                {
                    sendRoutingId = false;
                    pullResult = RoutingIdMsg(ref msg);
                }
                else
                {
                    pullResult = m_session!.PullMsg(ref msg);
                    if (pullResult == PullMsgResult.Ok && isNone)
                        pullResult = m_mechanism!.Encode(ref msg);
                }

                if (pullResult == PullMsgResult.Error)
                {
                    Error();
                    return;
                }

                if (pullResult == PullMsgResult.Empty)
                {
                    if (batchLen > 0)
                    {
                        await SendAllAsync(batch, 0, batchLen, ct);
                        batchLen = 0;
                    }

                    await m_outSignal.WaitAsync(ct);
                    continue;
                }

                bool fit = encoder.TryEncodeFrame(ref msg, batch.AsSpan(batchLen), out int written, out bool bodyTooLarge);

                if (bodyTooLarge)
                {
                    if (batchLen > 0)
                    {
                        await SendAllAsync(batch, 0, batchLen, ct);
                        batchLen = 0;
                    }

                    int headerLen = encoder.WriteHeader(ref msg, header);
                    await SendGatherAsync(header, headerLen, msg, ct);
                    msg.Close();
                    continue;
                }

                if (!fit)
                {
                    await SendAllAsync(batch, 0, batchLen, ct);
                    batchLen = 0;

                    fit = encoder.TryEncodeFrame(ref msg, batch.AsSpan(0), out written, out bodyTooLarge);
                    Debug.Assert(fit && !bodyTooLarge, "A frame under LargeMessageThreshold must fit a freshly emptied batch buffer.");
                }

                batchLen += written;
                msg.Close();

                if (batchLen >= batch.Length - V2SpanEncoder.MaxHeaderSize)
                {
                    await SendAllAsync(batch, 0, batchLen, ct);
                    batchLen = 0;
                }
            }
        }

        /// <summary>
        /// Receive loop for <see cref="DowngradeMode.None"/> and <see cref="DowngradeMode.V2"/>.
        /// </summary>
        private async Task V3ReceiveLoopAsync(CancellationToken ct)
        {
            bool processRoutingId = m_downgradeMode == DowngradeMode.V2;
            bool isNone = m_downgradeMode == DowngradeMode.None;
            bool pendingFlush = false;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                if (m_recvEnd == m_recvStart)
                {
                    if (pendingFlush)
                    {
                        m_session!.Flush();
                        pendingFlush = false;
                    }

                    await FillRecvBufferAsync(ct);
                }

                var sequence = new ReadOnlySequence<byte>(new ReadOnlyMemory<byte>(m_recvBuffer, m_recvStart, m_recvEnd - m_recvStart));
                int available = m_recvEnd - m_recvStart;

                var msg = new Msg();
                var decodeResult = m_decoder!.TryDecodeFrame(ref sequence, ref msg);
                m_recvStart += available - (int)sequence.Length;

                if (decodeResult == SpanDecodeResult.Error)
                {
                    Error();
                    return;
                }

                if (decodeResult != SpanDecodeResult.MessageReady)
                    continue;

                pendingFlush = true;

                if (isNone)
                {
                    var mechResult = m_mechanism!.Decode(ref msg);
                    if (mechResult == PushMsgResult.Error)
                    {
                        msg.Close();
                        Error();
                        return;
                    }

                    if (m_hasHeartbeatTimeoutTimer)
                    {
                        m_hasHeartbeatTimeoutTimer = false;
                        m_ioObject!.CancelTimer(HeartbeatTimeoutTimerId);
                    }

                    if (m_hasHeartbeatTtlTimer)
                    {
                        m_hasHeartbeatTtlTimer = false;
                        m_ioObject!.CancelTimer(HeartbeatTtlTimerId);
                    }

                    if (msg.HasCommand)
                    {
                        ProcessCommandMessage(ref msg);
                        msg.Close();
                        continue;
                    }
                }

                PushMsgResult pushResult;
                if (processRoutingId)
                {
                    processRoutingId = false;
                    pushResult = ProcessRoutingIdMsg(ref msg);
                }
                else
                {
                    pushResult = m_session!.PushMsg(ref msg);
                    while (pushResult == PushMsgResult.Full)
                    {
                        m_session!.Flush();
                        pendingFlush = false;
                        await m_inSignal.WaitAsync(ct);
                        pushResult = m_session!.PushMsg(ref msg);
                    }
                }

                if (pushResult == PushMsgResult.Error)
                {
                    Error();
                    return;
                }
            }
        }

        /// <summary>
        /// Send loop for the ZMTP1 (<see cref="DowngradeMode.V1"/>) and raw-socket
        /// (<see cref="DowngradeMode.Raw"/>) downgrades - a small adapter driving
        /// <paramref name="encoder"/> (a <see cref="V1Encoder"/> or <see cref="RawEncoder"/>)
        /// pull-style, matching <see cref="StreamEngine"/>'s <c>BeginSending</c> drive loop one
        /// message at a time rather than batched (these are legacy/fallback paths, not the
        /// perf-critical one the V2 span codecs cover).
        /// </summary>
        private async Task LegacySendLoopAsync(EncoderBase encoder, bool sendRoutingId, CancellationToken ct)
        {
            bool first = sendRoutingId;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var msg = new Msg();
                PullMsgResult pullResult = first ? RoutingIdMsg(ref msg) : m_session!.PullMsg(ref msg);
                first = false;

                if (pullResult == PullMsgResult.Error)
                {
                    Error();
                    return;
                }

                if (pullResult == PullMsgResult.Empty)
                {
                    await m_outSignal.WaitAsync(ct);
                    continue;
                }

                encoder.LoadMsg(ref msg);

                int n;
                do
                {
                    ByteArraySegment? segment = null;
                    n = encoder.Encode(ref segment, 0);
                    if (n > 0)
                        await SendAllAsync((byte[])segment!, segment!.Offset, n, ct);
                } while (n > 0);
            }
        }

        /// <summary>
        /// Receive loop for the ZMTP1/raw downgrades - drives <paramref name="decoder"/> (a
        /// <see cref="V1Decoder"/> or <see cref="RawDecoder"/>) via <see cref="DecoderBase.GetBuffer"/>/
        /// <see cref="DecoderBase.Decode"/>/<see cref="DecoderBase.PushMsg"/>, matching
        /// <see cref="StreamEngine"/>'s <c>ProcessInput</c>. For <see cref="DowngradeMode.V1"/>,
        /// <see cref="m_greetingLeftover"/> (bytes already read off the wire during the greeting)
        /// is replayed first, since it logically precedes anything else the peer sends.
        /// </summary>
        private async Task LegacyReceiveLoopAsync(DecoderBase decoder, bool processRoutingId, CancellationToken ct)
        {
            ByteArraySegment? inpos = null;
            int insize = 0;

            if (m_greetingLeftover is { Length: > 0 } leftover)
            {
                inpos = new ByteArraySegment(leftover);
                insize = leftover.Length;
            }

            bool first = processRoutingId;
            bool pendingFlush = false;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                if (insize == 0)
                {
                    if (pendingFlush)
                    {
                        m_session!.Flush();
                        pendingFlush = false;
                    }

                    decoder.GetBuffer(out var buf, out var bufSize);
                    int n = await ReceiveSomeAsync((byte[])buf, buf.Offset, bufSize, ct);
                    inpos = buf;
                    insize = n;
                }

                var result = decoder.Decode(inpos!, insize, out int processed);
                inpos!.AdvanceOffset(processed);
                insize -= processed;

                if (result == DecodeResult.Error)
                {
                    Error();
                    return;
                }

                if (result != DecodeResult.MessageReady)
                    continue;

                pendingFlush = true;

                bool isRoutingId = first;
                first = false;

                PushMsgResult pushResult = isRoutingId
                    ? decoder.PushMsg(ProcessRoutingIdMsg)
                    : decoder.PushMsg(m_session!.PushMsg);

                if (!isRoutingId)
                {
                    while (pushResult == PushMsgResult.Full)
                    {
                        m_session!.Flush();
                        pendingFlush = false;
                        await m_inSignal.WaitAsync(ct);
                        pushResult = decoder.PushMsg(m_session!.PushMsg);
                    }
                }

                if (pushResult == PushMsgResult.Error)
                {
                    Error();
                    return;
                }
            }
        }

        /// <summary>
        /// Gather-writes a large message's header (already-encoded, in <paramref name="header"/>)
        /// and body (straight from the message's own buffer, zero-copy) via a single
        /// <see cref="Socket.SendAsync(IList{ArraySegment{byte}}, SocketFlags)"/> call, retrying
        /// with the remaining unsent tail if the OS only accepted part of it. That overload has
        /// no <see cref="CancellationToken"/> parameter - cancellation is instead observed via
        /// socket disposal during <see cref="Terminate"/>/<see cref="Destroy"/>, which aborts
        /// whatever gather-write is in flight.
        /// </summary>
        private async Task SendGatherAsync(byte[] header, int headerLen, Msg msg, CancellationToken ct)
        {
            var segments = m_gatherSegments;
            segments.Clear();
            segments.Add(new ArraySegment<byte>(header, 0, headerLen));

            if (msg.Size > 0)
                segments.Add(new ArraySegment<byte>(msg.UnsafeData!, msg.UnsafeOffset, msg.Size));

            int total = headerLen + msg.Size;
            int sent = 0;
            while (sent < total)
            {
                ct.ThrowIfCancellationRequested();

                int n = await m_socket.SendAsync(segments, SocketFlags.None);
                if (n <= 0)
                    throw new SocketException((int)SocketError.ConnectionReset);

                sent += n;
                if (sent < total)
                    AdvanceSegments(segments, n);
            }
        }

        private static void AdvanceSegments(List<ArraySegment<byte>> segments, int sent)
        {
            while (sent > 0)
            {
                var seg = segments[0];
                if (sent < seg.Count)
                {
                    segments[0] = new ArraySegment<byte>(seg.Array!, seg.Offset + sent, seg.Count - sent);
                    return;
                }

                sent -= seg.Count;
                segments.RemoveAt(0);
            }
        }

        // ---- ZMTP1/2 downgrade routing-id exchange, ported from StreamEngine -----------

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>RoutingIdMsg</c>: the first message sent to a
        /// downgraded (ZMTP1/2, no mechanism) peer is our own routing id.
        /// </summary>
        private PullMsgResult RoutingIdMsg(ref Msg msg)
        {
            if (m_options.IdentitySize == 0)
                msg.InitEmpty();
            else
            {
                msg.InitPool(m_options.IdentitySize);
                msg.Put(m_options.Identity, 0, m_options.IdentitySize);
            }

            return PullMsgResult.Ok;
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>ProcessRoutingIdMsg</c>: the first message
        /// received from a downgraded peer is their routing id - tag it (or discard it) and,
        /// for a downgraded Pub/Xpub socket, inject the phony subscribe-all message right after.
        /// Deliberately does not retry on <see cref="PushMsgResult.Full"/> - matches
        /// <see cref="StreamEngine"/>'s identical (unguarded) behaviour here.
        /// </summary>
        private PushMsgResult ProcessRoutingIdMsg(ref Msg msg)
        {
            if (m_options.RecvIdentity)
            {
                msg.SetFlags(MsgFlags.Identity);
                m_session!.PushMsg(ref msg);
            }
            else
            {
                msg.Close();
                msg.InitEmpty();
            }

            if (m_subscriptionRequired)
            {
                var subscription = new Msg();
                subscription.InitPool(1);
                subscription.Put((byte)1);

                m_session!.PushMsg(ref subscription);
            }

            return PushMsgResult.Ok;
        }

        // ---- heartbeats (None mode only), ported from StreamEngine ---------------------

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>ProducePingMessage</c>: builds the PING command
        /// message and (re-)arms <see cref="HeartbeatTimeoutTimerId"/>.
        /// </summary>
        private PullMsgResult ProducePingMessage(ref Msg msg)
        {
            // 16-bit TTL + \4PING == 7
            int pingTtlLength = V3Protocol.PingCommand.Length + 1 + 2;

            msg.InitPool(pingTtlLength);
            msg.SetFlags(MsgFlags.Command);

            msg[0] = (byte)V3Protocol.PingCommand.Length;
            msg.Put(Encoding.ASCII.GetBytes(V3Protocol.PingCommand), 1, V3Protocol.PingCommand.Length);

            NetworkOrderBitsConverter.PutUInt16((ushort)m_options.HeartbeatTtl, msg, 1 + V3Protocol.PingCommand.Length);

            var result = m_mechanism!.Encode(ref msg);

            if (!m_hasHeartbeatTimeoutTimer && m_heartbeatTimeout > 0)
            {
                m_ioObject!.AddTimer(m_heartbeatTimeout, HeartbeatTimeoutTimerId);
                m_hasHeartbeatTimeoutTimer = true;
            }

            return result;
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>ProcessCommandMessage</c>.
        /// </summary>
        private void ProcessCommandMessage(ref Msg msg)
        {
            byte commandNameSize = msg[0];

            // Malformed command
            if (msg.Size < commandNameSize + 1)
                return;

            string commandName = msg.GetString(Encoding.ASCII, 1, commandNameSize);
            if (commandName == V3Protocol.PingCommand)
                ProcessPingMessage(ref msg);
        }

        /// <summary>
        /// Ports <see cref="StreamEngine"/>'s <c>ProcessPingMessage</c>: arms
        /// <see cref="HeartbeatTtlTimerId"/> from the peer's advertised TTL and builds
        /// <see cref="m_pongMsg"/>, then asks the send loop to produce it.
        /// </summary>
        private void ProcessPingMessage(ref Msg msg)
        {
            // 16-bit TTL + \4PING == 7
            int pingTtlLength = 1 + V3Protocol.PingCommand.Length + 2;
            const int pingMaxCtxLength = 16;

            // Malformed ping command
            if (msg.Size < pingTtlLength)
                return;

            ushort remoteHeartbeatTtl = NetworkOrderBitsConverter.ToUInt16(msg, V3Protocol.PingCommand.Length + 1);
            // The remote heartbeat is in 10ths of a second, so multiply by 100 for ms.
            remoteHeartbeatTtl *= 100;

            if (!m_hasHeartbeatTtlTimer && remoteHeartbeatTtl > 0)
            {
                m_ioObject!.AddTimer(remoteHeartbeatTtl, HeartbeatTtlTimerId);
                m_hasHeartbeatTtlTimer = true;
            }

            // As per ZMTP 3.1 the PING command might contain an up to 16 bytes context which
            // needs to be PONGed back - build and store the pong message here. Truncate it if
            // it's too long.
            int contextLength = Math.Min(msg.Size - pingTtlLength, pingMaxCtxLength);
            m_pongMsg.InitPool(1 + V3Protocol.PongCommand.Length + contextLength);
            m_pongMsg.SetFlags(MsgFlags.Command);
            m_pongMsg[0] = (byte)V3Protocol.PongCommand.Length;
            m_pongMsg.Put(Encoding.ASCII.GetBytes(V3Protocol.PongCommand), 1, V3Protocol.PongCommand.Length);
            if (contextLength > 0)
                m_pongMsg.Put(msg.Slice(pingTtlLength, contextLength), 1 + V3Protocol.PongCommand.Length);

            m_pongRequested = true;
            m_outSignal.Release();
        }
    }
}

#endif
