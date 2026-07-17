#if NET
using System;
using System.Buffers;
using System.Buffers.Binary;

namespace NetMQ.Core.Transports
{
    /// <summary>
    /// Result of a single <see cref="V2SpanDecoder.TryDecodeFrame"/> call.
    /// </summary>
    internal enum SpanDecodeResult
    {
        /// <summary>
        /// The supplied buffer was fully consumed but did not contain a
        /// complete frame; the caller should append more data (received
        /// from the socket) and call <see cref="V2SpanDecoder.TryDecodeFrame"/>
        /// again. Decoder state (partially-read header/body) is preserved
        /// across calls.
        /// </summary>
        NeedMoreData,

        /// <summary>
        /// A complete message frame was decoded into the <c>msg</c> out
        /// parameter.
        /// </summary>
        MessageReady,

        /// <summary>
        /// The frame violates the protocol (message size exceeds
        /// <c>MaxMessageSize</c> or the internal size_t range).
        /// </summary>
        Error
    }

    /// <summary>
    /// Span/sequence-based, incremental decoder for the ZMTP V2/V3 wire
    /// framing (flags byte + 1-or-8-byte length + body), matching the same
    /// wire format and MaxMessageSize semantics as <see cref="V2Decoder"/>.
    /// <para/>
    /// Unlike <see cref="V2Decoder"/> (which is driven by <see cref="EncoderBase"/>'s
    /// fixed-size buffer plumbing), this decoder is driven directly over a
    /// <see cref="ReadOnlySequence{T}"/> - e.g. the buffer produced by a
    /// <c>PipeReader</c> - and tolerates the header (or body) being split
    /// across arbitrarily many segments/chunks, including a header split
    /// across multiple <see cref="TryDecodeFrame"/> calls.
    /// </summary>
    internal sealed class V2SpanDecoder
    {
        private enum State
        {
            Flags,
            OneByteSize,
            EightByteSize,
            Body
        }

        // TODO: move this constant to a good place (0x7FFFFFC7) - kept in
        // sync with V2Decoder's identical check.
        private const long MaxSizeTValue = 0x7FFFFFC7;

        private readonly long m_maxMsgSize;
        private readonly Endianness m_endian;

        private State m_state = State.Flags;
        private MsgFlags m_flags;
        private int m_bodySize;
        private int m_bodyFilled;
        private Msg m_inProgress;

        public V2SpanDecoder(long maxMsgSize, Endianness endian)
        {
            m_maxMsgSize = maxMsgSize;
            m_endian = endian;

            m_inProgress = new Msg();
            m_inProgress.InitEmpty();
        }

        /// <summary>
        /// Attempts to decode a single message frame from the front of
        /// <paramref name="buffer"/>. On return, <paramref name="buffer"/>
        /// is advanced past whatever bytes were consumed (which may be less
        /// than its full length, e.g. when a frame boundary falls in the
        /// middle of the sequence, or all of it, when more data is needed).
        /// </summary>
        /// <param name="buffer">
        /// the available received bytes; advanced past the consumed prefix
        /// </param>
        /// <param name="msg">
        /// set to the decoded message when the return value is
        /// <see cref="SpanDecodeResult.MessageReady"/>; otherwise untouched
        /// </param>
        public SpanDecodeResult TryDecodeFrame(ref ReadOnlySequence<byte> buffer, ref Msg msg)
        {
            var reader = new SequenceReader<byte>(buffer);

            try
            {
                while (true)
                {
                    switch (m_state)
                    {
                        case State.Flags:
                        {
                            if (!reader.TryRead(out byte first))
                                return SpanDecodeResult.NeedMoreData;

                            m_flags = MsgFlags.None;
                            if ((first & V2Protocol.MoreFlag) != 0)
                                m_flags |= MsgFlags.More;
                            if ((first & V2Protocol.CommandFlag) != 0)
                                m_flags |= MsgFlags.Command;

                            m_state = (first & V2Protocol.LargeFlag) != 0
                                ? State.EightByteSize
                                : State.OneByteSize;

                            continue;
                        }

                        case State.OneByteSize:
                        {
                            if (!reader.TryRead(out byte size))
                                return SpanDecodeResult.NeedMoreData;

                            // Message size must not exceed the maximum allowed size.
                            if (m_maxMsgSize >= 0 && size > m_maxMsgSize)
                                return SpanDecodeResult.Error;

                            InitBody(size);
                            continue;
                        }

                        case State.EightByteSize:
                        {
                            if (reader.Remaining < 8)
                                return SpanDecodeResult.NeedMoreData;

                            Span<byte> tmp = stackalloc byte[8];
                            reader.TryCopyTo(tmp);
                            reader.Advance(8);

                            // The payload size is encoded as 64-bit unsigned integer.
                            ulong msgSize = m_endian == Endianness.Big
                                ? BinaryPrimitives.ReadUInt64BigEndian(tmp)
                                : BinaryPrimitives.ReadUInt64LittleEndian(tmp);

                            // Message size must not exceed the maximum allowed size.
                            if (m_maxMsgSize >= 0 && msgSize > (ulong)m_maxMsgSize)
                                return SpanDecodeResult.Error;

                            // Message size must fit within range of size_t data type.
                            if (msgSize > MaxSizeTValue)
                                return SpanDecodeResult.Error;

                            InitBody((int)msgSize);
                            continue;
                        }

                        case State.Body:
                        {
                            int remaining = m_bodySize - m_bodyFilled;

                            if (remaining > 0)
                            {
                                int avail = (int)Math.Min(reader.Remaining, remaining);
                                if (avail > 0)
                                {
                                    Span<byte> dest = m_inProgress.Slice(m_bodyFilled, avail);
                                    reader.TryCopyTo(dest);
                                    reader.Advance(avail);
                                    m_bodyFilled += avail;
                                }
                            }

                            if (m_bodyFilled < m_bodySize)
                                return SpanDecodeResult.NeedMoreData;

                            // Message is completely read - hand it off and
                            // start reading a new one (in_progress is a
                            // 0-byte Empty message after this point).
                            msg.Move(ref m_inProgress);
                            m_state = State.Flags;
                            return SpanDecodeResult.MessageReady;
                        }

                        default:
                            return SpanDecodeResult.Error;
                    }
                }
            }
            finally
            {
                buffer = buffer.Slice(reader.Consumed);
            }
        }

        private void InitBody(int size)
        {
            // in_progress is initialised at this point (either Empty from
            // construction/the previous frame) so in theory we should close
            // it before calling InitPool, however it's a 0-byte message and
            // thus we can treat it as uninitialised - matching V2Decoder.
            m_inProgress.InitPool(size);
            m_inProgress.SetFlags(m_flags);

            m_bodySize = size;
            m_bodyFilled = 0;
            m_state = State.Body;
        }
    }
}
#endif
