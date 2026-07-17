#if NET
using System;
using System.Buffers.Binary;

namespace NetMQ.Core.Transports
{
    /// <summary>
    /// Span-based, allocation-free encoder for the ZMTP V2/V3 wire framing
    /// (flags byte + 1-or-8-byte length + body), producing the exact same
    /// wire bytes as <see cref="V2Encoder"/>. Intended for the batch-loop
    /// send path of the async engine: small/medium frames are coalesced into
    /// a caller-owned batch buffer via <see cref="TryEncodeFrame"/>, while
    /// large bodies are written header-only via <see cref="WriteHeader"/> so
    /// the body itself can be sent zero-copy (e.g. gather-write).
    /// </summary>
    internal readonly struct V2SpanEncoder
    {
        /// <summary>
        /// Maximum size, in bytes, of the fixed-size framing header
        /// (1 flags byte + up to 8 length bytes).
        /// </summary>
        public const int MaxHeaderSize = 9;

        /// <summary>
        /// Message bodies at or above this size are reported via
        /// <paramref name="bodyTooLarge"/> from <see cref="TryEncodeFrame"/>
        /// instead of being copied into the destination span - callers
        /// should instead use <see cref="WriteHeader"/> and gather-write the
        /// body separately.
        /// </summary>
        public const int LargeMessageThreshold = 4096;

        private readonly Endianness m_endian;

        public V2SpanEncoder(Endianness endian)
        {
            m_endian = endian;
        }

        /// <summary>
        /// Returns the number of bytes the framing header will occupy for a
        /// message body of the given size: 2 bytes (flags + 1-byte length)
        /// for bodies up to 255 bytes, or 9 bytes (flags + 8-byte length)
        /// otherwise.
        /// </summary>
        public static int GetHeaderLength(int size) => size > 255 ? 9 : 2;

        /// <summary>
        /// Attempts to encode the header and body of <paramref name="msg"/>
        /// into <paramref name="dest"/> in a single copy, for coalescing
        /// several small messages into one batch buffer.
        /// </summary>
        /// <param name="msg">the message to encode</param>
        /// <param name="dest">the destination span to write the frame into</param>
        /// <param name="written">
        /// the number of bytes written to <paramref name="dest"/>; zero if
        /// the frame was not written
        /// </param>
        /// <param name="bodyTooLarge">
        /// <c>true</c> when the message body is at or above
        /// <see cref="LargeMessageThreshold"/>. In this case nothing is
        /// written and the caller should instead write the header via
        /// <see cref="WriteHeader"/> and send the body separately
        /// (e.g. via gather-write) to avoid copying large payloads.
        /// </param>
        /// <returns>
        /// <c>true</c> if the full frame (header + body) was written to
        /// <paramref name="dest"/>; <c>false</c> if the body was too large
        /// (see <paramref name="bodyTooLarge"/>) or <paramref name="dest"/>
        /// did not have enough room for the full frame.
        /// </returns>
        public bool TryEncodeFrame(ref Msg msg, Span<byte> dest, out int written, out bool bodyTooLarge)
        {
            int size = msg.Size;

            if (size >= LargeMessageThreshold)
            {
                bodyTooLarge = true;
                written = 0;
                return false;
            }

            bodyTooLarge = false;

            int headerLength = GetHeaderLength(size);
            int total = headerLength + size;

            if (dest.Length < total)
            {
                written = 0;
                return false;
            }

            int headerWritten = WriteHeader(ref msg, dest);
            System.Diagnostics.Debug.Assert(headerWritten == headerLength);

            if (size > 0)
                msg.Slice().CopyTo(dest.Slice(headerWritten));

            written = headerWritten + size;
            return true;
        }

        /// <summary>
        /// Writes only the framing header (flags + length) for
        /// <paramref name="msg"/> into <paramref name="dest"/>, without
        /// copying the message body. Used for large messages that are sent
        /// via gather-write instead of being coalesced into a batch buffer.
        /// </summary>
        /// <param name="msg">the message whose header is to be written</param>
        /// <param name="dest">
        /// the destination span, which must be at least
        /// <see cref="GetHeaderLength"/> bytes long (at most
        /// <see cref="MaxHeaderSize"/>)
        /// </param>
        /// <returns>the number of header bytes written: 2 or 9</returns>
        public int WriteHeader(ref Msg msg, Span<byte> dest)
        {
            int size = msg.Size;

            int protocolFlags = 0;
            if (msg.HasMore)
                protocolFlags |= V2Protocol.MoreFlag;
            if (size > 255)
                protocolFlags |= V2Protocol.LargeFlag;
            if (msg.HasCommand)
                protocolFlags |= V2Protocol.CommandFlag;

            dest[0] = (byte)protocolFlags;

            if (size > 255)
            {
                PutUnsignedLong(dest.Slice(1, 8), (ulong)size);
                return 9;
            }

            dest[1] = (byte)size;
            return 2;
        }

        private void PutUnsignedLong(Span<byte> dest, ulong value)
        {
            if (m_endian == Endianness.Big)
                BinaryPrimitives.WriteUInt64BigEndian(dest, value);
            else
                BinaryPrimitives.WriteUInt64LittleEndian(dest, value);
        }
    }
}
#endif
