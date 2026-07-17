#if NET
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using NetMQ.Core;
using NetMQ.Core.Transports;
using Xunit;

namespace NetMQ.Tests
{
    /// <summary>
    /// Round-trip tests for <see cref="V2SpanEncoder"/> and <see cref="V2SpanDecoder"/>,
    /// using the existing (non-span) <see cref="V2Encoder"/>/<see cref="V2Decoder"/> as an
    /// oracle: bytes produced by one implementation must be decoded identically by the other.
    /// </summary>
    public class SpanCodecTests
    {
        private static readonly int[] Sizes = { 0, 1, 255, 256, 64 * 1024, 1024 * 1024 };

        private static readonly MsgFlags[] FlagCombos =
        {
            MsgFlags.None,
            MsgFlags.More,
            MsgFlags.Command,
            MsgFlags.More | MsgFlags.Command
        };

        #region Helpers

        private static Msg MakeMsg(int size, MsgFlags flags, int seed)
        {
            var msg = new Msg();
            msg.InitPool(size);

            var rnd = new Random(seed);
            for (int i = 0; i < size; i++)
                msg.Put((byte)rnd.Next(256), i);

            msg.SetFlags(flags);
            return msg;
        }

        /// <summary>Encodes a single message to wire bytes using the existing (oracle) V2Encoder.</summary>
        private static byte[] EncodeWithOldEncoder(Msg msg, Endianness endian)
        {
            const int chunkSize = 8192;
            var encoder = new V2Encoder(chunkSize, endian);
            encoder.LoadMsg(ref msg);

            using var ms = new MemoryStream();
            while (true)
            {
                ByteArraySegment? buf = new ByteArraySegment(new byte[chunkSize]);
                int n = encoder.Encode(ref buf, chunkSize);
                if (n == 0)
                    break;

                for (int i = 0; i < n; i++)
                    ms.WriteByte(buf![i]);
            }

            return ms.ToArray();
        }

        /// <summary>Encodes a single message to wire bytes using the new V2SpanEncoder.</summary>
        private static byte[] EncodeWithSpanEncoder(Msg msg, Endianness endian)
        {
            var encoder = new V2SpanEncoder(endian);
            int headerLength = V2SpanEncoder.GetHeaderLength(msg.Size);
            var result = new byte[headerLength + msg.Size];

            bool expectTooLarge = msg.Size >= V2SpanEncoder.LargeMessageThreshold;

            if (encoder.TryEncodeFrame(ref msg, result, out int written, out bool bodyTooLarge))
            {
                Assert.False(expectTooLarge);
                Assert.False(bodyTooLarge);
                Assert.Equal(result.Length, written);
                return result;
            }

            Assert.Equal(expectTooLarge, bodyTooLarge);
            Assert.True(bodyTooLarge, "TryEncodeFrame failed without reporting bodyTooLarge or a too-small destination");

            int headerWritten = encoder.WriteHeader(ref msg, result);
            Assert.Equal(headerLength, headerWritten);
            msg.Slice().CopyTo(result.AsSpan(headerWritten));
            return result;
        }

        /// <summary>Feeds raw wire bytes through the existing (oracle) V2Decoder, in caller-supplied chunks.</summary>
        private static (List<(byte[] Data, MsgFlags Flags)> Messages, bool Error) DecodeAllWithOldDecoder(
            IEnumerable<byte[]> chunks, long maxMsgSize, Endianness endian)
        {
            var decoder = new V2Decoder(8192, maxMsgSize, endian);
            var messages = new List<(byte[], MsgFlags)>();

            foreach (var chunk in chunks)
            {
                int offset = 0;
                while (offset < chunk.Length)
                {
                    var segment = new ByteArraySegment(chunk, offset);
                    var result = decoder.Decode(segment, chunk.Length - offset, out int used);
                    offset += used;

                    if (result == DecodeResult.Error)
                        return (messages, true);

                    if (result == DecodeResult.MessageReady)
                    {
                        byte[]? data = null;
                        MsgFlags flags = default;
                        decoder.PushMsg(delegate(ref Msg m)
                        {
                            data = m.ToArray();
                            flags = m.Flags;
                            return PushMsgResult.Ok;
                        });
                        messages.Add((data!, flags));
                    }
                }
            }

            return (messages, false);
        }

        /// <summary>Streams chunks through a real <see cref="Pipe"/> and decodes them with a <see cref="V2SpanDecoder"/>.</summary>
        private static async Task<(List<(byte[] Data, MsgFlags Flags)> Messages, SpanDecodeResult? Error)> DecodeViaPipeAsync(
            V2SpanDecoder decoder, IEnumerable<byte[]> chunks)
        {
            var pipe = new System.IO.Pipelines.Pipe();

            var writeTask = Task.Run(async () =>
            {
                foreach (var chunk in chunks)
                {
                    await pipe.Writer.WriteAsync(chunk);
                }

                await pipe.Writer.CompleteAsync();
            });

            var messages = new List<(byte[], MsgFlags)>();
            SpanDecodeResult? error = null;

            while (true)
            {
                var readResult = await pipe.Reader.ReadAsync();
                var buffer = readResult.Buffer;

                while (true)
                {
                    var msg = default(Msg);
                    var result = decoder.TryDecodeFrame(ref buffer, ref msg);

                    if (result == SpanDecodeResult.MessageReady)
                    {
                        messages.Add((msg.ToArray(), msg.Flags));
                        msg.Close();
                        continue;
                    }

                    if (result == SpanDecodeResult.Error)
                        error = result;

                    break;
                }

                pipe.Reader.AdvanceTo(buffer.Start, readResult.Buffer.End);

                if (error != null)
                    break;

                if (readResult.IsCompleted)
                    break;
            }

            await pipe.Reader.CompleteAsync();
            await writeTask;

            return (messages, error);
        }

        private static List<byte[]> SplitIntoChunks(byte[] data, int chunkSize)
        {
            var chunks = new List<byte[]>();
            for (int i = 0; i < data.Length; i += chunkSize)
            {
                int len = Math.Min(chunkSize, data.Length - i);
                var c = new byte[len];
                Array.Copy(data, i, c, 0, len);
                chunks.Add(c);
            }

            // Ensure at least one (possibly empty) chunk so zero-length messages still flow through.
            if (chunks.Count == 0)
                chunks.Add(Array.Empty<byte>());

            return chunks;
        }

        #endregion

        #region Encoder: same wire bytes as V2Encoder

        [Theory]
        [InlineData(Endianness.Big)]
        [InlineData(Endianness.Little)]
        public void SpanEncoder_MatchesOldEncoder_ForAllSizesAndFlags(Endianness endian)
        {
            foreach (int size in Sizes)
            {
                // Skip the largest size for every flag combo to keep the test fast; still covered once below.
                foreach (var flags in FlagCombos)
                {
                    if (size == 1024 * 1024 && flags != MsgFlags.None)
                        continue;

                    var msg1 = MakeMsg(size, flags, seed: size ^ (int)flags);
                    var msg2 = msg1; // Msg is a struct sharing the same underlying buffer; encoding doesn't mutate content.

                    byte[] oldBytes = EncodeWithOldEncoder(msg1, endian);
                    byte[] newBytes = EncodeWithSpanEncoder(msg2, endian);

                    Assert.Equal(oldBytes, newBytes);
                }
            }
        }

        [Fact]
        public void SpanEncoder_TryEncodeFrame_RejectsWhenDestinationTooSmall()
        {
            var msg = MakeMsg(10, MsgFlags.None, seed: 1);
            var encoder = new V2SpanEncoder(Endianness.Big);

            Span<byte> tooSmall = stackalloc byte[5]; // header (2) + body (10) = 12 needed
            bool ok = encoder.TryEncodeFrame(ref msg, tooSmall, out int written, out bool bodyTooLarge);

            Assert.False(ok);
            Assert.False(bodyTooLarge);
            Assert.Equal(0, written);
        }

        [Fact]
        public void SpanEncoder_TryEncodeFrame_ReportsBodyTooLarge()
        {
            var msg = MakeMsg(V2SpanEncoder.LargeMessageThreshold, MsgFlags.None, seed: 2);
            var encoder = new V2SpanEncoder(Endianness.Big);

            var dest = new byte[V2SpanEncoder.MaxHeaderSize + msg.Size];
            bool ok = encoder.TryEncodeFrame(ref msg, dest, out int written, out bool bodyTooLarge);

            Assert.False(ok);
            Assert.True(bodyTooLarge);
            Assert.Equal(0, written);

            // Header-only path should still work and match the oracle's header bytes.
            int headerWritten = encoder.WriteHeader(ref msg, dest);
            byte[] oldBytes = EncodeWithOldEncoder(msg, Endianness.Big);
            Assert.Equal(oldBytes.AsSpan(0, headerWritten).ToArray(), dest.AsSpan(0, headerWritten).ToArray());
        }

        #endregion

        #region Decoder: matches V2Decoder, single-shot (no split)

        [Theory]
        [InlineData(Endianness.Big)]
        [InlineData(Endianness.Little)]
        public void SpanDecoder_MatchesOldDecoder_ForAllSizesAndFlags(Endianness endian)
        {
            foreach (int size in Sizes)
            {
                foreach (var flags in FlagCombos)
                {
                    if (size == 1024 * 1024 && flags != MsgFlags.None)
                        continue;

                    var original = MakeMsg(size, flags, seed: 1000 + size ^ (int)flags);
                    byte[] wireBytes = EncodeWithOldEncoder(original, endian);

                    var decoder = new V2SpanDecoder(-1, endian);
                    var buffer = new ReadOnlySequence<byte>(wireBytes);
                    var decoded = default(Msg);

                    var result = decoder.TryDecodeFrame(ref buffer, ref decoded);

                    Assert.Equal(SpanDecodeResult.MessageReady, result);
                    Assert.Equal(original.Size, decoded.Size);
                    Assert.Equal(flags, decoded.Flags);
                    Assert.Equal(original.ToArray(), decoded.ToArray());
                    Assert.Equal(0, buffer.Length); // fully consumed, exactly one frame
                }
            }
        }

        #endregion

        #region Decoder: split-buffer delivery (1..7-byte chunks) via a real Pipe

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task SpanDecoder_HandlesSplitDelivery(int chunkSize)
        {
            var sizes = new[] { 0, 1, 255, 256, 4096 };

            foreach (int size in sizes)
            {
                foreach (var flags in FlagCombos)
                {
                    var original = MakeMsg(size, flags, seed: 2000 + size ^ (int)flags ^ chunkSize);
                    byte[] wireBytes = EncodeWithOldEncoder(original, Endianness.Big);
                    var chunks = SplitIntoChunks(wireBytes, chunkSize);

                    var decoder = new V2SpanDecoder(-1, Endianness.Big);
                    var (messages, error) = await DecodeViaPipeAsync(decoder, chunks);

                    Assert.Null(error);
                    var message = Assert.Single(messages);
                    Assert.Equal(flags, message.Flags);
                    Assert.Equal(original.ToArray(), message.Data);
                }
            }
        }

        #endregion

        #region Decoder: many random frames pipelined through random-sized chunks

        [Fact]
        public async Task SpanDecoder_HandlesRandomPipelinedFrames()
        {
            var rnd = new Random(12345);
            const int messageCount = 64;

            var originals = new List<(byte[] Data, MsgFlags Flags)>();
            using var ms = new MemoryStream();

            for (int i = 0; i < messageCount; i++)
            {
                int size = rnd.Next(0, 2048);
                var flags = FlagCombos[rnd.Next(FlagCombos.Length)];
                var msg = MakeMsg(size, flags, seed: rnd.Next());

                originals.Add((msg.ToArray(), flags));

                byte[] wireBytes = EncodeWithOldEncoder(msg, Endianness.Little);
                ms.Write(wireBytes, 0, wireBytes.Length);
            }

            byte[] allBytes = ms.ToArray();

            // Chop the whole stream into random-sized chunks (1..37 bytes), independent of frame boundaries.
            var chunks = new List<byte[]>();
            int offset = 0;
            while (offset < allBytes.Length)
            {
                int len = Math.Min(rnd.Next(1, 38), allBytes.Length - offset);
                var chunk = new byte[len];
                Array.Copy(allBytes, offset, chunk, 0, len);
                chunks.Add(chunk);
                offset += len;
            }

            var decoder = new V2SpanDecoder(-1, Endianness.Little);
            var (messages, error) = await DecodeViaPipeAsync(decoder, chunks);

            Assert.Null(error);
            Assert.Equal(originals.Count, messages.Count);

            for (int i = 0; i < originals.Count; i++)
            {
                Assert.Equal(originals[i].Flags, messages[i].Flags);
                Assert.Equal(originals[i].Data, messages[i].Data);
            }
        }

        #endregion

        #region MaxMessageSize rejection parity

        [Fact]
        public void SpanDecoder_RejectsOverLimitMessage_OneByteLength_SameAsOldDecoder()
        {
            const long maxMsgSize = 100;
            var msg = MakeMsg(200, MsgFlags.None, seed: 3);
            byte[] wireBytes = EncodeWithOldEncoder(msg, Endianness.Big);

            var (_, oldError) = DecodeAllWithOldDecoder(new[] { wireBytes }, maxMsgSize, Endianness.Big);
            Assert.True(oldError);

            var decoder = new V2SpanDecoder(maxMsgSize, Endianness.Big);
            var buffer = new ReadOnlySequence<byte>(wireBytes);
            var decoded = default(Msg);
            var result = decoder.TryDecodeFrame(ref buffer, ref decoded);

            Assert.Equal(SpanDecodeResult.Error, result);
        }

        [Fact]
        public void SpanDecoder_RejectsOverLimitMessage_EightByteLength_SameAsOldDecoder()
        {
            const long maxMsgSize = 100;
            var msg = MakeMsg(1000, MsgFlags.None, seed: 4); // > 255, forces 8-byte length encoding
            byte[] wireBytes = EncodeWithOldEncoder(msg, Endianness.Big);

            var (_, oldError) = DecodeAllWithOldDecoder(new[] { wireBytes }, maxMsgSize, Endianness.Big);
            Assert.True(oldError);

            var decoder = new V2SpanDecoder(maxMsgSize, Endianness.Big);
            var buffer = new ReadOnlySequence<byte>(wireBytes);
            var decoded = default(Msg);
            var result = decoder.TryDecodeFrame(ref buffer, ref decoded);

            Assert.Equal(SpanDecodeResult.Error, result);
        }

        [Theory]
        [InlineData(Endianness.Big)]
        [InlineData(Endianness.Little)]
        public void SpanDecoder_RejectsSizeExceedingSizeTRange_SameAsOldDecoder(Endianness endian)
        {
            // Craft a header (flags=Large, 8-byte length > 0x7FFFFFC7) directly - too large to actually allocate.
            const ulong hugeSize = 0x7FFFFFC7UL + 1UL;
            var header = new byte[9];
            header[0] = (byte)V2Protocol.LargeFlag;

            if (endian == Endianness.Big)
            {
                for (int i = 0; i < 8; i++)
                    header[1 + i] = (byte)(hugeSize >> (8 * (7 - i)));
            }
            else
            {
                for (int i = 0; i < 8; i++)
                    header[1 + i] = (byte)(hugeSize >> (8 * i));
            }

            var (_, oldError) = DecodeAllWithOldDecoder(new[] { header }, -1, endian);
            Assert.True(oldError);

            var decoder = new V2SpanDecoder(-1, endian);
            var buffer = new ReadOnlySequence<byte>(header);
            var decoded = default(Msg);
            var result = decoder.TryDecodeFrame(ref buffer, ref decoded);

            Assert.Equal(SpanDecodeResult.Error, result);
        }

        #endregion

        #region Cross-check: span encoder output decodes correctly with the OLD decoder, and vice-versa

        [Theory]
        [InlineData(Endianness.Big)]
        [InlineData(Endianness.Little)]
        public void SpanEncoderOutput_DecodesCorrectly_WithOldDecoder(Endianness endian)
        {
            foreach (int size in new[] { 0, 1, 255, 256, 4096 })
            {
                foreach (var flags in FlagCombos)
                {
                    var original = MakeMsg(size, flags, seed: 5000 + size ^ (int)flags);
                    byte[] wireBytes = EncodeWithSpanEncoder(original, endian);

                    var (messages, error) = DecodeAllWithOldDecoder(new[] { wireBytes }, -1, endian);

                    Assert.False(error);
                    var message = Assert.Single(messages);
                    Assert.Equal(flags, message.Flags);
                    Assert.Equal(original.ToArray(), message.Data);
                }
            }
        }

        [Theory]
        [InlineData(Endianness.Big)]
        [InlineData(Endianness.Little)]
        public void OldEncoderOutput_DecodesCorrectly_WithSpanDecoder(Endianness endian)
        {
            foreach (int size in new[] { 0, 1, 255, 256, 4096 })
            {
                foreach (var flags in FlagCombos)
                {
                    var original = MakeMsg(size, flags, seed: 6000 + size ^ (int)flags);
                    byte[] wireBytes = EncodeWithOldEncoder(original, endian);

                    var decoder = new V2SpanDecoder(-1, endian);
                    var buffer = new ReadOnlySequence<byte>(wireBytes);
                    var decoded = default(Msg);
                    var result = decoder.TryDecodeFrame(ref buffer, ref decoded);

                    Assert.Equal(SpanDecodeResult.MessageReady, result);
                    Assert.Equal(flags, decoded.Flags);
                    Assert.Equal(original.ToArray(), decoded.ToArray());
                }
            }
        }

        #endregion
    }
}
#endif
