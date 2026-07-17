#if NET

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using NetMQ.Sockets;
using Xunit;

namespace NetMQ.Tests
{
    /// <summary>
    /// End-to-end smoke tests for the opt-in async I/O engine
    /// (<see cref="NetMQConfig.UseAsyncEngine"/> / <c>NETMQ_ASYNC_ENGINE=1</c>), exercised over
    /// tcp://. Complements <see cref="AsyncIOThreadTests"/> (actor loop, no sockets) and
    /// <c>IpcTests</c> (ipc:// transport); this file is the tcp:// data-plane/handshake path
    /// through <c>AsyncStreamEngine</c>.
    /// </summary>
    public class AsyncEngineTests : IClassFixture<CleanupAfterFixture>
    {
        public AsyncEngineTests()
        {
            NetMQConfig.Cleanup();
            NetMQConfig.UseAsyncEngine = true;
        }

        /// <summary>
        /// Ensures every test in this class restores the flag and tears the context down,
        /// even on failure/timeout - so a broken test here can't leak the flag into other
        /// test classes that run flag-off.
        /// </summary>
        private static void RunFlagOn(Action body)
        {
            try
            {
                body();
            }
            finally
            {
                NetMQConfig.UseAsyncEngine = false;
                NetMQConfig.Cleanup();
            }
        }

        [Fact]
        public void DealerRouterRoundTripIncluding1MbFrame()
        {
            RunFlagOn(() =>
            {
                using var router = new RouterSocket();
                using var dealer = new DealerSocket();

                int port = router.BindRandomPort("tcp://127.0.0.1");
                dealer.Connect($"tcp://127.0.0.1:{port}");

                // small round trip, dealer -> router -> dealer
                Assert.True(dealer.TrySendFrame(TimeSpan.FromSeconds(10), "hello"));

                Assert.True(router.TryReceiveFrameBytes(TimeSpan.FromSeconds(10), out byte[]? routingId, out bool more));
                Assert.True(more);
                Assert.True(router.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? request));
                Assert.Equal("hello", request);

                router.SendMoreFrame(routingId!).SendFrame("world");

                Assert.True(dealer.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? reply));
                Assert.Equal("world", reply);

                // 1MB frame in both directions to exercise the gather-write/large-message
                // path in AsyncStreamEngine's send/receive loops.
                var big = new byte[1024 * 1024];
                new Random(12345).NextBytes(big);

                Assert.True(dealer.TrySendFrame(TimeSpan.FromSeconds(10), big));

                Assert.True(router.TryReceiveFrameBytes(TimeSpan.FromSeconds(10), out _, out more));
                Assert.True(more);
                Assert.True(router.TryReceiveFrameBytes(TimeSpan.FromSeconds(10), out byte[]? bigReceived));
                Assert.Equal(big, bigReceived);

                router.SendMoreFrame(routingId!).SendFrame(big);

                Assert.True(dealer.TryReceiveFrameBytes(TimeSpan.FromSeconds(10), out byte[]? bigReply));
                Assert.Equal(big, bigReply);
            });
        }

        [Fact]
        public void PairRoundTripOverTcp()
        {
            RunFlagOn(() =>
            {
                using var server = new PairSocket();
                using var client = new PairSocket();

                int port = server.BindRandomPort("tcp://127.0.0.1");
                client.Connect($"tcp://127.0.0.1:{port}");

                for (int i = 0; i < 20; i++)
                {
                    var payload = $"msg-{i}";
                    Assert.True(client.TrySendFrame(TimeSpan.FromSeconds(10), payload));
                    Assert.True(server.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? received));
                    Assert.Equal(payload, received);
                }
            });
        }

        [Fact]
        public void HeartbeatEnabledPairStaysAliveAcrossMultipleIntervals()
        {
            RunFlagOn(() =>
            {
                using var server = new PairSocket();
                using var client = new PairSocket();

                // Short heartbeat interval with a generously long timeout/TTL: the peers
                // should exchange several PING/PONG rounds and never be torn down as long
                // as heartbeats (and thus the connection) keep flowing.
                client.Options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
                client.Options.HeartbeatTimeout = TimeSpan.FromSeconds(5);
                server.Options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
                server.Options.HeartbeatTimeout = TimeSpan.FromSeconds(5);

                int port = server.BindRandomPort("tcp://127.0.0.1");
                client.Connect($"tcp://127.0.0.1:{port}");

                // Prime the connection so the handshake has definitely completed before we
                // start timing the heartbeat window.
                Assert.True(client.TrySendFrame(TimeSpan.FromSeconds(10), "ping"));
                Assert.True(server.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? primed));
                Assert.Equal("ping", primed);

                // Sit idle for well over 2 heartbeat intervals (100ms) - several multiples,
                // to absorb scheduler jitter - then prove the connection is still usable.
                Thread.Sleep(TimeSpan.FromMilliseconds(600));

                Assert.True(server.TrySendFrame(TimeSpan.FromSeconds(10), "still-alive"));
                Assert.True(client.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? received));
                Assert.Equal("still-alive", received);
            });
        }

        [Fact]
        public void CleanupAfterUseDoesNotHang()
        {
            RunFlagOn(() =>
            {
                using (var server = new PairSocket())
                using (var client = new PairSocket())
                {
                    int port = server.BindRandomPort("tcp://127.0.0.1");
                    client.Connect($"tcp://127.0.0.1:{port}");

                    Assert.True(client.TrySendFrame(TimeSpan.FromSeconds(10), "hi"));
                    Assert.True(server.TryReceiveFrameString(TimeSpan.FromSeconds(10), out _));
                }

                var sw = Stopwatch.StartNew();
                NetMQConfig.Cleanup();
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), "NetMQConfig.Cleanup() took too long to return.");
            });
        }
    }
}

#endif
