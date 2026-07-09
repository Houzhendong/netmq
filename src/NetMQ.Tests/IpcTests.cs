using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using NetMQ.Sockets;
using Xunit;

namespace NetMQ.Tests
{
    /// <summary>
    /// Tests for the ipc:// transport backed by real Unix Domain Sockets (AF_UNIX).
    /// Supported on Windows 10 1803+, Linux and macOS.
    /// </summary>
    public class IpcTests : IClassFixture<CleanupAfterFixture>
    {
        public IpcTests() => NetMQConfig.Cleanup();

        private static string CreateTempIpcPath()
        {
            return Path.Combine(Path.GetTempPath(), $"netmq-test-{Guid.NewGuid():N}.sock");
        }

        // Best-effort delete used in cleanup: the listener's socket/lock files are
        // released asynchronously on its I/O thread, so they may briefly still be
        // in use immediately after the owning socket's Dispose() call returns.
        private static void DeleteIfExists(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch
            {
            }
        }

        [Fact]
        public void ReqRepOverIpc()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var rep = new ResponseSocket())
                using (var req = new RequestSocket())
                {
                    rep.Bind(address);
                    req.Connect(address);

                    // string roundtrip in both directions
                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), "hello"));
                    Assert.True(rep.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? request));
                    Assert.Equal("hello", request);

                    Assert.True(rep.TrySendFrame(TimeSpan.FromSeconds(5), "world"));
                    Assert.True(req.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? reply));
                    Assert.Equal("world", reply);

                    // large (1MB) frame roundtrip to exercise the stream engine over UDS
                    var large = new byte[1024 * 1024];
                    new Random(42).NextBytes(large);

                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), large));
                    Assert.True(rep.TryReceiveFrameBytes(TimeSpan.FromSeconds(5), out byte[]? received));
                    Assert.Equal(large, received);

                    Assert.True(rep.TrySendFrame(TimeSpan.FromSeconds(5), received!));
                    Assert.True(req.TryReceiveFrameBytes(TimeSpan.FromSeconds(5), out byte[]? echoed));
                    Assert.Equal(large, echoed);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void PubSubOverIpc()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var pub = new PublisherSocket())
                using (var sub = new SubscriberSocket())
                {
                    pub.Bind(address);
                    sub.Connect(address);
                    sub.SubscribeToAnyTopic();

                    // let the subscriber connect to the publisher before sending a message
                    Thread.Sleep(200);

                    // standard slow-joiner handling: publish repeatedly until the
                    // subscriber receives, with an overall deadline
                    var stopwatch = Stopwatch.StartNew();
                    string? received = null;

                    while (stopwatch.Elapsed < TimeSpan.FromSeconds(10))
                    {
                        pub.SendFrame("ping");

                        if (sub.TryReceiveFrameString(TimeSpan.FromMilliseconds(100), out received))
                            break;
                    }

                    Assert.Equal("ping", received);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void WildcardBind()
        {
            string? resolvedPath = null;

            try
            {
                using (var rep = new ResponseSocket())
                using (var req = new RequestSocket())
                {
                    rep.Bind("ipc://*");

                    var lastEndpoint = rep.Options.LastEndpoint;

                    Assert.NotNull(lastEndpoint);
                    Assert.StartsWith("ipc://", lastEndpoint);
                    Assert.NotEqual("ipc://*", lastEndpoint);

                    resolvedPath = lastEndpoint!.Substring("ipc://".Length);

                    req.Connect(lastEndpoint);

                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), "hello"));
                    Assert.True(rep.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? request));
                    Assert.Equal("hello", request);

                    Assert.True(rep.TrySendFrame(TimeSpan.FromSeconds(5), "world"));
                    Assert.True(req.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? reply));
                    Assert.Equal("world", reply);
                }
            }
            finally
            {
                if (resolvedPath != null)
                    File.Delete(resolvedPath);
            }
        }

        [Fact]
        public void SocketFileCreatedAndCleanedUp()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var rep = new ResponseSocket())
                {
                    rep.Bind(address);

                    // binding a real AF_UNIX socket creates a file at the path on all OSes
                    Assert.True(File.Exists(path), $"Expected socket file to exist at {path} after bind");
                }

                NetMQConfig.Cleanup();

                // closing the listener deletes the socket file (best-effort) — poll up to 2s
                var stopwatch = Stopwatch.StartNew();
                while (File.Exists(path) && stopwatch.Elapsed < TimeSpan.FromSeconds(2))
                    Thread.Sleep(50);

                Assert.False(File.Exists(path), $"Expected socket file at {path} to be deleted after socket close");
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void StaleSocketFileIsReplaced()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                // fake a stale socket file left behind by a crashed process
                File.WriteAllBytes(path, new byte[0]);

                using (var rep = new ResponseSocket())
                using (var req = new RequestSocket())
                {
                    // bind must unlink the stale file and succeed
                    rep.Bind(address);
                    req.Connect(address);

                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), "hello"));
                    Assert.True(rep.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? request));
                    Assert.Equal("hello", request);

                    Assert.True(rep.TrySendFrame(TimeSpan.FromSeconds(5), "world"));
                    Assert.True(req.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? reply));
                    Assert.Equal("world", reply);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void DoubleBindThrowsAddressAlreadyInUse()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var repA = new ResponseSocket())
                {
                    repA.Bind(address);

                    using (var repB = new ResponseSocket())
                    {
                        // binding a second socket to a path that is already backed by a
                        // live listener must fail loudly, not silently unlink repA's
                        // socket file and hijack the path.
                        Assert.Throws<AddressAlreadyInUseException>(() => repB.Bind(address));
                    }

                    // repA must still be alive and working after the failed double-bind
                    using (var req = new RequestSocket())
                    {
                        req.Connect(address);

                        Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), "hello"));
                        Assert.True(repA.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? request));
                        Assert.Equal("hello", request);
                    }
                }
            }
            finally
            {
                DeleteIfExists(path);
                DeleteIfExists(path + ".lock");
            }
        }

        [Fact]
        public void UnbindThenRebindOnSamePathDoesNotHijackNewListener()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var repA = new ResponseSocket())
                {
                    repA.Bind(address);

                    // Unbind only posts an asynchronous Term command to repA's I/O
                    // thread - repA's deferred Close() (which unlinks the socket file)
                    // may not have run by the time this call returns.
                    repA.Unbind(address);
                }

                using (var repB = new ResponseSocket())
                using (var req = new RequestSocket())
                {
                    // Rebind as soon as possible, retrying while repA's deferred
                    // teardown is still holding the path's lock file.
                    var bindStopwatch = Stopwatch.StartNew();
                    while (true)
                    {
                        try
                        {
                            repB.Bind(address);
                            break;
                        }
                        catch (AddressAlreadyInUseException) when (bindStopwatch.Elapsed < TimeSpan.FromSeconds(5))
                        {
                            Thread.Sleep(20);
                        }
                    }

                    // Give repA's deferred Close() plenty of time to run. It must not
                    // delete repB's now-live socket file out from under it.
                    Thread.Sleep(500);
                    Assert.True(File.Exists(path), "Expected repB's socket file to survive repA's deferred cleanup");

                    req.Connect(address);

                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(5), "hello"));
                    Assert.True(repB.TryReceiveFrameString(TimeSpan.FromSeconds(5), out string? request));
                    Assert.Equal("hello", request);
                }
            }
            finally
            {
                DeleteIfExists(path);
                DeleteIfExists(path + ".lock");
            }
        }

        [Fact]
        public void ConnectBeforeBind()
        {
            var path = CreateTempIpcPath();
            var address = "ipc://" + path;

            try
            {
                using (var req = new RequestSocket())
                using (var rep = new ResponseSocket())
                {
                    // connect first — the message must be delivered once the
                    // listener appears (exercises the reconnect timer path)
                    req.Connect(address);
                    Assert.True(req.TrySendFrame(TimeSpan.FromSeconds(10), "hello"));

                    Thread.Sleep(100);

                    rep.Bind(address);

                    Assert.True(rep.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? request));
                    Assert.Equal("hello", request);

                    Assert.True(rep.TrySendFrame(TimeSpan.FromSeconds(10), "world"));
                    Assert.True(req.TryReceiveFrameString(TimeSpan.FromSeconds(10), out string? reply));
                    Assert.Equal("world", reply);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
