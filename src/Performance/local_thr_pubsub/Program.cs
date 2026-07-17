using System;
using System.Diagnostics;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;

namespace local_thr_pubsub
{
    internal static class Program
    {
        private const int WatchdogTimeoutSeconds = 120;

        private static int Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("usage: local_thr_pubsub <bind-to> <message-size> <message-count>");
                return 1;
            }

            string bindTo = args[0];
            int messageSize = int.Parse(args[1]);
            int messageCount = int.Parse(args[2]);

            int received = 0;
            var watchdogStopwatch = Stopwatch.StartNew();
            var timedOut = false;

            using (var timeoutTimer = new Timer(_ =>
            {
                if (watchdogStopwatch.Elapsed.TotalSeconds >= WatchdogTimeoutSeconds)
                {
                    timedOut = true;
                    Console.WriteLine("TIMEOUT received=" + Volatile.Read(ref received));
                    Environment.Exit(2);
                }
            }, null, 1000, 1000))
            {
                using (var subSocket = new SubscriberSocket())
                {
                    subSocket.Options.ReceiveHighWatermark = 0;
                    subSocket.SubscribeToAnyTopic();
                    subSocket.Bind(bindTo);

                    var msg = new Msg();
                    msg.InitEmpty();

                    subSocket.Receive(ref msg);
                    Interlocked.Increment(ref received);

                    var stopWatch = Stopwatch.StartNew();
                    for (int i = 0; i != messageCount - 1; i++)
                    {
                        subSocket.Receive(ref msg);
                        Interlocked.Increment(ref received);
                        if (msg.Size != messageSize)
                        {
                            Console.WriteLine("message of incorrect size received. Received: " + msg.Size + " Expected: " + messageSize);
                            return -1;
                        }
                    }
                    stopWatch.Stop();

                    if (timedOut)
                        return 2;

                    var millisecondsElapsed = stopWatch.ElapsedMilliseconds;
                    if (millisecondsElapsed == 0)
                        millisecondsElapsed = 1;

                    msg.Close();

                    double messagesPerSecond = (double)messageCount / millisecondsElapsed * 1000;
                    double megabits = messagesPerSecond * messageSize * 8 / 1000000;

                    Console.WriteLine("message size: {0} [B]", messageSize);
                    Console.WriteLine("message count: {0}", messageCount);
                    Console.WriteLine("mean throughput: {0:0.000} [msg/s]", messagesPerSecond);
                    Console.WriteLine("mean throughput: {0:0.000} [Mb/s]", megabits);
                }
            }

            return 0;
        }
    }
}
