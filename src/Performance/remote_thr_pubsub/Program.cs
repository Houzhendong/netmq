using System;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;

namespace remote_thr_pubsub
{
    internal static class Program
    {
        private const int SubscribeWaitTimeoutSeconds = 60;

        private static int Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("usage: remote_thr_pubsub <connect-to> <message-size> <message-count>");
                return 1;
            }

            string connectTo = args[0];
            int messageSize = int.Parse(args[1]);
            int messageCount = int.Parse(args[2]);

            using (var pub = new XPublisherSocket())
            {
                pub.Options.SendHighWatermark = 0;
                pub.Connect(connectTo);

                // Wait for the subscriber's subscription frame (first byte 0x01 = subscribe)
                // to arrive so we know the subscriber is connected and ready before we start
                // firing the throughput burst.
                if (!pub.TryReceiveFrameBytes(TimeSpan.FromSeconds(SubscribeWaitTimeoutSeconds), out byte[]? subscriptionFrame))
                {
                    Console.WriteLine("TIMEOUT waiting for subscription");
                    return 2;
                }

                if (subscriptionFrame == null || subscriptionFrame.Length == 0 || subscriptionFrame[0] != 0x01)
                {
                    Console.WriteLine("unexpected first frame received (expected subscribe frame)");
                    return 3;
                }

                for (int i = 0; i != messageCount; i++)
                {
                    var msg = new Msg();
                    msg.InitPool(messageSize);
                    pub.Send(ref msg, more: false);
                    msg.Close();
                }

                // Give the transport a moment to flush the last messages out before we tear
                // the socket down.
                Thread.Sleep(1000);
            }

            return 0;
        }
    }
}
