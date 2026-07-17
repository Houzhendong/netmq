using System;
using AsyncIO;

namespace NetMQ.Monitoring
{
    /// <summary>
    /// Base class for all event arguments raised by <see cref="NetMQMonitor"/>.
    /// </summary>
    public abstract class NetMQMonitorEventArgs : EventArgs
    {
        /// <summary>
        /// Create a new NetMQMonitorEventArgs that contains the given monitor and address.
        /// </summary>
        /// <param name="monitor">The <see cref="NetMQMonitor"/> that raised this event.</param>
        /// <param name="address">The address of the event.</param>
        /// <param name="socketEvent">The type of socket event that occurred.</param>
        protected NetMQMonitorEventArgs(NetMQMonitor monitor, string address, SocketEvents socketEvent)
        {
            Monitor = monitor;
            Address = address;
            SocketEvent = socketEvent;
        }

        /// <summary>
        /// Gets the <see cref="NetMQMonitor"/> that raised this event.
        /// </summary>
        public NetMQMonitor Monitor { get; }

        /// <summary>
        /// Gets the address of the event.
        /// </summary>
        public string Address { get; }

        /// <summary>
        /// Gets the type of socket event that occurred.
        /// </summary>
        public SocketEvents SocketEvent { get; }
    }

    /// <summary>
    /// A subclass of <see cref="NetMQMonitorEventArgs"/> that also holds a socket.
    /// </summary>
    public class NetMQMonitorSocketEventArgs : NetMQMonitorEventArgs
    {
        /// <summary>
        /// Create a new NetMQMonitorSocketEventArgs that contains the given monitor, address, and socket.
        /// </summary>
        /// <param name="monitor">The <see cref="NetMQMonitor"/> that raised this event.</param>
        /// <param name="address">The address of the event.</param>
        /// <param name="socketEvent">The type of socket event that occurred.</param>
        /// <param name="socket">The socket upon which this event occurred.</param>
        public NetMQMonitorSocketEventArgs(NetMQMonitor monitor, string address, AsyncSocket? socket, SocketEvents socketEvent)
            : base(monitor, address, socketEvent)
        {
            Socket = socket;
        }

        /// <summary>
        /// Create a new NetMQMonitorSocketEventArgs for an event raised by the async engine, which
        /// hands over a BCL <see cref="System.Net.Sockets.Socket"/> rather than an
        /// <see cref="AsyncSocket"/>.
        /// </summary>
        /// <param name="monitor">The <see cref="NetMQMonitor"/> that raised this event.</param>
        /// <param name="address">The address of the event.</param>
        /// <param name="socketEvent">The type of socket event that occurred.</param>
        /// <param name="netSocket">The BCL socket upon which this event occurred.</param>
        public NetMQMonitorSocketEventArgs(NetMQMonitor monitor, string address, System.Net.Sockets.Socket? netSocket, SocketEvents socketEvent)
            : base(monitor, address, socketEvent)
        {
            NetSocket = netSocket;
        }

        /// <summary>
        /// Gets the socket upon which this event occurred.
        /// </summary>
        public AsyncSocket? Socket { get; }

        /// <summary>
        /// Gets the BCL socket upon which this event occurred, when this event was raised by the
        /// async engine (<c>NETMQ_ASYNC_ENGINE=1</c> / <see cref="NetMQConfig.UseAsyncEngine"/>).
        /// <c>null</c> for events raised by the classic engine, which populates
        /// <see cref="Socket"/> instead.
        /// </summary>
        public System.Net.Sockets.Socket? NetSocket { get; }
    }

    /// <summary>
    /// A subclass of <see cref="NetMQMonitorEventArgs"/> that also holds an error code.
    /// </summary>
    public class NetMQMonitorErrorEventArgs : NetMQMonitorEventArgs
    {
        /// <summary>
        /// Create a new NetMQMonitorErrorEventArgs that contains the given monitor, address, and error-code.
        /// </summary>
        /// <param name="monitor">The <see cref="NetMQMonitor"/> that raised this event.</param>
        /// <param name="address">The address of the event.</param>
        /// <param name="socketEvent">The type of socket event that occurred.</param>
        /// <param name="errorCode">The error code associated with this event.</param>
        public NetMQMonitorErrorEventArgs(NetMQMonitor monitor, string address, ErrorCode errorCode, SocketEvents socketEvent)
            : base(monitor, address, socketEvent)
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Gets the error code associated with this event.
        /// </summary>
        public ErrorCode ErrorCode { get; }
    }

    /// <summary>
    /// A subclass of <see cref="NetMQMonitorEventArgs"/> that also holds an interval.
    /// </summary>
    public class NetMQMonitorIntervalEventArgs : NetMQMonitorEventArgs
    {
        /// <summary>
        /// Create a new NetMQMonitorIntervalEventArgs containing the given NetMQMonitor, address, and interval.
        /// </summary>
        /// <param name="monitor">the NetMQMonitor</param>
        /// <param name="address">The a string denoting the address</param>
        /// <param name="interval">The interval, in milliseconds.</param>
        /// <param name="socketEvent">The type of socket event that occurred.</param>
        public NetMQMonitorIntervalEventArgs(NetMQMonitor monitor, string address, int interval, SocketEvents socketEvent)
            : base(monitor, address, socketEvent)
        {
            Interval = interval;
        }

        /// <summary>
        /// Gets the interval, in milliseconds.
        /// </summary>
        public int Interval { get; }
    }
}
