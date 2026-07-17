/*
    Copyright (c) 2011 250bpm s.r.o.
    Copyright (c) 2011-2015 Other contributors as noted in the AUTHORS file

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

using System;
using System.Diagnostics;
using System.Net.Sockets;
using AsyncIO;

namespace NetMQ.Core.Transports.Ipc
{
    /// <summary>
    /// An IpcListener listens on a Unix Domain Socket for incoming ipc:// connections.
    /// </summary>
    internal sealed class IpcListener : Own, IProactorEvents
    {
        private readonly IOObject m_ioObject;

        /// <summary>
        /// Address to listen on.
        /// </summary>
        private readonly IpcAddress m_address;

        /// <summary>
        /// Underlying socket.
        /// </summary>
        private AsyncSocket? m_handle;

        /// <summary>
        /// Socket the listener belongs to.
        /// </summary>
        private readonly SocketBase m_socket;

        /// <summary>
        /// String representation of endpoint to bind to
        /// </summary>
        private string? m_endpoint;

        /// <summary>
        /// An exclusive advisory lock on a sibling "&lt;path&gt;.lock" file, held for the
        /// lifetime of a successful bind. Its purpose is to let a later SetAddress call
        /// (this or another process) distinguish a path that is genuinely still backing
        /// a live listener from a stale leftover, and to let Close() know it is still
        /// safe to unlink the socket file - see SetAddress and Close for details.
        /// </summary>
        private System.IO.FileStream? m_lockStream;

        /// <summary>
        /// Create a new IpcListener on the given IOThread and socket.
        /// </summary>
        /// <param name="ioThread">the IOThread for this to live within</param>
        /// <param name="socket">a SocketBase to listen on</param>
        /// <param name="options">socket-related Options</param>
        public IpcListener(IIOThread ioThread, SocketBase socket, Options options)
            : base(ioThread, options)
        {
            m_ioObject = new IOObject(ioThread);
            m_address = new IpcAddress();
            m_handle = null;
            m_socket = socket;
        }

        /// <summary>
        /// Release any contained resources (here - does nothing).
        /// </summary>
        public override void Destroy()
        {
            Debug.Assert(m_handle == null);
        }

        protected override void ProcessPlug()
        {
            Assumes.NotNull(m_handle);

            m_ioObject.SetHandler(this);
            m_ioObject.AddSocket(m_handle);

            Accept();
        }

        /// <summary>
        /// Process a termination request.
        /// </summary>
        /// <param name="linger">a time (in milliseconds) for this to linger before actually going away. -1 means infinite.</param>
        protected override void ProcessTerm(int linger)
        {
            Assumes.NotNull(m_handle);

            m_ioObject.SetHandler(this);
            m_ioObject.RemoveSocket(m_handle);
            Close();
            base.ProcessTerm(linger);
        }

        /// <summary>
        /// Set address to listen on.
        /// </summary>
        /// <param name="addr">a string denoting the address to set this to</param>
        public void SetAddress(string addr)
        {
            m_address.Resolve(addr, false);

            Assumes.NotNull(m_address.Endpoint);
            Assumes.NotNull(m_address.Path);

            // Acquire an exclusive advisory lock on a sibling lock-file before touching
            // anything at the bind path. If another listener (this or another process)
            // is already holding it, the path is genuinely in use by a live listener -
            // fail loudly instead of silently unlinking and hijacking it. The lock is
            // released either in Close() (below), or here if bind subsequently fails.
            string lockPath = m_address.Path + ".lock";

            try
            {
                m_lockStream = new System.IO.FileStream(lockPath, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None);
            }
            // A live holder of the lock surfaces as IOException (sharing violation) on
            // most platforms/runtimes, but as UnauthorizedAccessException on .NET
            // Framework when the file is caught mid-delete by another listener's
            // concurrent teardown - treat both as "already in use".
            catch (Exception ex) when (ex is System.IO.IOException || ex is UnauthorizedAccessException)
            {
                throw new AddressAlreadyInUseException($"Cannot bind address ( {m_address} ) - already in use.");
            }

            // Delete a stale socket file left over from a previous run, if any
            // (libzmq unlinks the file unconditionally before binding). Safe now that
            // we hold the lock above: nothing still using this path could have raced
            // in ahead of us, so any file here is a stale remnant of a previous run.
            try
            {
                System.IO.File.Delete(m_address.Path);
            }
            catch
            {
            }

            try
            {
                m_handle = AsyncSocket.Create(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                Assumes.NotNull(m_handle);

                m_handle.Bind(m_address.Endpoint);
                m_handle.Listen(m_options.Backlog);

                m_endpoint = m_address.ToString();

                m_socket.EventListening(m_endpoint, m_handle);
            }
            catch (SocketException ex)
            {
                Close();
                throw NetMQException.Create(ex);
            }
        }

        private void Accept()
        {
            Assumes.NotNull(m_handle);

            // start accepting socket async
            m_handle.Accept();
        }

        /// <summary>
        /// This is called when socket input has been completed.
        /// </summary>
        /// <param name="socketError">This indicates the status of the input operation - whether Success or some error.</param>
        /// <param name="bytesTransferred">the number of bytes that were transferred</param>
        /// <exception cref="NetMQException">A non-recoverable socket-error occurred.</exception>
        public void InCompleted(SocketError socketError, int bytesTransferred)
        {
            Assumes.NotNull(m_handle);
            Assumes.NotNull(m_endpoint);

            switch (socketError)
            {
                case SocketError.Success:
                {
                    var acceptedSocket = m_handle.GetAcceptedSocket();

                    // Create the engine object for this connection.
                    var engine = new StreamEngine(acceptedSocket, m_options, m_endpoint);

                    // Choose I/O thread to run connector in. Given that we are already
                    // running in an I/O thread, there must be at least one available.
                    IIOThread? ioThread = ChooseIOThread(m_options.Affinity);

                    Assumes.NotNull(ioThread);

                    // Create and launch a session object.
                    SessionBase session = SessionBase.Create(ioThread, false, m_socket, m_options, new Address(NetMQ.Core.Address.IpcProtocol, m_address.Path ?? string.Empty));
                    session.IncSeqnum();
                    LaunchChild(session);

                    SendAttach(session, engine, false);

                    m_socket.EventAccepted(m_endpoint, acceptedSocket);

                    Accept();
                    break;
                }
                case SocketError.ConnectionReset:
                case SocketError.NoBufferSpaceAvailable:
                case SocketError.TooManyOpenSockets:
                {
                    m_socket.EventAcceptFailed(m_endpoint, socketError.ToErrorCode());

                    Accept();
                    break;
                }
                default:
                {
                    NetMQException exception = NetMQException.Create(socketError);

                    m_socket.EventAcceptFailed(m_endpoint, exception.ErrorCode);
                    throw exception;
                }
            }
        }

        /// <summary>
        /// Close the listening socket and remove the socket file (the listener owns the file).
        /// </summary>
        private void Close()
        {
            if (m_handle != null)
            {
                try
                {
                    m_handle.Dispose();
                    if (m_endpoint is not null)
                        m_socket.EventClosed(m_endpoint, m_handle);
                }
                catch (SocketException ex)
                {
                    if (m_endpoint is not null)
                        m_socket.EventCloseFailed(m_endpoint, ex.SocketErrorCode.ToErrorCode());
                }

                m_handle = null;
            }

            // Best-effort removal of the socket file we created when binding, done while
            // we still hold the lock file acquired in SetAddress: a would-be rebinder
            // must acquire that same lock before it may touch this path, so nothing can
            // have replaced this file out from under us yet - it is safe to unlink.
            if (m_address.Path is not null)
            {
                try
                {
                    System.IO.File.Delete(m_address.Path);
                }
                catch
                {
                }
            }

            // Now release the lock, allowing a waiting rebind to proceed, and clean up
            // the lock file itself (best-effort; a benign race here just means the next
            // bind recreates it).
            if (m_lockStream is not null)
            {
                m_lockStream.Dispose();
                m_lockStream = null;

                if (m_address.Path is not null)
                {
                    try
                    {
                        System.IO.File.Delete(m_address.Path + ".lock");
                    }
                    catch
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Get the bound address for use with wildcards
        /// </summary>
        public string Address => m_address.ToString();

        /// <summary>
        /// The ipc transport has no port; this is always zero.
        /// </summary>
        public int Port => 0;

        /// <summary>
        /// This method would be called when a message Send operation has been completed - except in this case it simply throws a NotImplementedException.
        /// </summary>
        /// <param name="socketError">a SocketError value that indicates whether Success or an error occurred</param>
        /// <param name="bytesTransferred">the number of bytes that were transferred</param>
        /// <exception cref="NotImplementedException">OutCompleted is not implemented on IpcListener.</exception>
        void IProactorEvents.OutCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// This would be called when a timer expires, although here it only throws a NotSupportedException.
        /// </summary>
        /// <param name="id">an integer used to identify the timer (not used here)</param>
        /// <exception cref="NotSupportedException">TimerEvent is not supported on IpcListener.</exception>
        public void TimerEvent(int id)
        {
            throw new NotSupportedException();
        }
    }
}
