using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace NetMQ.Core.Transports.Udp
{
    /// <summary>
    /// UDP listener that binds a UDP socket and creates a session with a UdpEngine.
    /// Unlike TCP, UDP is connectionless so there is no accept loop.
    /// </summary>
    internal sealed class UdpListener : Own, IProactorEvents
    {
        private readonly IOObject m_ioObject;
        private readonly UdpAddress m_address;
        private readonly SocketBase m_socket;

        private string? m_endpoint;
        private int m_port;

        public UdpListener(IIOThread ioThread, SocketBase socket, Options options)
            : base(ioThread, options)
        {
            m_ioObject = new IOObject(ioThread);
            m_address = new UdpAddress();
            m_socket = socket;
        }

        public override void Destroy()
        {
        }

        public void SetAddress(string addr)
        {
            m_address.Resolve(addr, m_options.IPv4Only);

            Assumes.NotNull(m_address.Address);

            m_port = m_address.Address.Port;
            m_endpoint = m_address.ToString();
        }

        public int Port => m_port;

        public string? Address => m_endpoint;

        protected override void ProcessPlug()
        {
            m_ioObject.SetHandler(this);

            // Create a session for this UDP endpoint
            IIOThread? ioThread = ChooseIOThread(m_options.Affinity);
            Assumes.NotNull(ioThread);

            var paddr = new Address(Core.Address.UdpProtocol, m_address.Address!.ToString());
            paddr.Resolved = m_address;

            SessionBase session = SessionBase.Create(ioThread, false, m_socket, m_options, paddr);
            session.IncSeqnum();
            LaunchChild(session);

            // Create and attach the UDP engine
            var engine = new UdpEngine(m_options, paddr, true);
            SendAttach(session, engine, false);
        }

        protected override void ProcessTerm(int linger)
        {
            m_ioObject.Unplug();
            base.ProcessTerm(linger);
        }

        public void InCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotSupportedException();
        }

        public void OutCompleted(SocketError socketError, int bytesTransferred)
        {
            throw new NotSupportedException();
        }

        public void TimerEvent(int id)
        {
            throw new NotSupportedException();
        }
    }
}
