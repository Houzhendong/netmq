/*
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2009 iMatix Corporation
    Copyright (c) 2007-2015 Other contributors as noted in the AUTHORS file

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

#if NET

using System.Net.Sockets;

namespace NetMQ.Core.Transports
{
    /// <summary>
    /// Single indirection point the async listeners/connectors go through to hand a freshly
    /// accepted/connected <see cref="Socket"/> off to the async engine.
    /// </summary>
    /// <remarks>
    /// Handles off to <c>AsyncStreamEngine</c>, which implements <see cref="IEngine"/> against
    /// the frozen constructor contract
    /// <c>AsyncStreamEngine(System.Net.Sockets.Socket socket, Options options, string endpoint)</c>.
    /// Called from <c>Core/Transports/Tcp/AsyncTcpListener.cs</c>, <c>AsyncTcpConnector.cs</c>,
    /// <c>Core/Transports/Ipc/AsyncIpcListener.cs</c> and <c>AsyncIpcConnector.cs</c>.
    /// </remarks>
    internal static class AsyncEngineFactory
    {
        public static IEngine Create(Socket socket, Options options, string endpoint)
        {
            return new AsyncStreamEngine(socket, options, endpoint);
        }
    }
}

#endif
