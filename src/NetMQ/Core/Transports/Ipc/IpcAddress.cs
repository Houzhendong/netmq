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
using System.Net;

namespace NetMQ.Core.Transports.Ipc
{
    /// <summary>
    /// An IpcAddress resolves an ipc:// endpoint name to a filesystem path
    /// backed by a Unix Domain Socket.
    /// </summary>
    internal class IpcAddress : Address.IZAddress
    {
        /// <summary>
        /// Provide a string representation of this IpcAddress, in the form ipc://path.
        /// </summary>
        /// <returns>a string representation of this IpcAddress</returns>
        public override string ToString()
        {
            if (Path == null)
                return string.Empty;

            return Protocol + "://" + Path;
        }

        /// <summary>
        /// Resolve the given ipc name to a Unix Domain Socket endpoint.
        /// A name of "*" requests a unique, system-assigned temporary path (wildcard bind).
        /// Any other name is used verbatim as the filesystem path.
        /// </summary>
        /// <param name="name">the ipc name (filesystem path, or "*" for a temporary path)</param>
        /// <param name="ip4Only">not applicable to the ipc transport (ignored)</param>
        public void Resolve(string name, bool ip4Only)
        {
            if (name == "*")
            {
                // Wildcard bind: generate a unique temporary socket path,
                // matching libzmq's behaviour of binding to a temporary file.
                name = System.IO.Path.Combine(
                    System.IO.Path.GetTempPath(),
                    "netmq-" + Guid.NewGuid().ToString("N") + ".sock");
            }

            Path = name;
            Endpoint = new AsyncIO.UnixEndPoint(name);
        }

        /// <summary>
        /// Get the resolved Unix Domain Socket endpoint, or null if not yet resolved.
        /// </summary>
        public AsyncIO.UnixEndPoint? Endpoint { get; private set; }

        /// <summary>
        /// Get the resolved filesystem path of the socket, or null if not yet resolved.
        /// </summary>
        public string? Path { get; private set; }

        /// <summary>
        /// The ipc transport does not use an IPEndPoint - this is always null.
        /// </summary>
        public IPEndPoint? Address => null;

        public string Protocol => Core.Address.IpcProtocol;
    }
}
