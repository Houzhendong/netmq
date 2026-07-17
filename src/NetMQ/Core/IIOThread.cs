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

namespace NetMQ.Core
{
    /// <summary>
    /// Abstraction over the services that an "I/O thread" provides to the objects that
    /// live on it: a mailbox for inter-thread commands, a load figure used for
    /// load-balancing in <see cref="Ctx.ChooseIOThread"/>, and a timer service.
    /// </summary>
    /// <remarks>
    /// This is implemented by the classic, Proactor-based <see cref="IOThread"/> and (on
    /// modern .NET, behind the async-engine opt-in flag) by <c>AsyncIOThread</c>. Consumers
    /// such as <see cref="Own"/>, <see cref="SessionBase"/>, <see cref="IOObject"/>, and the
    /// transport listeners/connectors/engines depend on this interface rather than the
    /// concrete <see cref="IOThread"/> type, so that the same code works unmodified on
    /// either I/O-thread implementation.
    /// </remarks>
    internal interface IIOThread
    {
        /// <summary>
        /// The mailbox this I/O thread receives inter-thread commands through.
        /// </summary>
        IMailbox Mailbox { get; }

        /// <summary>
        /// The current load on this I/O thread (roughly, the number of sockets/objects
        /// it is servicing). Used by <see cref="Ctx.ChooseIOThread"/> to pick the least
        /// busy thread.
        /// </summary>
        int Load { get; }

        /// <summary>
        /// The thread-id assigned to this I/O thread within its <see cref="Ctx"/>.
        /// </summary>
        int Tid { get; }

        /// <summary>
        /// Start the I/O thread running.
        /// </summary>
        void Start();

        /// <summary>
        /// Ask the I/O thread to stop. This is asynchronous - the thread will process
        /// the stop request in turn.
        /// </summary>
        void Stop();

        /// <summary>
        /// Synchronously wait for the I/O thread to finish and release its resources.
        /// </summary>
        void Destroy();

        /// <summary>
        /// Schedule a timer that will call <paramref name="sink"/>'s
        /// <see cref="ITimerEvent.TimerEvent"/> after <paramref name="timeout"/>
        /// milliseconds, identified by <paramref name="id"/>.
        /// </summary>
        void AddTimer(long timeout, IProactorEvents sink, int id);

        /// <summary>
        /// Cancel a timer previously scheduled via <see cref="AddTimer"/> for the given
        /// sink/id pair.
        /// </summary>
        void CancelTimer(IProactorEvents sink, int id);
    }
}
