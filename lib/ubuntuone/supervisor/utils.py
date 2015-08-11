# Copyright 2008-2015 Canonical
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# For further info, check  http://launchpad.net/filesync-server

"""Utilities to work with supervisor."""

import json
import sys
import time

from supervisor.events import ProcessCommunicationEvent
from twisted.internet import reactor, task, protocol
from twisted.protocols import basic


def send_heartbeat(out=sys.stdout, time=time.time, flush=True):
    """Write the heartbeat event to stdout."""
    out.write(ProcessCommunicationEvent.BEGIN_TOKEN)
    timestamp = time()
    out.write(json.dumps({"type": "heartbeat", "time": timestamp}))
    out.write(ProcessCommunicationEvent.END_TOKEN)
    if flush:
        out.flush()


def heartbeat_generator(interval, out=sys.stdout, time=time.time):
    """A generator that sends a heartbeat to supervisor on each loop."""
    start_time = 0
    while True:
        if interval and time() - start_time >= interval:
            send_heartbeat(out=out)
            start_time = time()
        yield


class HeartbeatWriter(basic.LineOnlyReceiver):
    """A twisted friendly heartbeat writer."""

    delimiter = '\n'

    def __init__(self, interval, logger, reactor=reactor):
        # there is no __init__ to call in basic.LineOnlyReceiver
        self.logger = logger
        self.reactor = reactor
        self.interval = interval
        self.logger.info("Initialized HeartbeatWriter with: interval=%s",
                         self.interval)
        self.loop = task.LoopingCall(self.send)
        self.loop.clock = self.reactor

    def send(self):
        """Send the heartbeat."""
        if not self.transport:
            self.logger.warning("Can't send heartbeat without a transport.")
        else:
            send_heartbeat(self.transport, time=self.reactor.seconds,
                           flush=False)

    def connectionMade(self):
        """We are connected, just send a heartbeat."""
        basic.LineOnlyReceiver.connectionMade(self)
        self.loop.start(self.interval)

    def connectionLost(self, reason=protocol.connectionDone):
        """Cleanup as we are disconnected."""
        basic.LineOnlyReceiver.connectionLost(self, reason)
        self.logger.info("HeartbeatWriter connectionLost: %s", reason)
        self.loop.stop()
        self.reactor = self.logger = self.loop = None

    def lineReceived(self, line):
        """Do nothing, we don't care about stdin."""
        pass
