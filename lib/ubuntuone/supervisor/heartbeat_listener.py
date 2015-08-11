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

"""Heartbeat event listener for supervisor.

Example config:
 [eventlistener:heartbeat]
 command=python <path/to/>heartbeat_eventlistener.py --interval=30 \
         --timeout=60 --groups=filesync,ssl-proxy,etc
 events=PROCESS_COMMUNICATION
"""

import json
import logging
import os
import sys
import time
import xmlrpclib

from supervisor import childutils, states
from supervisor.events import EventTypes, getEventNameByType

from ubuntuone.storage.server.logger import configure_logger


PROCESS_COMMUNICATION_STDOUT = \
    getEventNameByType(EventTypes.PROCESS_COMMUNICATION_STDOUT)
TICK_5 = getEventNameByType(EventTypes.TICK_5)


class HeartbeatListener(object):
    """A supervisor event Listener for PROCESS_COMMUNICATION events.

    It supports events with a json payload that must have a dict with a 'type'
    item in order to dispatch to the event handler.
    """

    def __init__(self, timeout, interval, groups, processes, rpc, logger=None,
                 timer=time.time, stdin=sys.stdin, stdout=sys.stdout,
                 stderr=sys.stderr):
        # there is no __init__ to call in basic.LineReceiver
        self.logger = logger or logging.getLogger('heartbeat')
        self.timeout = timeout
        self.interval = interval
        self.groups = groups
        self.processes = processes
        self.data = {}
        self.rpc = rpc
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.timer = timer
        self.running = False
        self.tick_count = 0
        self.logger.info("Initialized with: timeout=%s, interval=%s",
                         self.timeout, self.interval)

    def stop(self):
        """Set the running flag to False."""
        self.running = False

    def runforever(self):
        """Main loop."""
        self.running = True
        while True:
            if not self.running:
                break
            self._handle_event()

    def _handle_event(self):
        """Handle an event."""
        headers, payload = childutils.listener.wait(self.stdin, self.stdout)
        try:
            self.logger.debug("Event '%s' received: %r",
                              headers['eventname'], payload)
            if headers['eventname'] == PROCESS_COMMUNICATION_STDOUT:
                payload_raw_headers, payload_data = payload.split("\n", 1)
                payload_headers = childutils.get_headers(payload_raw_headers)
                event_type = None
                try:
                    data = json.loads(payload_data)
                    event_type = data.pop('type')
                    method = getattr(self, 'handle_%s' % (event_type,), None)
                    method(payload_headers['processname'],
                           payload_headers['groupname'],
                           payload_headers['pid'], data)
                except Exception, e:
                    # failed to parse the payload or to dispatch the event
                    self.logger.exception(
                        "Unable to handle event type '%s' - %r",
                        event_type, payload)
            # handle the tick events to fire a check on the processes.
            elif headers['eventname'] == TICK_5:
                self.tick_count += 1
                if self.tick_count * 5 >= self.interval:
                    self.tick_count = 0
                    try:
                        self.check_processes()
                    except Exception, e:
                        print e
                        self.logger.exception(
                            "Oops, failed to check the processes.")
            else:
                self.logger.warning("Received unsupported event: %s - %r",
                                    headers['eventname'], payload)

        finally:
            childutils.listener.ok(self.stdout)

    def handle_heartbeat(self, process_name, group_name, pid, payload):
        """handle a heartbeat event."""
        heartbeat = payload['time']
        self.data[process_name] = {'pid': pid,
                                   'time': heartbeat,
                                   'received': time.time()}

    def check_processes(self):
        """Check heartbeat of configured processes.

        Restart any process that didn't send a heartbeat event.
        """
        self.logger.info("Checking processes.")
        infos = self.rpc.supervisor.getAllProcessInfo()
        now = time.time()
        for info in infos:
            pid = info['pid']
            name = info['name']
            group = info['group']
            pname = '%s:%s' % (group, name)
            if not pid or info['state'] != states.ProcessStates.RUNNING:
                # nothing to check, as there is no pid yet for this process
                self.logger.info("Ignoring %s (%s) as isn't running.",
                                 pname, pid)
                continue
            in_processes_list = name in self.processes
            if group not in self.groups and not in_processes_list:
                self.logger.info("Ignoring %s (%s) as isn't tracked.",
                                 pname, pid)
                continue
            self.logger.info("Checking %s (%s)", pname, pid)
            # define the restart target (group or specific process)
            target = group + ":" if not in_processes_list else pname
            if name in self.data or pname in self.data:
                heartbeat_info = self.data.get(name, self.data.get(pname))
                if now - heartbeat_info['time'] > self.timeout:
                    # no heartbeat for more than 1 minute, restart it!
                    self.restart(target, heartbeat_info['time'])
            else:
                # the process is tracked, but we don't have any info about it
                self.logger.warning("Restarting process %s (%s), as we never"
                                    " received a hearbeat event from it",
                                    pname, pid)
                self.restart(target, "never")

    def restart(self, name, when_client_heartbeat):
        """Restart process 'name'."""
        self.logger.info('Restarting %s (last hearbeat: %s)',
                         name, when_client_heartbeat)
        try:
            self.rpc.supervisor.stopProcess(name)
            self.logger.debug("Process %s stopped.", name)
        except xmlrpclib.Fault, what:
            self.logger.error(
                'Failed to stop process %s (last heartbeat: %s), exiting: %s',
                name, when_client_heartbeat, what)
            raise
        try:
            self.rpc.supervisor.startProcess(name)
            self.logger.debug("Process %s started.", name)
        except xmlrpclib.Fault, what:
            self.logger.error('Failed to start process %s after stopping it, '
                              'exiting: %s', name, what)
            raise


def comma_separated_list_parser(value):
    """A configglue parser for comma separated list."""
    if not value:
        return []
    return value.split(",")


# default config
default_config = """
[__main__]
timeout.default = 60
timeout.parser = int
timeout.help = Restart a process if no heartbeat was received in the timeout
interval.default = 30
interval.parser = int
interval.help = Interval to check for hung processes.
server_url.default = SUPERVISOR_SERVER_URL
server_url.parser = getenv
server_url.help = Supervisor xmlrpc url.
groups.help = Groups to monitor.
groups.parser = comma_separated_list
processes.help = Processes to monitor.
processes.parser = comma_separated_list
log_file.default = tmp/heartbeat.log
log_file.help = The path to the log file.
log_level.default = INFO
log_level.help = The logging level (DEBUG, INFO, WARNING, ERROR)
log_format.default = %(asctime)s %(levelname)-8s %(name)s: %(message)s
log_format.help = The logging format
"""

# main entry point, parse args and start the service.
if __name__ == '__main__':
    from configglue import configglue
    from StringIO import StringIO
    config_file = StringIO()
    config_file.write(default_config)
    config_file.seek(0)

    (parser, options, argv) = configglue(
        config_file,
        args=sys.argv[1:],
        usage="%prog [options]",
        extra_parsers=[("comma_separated_list", comma_separated_list_parser)])

    if not options.groups or not options.interval or not options.timeout:
        parser.print_help()
        sys.exit(2)

    # configure the logger
    try:
        level = logging._levelNames[options.log_level]
    except KeyError:
        # if level doesn't exist fallback to DEBUG
        level = logging.DEBUG

    root_logger = logging.getLogger()
    configure_logger(logger=root_logger, filename=options.log_file,
                     level=level, log_format=options.log_format)

    rpc = childutils.getRPCInterface(os.environ)
    hbl = HeartbeatListener(options.timeout, options.interval, options.groups,
                            options.processes, rpc)
    hbl.runforever()
