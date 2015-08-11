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

"""Tests for Heartbeat event listener."""

import json
import logging
import time
import xmlrpclib

from StringIO import StringIO
from unittest import TestCase

from mocker import Mocker, expect
from supervisor import states, childutils

from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.supervisor.heartbeat_listener import HeartbeatListener

RUNNING = states.ProcessStates.RUNNING


class HeartbeatListenerTestCase(TestCase):
    """Tests for HeartbeatListener class."""

    def setUp(self):
        super(HeartbeatListenerTestCase, self).setUp()
        self.stdin = StringIO()
        self.stdout = StringIO()
        self.stderr = StringIO()
        self.mocker = Mocker()
        self.rpc = self.mocker.mock()
        self.listener = HeartbeatListener(1, 10, ['foo'], [], self.rpc,
                                          stdin=self.stdin, stdout=self.stdout,
                                          stderr=self.stderr)
        self.next_fail = {}
        self.handler = MementoHandler()
        self.listener.logger.addHandler(self.handler)
        self.listener.logger.setLevel(logging.DEBUG)
        self.handler.setLevel(logging.DEBUG)
        self.listener.logger.propagate = False
        self.processes = [dict(name="heartbeat", group="heartbeat", pid="101",
                               state=RUNNING)]
        self.handler.debug = True

    def tearDown(self):
        self.listener.logger.removeHandler(self.handler)
        self.handler.close()
        self.next_fail = None
        self.handler = None
        self.listener = None
        super(HeartbeatListenerTestCase, self).tearDown()

    def fail_next_stop(self, pname):
        """Make next stopProcess to fail."""
        expect(self.rpc.supervisor.stopProcess(pname)).throw(
            xmlrpclib.Fault(42, "Failed to stop the process."))

    def fail_next_start(self, pname):
        """Make next startProcess to fail."""
        expect(self.rpc.supervisor.startProcess(pname)).throw(
            xmlrpclib.Fault(42, "Failed to start the process."))

    def test_restart(self):
        """Test the restart method."""
        expect(self.rpc.supervisor.stopProcess("foo"))
        expect(self.rpc.supervisor.startProcess("foo"))
        with self.mocker:
            self.listener.restart("foo", "testing")
        self.assertTrue(self.handler.check_info("Restarting foo (last "
                                                "hearbeat: testing)"))

    def test_restart_fail_stop(self):
        """Test the restart method failing to stop the process."""
        self.fail_next_stop("foo")
        last = time.time()
        with self.mocker:
            try:
                self.listener.restart("foo", last)
            except xmlrpclib.Fault:
                msg = ("Failed to stop process %s (last heartbeat: %s), "
                       "exiting: %s") % \
                    ("foo", last, "<Fault 42: 'Failed to stop the process.'>")
                self.assertTrue(self.handler.check_error(msg))
            else:
                self.fail("Should get an xmlrpclib.Fault")

    def test_restart_fail_start(self):
        """Test the restart method failing to start the process."""
        expect(self.rpc.supervisor.stopProcess("foo"))
        self.fail_next_start("foo")
        last = time.time()
        with self.mocker:
            try:
                self.listener.restart("foo", last)
            except xmlrpclib.Fault:
                msg = (
                    'Failed to start process %s after stopping it, exiting: %s'
                ) % ("foo", "<Fault 42: 'Failed to start the process.'>")
                self.assertTrue(self.handler.check_error(msg))
            else:
                self.fail("Should get an xmlrpclib.Fault")

    def test_check_processes(self):
        """Test the check_processes method."""
        # add the fake process to the process list
        self.processes.append(dict(name="foo", group="foo", pid="42",
                                   state=RUNNING))
        self.processes.append(dict(name="bar", group="bar", pid="43",
                                   state=RUNNING))
        self.listener.processes = ['bar']
        # 2 process to restart
        self.listener.data['foo'] = {
            'time': time.time() - (self.listener.timeout + 2)}
        self.listener.data['bar'] = {
            'time': time.time() - (self.listener.timeout + 3)}
        self.listener.data['p-1'] = {
            'time': time.time() - (self.listener.timeout - 1)}
        expect(self.rpc.supervisor.getAllProcessInfo()).result(self.processes)
        expect(self.rpc.supervisor.stopProcess("foo:"))
        expect(self.rpc.supervisor.startProcess("foo:"))
        expect(self.rpc.supervisor.stopProcess("bar:bar"))
        expect(self.rpc.supervisor.startProcess("bar:bar"))
        with self.mocker:
            self.listener.check_processes()

    def test_check_processes_no_data(self):
        """Test the check_processes method with no data of a process."""
        # add the fake process to the process list
        self.processes.append(dict(name="foo", group="foo", pid="42",
                                   state=RUNNING))
        self.processes.append(dict(name="bar", group="bar", pid="43",
                                   state=RUNNING))
        self.listener.processes = ['bar']
        expect(self.rpc.supervisor.getAllProcessInfo()).result(self.processes)
        expect(self.rpc.supervisor.stopProcess("foo:"))
        expect(self.rpc.supervisor.startProcess("foo:"))
        expect(self.rpc.supervisor.stopProcess("bar:bar"))
        expect(self.rpc.supervisor.startProcess("bar:bar"))
        with self.mocker:
            # one process to restart
            self.listener.check_processes()
        self.assertTrue(self.handler.check_warning(
            "Restarting process foo:foo (42), as we never received a hearbeat"
            " event from it"))
        self.assertTrue(self.handler.check_warning(
            "Restarting process bar:bar (43), as we never received a hearbeat"
            " event from it"))

    def test_check_processes_untracked(self):
        """Test the check_processes method with a untracked proccess."""
        # add the fake process to the process list
        self.processes.append(dict(name="foo-untracked", group="untracked",
                                   pid="43", state=RUNNING))
        # add a new tracked process from an untracked group
        self.processes.append(dict(name="bar-untracked", group="bar", pid="44",
                                   state=RUNNING))
        self.listener.processes = ['bar']
        expect(self.rpc.supervisor.getAllProcessInfo()).result(self.processes)
        with self.mocker:
            self.listener.check_processes()
        self.assertTrue(self.handler.check_info(
            "Ignoring untracked:foo-untracked (43) as isn't tracked."))
        self.assertTrue(self.handler.check_info(
            "Ignoring bar:bar-untracked (44) as isn't tracked."))

    def test_check_processes_not_running(self):
        """Test the check_processes method if the proccess isn't running."""
        # add the fake process to the process list
        self.processes.append(dict(name="foo", group="foo", pid="42",
                                   state=states.ProcessStates.STARTING))
        # add a new tracked process from an untracked group
        self.processes.append(dict(name="bar", group="bar", pid="43",
                                   state=states.ProcessStates.STARTING))
        self.listener.processes = ['bar']
        # 2 processes to restart
        self.listener.data['foo'] = {
            'time': time.time() - (self.listener.timeout + 2)}
        self.listener.data['bar'] = {
            'time': time.time() - (self.listener.timeout + 2)}
        expect(self.rpc.supervisor.getAllProcessInfo()).result(self.processes)
        with self.mocker:
            self.listener.check_processes()
        self.assertTrue(self.handler.check_info(
            "Ignoring foo:foo (42) as isn't running."))
        self.assertTrue(self.handler.check_info(
            "Ignoring bar:bar (43) as isn't running."))

    def test_handle_heartbeat(self):
        """Test handle_heartbeat method."""
        payload = {"time": time.time()}
        self.listener.handle_heartbeat('process_name', 'group_name',
                                       '42', payload)
        info = {"pid": "42", "time": payload["time"],
                "received": self.listener.data["process_name"]["received"]}
        self.assertEqual({"process_name": info}, self.listener.data)

    def test_handle_event(self):
        """Test handle_event method."""
        # patch handle_heartbeat
        called = []

        def handle_heartbeat(process_name, group_name, pid, payload):
            """Fake handle_heartbeat."""
            called.append((process_name, group_name, pid, payload))

        self.listener.handle_heartbeat = handle_heartbeat
        payload_dict = {u"time": time.time(), "type": "heartbeat"}
        raw_data = ("processname:ticker groupname:ticker pid:42\n" +
                    json.dumps(payload_dict))
        raw_header = ("ver:3.0 server:supervisor serial:1 pool:listener "
                      "poolserial:10 eventname:PROCESS_COMMUNICATION_STDOUT"
                      " len:%s\n" % len(raw_data))
        self.stdin.write(raw_header + raw_data)
        self.stdin.seek(0)
        headers = childutils.get_headers(raw_header)
        self.listener._handle_event()
        # check
        self.assertEqual(1, len(called))
        del payload_dict['type']
        self.assertEqual(('ticker', 'ticker', '42', payload_dict), called[0])
        self.assertTrue(self.handler.check_debug(
            "Event '%s' received: %r" % (headers['eventname'], raw_data)))
        # check the stdout info
        self.assertEqual(["READY", "RESULT 2", "OK"],
                         self.stdout.getvalue().split("\n"))

    def test_invalid_event_type(self):
        """Test with an invalid type."""
        payload_dict = {u"time": time.time(), "type": "ping"}
        raw_data = 'processname:ticker groupname:ticker pid:42\n' + \
            json.dumps(payload_dict)
        raw_header = ("ver:3.0 server:supervisor serial:1 pool:listener "
                      "poolserial:10 eventname:PROCESS_COMMUNICATION_STDOUT"
                      " len:%s\n" % len(raw_data))
        self.stdin.write(raw_header + raw_data)
        self.stdin.seek(0)
        self.listener._handle_event()
        # check
        self.assertTrue(self.handler.check_error(
            "Unable to handle event type '%s' - %r" % ('ping', raw_data)))

    def test_invalid_payload(self):
        """Test with an invalid payload."""
        payload_dict = {u"time": time.time(), "type": "ping"}
        raw_data = 'processname:ticker groupname:ticker pid:42\n' + \
            json.dumps(payload_dict) + "<!foo>"
        raw_header = ("ver:3.0 server:supervisor serial:1 pool:listener "
                      "poolserial:10 eventname:PROCESS_COMMUNICATION_STDOUT"
                      " len:%s\n" % len(raw_data))
        self.stdin.write(raw_header + raw_data)
        self.stdin.seek(0)
        self.listener._handle_event()
        # check
        self.assertTrue(self.handler.check_error(
            "Unable to handle event type '%s' - %r" % ('None', raw_data)))

    def test_unhandled_event(self):
        """A unhandled event type."""
        payload_dict = {u"time": time.time(), "type": "ping"}
        raw_data = 'processname:ticker groupname:ticker pid:42\n' + \
            json.dumps(payload_dict)
        raw_header = "ver:3.0 server:supervisor serial:1 pool:heartbeat " + \
            "poolserial:1 eventname:UNKNOWN len:%s\n" % len(raw_data)
        self.stdin.write(raw_header + raw_data)
        self.stdin.seek(0)
        self.listener._handle_event()
        # check
        self.assertTrue(self.handler.check_warning(
            "Received unsupported event: %s - %r" % ('UNKNOWN', raw_data)))

    def test_check_interval(self):
        """Check that we properly check on the specified interval."""
        header = "ver:3.0 server:supervisor serial:1 pool:heartbeat " + \
                 "poolserial:1 eventname:TICK_5 len:0\n"
        expect(self.rpc.supervisor.getAllProcessInfo()).result([])
        self.stdin.write(header)
        self.stdin.seek(0)
        self.listener._handle_event()
        self.assertEqual(self.listener.tick_count, 1)
        self.stdin.seek(0)
        with self.mocker:
            self.listener._handle_event()
