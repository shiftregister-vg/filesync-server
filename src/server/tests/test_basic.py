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

"""Basic testing, no auth required."""

import gc
import logging
import os
import shutil
import struct
import sys

from StringIO import StringIO

from mocker import Mocker, ANY
from twisted.internet import reactor, defer
from twisted.internet.error import ConnectionDone
from twisted.web import client, error

import metrics
import metrics.services
from config import config
from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.storageprotocol import request
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.supervisor import utils as supervisor_utils
from backends.filesync.data import model


class TestBasic(TestWithDatabase):
    """Test the basic stuff."""

    def test_server(self):
        """Stop and restart the server."""
        d = self.service.stopService()
        d.addCallback(lambda _: self.service.startService())
        return d

    def test_connect(self):
        """Create a simple client that just connects."""
        def dummy(client):
            client.test_done("ok")
        return self.callback_test(dummy)

    def test_disconnect_with_user_locked(self):
        """Create a simple client that just connects."""
        # lock the user:
        usr = self.user_store.get(model.StorageUser, 0)
        usr.locked = True
        self.user_store.commit()
        # add the log handler
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.INFO)
        logger.addHandler(hdlr)
        # define a connectionLostHandler to know when the client
        # gets disconnected.
        d = defer.Deferred()

        def conn_lost_handler(r):
            """Connection lost!"""
            d.callback(None)

        @defer.inlineCallbacks
        def dummy(client):
            # set the connection lost handler
            client.connectionLostHandler = conn_lost_handler
            # trigger an operation, which should
            client.dummy_authenticate("open sesame")
            yield d
            # check we logged a warning about this.
            self.assertTrue(hdlr.check_warning(
                "Shutting down protocol: user locked"))
        return self.callback_test(dummy, add_default_callbacks=True)

    def test_disconnect_with_user_locked_after_auth(self):
        """Client gets disconnected if the user is locked after auth."""
        # add the log handler
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.INFO)
        logger.addHandler(hdlr)
        # define a connectionLostHandler to know when the client
        # gets disconnected.
        d = defer.Deferred()

        def conn_lost_handler(r):
            """Connection lost!"""
            d.callback(None)

        @defer.inlineCallbacks
        def dummy(client):
            # set the connection lost handler
            client.connectionLostHandler = conn_lost_handler
            # trigger an operation, which should
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()
            # lock the user:
            usr = self.user_store.get(model.StorageUser, 0)
            usr.locked = True
            self.user_store.commit()
            client.make_dir(request.ROOT, root_id, u"open sesame")
            yield d
            # check we logged a warning about this.
            self.assertTrue(hdlr.check_warning(
                "Shutting down protocol: user locked"))
        return self.callback_test(dummy, add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_server_start_stop_metrics(self):
        """Test start/stop metrics."""
        mocker = Mocker()

        service_meter = mocker.mock(name='meter')
        self.service.metrics = service_meter

        revno = mocker.mock(name='revno')
        self.patch(metrics.services, 'revno', revno)

        service_meter.meter('server_stop')
        service_meter.decrement('services_active')
        service_meter.meter('server_start')
        service_meter.increment('services_active')

        service_meter.timing("busy.ping", ANY)
        mocker.count(0, None)

        revno()
        mocker.count(0, None)

        service_meter.connection
        mocker.result(None)

        with mocker:
            yield self.service.stopService()
            yield self.service.startService()

    def test_ping(self):
        """Ping should be done in less than 2ms."""
        def ping(client):
            def done(result):
                self.assert_(result.rtt < 2)
                client.test_done("ok")
            d = client.ping()
            d.addCallbacks(done, client.test_fail)

        return self.callback_test(ping)

    def test_noop(self):
        """How do you test a noop?"""
        def noop(client):
            d = client.noop()
            # this delay is just so the server has time to shut down
            d.addCallbacks(
                lambda x: reactor.callLater(0.1, client.test_done, x),
                client.test_fail)

        return self.callback_test(noop)

    def test_protocol_ok(self):
        """Protocol version must match."""
        def protocol_version(client):
            def done(result):
                client.test_done("ok")
            d = client.protocol_version()
            d.addCallbacks(done, client.test_fail)
        return self.callback_test(protocol_version)

    def test_protocol_error(self):
        """Protocol version check must fail."""
        def protocol_version(client):
            def done(result):
                client.test_done("ok")
            client.PROTOCOL_VERSION = -1
            d = client.protocol_version()
            d.addCallbacks(client.test_fail, done)
        return self.callback_test(protocol_version)

    def test_size_too_big(self):
        """Connection must be closed because message is too big."""
        def size_too_big(client):
            def done(result):
                client.test_done("ok")
            data = "*" * (2 ** 16 + 1)
            sz = struct.pack(request.SIZE_FMT, len(data))
            client.transport.write(sz + data)
            client.connectionLostHandler = done
        return self.callback_test(size_too_big)

    def test_flood(self):
        """Send lots of mixed messages, check that nothing breaks."""
        def flood(client):

            def done(result):
                client.pending_tests -= 1
                client.recv_ok += 1
                if client.pending_tests == 0:
                    client.test_done("ok")
                elif (client.recv_ok % 2) == 0:
                    # keep sending messages for a while
                    send_message()

            def send_message():
                if (client.messages_sent % 2) == 0:
                    # one ping, one protocol version
                    d = client.ping()
                else:
                    d = client.protocol_version()
                d.addCallbacks(done, client.test_fail)
                client.messages_sent += 1
                client.pending_tests += 1

            client.recv_ok = 0
            client.pending_tests = 0
            client.messages_sent = 0
            for i in range(256):
                send_message()
        return self.callback_test(flood, timeout=12)

    @defer.inlineCallbacks
    def test_reactor_inspector_running(self):
        """Test that the looping call is runing."""
        ri = self.service._reactor_inspector
        # Check that the server factory has correct reactor_inspector reference
        self.assertTrue(ri is self.service.factory.reactor_inspector)
        self.assertTrue(ri.running)
        self.assertEqual(ri.loop_time, 3)
        yield self.service.stopService()
        self.assertTrue(ri.stopped)
        yield self.service.startService()

    def test_heartbeat_disabled(self):
        """Test that the hearbeat is disabled."""
        self.assertFalse(self.service.heartbeat_writer)


class ServerPingTest(TestWithDatabase):
    """ Test the server side Ping loop. """

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(ServerPingTest, self).setUp()
        self.timeout = self.service.factory.protocol.PING_TIMEOUT
        self.interval = self.service.factory.protocol.PING_INTERVAL
        self.service.factory.protocol.PING_TIMEOUT = 3
        self.service.factory.protocol.PING_INTERVAL = 0.1

    def tearDown(self):
        """Cleanup."""
        self.service.factory.protocol.PING_TIMEOUT = self.timeout
        self.service.factory.protocol.PING_INTERVAL = self.interval
        return super(ServerPingTest, self).tearDown()

    def test_server_ping(self):
        """ basic server ping test """
        def auth(client):
            ping_cb = defer.Deferred()

            def handle_PING(message):
                """ custom handle_PING that run the callback on the first
                ping request
                """
                client.__class__.handle_PING(client, message)
                ping_cb.callback(True)
            client.handle_PING = handle_PING
            ping_cb.addCallbacks(self.assertTrue,
                                 client.test_fail)
            ping_cb.addCallbacks(client.test_done, client.test_fail)
            return ping_cb

        return self.callback_test(auth)

    def test_multiple_server_ping(self):
        """ test that multiple pings are received from the server. """
        def auth(client):
            ping_cb = defer.Deferred()

            def handle_PING(message):
                """ custom handle_PING that save the num of ping requests. """
                ping_count = getattr(self._state, 'ping_count', 0)
                self._save_state('ping_count', ping_count + 1)
                client.__class__.handle_PING(client, message)
                if self._state.ping_count == 5:
                    ping_cb.callback(self._state.ping_count)
            client.handle_PING = handle_PING
            ping_cb.addCallbacks(lambda result: self.assertEquals(5, result),
                                 client.test_fail)
            ping_cb.addCallbacks(client.test_done, client.test_fail)
            return ping_cb

        return self.callback_test(auth)

    def test_no_response_server_ping(self):
        """test that the client is disconnected when there is no response
        to a server ping.
        """
        def auth(client):
            def handle_PING(message):
                """ don't reply to the ping, just save each request. """
                pings = getattr(self._state, 'pings', 0)
                self._save_state('pings', pings + 1)
            client.handle_PING = handle_PING

            def my_connection_lost(reason=None):
                """ check if we received a ping and finish the test. """
                client.__class__.connectionLost(client, reason)
                self.assertEquals(getattr(self._state, 'pings', 0), 1)
                client.test_done('ok')

            client.connectionLost = my_connection_lost
        d = self.callback_test(auth)
        return d

    @defer.inlineCallbacks
    def test_pingloop_reset_when_no_idle(self):
        """Test that the looping ping reset is called with non-idle client."""
        @defer.inlineCallbacks
        def auth(client):
            # stop the real looping ping and patch the reset method.
            yield client.dummy_authenticate('open sesame')
            server = self.service.factory.protocols[0]
            server.ping_loop.shutdown.cancel()

            # Wait for loop to finish
            d = defer.Deferred()
            real_reschedule = server.ping_loop.reschedule

            def reschedule():
                """Callback d the first time we hit this."""
                if not d.called:
                    d.callback(None)
                real_reschedule()
            self.patch(server.ping_loop, 'reschedule', reschedule)
            yield d

            self.patch(server.ping_loop, 'reset', d.callback)

            called = []

            self.patch(server.ping_loop, 'reset', lambda: called.append(1))
            yield client.ping()
            self.assertEqual(0, len(called))
            yield client.list_shares()
            self.assertEqual(1, len(called))
            yield client.list_shares()
            self.assertEqual(2, len(called))
        yield self.callback_test(auth, add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_disconnect_when_idle(self):
        """Test that the server disconnects idle clients."""
        @defer.inlineCallbacks
        def auth(client):

            yield client.dummy_authenticate('open sesame')
            d = defer.Deferred()
            client.connectionLostHandler = d.callback
            # add the log handler
            logger = logging.getLogger('storage.server')
            hdlr = MementoHandler()
            hdlr.setLevel(logging.INFO)
            logger.addHandler(hdlr)
            # patch the looping ping values
            server = self.service.factory.protocols[0]
            server.ping_loop.interval = 0.1
            server.ping_loop.idle_timeout = 0.3
            # reschedule the ping loop
            server.ping_loop.reset()
            try:
                yield d
            except ConnectionDone:
                msg = "Disconnecting - idle timeout"
                self.assertTrue(hdlr.check_info(msg))
            else:
                self.fail("Should get disconnected.")
            finally:
                logger.removeHandler(hdlr)

        yield self.callback_test(auth, add_default_callbacks=True)


class ServerStatusTest(TestWithDatabase):
    """Test the server status service."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(ServerStatusTest, self).setUp()
        self.url = "http://localhost:%s/status" % self.service.status_port
        # get the site instance from the service
        self.site = self.service.status_service.args[1]

    @defer.inlineCallbacks
    def test_status_OK(self):
        """Test the OK status response."""
        response = yield client.getPage(self.url)
        self.assertEquals(response, 'Status OK\n')

    @defer.inlineCallbacks
    def test_status_ERROR(self):
        """Test the OK status response."""
        status = self.site.resource.children['status']
        # override user_id with a non-existing user
        status.user_id = sys.maxint
        try:
            yield client.getPage(self.url)
        except error.Error, e:
            self.assertEquals('500', e.status)
        else:
            self.fail('An error is expected.')

    @defer.inlineCallbacks
    def test_status_not_found(self):
        """Test the OK status response."""
        status = self.site.resource.children['status']
        # override user_id with a non-existing user
        status.user_id = sys.maxint
        try:
            yield client.getPage(self.url + "/foo")
        except error.Error, e:
            self.assertEquals('404', e.status)
        else:
            self.fail('An error is expected.')


class DumpTest(TestWithDatabase):
    """Test the different dump services."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set up."""
        tmp_dir = os.path.join(os.getcwd(), self.mktemp())
        self.patch(config.general, 'log_folder', tmp_dir)
        yield super(DumpTest, self).setUp()
        if os.path.exists(config.general.log_folder):
            shutil.rmtree(config.general.log_folder)
        os.makedirs(config.general.log_folder)
        self.addCleanup(shutil.rmtree, tmp_dir)

    @defer.inlineCallbacks
    def test_meliae_dump(self):
        """Check that the dump works."""
        from meliae import scanner
        collect_called = []
        self.patch(gc, 'collect', lambda: collect_called.append(True))
        self.patch(scanner, 'dump_all_objects', lambda _: None)
        page = yield client.getPage("http://localhost:%i/+meliae" %
                                    (self.service.status_port))
        self.assertIn("Output written to:", page)
        self.assertTrue(collect_called)

    @defer.inlineCallbacks
    def test_meliae_dump_error(self):
        """Check the error case."""
        from meliae import scanner

        def broken_meliae_dump(filename):
            """stub."""
            raise ValueError('foo')

        self.patch(scanner, 'dump_all_objects', broken_meliae_dump)
        page = yield client.getPage("http://localhost:%i/+meliae" %
                                    (self.service.status_port))
        self.assertIn("Error while trying to dump memory", page)

    @defer.inlineCallbacks
    def test_gc_dumps_count_ok(self):
        """Check that the count dump works."""
        self.patch(gc, 'get_count', lambda: (400, 20, 3))
        self.patch(gc, 'garbage', [])
        page = yield client.getPage("http://localhost:%i/+gc-stats" %
                                    (self.service.status_port,))
        self.assertIn("GC count is (400, 20, 3)", page)

    @defer.inlineCallbacks
    def test_gc_dumps_garbage_ok(self):
        """Check that the garbage dump works."""
        self.patch(gc, 'get_count', lambda: None)
        self.patch(gc, 'garbage', ['foo', 666])
        page = yield client.getPage("http://localhost:%i/+gc-stats" %
                                    (self.service.status_port,))
        self.assertIn("2 garbage items written", page)

    @defer.inlineCallbacks
    def test_gc_dump_error_generic(self):
        """Something bad happens when dumping gc."""
        def oops():
            """Break it."""
            raise ValueError('foo')

        self.patch(gc, 'get_count', oops)
        page = yield client.getPage("http://localhost:%i/+gc-stats" %
                                    (self.service.status_port,))
        self.assertIn("Error while trying to dump GC", page)

    @defer.inlineCallbacks
    def test_gc_dump_error_garbage(self):
        """Support something that breaks in repr."""
        class Strange(object):
            """Weird object that breaks on repr."""
            def __repr__(self):
                raise ValueError('foo')

        self.patch(gc, 'garbage', [Strange()])
        page = yield client.getPage("http://localhost:%i/+gc-stats" %
                                    (self.service.status_port,))
        self.assertIn("1 garbage items written", page)


class TestS3SSL(TestBasic):
    """Test the basic stuff using ssl with s3."""

    s4_use_ssl = True


class TestServerHeartbeat(TestWithDatabase):
    """Server Heartbeat tests."""
    heartbeat_interval = 0.1

    @defer.inlineCallbacks
    def setUp(self):
        self.stdout = StringIO()
        send_heartbeat = supervisor_utils.send_heartbeat
        self.patch(supervisor_utils, 'send_heartbeat',
                   lambda *a, **kw: send_heartbeat(out=self.stdout))
        yield super(TestServerHeartbeat, self).setUp()

    @defer.inlineCallbacks
    def test_heartbeat_stdout(self):
        """Test that the heartbeat is working."""
        d = defer.Deferred()
        reactor.callLater(0.2, d.callback, None)
        yield d
        self.assertIn('<!--XSUPERVISOR:BEGIN-->', self.stdout.buflist)
        self.assertIn('<!--XSUPERVISOR:END-->', self.stdout.buflist)
