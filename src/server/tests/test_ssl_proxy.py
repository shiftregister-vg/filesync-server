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

"""ssl_proxy tests."""

import logging
import re
import unittest

from StringIO import StringIO

import OpenSSL
import twisted

from mocker import Mocker, expect
from twisted.internet import defer, reactor, error as txerror, ssl
from twisted.python import failure
from twisted.web import client, error as web_error
from twisted.trial.unittest import TestCase

from config import config

from ubuntuone.devtools.handlers import MementoHandler
from metrics.metricsconnector import MetricsConnector
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storage.server import ssl_proxy
from ubuntuone.storage.server.server import PREFERRED_CAP
from ubuntuone.storageprotocol.client import (
    StorageClientFactory, StorageClient)
from ubuntuone.supervisor import utils as supervisor_utils


class SSLProxyServiceTest(TestWithDatabase):
    """Tests for the service instance."""

    ssl_proxy_heartbeat_interval = 0

    @defer.inlineCallbacks
    def setUp(self):
        yield super(SSLProxyServiceTest, self).setUp()
        self.configure_logging()
        self._old_heartbeat_interval = config.ssl_proxy.heartbeat_interval
        self.metrics = MetricReceiver()
        namespace = config.ssl_proxy.metrics_namespace
        instance = MetricsConnector.new_txmetrics(connection=self.metrics,
                                                  namespace=namespace)
        MetricsConnector.register_metrics("ssl-proxy", namespace, instance)
        config.ssl_proxy.heartbeat_interval = self.ssl_proxy_heartbeat_interval

    def configure_logging(self):
        """Configure logging for the tests."""
        logger = logging.getLogger("ssl_proxy")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        self.handler = MementoHandler()
        logger.addHandler(self.handler)
        self.addCleanup(logger.removeHandler, self.handler)

    @defer.inlineCallbacks
    def tearDown(self):
        config.ssl_proxy.heartbeat_interval = self._old_heartbeat_interval
        yield super(SSLProxyServiceTest, self).tearDown()
        MetricsConnector.unregister_metrics()

    @defer.inlineCallbacks
    def test_start_stop(self):
        """Test for start/stoService."""
        ssl_service = ssl_proxy.ProxyService(
            self.ssl_cert, self.ssl_key, self.ssl_cert_chain, 0,  # port
            "localhost", self.port, "ssl-proxy-test", 0)
        # mimic what twistd will call when running the .tac file
        yield ssl_service.privilegedStartService()
        yield ssl_service.stopService()


class SSLProxyTestCase(TestWithDatabase):
    """Tests for ssl proxy server."""

    ssl_proxy_heartbeat_interval = 0

    @defer.inlineCallbacks
    def setUp(self):
        yield super(SSLProxyTestCase, self).setUp()
        self.configure_logging()
        self.ssl_service = ssl_proxy.ProxyService(self.ssl_cert,
                                                  self.ssl_key,
                                                  self.ssl_cert_chain,
                                                  0,  # port
                                                  "localhost", self.port,
                                                  "ssl-proxy-test", 0)
        # keep metrics in our MetricReceiver
        self.metrics = MetricReceiver()
        namespace = config.ssl_proxy.metrics_namespace
        instance = MetricsConnector.new_txmetrics(connection=self.metrics,
                                                  namespace=namespace)
        MetricsConnector.register_metrics("ssl-proxy", namespace, instance)
        self._old_heartbeat_interval = config.ssl_proxy.heartbeat_interval
        config.ssl_proxy.heartbeat_interval = self.ssl_proxy_heartbeat_interval
        yield self.ssl_service.startService()

    def configure_logging(self):
        """Configure logging for the tests."""
        logger = logging.getLogger("ssl_proxy")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        self.handler = MementoHandler()
        logger.addHandler(self.handler)
        self.addCleanup(logger.removeHandler, self.handler)

    @defer.inlineCallbacks
    def tearDown(self):
        config.ssl_proxy.heartbeat_interval = self._old_heartbeat_interval
        yield super(SSLProxyTestCase, self).tearDown()
        yield self.ssl_service.stopService()
        MetricsConnector.unregister_metrics()

    @property
    def ssl_port(self):
        """SSL port."""
        return self.ssl_service.port


class BasicSSLProxyTestCase(SSLProxyTestCase):
    """Basic tests for the ssl proxy service."""

    def test_server(self):
        """Stop and restart the server."""
        d = self.ssl_service.stopService()
        d.addCallback(lambda _: self.ssl_service.startService())
        return d

    def test_connect(self):
        """Create a simple client that just connects."""

        def dummy(client):
            client.test_done("ok")

        return self.callback_test(dummy, use_ssl=True)

    def test_both_ways(self):
        """Test that communication works both ways."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.protocol_version()

        return self.callback_test(auth, add_default_callbacks=True,
                                  use_ssl=True)

    @unittest.skip('Should fail with connectionDone')
    @defer.inlineCallbacks
    def test_ssl_handshake_backend_dead(self):
        """No ssl handshake failure if the backend is dead."""
        # turn off the backend
        yield self.service.stopService()
        self.addCleanup(self.service.startService)
        # patch connectionMade to get a reference to the client.
        client_d = defer.Deferred()
        orig_connectionMade = StorageClient.connectionMade

        def connectionMade(s):
            """Intercecpt connectionMade."""
            orig_connectionMade(s)
            client_d.callback(s)

        self.patch(StorageClient, 'connectionMade', connectionMade)
        f = StorageClientFactory()
        # connect to the servr
        reactor.connectSSL(
            "localhost", self.ssl_port, f, ssl.ClientContextFactory())
        storage_client = yield client_d
        # try to do anything and fail with ConnectionDone
        try:
            yield storage_client.set_caps(PREFERRED_CAP)
        except txerror.ConnectionDone:
            pass
        except OpenSSL.SSL.Error as e:
            self.fail("Got %s" % e)
        else:
            self.fail("Should get a ConnectionDone.")

    def test_producers_registered(self):
        """Test that both producers are registered."""
        orig = self.ssl_service.factory.buildProtocol
        called = []

        def catcher(*a, **kw):
            """collect calls to buildProtocol."""
            p = orig(*a, **kw)
            called.append(p)
            return p

        self.patch(self.ssl_service.factory, 'buildProtocol', catcher)

        @defer.inlineCallbacks
        def auth(client):
            yield client.protocol_version()
            proto = called[0]
            # check that the producers match
            # backend transport is the frontend producer
            self.assertIdentical(
                proto.peer.transport, proto.transport.producer)
            # frontend transport is the backend producer
            self.assertIdentical(
                proto.transport, proto.peer.transport.producer)

        return self.callback_test(auth, add_default_callbacks=True,
                                  use_ssl=True)

    if twisted.version.major >= 11:
        test_producers_registered.skip = "already fixed in twisted >= 11"

    @defer.inlineCallbacks
    def test_server_status_ok(self):
        """Check that server status page works."""
        page = yield client.getPage("http://localhost:%i/status" %
                                    self.ssl_service.status_port)
        self.assertEqual("OK", page)

    @defer.inlineCallbacks
    def test_server_status_fail(self):
        """Check that server status page works."""
        # shutdown the tcp port of the storage server.
        self.service.tcp_service.stopService()
        d = client.getPage("http://localhost:%i/status" %
                           (self.ssl_service.status_port,))
        e = yield self.assertFailure(d, web_error.Error)
        self.assertEqual("503", e.status)
        self.assertEqual("Service Unavailable", e.message)
        self.assertIn('Connection was refused by other side: 111', e.response)

    def test_heartbeat_disabled(self):
        """Test that the hearbeat is disabled."""
        self.assertFalse(self.ssl_service.heartbeat_writer)


class SSLProxyHeartbeatTestCase(SSLProxyTestCase):
    """Tests for ssl proxy server heartbeat."""

    ssl_proxy_heartbeat_interval = 0.1

    @defer.inlineCallbacks
    def setUp(self):
        self.stdout = StringIO()
        send_heartbeat = supervisor_utils.send_heartbeat
        self.patch(supervisor_utils, 'send_heartbeat',
                   lambda *a, **kw: send_heartbeat(out=self.stdout))
        yield super(SSLProxyHeartbeatTestCase, self).setUp()

    @defer.inlineCallbacks
    def test_heartbeat_stdout(self):
        """Test that the heartbeat is working."""
        d = defer.Deferred()
        reactor.callLater(0.2, d.callback, None)
        yield d
        self.assertIn('<!--XSUPERVISOR:BEGIN-->', self.stdout.buflist)
        self.assertIn('<!--XSUPERVISOR:END-->', self.stdout.buflist)


class ProxyServerTest(TestCase):
    """Tests for ProxyServer class."""

    @defer.inlineCallbacks
    def setUp(self):
        yield super(ProxyServerTest, self).setUp()
        self.server = ssl_proxy.ProxyServer()
        # setup a client too
        self.peer = ssl_proxy.ProxyClient()
        self.peer.setPeer(self.server)

    @defer.inlineCallbacks
    def tearDown(self):
        self.server = None
        yield super(ProxyServerTest, self).tearDown()
        MetricsConnector.unregister_metrics()

    def test_connectionMade(self):
        """Test connectionMade with handshake done."""
        mocker = Mocker()
        metrics = self.server.metrics = mocker.mock()
        transport = self.server.transport = mocker.mock()
        self.server.factory = ssl_proxy.SSLProxyFactory(0, 'host', 0)
        called = []
        self.patch(reactor, 'connectTCP',
                   lambda *a: called.append('connectTCP'))
        expect(metrics.meter('frontend_connection_made', 1))
        expect(transport.getPeer()).result("host:port info").count(1)
        expect(transport.pauseProducing())

        with mocker:
            self.server.connectionMade()
            self.assertEqual(called, ['connectTCP'])

    def test_connectionLost(self):
        """Test connectionLost method."""
        mocker = Mocker()
        metrics = self.server.metrics = mocker.mock()
        transport = self.server.transport = mocker.mock()
        self.server.peer = self.peer
        peer_transport = self.peer.transport = mocker.mock()
        expect(metrics.meter('frontend_connection_lost', 1))
        expect(transport.getPeer()).result("host:port info").count(1)
        expect(peer_transport.loseConnection())

        with mocker:
            self.server.connectionLost()


class MetricReceiver(object):
    """A receiver for metrics."""

    def __init__(self):
        """Initialize the received message list."""
        self.received = []

    def __contains__(self, pattern):
        regex = re.compile(pattern)
        for message in self.received:
            if any(regex.findall(message)):
                return True
        return False

    def connect(self, transport=None):
        """Not implemented."""
        pass

    def disconnect(self):
        """Not implemented."""
        pass

    def write(self, message):
        """Store the received message and stack."""
        self.received.append(message)


class SSLProxyMetricsTestCase(SSLProxyTestCase):
    """Tests for ssl proxy metrics using real connections."""

    @defer.inlineCallbacks
    def setUp(self):
        yield super(SSLProxyMetricsTestCase, self).setUp()
        # keep the protocols created in a list
        self.protocols = []
        buildProtocol = self.ssl_service.factory.buildProtocol

        def build_protocol(*a, **kw):
            """Keep a reference to the just created protocol instance."""
            p = buildProtocol(*a, **kw)
            self.protocols.append(p)
            return p

        self.patch(self.ssl_service.factory, 'buildProtocol', build_protocol)

    @defer.inlineCallbacks
    def test_start_stop(self):
        """Start/stop metrics."""
        self.assertIn('server_start', self.metrics)
        yield self.ssl_service.stopService()
        self.assertIn('server_stop', self.metrics)

    @defer.inlineCallbacks
    def test_frontend_connection_made(self):
        """Frontend connectionMade metrics."""

        def dummy(client):
            client.test_done('ok')

        yield self.callback_test(dummy, use_ssl=True)
        self.assertIn('frontend_connection_made', self.metrics)
        self.assertTrue(self.handler.check_debug('Frontend connection made'))

    @defer.inlineCallbacks
    def test_frontend_connection_lost(self):
        """Frontend connectionLost metrics."""
        d = defer.Deferred()

        def dummy(client):
            # patch ProxyServer.connectionLost
            orig_connectionLost = self.protocols[0].connectionLost

            def connectionLost(reason):
                """Catch disconnect and force a ConnectionLost."""
                orig_connectionLost(txerror.ConnectionLost())
                d.callback(None)

            self.patch(self.protocols[0], 'connectionLost', connectionLost)
            client.kill()  # kill the client and trigger a connection lost
            client.test_done('ok')

        yield self.callback_test(dummy, use_ssl=True)
        yield d
        self.assertIn('frontend_connection_lost', self.metrics)
        self.assertTrue(self.handler.check_debug('Frontend connection lost'))

    @defer.inlineCallbacks
    def test_backend_connection_made(self):
        """Backend connectionMade metrics."""

        def dummy(client):
            client.test_done('ok')

        yield self.callback_test(dummy, use_ssl=True)
        self.assertIn('backend_connection_made', self.metrics)
        self.assertTrue(self.handler.check_debug('Backend connection made'))

    @defer.inlineCallbacks
    def test_backend_connection_lost(self):
        """Backend connectionLost metrics."""
        d = defer.Deferred()

        def dummy(client):
            orig_connectionLost = self.protocols[0].peer.connectionLost

            def connectionLost(reason):
                """Catch disconnect and force a ConnectionLost."""
                orig_connectionLost(failure.Failure(txerror.ConnectionLost()))
                d.callback(None)

            self.patch(
                self.protocols[0].peer, 'connectionLost', connectionLost)
            self.service.factory.protocols[0].shutdown()
            client.test_done('ok')

        yield self.callback_test(dummy, use_ssl=True)
        yield d
        self.assertIn('backend_connection_lost', self.metrics)
        self.assertTrue(self.handler.check_debug('Backend connection lost'))

    @defer.inlineCallbacks
    def test_backend_connection_done(self):
        """Backend connectionDone metrics."""
        d = defer.Deferred()

        def dummy(client):
            orig_connectionLost = self.protocols[0].peer.connectionLost

            def connectionLost(reason):
                """Catch disconnect and force a ConnectionLost."""
                orig_connectionLost(failure.Failure(txerror.ConnectionDone()))
                d.callback(None)

            self.patch(
                self.protocols[0].peer, 'connectionLost', connectionLost)
            self.service.factory.protocols[0].shutdown()
            client.test_done('ok')

        yield self.callback_test(dummy, use_ssl=True)
        yield d
        self.assertIn('backend_connection_done', self.metrics)
        self.assertTrue(self.handler.check_debug('Backend connection done'))
