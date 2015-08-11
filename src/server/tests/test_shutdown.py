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

"""Test server shutdown."""

from twisted.trial.unittest import TestCase as TwistedTestCase
from twisted.internet import reactor, defer, error

from txstatsd.metrics.countermetric import CounterMetric
from txstatsd.metrics.metermetric import MeterMetric

from s4 import s4
from backends.filesync.data.services import make_storage_user
from backends.filesync.data.testing.testcase import StorageDALTestCase
from ubuntuone.storage.server.auth import DummyAuthProvider
from ubuntuone.storage.server.testing.testcase import StorageServerService
from ubuntuone.storageprotocol import request
from ubuntuone.storageprotocol.content_hash import content_hash_factory
from ubuntuone.storageprotocol.client import (
    StorageClientFactory, StorageClient)


class TestClient(StorageClient):
    """A simple client for tests."""

    def connectionMade(self):
        """Setup and call callback."""
        StorageClient.connectionMade(self)
        self.factory.connected(self)


class TestClientFactory(StorageClientFactory):
    """A test oriented protocol factory."""
    protocol = TestClient


class TestShutdown(TwistedTestCase, StorageDALTestCase):
    """Test the basic stuff."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup for testing."""
        # make sure we start with clean state
        yield super(TestShutdown, self).setUp()
        self.s4_site = site = s4.server.Site(s4.Root())
        self.s4_conn = reactor.listenTCP(0, site)
        # since storageusers are not automatically created, we need to create
        self.usr0 = make_storage_user(0, u"dummy", u"", 2 ** 20)

    @defer.inlineCallbacks
    def tearDown(self):
        """Tear down after testing."""
        yield self.s4_conn.stopListening()
        yield super(TestShutdown, self).tearDown()

    @defer.inlineCallbacks
    def test_shutdown_upload(self):
        """Stop and restart the server."""
        # create a server
        service = StorageServerService(
            0, "localhost", self.s4_conn.getHost().port, False,
            s4.AWS_DEFAULT_ACCESS_KEY_ID,
            s4.AWS_DEFAULT_SECRET_ACCESS_KEY,
            auth_provider_class=DummyAuthProvider,
            heartbeat_interval=0)
        yield service.startService()

        # create a user, connect a client
        usr0 = make_storage_user(0, u"dummy", u"", 2 ** 20)
        d = defer.Deferred()
        f = TestClientFactory()
        f.connected = d.callback
        reactor.connectTCP("localhost", service.port, f)
        client = yield d

        # auth, get root, create a file
        yield client.dummy_authenticate("open sesame")
        root = yield client.get_root()
        mkfile_req = yield client.make_file(request.ROOT, root, "hola")

        # try to upload something that will fail when sending data
        empty_hash = content_hash_factory().content_hash()
        # lose the connection if something wrong
        try:
            yield client.put_content(request.ROOT, mkfile_req.new_id,
                                     empty_hash, "fake_hash", 1234, 1000, None)
        except:
            client.transport.loseConnection()

        # see that the server has not protocols alive
        yield service.factory.wait_for_shutdown()
        ujobs = usr0.get_uploadjobs(node_id=mkfile_req.new_id)
        self.assertEqual(ujobs, [])

        yield service.stopService()

    @defer.inlineCallbacks
    def test_shutdown_metrics(self):
        """Stop and restart the server."""
        # create a server
        service = StorageServerService(
            0, "localhost", self.s4_conn.getHost().port, False,
            s4.AWS_DEFAULT_ACCESS_KEY_ID,
            s4.AWS_DEFAULT_SECRET_ACCESS_KEY,
            auth_provider_class=DummyAuthProvider,
            heartbeat_interval=0)

        yield service.startService()

        # ensure we employ the correct metric name.
        name = service.metrics.fully_qualify_name('server_start')
        self.assertTrue(isinstance(service.metrics._metrics[name],
                        MeterMetric))
        name = service.metrics.fully_qualify_name('services_active')
        self.assertTrue(isinstance(service.metrics._metrics[name],
                        CounterMetric))
        self.assertEquals(service.metrics._metrics[name].count(), 1)

        yield service.stopService()

    @defer.inlineCallbacks
    def test_requests_leak(self):
        """Test that the server waits for pending requests."""
        # create a server
        self.service = StorageServerService(
            0, "localhost", self.s4_conn.getHost().port, False,
            s4.AWS_DEFAULT_ACCESS_KEY_ID,
            s4.AWS_DEFAULT_SECRET_ACCESS_KEY,
            auth_provider_class=DummyAuthProvider,
            heartbeat_interval=0)
        # start it
        yield self.service.startService()

        make_storage_user(0, u"dummy", u"", 2 ** 20)
        client_d = defer.Deferred()
        f = TestClientFactory()
        f.connected = client_d.callback
        reactor.connectTCP("localhost", self.service.port, f)
        client = yield client_d
        # start with a client
        yield client.dummy_authenticate("open sesame")
        root_id = yield client.get_root()
        # create a bunch of files
        mk_deferreds = []

        def handle_conn_done(f):
            """Ignore ConnectionDone errors."""
            if f.check(error.ConnectionDone):
                return
            return f
        for i in range(10):
            mk = client.make_file(request.ROOT, root_id, "hola_%s" % i)
            mk.addErrback(handle_conn_done)
            mk_deferreds.append(mk)
        try:
            reactor.callLater(0.1, client.transport.loseConnection)
            yield defer.DeferredList(mk_deferreds)
        finally:
            self.assertTrue(self.service.factory.protocols[0].requests)
            # wait for protocol shutdown (this was hanging waiting for requests
            # to finish)
            yield self.service.factory.protocols[0].wait_for_shutdown()
            # see that the server has not protocols alive
            yield self.service.factory.wait_for_shutdown()
            # cleanup
            yield self.service.stopService()

    test_requests_leak.skip = """
        There seems to be a race condition on this test.
        Skipped because of https://bugs.launchpad.net/bugs/989680
    """
