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

"""OOPS testing."""

import os
import sys
import time

from twisted.internet import reactor, defer

from oops_datedir_repo import serializer
from ubuntuone.storage.server import server
from ubuntuone.storage.server.testing.testcase import (
    TestWithDatabase,
    FactoryHelper)
from ubuntuone.storageprotocol import protocol_pb2


class TestOops(TestWithDatabase):
    """Test the the oops system stuff."""

    createOOPSFiles = True

    def setUp(self):
        self._max_oops_line = server.MAX_OOPS_LINE
        return super(TestOops, self).setUp()

    def tearDown(self):
        server.MAX_OOPS_LINE = self._max_oops_line
        return super(TestOops, self).tearDown()

    def get_oops_data(self):
        """Read oops data for first oops."""
        oopses = list(self.get_oops())
        self.assert_(len(oopses) > 0)
        with open(oopses[0], 'r') as f:
            oops_data = serializer.read(f)

        return oops_data

    def test_ping_error(self):
        """Fail on ping."""
        d = defer.Deferred()

        @defer.inlineCallbacks
        def login(client):
            try:
                pd = self.service.factory.protocols[0].wait_for_poison()
                self.service.factory.protocols[0].poison("ping")
                message = protocol_pb2.Message()
                message.id = 5
                message.type = protocol_pb2.Message.PING
                client.sendMessage(message)
                yield pd

                oops_data = self.get_oops_data()
                self.assertEqual("Service was poisoned with: ping",
                                 oops_data["value"])
                self.assertEqual(None, oops_data["username"])
                d.callback(True)
            except Exception, e:
                d.errback(e)
            client.transport.loseConnection()
            factory.timeout.cancel()

        factory = FactoryHelper(login)
        reactor.connectTCP('localhost', self.port, factory)
        return d

    def make_auth_fail(poison):
        "build test case"
        def inner(self):
            """Fail on ping."""
            d = defer.Deferred()

            @defer.inlineCallbacks
            def login(client):
                pd = self.service.factory.protocols[0].wait_for_poison()
                self.service.factory.protocols[0].poison(poison)
                message = protocol_pb2.Message()
                message.id = 5
                message.type = protocol_pb2.Message.AUTH_REQUEST
                client.handle_ERROR = lambda *args: True
                client.sendMessage(message)
                yield pd

                try:
                    oops_data = self.get_oops_data()
                    self.assertEqual("Service was poisoned with: " + poison,
                                     oops_data["value"])
                    self.assertEqual(None, oops_data["username"])
                    d.callback(True)
                except Exception, e:
                    d.errback(e)
                client.transport.loseConnection()
                factory.timeout.cancel()

            factory = FactoryHelper(login)
            reactor.connectTCP('localhost', self.port, factory)
            return d
        return inner

    test_request_start = make_auth_fail("request_start")
    test_request_schedule = make_auth_fail("request_schedule")
    test_authenticate_start = make_auth_fail("authenticate_start")
    test_authenticate_cont = make_auth_fail("authenticate_continue")

    def test_user_extra_data(self):
        """Test that the user id and username is included in the extra data"""
        @defer.inlineCallbacks
        def poisoned_ping(client):
            try:
                pd = self.service.factory.protocols[0].wait_for_poison()
                self.service.factory.protocols[0].poison("ping")
                message = protocol_pb2.Message()
                message.id = 5
                message.type = protocol_pb2.Message.PING
                client.sendMessage(message)
                yield pd

                oops_data = self.get_oops_data()
                self.assertEqual("Service was poisoned with: ping",
                                 oops_data["value"])
                self.assertEqual("0,0,usr0", oops_data["username"])
            except Exception, e:
                raise e

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: poisoned_ping(client))
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)


class TestOopsToStderr(TestOops):
    """Test writing oops to stderr."""

    createOOPSFiles = False

    def setUp(self):
        self.former_stderr = sys.stderr
        self.tmpdir = self.mktemp()
        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        self.stderr_filename = os.path.join(self.tmpdir,
                                            '%s.stderr' % (time.time(),))
        sys.stderr = open(self.stderr_filename, 'w')
        return super(TestOopsToStderr, self).setUp()

    def tearDown(self):
        sys.stderr.close()
        sys.stderr = self.former_stderr
        return super(TestOopsToStderr, self).tearDown()

    def get_oops_data(self):
        """Read oops data for first oops from stderr."""
        sys.stderr.flush()
        with open(self.stderr_filename) as f:
            oops_data = serializer.read(f)

        return oops_data
