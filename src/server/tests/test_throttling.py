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

"""Test content operations."""

import os
import zlib

from StringIO import StringIO

from twisted.internet import threads, defer
from twisted.internet.protocol import connectionDone


from backends.filesync.data import get_storage_store, model, storage_tm

from ubuntuone.storageprotocol import request, client
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32
from ubuntuone.storage.server.testing.testcase import (
    TestWithDatabase,
    ClientTestHelper,
)

NO_CONTENT_HASH = ""
EMPTY_HASH = model.EMPTY_CONTENT_HASH


class ThrottlingTestClient(client.ThrottlingStorageClient, ClientTestHelper):
    """ThrottlingStorageClient for the tests"""

    def connectionMade(self):
        """connection!"""
        client.ThrottlingStorageClient.connectionMade(self)
        ClientTestHelper.connectionMade(self)

    def connectionLost(self, reason=connectionDone):
        """connection lost"""
        self.factory.unregisterProtocol(self)
        ClientTestHelper.connectionLost(self, reason=reason)


class ThrottlingTestFactory(client.ThrottlingStorageClientFactory):
    """ThrottlingStorageClientFactory for the tests"""
    protocol = ThrottlingTestClient


class TestThrottling(TestWithDatabase):
    """Test thorttling in get/put content"""

    factory_class = ThrottlingTestFactory

    def test_getcontent_file(self, check_file_content=True):
        """Get the content from a file."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def check_file(req):
            if req.data != deflated_data:
                raise Exception("data does not match")

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root: client.make_file(request.ROOT, root, "hola"),
                client.test_fail)
            d.addCallback(self.save_req, 'req')
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT,
                    mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(lambda _: client.get_content(
                          request.ROOT, self._state.req.new_id, hash_value))
            if check_file_content:
                d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth, timeout=1.5)

    @defer.inlineCallbacks
    def test_getcontent_file_slow(self):
        """Get content from a file with very low BW and fail with timeout."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def auth(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            # make a file and put content in it
            mkfile_req = yield client.make_file(request.ROOT, root, "hola")
            yield client.put_content(request.ROOT, mkfile_req.new_id,
                                     NO_CONTENT_HASH, hash_value, crc32_value,
                                     size, deflated_size,
                                     StringIO(deflated_data))

            # set the read limit, and get content
            client.factory.factory.readLimit = 1000
            yield client.get_content(request.ROOT, mkfile_req.new_id,
                                     hash_value)

        d = self.callback_test(auth, add_default_callbacks=True,
                               timeout=0.1)
        err = yield self.assertFailure(d, Exception)
        self.assertEqual(str(err), "timeout")

    def test_putcontent(self, num_files=1):
        """Test putting content to a file."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):

            def check_file(result):

                def _check_file():
                    storage_tm.begin()
                    try:
                        store = get_storage_store()
                        content_blob = store.get(model.ContentBlob, hash_value)
                        if not content_blob:
                            raise ValueError("content blob is not there")
                    finally:

                        storage_tm.abort()
                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            filenames = iter('hola_%d' % i for i in xrange(num_files))
            for i in range(num_files):
                d.addCallbacks(lambda _: client.get_root(), client.test_fail)
                d.addCallbacks(
                    lambda root: client.make_file(request.ROOT, root,
                                                  filenames.next()),
                    client.test_fail)
                d.addCallbacks(
                    lambda mkfile_req: client.put_content(
                        request.ROOT,
                        mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                        crc32_value, size, deflated_size,
                        StringIO(deflated_data)),
                    client.test_fail)
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
            return d

        return self.callback_test(auth, timeout=1)

    test_putcontent.skip = "This is failing in PQM, Bug #423779"

    def test_putcontent_slow(self, num_files=1):
        """Test putting content to a file with very low bandwidth and fail
        with timeout.
        """
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):

            def check_file(result):

                def _check_file():
                    storage_tm.begin()
                    try:
                        store = get_storage_store()
                        content_blob = store.get(model.ContentBlob, hash_value)
                        if not content_blob:
                            raise ValueError("content blob is not there")
                    finally:
                        storage_tm.abort()

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            filename = 'hola_1'
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root: client.make_file(request.ROOT, root, filename),
                client.test_fail)

            def set_write_limit(r):
                client.factory.factory.writeLimit = 100
                return r

            d.addCallback(set_write_limit)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            return d
        d1 = defer.Deferred()
        test_d = self.callback_test(auth, timeout=1)
        test_d.addCallbacks(d1.errback, lambda r: d1.callback(None))
        return d1
