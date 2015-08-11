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

import logging
import os
import re
import s3lib
import uuid
import zlib

from StringIO import StringIO

from mocker import Mocker, expect, ARGS, KWARGS, ANY
from twisted.python.failure import Failure
from twisted.internet import defer, reactor, threads, error, task, address
from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web import error as web_error
from twisted.test.proto_helpers import StringTransport

from oops_datedir_repo import serializer
from config import config
from backends.filesync.data import errors, model
from ubuntuone.storage.server import server, content, errors as storage_errors
from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.storage.server.testing.testcase import (
    TestWithDatabase,
    BufferedConsumer,
    FactoryHelper,
)
from ubuntuone.storageprotocol import (
    request,
    client as sp_client,
    errors as protoerrors,
    protocol_pb2,
)
from ubuntuone.storageprotocol.content_hash import (
    content_hash_factory,
    crc32,
    magic_hash_factory,
)
from s3lib.s3lib import ProducerStopped
from s3lib.producers import S3Producer
from s3lib.multipart import MultipartUpload


NO_CONTENT_HASH = ""
EMPTY_HASH = content_hash_factory().content_hash()


def get_magic_hash(data):
    """Return the magic hash for the data."""
    magic_hash_object = magic_hash_factory()
    magic_hash_object.update(data)
    return magic_hash_object.content_hash()._magic_hash


class TestGetContent(TestWithDatabase):
    """Test get_content command."""

    def test_getcontent_unknown(self):
        """Get the content from an unknown file."""

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.get_content(
                    request.ROOT, r, request.UNKNOWN_HASH),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))

        return self.callback_test(auth)

    def test_getcontent_no_content(self):
        """Get the contents a file with no content"""
        file = self.usr0.root.make_file(u"file")

        def auth(client):
            """Do the real work """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(
                lambda r: client.get_content(request.ROOT, file.id, ''))
            d.addCallbacks(client.test_done, client.test_fail)
        d = self.callback_test(auth)
        self.assertFails(d, 'DOES_NOT_EXIST')
        return d

    def test_getcontent_not_owned_file(self):
        """Get the contents of a directory not owned by the user."""
        # create another user
        dir_id = self.usr1.root.make_subdirectory(u"subdir1").id

        # try to get the content of the directory with a different user
        def auth(client):
            """Do the real work """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root_id: client.make_file(request.ROOT, root_id, "foo"),
                client.test_fail)
            d.addCallback(
                lambda r: client.get_content(request.ROOT, dir_id, EMPTY_HASH))
            d.addCallbacks(client.test_done, client.test_fail)

        d = self.callback_test(auth)
        self.assertFails(d, 'DOES_NOT_EXIST')
        return d

    def test_getcontent_empty_file(self):
        """Make sure get content of empty files work."""
        data = ""
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def check_file(req):
            if zlib.decompress(req.data) != "":
                raise Exception("data does not match")

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root_id: client.make_file(request.ROOT, root_id, "foo"),
                client.test_fail)
            d.addCallback(self.save_req, 'req')
            d.addCallback(lambda r: client.put_content(
                request.ROOT, r.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallback(lambda _: client.get_content(
                          request.ROOT, self._state.req.new_id, EMPTY_HASH))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_getcontent_file(self, check_file_content=True):
        """Get the content from a file."""
        data = "*" * 100000
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
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(lambda _: client.get_content(
                          request.ROOT, self._state.req.new_id, hash_value))
            if check_file_content:
                d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_getcontent_file_transfer_stalled(self):
        """Support the transfer gets stalled when getting a file."""
        data = os.urandom(100000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        # reduce timeout to make it testable
        self.patch(s3lib.contrib.http_client.HTTPProducer,
                   'normal_timeout', .5)

        # avoid the client to signal that the connection is lost
        self.patch(s3lib.contrib.http_client.HTTPProducer,
                   'stopProducing', lambda s: None)

        def change_s3_behaviour(*a, **k):
            """Make it stop producing, and put the original method back."""
            factory = orig_get_from_s3(*a, **k)
            orig_data_rec = factory.protocol.rawDataReceived

            def send_and_stall(self, data):
                """Send data first time only, then stall."""
                if should_stall:
                    return
                should_stall.append(True)
                orig_data_rec(self, data)

            factory.protocol.rawDataReceived = send_and_stall
            self.addCleanup(setattr,
                            factory.protocol, 'rawDataReceived', orig_data_rec)
            content.Node._get_from_s3 = orig_get_from_s3
            return factory

        should_stall = []
        orig_get_from_s3 = content.Node._get_from_s3
        content.Node._get_from_s3 = change_s3_behaviour

        @defer.inlineCallbacks
        def test(client):
            """Execute the test."""
            # authenticate and put a file
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            req = yield client.make_file(request.ROOT, root, "hola")
            yield client.put_content(request.ROOT, req.new_id, NO_CONTENT_HASH,
                                     hash_value, crc32_value, size,
                                     deflated_size, StringIO(deflated_data))

            # get the file, it should return TRY_AGAIN
            d = client.get_content(request.ROOT, req.new_id, hash_value)
            yield self.assertFails(d, 'TRY_AGAIN')
        return self.callback_test(test, add_default_callbacks=True)

    def test_getcontent_cancel_after_other_request(self):
        """Simulate getting the cancel after another request in the middle."""
        data = os.urandom(100000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        # this is for the get content to send a lot of BYTES (which will leave
        # time for the cancel to arrive to the server) but not needing to
        # actually *put* a lot of content
        server.BytesMessageProducer.payload_size = 500

        # replace handle_GET_CONTENT so we can get the request reference
        def handle_get_content(s, message):
            """Handle GET_CONTENT message."""
            request = server.GetContentResponse(s, message)
            self.request = request
            request.start()
        self.patch(server.StorageServer, 'handle_GET_CONTENT',
                   handle_get_content)

        # monkeypatching to simulate that we're not working on that request
        # at the moment the CANCEL arrives
        orig_lie_method = server.GetContentResponse.processMessage

        def lie_about_current(self, *a, **k):
            """Lie that the request is not started."""
            self.started = False
            orig_lie_method(self, *a, **k)
            self.started = True
            server.GetContentResponse.processMessage = orig_lie_method

        server.GetContentResponse.processMessage = lie_about_current

        def auth(client):
            d = client.dummy_authenticate("open sesame")

            # monkeypatching to assure that the lock is released
            orig_check_method = server.GetContentResponse._processMessage

            def middle_check(innerself, *a, **k):
                """Check that the lock is released."""
                orig_check_method(innerself, *a, **k)
                self.assertFalse(innerself.protocol.request_locked)
                server.GetContentResponse._processMessage = orig_check_method
                d.addCallbacks(client.test_done, client.test_fail)

            server.GetContentResponse._processMessage = middle_check

            class HelperClass(object):

                def __init__(innerself):
                    innerself.cancelled = False
                    innerself.req = None

                def cancel(innerself, *args):
                    if innerself.cancelled:
                        return
                    innerself.cancelled = True

                    def _cancel(_):
                        """Directly cancel the server request."""
                        m = protocol_pb2.Message()
                        m.id = self.request.id
                        m.type = protocol_pb2.Message.CANCEL_REQUEST
                        self.request.cancel_message = m
                        self.request.processMessage(m)
                    d.addCallbacks(_cancel)

                def store_getcontent_result(innerself, req):
                    innerself.req = req

            hc = HelperClass()

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
            d.addCallback(lambda _: client.get_content_request(request.ROOT,
                          self._state.req.new_id, hash_value, 0, hc.cancel))
            d.addCallback(hc.store_getcontent_result)
        return self.callback_test(auth)

    def test_getcontent_cancel_inside_download(self):
        """Start to get the content from a file, and cancel in the middle."""
        data = os.urandom(100000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        # this is for the get content to send a lot of BYTES (which will leave
        # time for the cancel to arrive to the server) but not needing to
        # actually *put* a lot of content
        server.BytesMessageProducer.payload_size = 500

        # replace handle_GET_CONTENT so we can get the request reference
        def handle_get_content(s, message):
            """Handle GET_CONTENT message."""
            request = server.GetContentResponse(s, message)
            self.request = request
            request.start()
        self.patch(server.StorageServer, 'handle_GET_CONTENT',
                   handle_get_content)

        def auth(client):
            d = client.dummy_authenticate("open sesame")

            # monkeypatching to assure that the producer was cancelled
            orig_method = server.GetContentResponse.unregisterProducer

            def check(*a, **k):
                """Assure that it was effectively cancelled."""
                d.addCallbacks(client.test_done, client.test_fail)
                orig_method(*a, **k)
                server.GetContentResponse.unregisterProducer = orig_method

            server.GetContentResponse.unregisterProducer = check

            class HelperClass(object):

                def __init__(innerself):
                    innerself.cancelled = False
                    innerself.req = None

                def cancel(innerself, *args):
                    if innerself.cancelled:
                        return
                    innerself.cancelled = True

                    def _cancel(_):
                        """Directly cancel the server request."""
                        m = protocol_pb2.Message()
                        m.id = self.request.id
                        m.type = protocol_pb2.Message.CANCEL_REQUEST
                        self.request.cancel_message = m
                        self.request.processMessage(m)
                    d.addCallbacks(_cancel)

                def store_getcontent_result(innerself, req):
                    innerself.req = req

            hc = HelperClass()

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
            d.addCallback(lambda _: client.get_content_request(request.ROOT,
                          self._state.req.new_id, hash_value, 0, hc.cancel))
            d.addCallback(hc.store_getcontent_result)
        return self.callback_test(auth)

    def test_getcontent_cancel_after_download(self):
        """Start to get the content from a file, and cancel in the middle"""
        data = "*" * 100000
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            d = client.dummy_authenticate("open sesame")

            class HelperClass(object):

                def __init__(innerself):
                    innerself.cancelled = False
                    innerself.req = None
                    innerself.received = ""

                def cancel(innerself, newdata):
                    innerself.received += newdata
                    if innerself.received != deflated_data:
                        return

                    # got everything, now generate the cancel
                    if innerself.cancelled:
                        client.test_fail()
                    innerself.cancelled = True
                    d.addCallbacks(lambda _: innerself.req.cancel())
                    d.addCallbacks(client.test_done, client.test_fail)

                def store_getcontent_result(innerself, req):
                    innerself.req = req

            hc = HelperClass()

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
            d.addCallback(lambda _: client.get_content_request(request.ROOT,
                          self._state.req.new_id, hash_value, 0, hc.cancel))
            d.addCallback(hc.store_getcontent_result)
        return self.callback_test(auth)

    def test_getcontent_with_s3_500(self):
        """Test getting content when s3 throws a 500"""
        self.s4_site.resource.fail_next_get()
        d = self.test_getcontent_file(check_file_content=False)
        self.assertFails(d, 'TRY_AGAIN')
        return d

    def test_getcontent_with_s3_400(self):
        """Test getting content when s3 throws a 400"""
        self.s4_site.resource.fail_next_get(error=http.BAD_REQUEST)
        d = self.test_getcontent_file(check_file_content=False)
        self.assertFails(d, 'TRY_AGAIN')
        return d

    def test_getcontent_with_s3_416(self):
        """Test getting content when s3 throws a 416"""
        self.s4_site.resource.fail_next_get(
            error=http.REQUESTED_RANGE_NOT_SATISFIABLE)
        d = self.test_getcontent_file(check_file_content=False)
        self.assertFails(d, 'NOT_AVAILABLE')
        return d

    def test_getcontent_doesnt_exist(self):
        """Get the content from an unexistent node."""

        def auth(client):
            """Do the test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(lambda _: client.get_content(request.ROOT,
                                                        uuid.uuid4(),
                                                        EMPTY_HASH),
                           client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        d = self.callback_test(auth)
        self.assertFails(d, 'DOES_NOT_EXIST')
        return d

    def test_getcontent_fallback(self):
        """get_content() falls back to the fallback S3 bucket."""

        def put_content(client, share_id, file_id, data,
                        previous_hash=NO_CONTENT_HASH):
            """A helper to put the given data in the given bucket."""
            deflated_data = zlib.compress(data)
            hash_object = content_hash_factory()
            hash_object.update(data)
            hash_value = hash_object.content_hash()
            crc32_value = crc32(data)
            size = len(data)
            deflated_size = len(deflated_data)
            return client.put_content(
                share_id, file_id, previous_hash, hash_value, crc32_value,
                size, deflated_size, StringIO(deflated_data))

        @defer.inlineCallbacks
        def auth(client):
            """Do the test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            # Create files in the original bucket.
            file1_id = (yield client.make_file(request.ROOT, root,
                                               "file1")).new_id
            file1_hash = (yield put_content(client, request.ROOT, file1_id,
                                            "original content of file1")).hash
            file2_id = (yield client.make_file(request.ROOT, root,
                                               "file2")).new_id
            file2_hash = (yield put_content(client, request.ROOT, file2_id,
                                            "original content of file2")).hash

            # Now alter the configuration to use a bucket called staging.
            self.s4_site.resource._add_bucket("staging")

            old_fallback = config.api_server.s3_fallback_bucket
            old_bucket = config.api_server.s3_bucket
            config.api_server.s3_fallback_bucket = config.api_server.s3_bucket
            config.api_server.s3_bucket = 'staging'

            try:
                # Update file1.  This will write to the staging bucket.
                prev_hash = content_hash_factory()
                prev_hash.update("original content of file1")
                file1 = yield put_content(
                    client, request.ROOT, file1_id, "new content of file1",
                    previous_hash=prev_hash.content_hash())
                file1_hash = file1.hash

                # We can access the content of both files.
                req = yield client.get_content(request.ROOT, file1_id,
                                               file1_hash)
                self.assertEquals(zlib.decompress(req.data),
                                  "new content of file1")
                req = yield client.get_content(request.ROOT, file2_id,
                                               file2_hash)
                self.assertEquals(zlib.decompress(req.data),
                                  "original content of file2")
            finally:
                config.api_server.s3_fallback_bucket = old_fallback
                config.api_server.s3_bucket = old_bucket

            # Now that we're back on the original bucket, we should
            # not be able to find the new content.
            yield self.assertFailure(
                client.get_content(request.ROOT, file1_id,
                                   file1_hash),
                request.StorageRequestError)

        return self.callback_test(auth, add_default_callbacks=True)

    def _getcontent_file_factoryerror(self, factory_error, log_msgs):
        """Generic get content test with errors in the factory."""
        data = "foobar"
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        old_get_from_s3 = content.Node._get_from_s3
        old_s3_retries = content.Node.s3_retries
        old_s3_retry_wait = content.Node.s3_retry_wait
        # change the retrier parameters of this Node instance
        content.Node.s3_retries = 2
        content.Node.s3_retry_wait = 0.001

        def monkeypatch(r):
            """Monkeypatch Node.get_from_s3 to fail with TimeoutError. """

            def get_from_s3(innerself, *args, **kwargs):
                """Just create the Factory and make it fail."""
                factory = s3lib.contrib.http_client.HTTPStreamingClientFactory(
                    'http://example.com', method='GET', postdata={},
                    headers={}, agent="S3lib", streaming=True)
                factory.deferred.errback(factory_error())
                return factory

            content.Node._get_from_s3 = get_from_s3
            return r

        def restore(r):
            """Restore the monkepatched Node._get_from_s3."""
            content.Node._get_from_s3 = old_get_from_s3
            content.Node.s3_retries = old_s3_retries
            content.Node.s3_retry_wait = old_s3_retry_wait
            return r

        # add a memento handler, to check that we log the retries
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        logger.addHandler(hdlr)
        hdlr.setLevel(logging.WARNING)

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addBoth(monkeypatch)
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root_id: client.make_file(request.ROOT, root_id, "foo"),
                client.test_fail)
            d.addCallback(self.save_req, 'req')
            d.addCallback(lambda r: client.put_content(
                request.ROOT, r.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallback(lambda _: client.get_content(
                          request.ROOT, self._state.req.new_id, hash_value))
            d.addBoth(restore)

            def check_logs(r):
                """Check that we log the retries."""
                logger.removeHandler(hdlr)
                self.assertTrue(hdlr.check_warning(*log_msgs))
                return r

            d.addBoth(check_logs)
            d.addCallbacks(client.test_done, client.test_fail)

        d = self.callback_test(auth)
        self.assertFails(d, 'TRY_AGAIN')
        return d

    @defer.inlineCallbacks
    def test_getcontent_file_timeout(self):
        """Test getcontent of a file and TimeoutError handling."""
        msgs = ("Retrying: <bound method Node._get_producer",
                "failed with: TimeoutError('User timeout caused "
                "connection failure.') - retry_count: 0",)
        yield self._getcontent_file_factoryerror(error.TimeoutError, msgs)

    @defer.inlineCallbacks
    def test_getcontent_file_connectionlost(self):
        """Test getcontent of a file and ConnectionLost handling."""
        msgs = ("Sending TRY_AGAIN to client", "ConnectionLost")
        yield self._getcontent_file_factoryerror(error.ConnectionLost, msgs)

    @defer.inlineCallbacks
    def test_when_to_release(self):
        """GetContent should assign resources before release."""
        storage_server = self.service.factory.buildProtocol('addr')
        mocker = Mocker()

        producer = mocker.mock()
        expect(producer.deferred).count(2).result(defer.Deferred())
        producer.consumer = ANY

        node = mocker.mock()
        expect(node.deflated_size).result(0)
        expect(node.size).count(2).result(0)
        expect(node.content_hash).count(2).result('hash')
        expect(node.crc32).result(0)
        expect(node.get_content(KWARGS)).result(defer.succeed(producer))

        user = mocker.mock()
        expect(user.get_node(ARGS)
               ).result(defer.succeed(node))
        expect(user.username).result('')
        storage_server.user = user

        message = mocker.mock()
        expect(message.get_content.share).result(str(uuid.uuid4()))
        expect(message.get_content.node).count(3)
        expect(message.get_content.hash).count(2)
        expect(message.get_content.offset)

        self.patch(server.GetContentResponse, 'sendMessage', lambda *a: None)
        gc = server.GetContentResponse(storage_server, message)
        gc.id = 321

        # when GetContentResponse calls protocol.release(), it already
        # must have assigned the producer
        assigned = []
        storage_server.release = lambda a: assigned.append(gc.message_producer)
        with mocker:
            yield gc._start()
        self.assertNotEqual(assigned[0], None)


class TestPutContent(TestWithDatabase):
    """Test put_content command."""

    def setUp(self):
        """Set up."""
        d = super(TestPutContent, self).setUp()
        self.handler = MementoHandler()
        self.handler.setLevel(0)
        self._logger = logging.getLogger('storage.server')
        self._logger.addHandler(self.handler)
        return d

    def tearDown(self):
        """Tear down."""
        self._logger.removeHandler(self.handler)
        return super(TestPutContent, self).tearDown()

    def test_putcontent_cancel(self):
        """Test putting content to a file and cancelling it."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):

            def cancel_it(request):
                request.cancel()
                return request

            def test_done(request):
                if request.cancelled and request.finished:
                    client.test_done()
                else:
                    reactor.callLater(.1, test_done, request)

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallback(
                lambda root: client.make_file(request.ROOT, root, "hola"))
            d.addCallback(
                lambda mkfile_req: client.put_content_request(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)))
            d.addCallback(cancel_it)

            def wait_and_trap(req):
                d = req.deferred
                d.addErrback(
                    lambda failure:
                    req if failure.check(request.RequestCancelledError)
                    else failure)
                return d

            d.addCallback(wait_and_trap)
            d.addCallbacks(test_done, client.test_fail)
            return d
        return self.callback_test(auth)

    def test_putcontent_cancel_after(self):
        """Test putting content to a file and cancelling it after finished."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallback(
                lambda root: client.make_file(request.ROOT, root, "hola"))
            d.addCallback(
                lambda mkfile_req: client.put_content_request(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)))
            d.addCallback(self.save_req, 'request')
            d.addCallback(lambda _: self._state.request.deferred)
            d.addCallback(lambda _: self._state.request.cancel())
            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(auth)

    def test_putcontent_cancel_middle(self):
        """Test putting content to a file and cancelling it in the middle."""
        size = int(config.api_server.storage_chunk_size * 1.5)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)
        self.usr0.update(max_storage_bytes=size * 2)

        def auth(client):

            class Helper(object):

                def __init__(innerself):
                    innerself.notifs = 0
                    innerself.request = None
                    innerself.data = StringIO(deflated_data)

                def store_request(innerself, request):
                    innerself.request = request
                    return request

                def read(innerself, cant):
                    '''If second read, cancel and trigger test.'''
                    innerself.notifs += 1
                    if innerself.notifs == 2:
                        innerself.request.cancel()
                    if innerself.notifs > 2:
                        client.test_fail(ValueError("called beyond cancel!"))
                    return innerself.data.read(cant)
            helper = Helper()

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallback(
                lambda root: client.make_file(request.ROOT, root, "hola"))
            d.addCallback(lambda mkfile_req: client.put_content_request(
                request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, helper))
            d.addCallback(helper.store_request)
            d.addCallback(lambda request: request.deferred)
            d.addErrback(
                lambda failure:
                helper.request if failure.check(request.RequestCancelledError)
                else failure)
            d.addCallback(lambda _: client.test_done())
            return d
        return self.callback_test(auth)

    @defer.inlineCallbacks
    def test_putcontent(self, num_files=1, size=300000):
        """Test putting content to a file."""
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def auth(client):
            """Authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            # hook to test stats (don't have graphite in dev)
            meter = []
            self.service.factory.metrics.meter = lambda *a: meter.append(a)
            gauge = []
            self.service.factory.metrics.gauge = lambda *a: gauge.append(a)

            for i in range(num_files):
                fname = 'hola_%d' % i
                mkfile_req = yield client.make_file(request.ROOT, root, fname)
                yield client.put_content(request.ROOT, mkfile_req.new_id,
                                         NO_CONTENT_HASH, hash_value,
                                         crc32_value, size, deflated_size,
                                         StringIO(deflated_data))

                try:
                    self.usr0.volume().get_content(hash_value)
                except errors.DoesNotExist:
                    raise ValueError("content blob is not there")

                # check upload stat and the offset sent
                self.assertTrue(('UploadJob.upload', 0) in gauge)
                self.assertTrue(('UploadJob.upload.begin', 1) in meter)
                self.assertTrue(self.handler.check_debug(
                                "UploadJob begin content from offset 0"))

        yield self.callback_test(auth, add_default_callbacks=True)

    def test_putcontent_connectionlost(self):
        """Test putting content to a file and losing connection meanwhile."""
        size = int(config.api_server.storage_chunk_size * 1.5)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)

        self.usr0.update(max_storage_bytes=size * 2)

        # errback with connection lost when adding data
        real_add_data = content.UploadJob.add_data
        first_time = [True]

        def fake_add_data(fake_self, data):
            """Add data and lose connection."""
            real_add_data(fake_self, data)
            if first_time:
                del first_time[0]
                fake_self.factory.clientConnectionFailed(
                    None, error.ConnectionLost())
                # patch _disconnectedDeferred to avoid AlreadyCalledError, as
                # we just ^ fired the deferred manually
                d = fake_self.factory._disconnectedDeferred
                d.callback = lambda x: None
                content.UploadJob.add_data = real_add_data

        content.UploadJob.add_data = fake_add_data

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            req = yield client.make_file(request.ROOT, root, 'hola')
            args = (request.ROOT, req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            try:
                yield client.put_content(*args)
            except protoerrors.TryAgainError:
                pass
            else:
                self.fail("Should fail with TryAgain")

        return self.callback_test(test, add_default_callbacks=True)

    def test_put_content_in_not_owned_file(self):
        """Test putting content in other user file"""
        # create another user
        file_id = self.usr1.root.make_file(u"a_dile").id
        # try to put the content in this file, but with other user
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            """do the real work"""
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda _: client.put_content(
                    request.ROOT, file_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data)),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
            return d

        d = self.callback_test(auth)
        self.assertFails(d, 'DOES_NOT_EXIST')
        return d

    def test_putcontent_with_s3_500(self):
        """Test putting content when s3 throws a 500"""
        self.s4_site.resource.fail_next_put()
        d = self.test_putcontent()
        self.assertFails(d, 'TRY_AGAIN')
        return d

    def test_putcontent_twice_with_s3_500_in_the_middle(self):
        """Test putting content when s3 throws a 500"""
        self.s4_site.resource.fail_next_put()
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
                    try:
                        self.usr0.volume().get_content(hash_value)
                    except errors.DoesNotExist:
                        raise ValueError("content blob is not there")

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root: client.make_file(request.ROOT, root, 'foo'),
                client.test_fail)
            d.addCallback(lambda mk: setattr(self, 'file_id', mk.new_id))
            d.addCallbacks(
                lambda _: client.put_content(
                    request.ROOT,
                    self.file_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addErrback(
                lambda _: client.put_content(request.ROOT, self.file_id,
                                             NO_CONTENT_HASH, hash_value,
                                             crc32_value, size, deflated_size,
                                             StringIO(deflated_data)))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
            return d
        d = self.callback_test(auth)

        return d

    def test_putcontent_duplicated(self):
        """Test putting the same content twice"""
        d = self.test_putcontent(num_files=2)

        def callback(a):
            "check that only one object has been stored"
            objects = self.s4_site.resource.buckets['test'].bucket_children

            def non_chunks(objects):
                """iterator of stored objects with meta = {}"""
                for obj in objects:
                    d = dict(obj.iter_meta())
                    if d == {}:
                        yield obj
            objects = list(non_chunks(objects.itervalues()))
            if len(objects) > 1:
                return Failure(AssertionError('too many objects stored'))
            else:
                return a
        d.addCallback(callback)
        return d

    def test_putcontent_twice_simple(self):
        """Test putting content twice."""
        data = "*" * 100
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
                    try:
                        self.usr0.volume().get_content(hash_value)
                    except errors.DoesNotExist:
                        raise ValueError("content blob is not there")

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.get_root())
            d.addCallback(lambda root_id:
                          client.make_file(request.ROOT, root_id, "hola"))
            d.addCallback(self.save_req, "file")
            d.addCallback(lambda req: client.put_content(
                request.ROOT, req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallback(lambda _: client.put_content(request.ROOT,
                          self._state.file.new_id, hash_value, hash_value,
                          crc32_value, size, deflated_size,
                          StringIO(deflated_data)))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_putcontent_twice_samefinal(self):
        """Test putting content twice."""
        data = "*" * 100
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
                    try:
                        self.usr0.volume().get_content(hash_value)
                    except errors.DoesNotExist:
                        raise ValueError("content blob is not there")

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.get_root())
            d.addCallback(lambda root_id:
                          client.make_file(request.ROOT, root_id, "hola"))
            d.addCallback(self.save_req, "file")
            d.addCallback(lambda req: client.put_content(
                request.ROOT, req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))

            # don't care about previous hash, as long the final hash is ok
            d.addCallback(lambda _: client.put_content(
                request.ROOT, self._state.file.new_id, NO_CONTENT_HASH,
                hash_value, crc32_value, size, deflated_size,
                StringIO(deflated_data)))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def mkauth(self, data=None, previous_hash=None,
               hash_value=None, crc32_value=None, size=None):
        """Base function to create tests of wrong hints."""
        if data is None:
            data = "*" * 1000
        deflated_data = zlib.compress(data)
        if previous_hash is None:
            previous_hash = content_hash_factory().content_hash()
        if hash_value is None:
            hash_object = content_hash_factory()
            hash_object.update(data)
            hash_value = hash_object.content_hash()
        if crc32_value is None:
            crc32_value = crc32(data)
        if size is None:
            size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root_id: client.make_file(request.ROOT, root_id, "foo"),
                client.test_fail)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT, mkfile_req.new_id, previous_hash, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data)),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda r: client.test_done("ok"))
        return auth

    def test_putcontent_bad_prev_hash(self):
        """Test wrong prev hash hint."""
        return self.callback_test(
            self.mkauth(previous_hash="sha1:notthehash"))

    def test_putcontent_bad_hash(self):
        """Test wrong hash hint."""
        return self.callback_test(
            self.mkauth(hash_value="sha1:notthehash"))

    def test_putcontent_bad_c3c32(self):
        """Test wrong crc32 hint."""
        return self.callback_test(
            self.mkauth(crc32_value=100))

    def test_putcontent_bad_size(self):
        """Test wrong size hint."""
        return self.callback_test(
            self.mkauth(size=20))

    def test_putcontent_notify(self):
        """Make sure put_content generates a notification."""
        data = "*" * 100000
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root_id: client.make_file(request.ROOT, root_id, "foo"),
                client.test_fail)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT,
                    mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data)),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_putcontent_nofile(self):
        """Test putting content to an inexistent file."""

        def auth(client):
            """Do the real work."""
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            args = (request.ROOT, uuid.uuid4(), '', '', 0, 0, 0, '')
            d.addCallbacks(lambda _: client.put_content(*args),
                           client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        d = self.callback_test(auth)
        self.assertFails(d, 'DOES_NOT_EXIST')
        return d

    def test_remove_uploadjob_deleted_file(self):
        """make sure we dont raise exceptions on deleted files"""
        so_file = self.usr0.root.make_file(u"foobar")
        upload_job = so_file.make_uploadjob(
            so_file.content_hash, "sha1:100", 0, 100, 100)
        # kill file
        so_file.delete()
        upload_job.delete()

    def test_putcontent_conflict_middle(self):
        """Test putting content to a file and changing it in the middle."""
        data = os.urandom(3000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):

            class Helper(object):

                def __init__(innerself):
                    innerself.notifs = 0
                    innerself.request = None
                    innerself.data = StringIO(deflated_data)
                    innerself.node_id = None

                def save_node(innerself, node):
                    innerself.node_id = node.new_id
                    return node

                def store_request(innerself, request):
                    innerself.request = request
                    return request

                def read(innerself, data):
                    """Change the file when this client starts uploading it."""
                    # modify the file and cause a conflict
                    ho = content_hash_factory()
                    ho.update('randomdata')
                    hash_value = ho.content_hash()
                    filenode = self.usr0.get_node(innerself.node_id)
                    filenode.make_content(filenode.content_hash, hash_value,
                                          32, 1000, 1000, uuid.uuid4())
                    return innerself.data.read(data)

            helper = Helper()

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallback(
                lambda root: client.make_file(request.ROOT, root, "hola"))
            d.addCallback(helper.save_node)
            d.addCallback(
                lambda mkfile_req: client.put_content_request(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size, helper))
            d.addCallback(helper.store_request)
            d.addCallback(lambda request: request.deferred)
            d.addErrback(
                lambda f:
                helper.request if f.check(protoerrors.ConflictError) else f)
            d.addCallback(lambda _: client.test_done())
            return d
        d = self.callback_test(auth)
        return d

    def test_putcontent_update_used_bytes(self):
        """Test putting content to a file and check that user used bytes
        is updated.
        """
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):

            def check_used_bytes(result):

                def _check_file():
                    quota = self.usr0.get_quota()
                    self.assertEquals(size, quota.used_storage_bytes)

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(lambda root: client.make_file(request.ROOT, root,
                                                         'hola_1'),
                           client.test_fail)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(check_used_bytes)
            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(auth)

    @defer.inlineCallbacks
    def test_putcontent_quota_exceeded(self):
        """Test the QuotaExceeded handling."""
        self.usr0.update(max_storage_bytes=1)
        try:
            yield self.test_putcontent()
        except protoerrors.QuotaExceededError as e:
            self.assertEquals(e.free_bytes, 1)
            self.assertEquals(e.share_id, request.ROOT)
        else:
            self.fail('Should fail with QuotaExceededError!')

    def test_putcontent_generations(self):
        """Put content on a file and receive new generation."""
        data = os.urandom(30)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")

            # create the dir
            root_id = yield client.get_root()
            make_req = yield client.make_file(request.ROOT, root_id, "hola")

            # put content and check
            args = (request.ROOT, make_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            putc_req = yield client.put_content(*args)
            self.assertEqual(putc_req.new_generation,
                             make_req.new_generation + 1)
        return self.callback_test(test, add_default_callbacks=True)

    def test_putcontent_corrupt(self):
        """Put content on a file with corrupt data."""
        data = os.urandom(30)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data) + 10
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            meter = []
            self.service.factory.metrics.meter = lambda *a: meter.append(a)

            # create the dir
            root_id = yield client.get_root()
            make_req = yield client.make_file(request.ROOT, root_id, "hola")

            # put content and check
            args = (request.ROOT, make_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            try:
                yield client.put_content(*args)
            except Exception as ex:
                self.assertTrue(isinstance(ex, protoerrors.UploadCorruptError))
            self.assertTrue(
                self.handler.check_debug('UploadCorrupt', str(size)))
            self.assertTrue(('oops_saved',) not in meter)
        return self.callback_test(test, add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_when_to_release(self):
        """PutContent should assign resources before release."""
        storage_server = self.service.factory.buildProtocol('addr')
        mocker = Mocker()

        user = mocker.mock()
        upload_job = mocker.mock()
        expect(user.get_upload_job(ARGS, KWARGS)).result(
            defer.succeed(upload_job))
        expect(upload_job.deferred).result(defer.succeed(None))
        expect(upload_job.offset).result(0)
        expect(upload_job.connect()).result(defer.succeed(None))
        expect(upload_job.registerProducer(ARGS))
        expect(upload_job.upload_id).result("hola")
        expect(user.username).count(2).result('')
        storage_server.user = user

        message = mocker.mock()
        expect(message.put_content.share).result(str(uuid.uuid4()))
        expect(message.put_content.node).count(3)
        expect(message.put_content.previous_hash)
        expect(message.put_content.hash).count(2)
        expect(message.put_content.crc32)
        expect(message.put_content.size).count(2)
        expect(message.put_content.deflated_size)
        expect(message.put_content.magic_hash)
        expect(message.put_content.upload_id).count(2)

        self.patch(server.PutContentResponse, 'sendMessage',
                   lambda *r: None)
        pc = server.PutContentResponse(storage_server, message)
        pc.id = 123

        # when PutContentResponse calls protocol.release(), it already
        # must have assigned the upload job
        assigned = []
        storage_server.release = lambda r: assigned.append(pc.upload_job)

        with mocker:
            yield pc._start()
        self.assertEqual(assigned[0], upload_job)

    def test_putcontent_bad_data(self):
        """Test putting bad data to a file."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        # insert bad data in the deflated_data
        deflated_data = deflated_data[:10] + 'break it' + deflated_data[10:]

        @defer.inlineCallbacks
        def test(client):
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            mkfile_req = yield client.make_file(
                request.ROOT, root, u'a_file.txt')
            yield client.put_content(request.ROOT, mkfile_req.new_id,
                                     NO_CONTENT_HASH, hash_value, crc32_value,
                                     size, deflated_size,
                                     StringIO(deflated_data))
        d = self.callback_test(test, add_default_callbacks=True)
        self.assertFails(d, 'UPLOAD_CORRUPT')
        return d

    def _get_users(self, max_storage_bytes):
        """Get both storage and content users."""
        s_user = self.obj_factory.make_user(
            max_storage_bytes=max_storage_bytes)
        c_user = content.User(self.service.factory.content, s_user.id,
                              s_user.root_volume_id, s_user.username,
                              s_user.visible_name)
        return s_user, c_user

    @defer.inlineCallbacks
    def test_putcontent_handle_error_in_uploadjob_deferred(self):
        """PutContent should handle errors in upload_job.deferred.

        Test that a PutContent fails and is terminated as soon we get an
        error, instead of wait until the full upload is done.
        """
        chunk_size = config.api_server.storage_chunk_size
        user, content_user = self._get_users(chunk_size ** 2)
        # create the file
        a_file = user.root.make_file(u"A new file")
        # build the upload data
        data = os.urandom(int(chunk_size * 1.5))
        size = len(data)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)

        # get a server instance
        storage_server = self.service.factory.buildProtocol('addr')
        storage_server.transport = StringTransport()
        # twisted 10.0.0 (lucid) returns an invalid peer in transport.getPeer()
        peerAddr = address.IPv4Address('TCP', '192.168.1.1', 54321)
        storage_server.transport.peerAddr = peerAddr
        storage_server.user = content_user
        storage_server.working_caps = server.PREFERRED_CAP

        message = protocol_pb2.Message()
        message.put_content.share = ''
        message.put_content.node = str(a_file.id)
        message.put_content.previous_hash = ''
        message.put_content.hash = hash_value
        message.put_content.crc32 = crc32_value
        message.put_content.size = size
        message.put_content.deflated_size = deflated_size
        message.id = 10
        message.type = protocol_pb2.Message.PUT_CONTENT

        begin_d = defer.Deferred()
        self.patch(server.PutContentResponse, 'sendMessage',
                   lambda *r: begin_d.callback(None))
        error_d = defer.Deferred()
        self.patch(
            server.PutContentResponse, 'sendError',
            lambda _, error, comment: error_d.callback((error, comment)))
        pc = server.PutContentResponse(storage_server, message)
        pc.id = 123

        # s3 put should fail early
        s3_class = self.service.factory.s3_class

        def s3_put(*args, **kwargs):
            """s3lib put that always fails."""
            factory = s3lib.contrib.http_client.HTTPStreamingClientFactory(
                'http://example.com', method='GET', postdata={}, headers={},
                agent="S3lib", streaming=True)
            factory.connect_deferred.callback(None)
            factory.deferred.errback(error.TimeoutError())
            return factory

        self.patch(s3_class, 'put', s3_put)

        # start uploading
        pc.start()
        # only one packet, in order to trigger the _start_receiving code
        # path.
        yield begin_d
        msg = protocol_pb2.Message()
        msg.type = protocol_pb2.Message.BYTES
        msg.bytes.bytes = deflated_data[:65536]
        pc._processMessage(msg)
        # check the error
        error_type, comment = yield error_d
        self.assertEqual(error_type, protocol_pb2.Error.TRY_AGAIN)
        self.assertEqual(
            comment,
            'TryAgain (TimeoutError: User timeout caused connection failure.)')
        # check that the put_content response is properly termintated
        yield pc.deferred
        self.assertTrue(pc.finished)

    @defer.inlineCallbacks
    def test_putcontent_skip_BYTES_on_error_in_uploadjob_deferred(self):
        """PutContent should skip BYTES after an error.

        Test that a PutContent fails and that BYTES messages are ignored after
        the error.
        """
        chunk_size = config.api_server.storage_chunk_size
        user, content_user = self._get_users(chunk_size ** 2)
        # create the file
        a_file = user.root.make_file(u"A new file")
        # build the upload data
        data = os.urandom(int(chunk_size * 1.5))
        size = len(data)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)

        # get a server instance
        storage_server = self.service.factory.buildProtocol('addr')
        storage_server.transport = StringTransport()
        # twisted 10.0.0 (lucid) returns an invalid peer in transport.getPeer()
        peerAddr = address.IPv4Address('TCP', '192.168.1.1', 54321)
        storage_server.transport.peerAddr = peerAddr
        storage_server.user = content_user
        storage_server.working_caps = server.PREFERRED_CAP

        message = protocol_pb2.Message()
        message.put_content.share = ''
        message.put_content.node = str(a_file.id)
        message.put_content.previous_hash = ''
        message.put_content.hash = hash_value
        message.put_content.crc32 = crc32_value
        message.put_content.size = size
        message.put_content.deflated_size = deflated_size
        message.id = 10
        message.type = protocol_pb2.Message.PUT_CONTENT

        error_d = defer.Deferred()
        self.patch(
            server.PutContentResponse, 'sendError',
            lambda _, error, comment: error_d.callback((error, comment)))
        pc = server.PutContentResponse(storage_server, message)
        pc.id = 123

        # s3 put should fail early
        def s3_put(*args, **kwargs):
            """s3lib put that always fails."""
            factory = s3lib.contrib.http_client.HTTPStreamingClientFactory(
                'http://example.com', method='GET', postdata={}, headers={},
                agent="S3lib", streaming=True)
            factory.deferred.errback(error.TimeoutError())
            return factory

        self.patch(content.UploadJob, '_start_receiving',
                   lambda *r: defer.fail(error.TimeoutError()))

        # start uploading
        pc.start()
        # check the error
        error_type, comment = yield error_d
        self.assertEquals(error_type, protocol_pb2.Error.TRY_AGAIN)
        self.assertEqual(
            comment,
            'TryAgain (TimeoutError: User timeout caused connection failure.)')
        # check that the put_content response is properly termintated
        yield pc.deferred
        self.assertTrue(pc.finished)

    @defer.inlineCallbacks
    def _get_client_helper(self, auth_token):
        """Simplify the testing code by getting a client for the user."""
        connect_d = defer.Deferred()
        factory = FactoryHelper(connect_d.callback, caps=server.PREFERRED_CAP)
        connector = reactor.connectTCP("localhost", self.port, factory)
        client = yield connect_d
        yield client.dummy_authenticate(auth_token)
        defer.returnValue((client, connector))

    @defer.inlineCallbacks
    def test_putcontent_reuse_content_different_user_no_magic(self):
        """Different user with no magic hash: upload everything again."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        mhash_object = magic_hash_factory()
        mhash_object.update(data)

        client, connector = yield self._get_client_helper("open sesame")
        root = yield client.get_root()

        # first file, it should ask for all the content not magic here
        mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
        upload_req = self._put_content(client, mkfile_req.new_id,
                                       hash_value, crc32_value, size,
                                       deflated_data, None)
        yield upload_req.deferred

        # startup another client for a different user (on the same shard)
        client.kill()
        connector.disconnect()
        client, connector = yield self._get_client_helper("usr3")
        root = yield client.get_root()
        mkfile_req = yield client.make_file(request.ROOT, root, 'chau')
        upload_req = self._put_content(client, mkfile_req.new_id,
                                       hash_value, crc32_value, size,
                                       deflated_data, None)
        yield upload_req.deferred

        # the BEGIN_CONTENT should be from 0
        message = [m for m in client.messages
                   if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
        self.assertEqual(message.begin_content.offset, 0)

        # check all went ok by getting the content
        get_req = yield client.get_content(request.ROOT,
                                           mkfile_req.new_id, hash_value)
        self.assertEqual(get_req.data, deflated_data)
        client.kill()
        connector.disconnect()

    @defer.inlineCallbacks
    def test_putcontent_reuse_content_different_user_with_magic(self):
        """Different user but with magic hash: don't upload all again."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        mhash_object = magic_hash_factory()
        mhash_object.update(data)
        mhash_value = mhash_object.content_hash()._magic_hash

        client, connector = yield self._get_client_helper("open sesame")
        root = yield client.get_root()

        # first file, it should ask for all the content not magic here
        mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
        upload_req = self._put_content(client, mkfile_req.new_id, hash_value,
                                       crc32_value, size, deflated_data, None)
        yield upload_req.deferred

        # hook to test stats (don't have graphite in dev)
        meter = []
        self.service.factory.metrics.meter = lambda *a: meter.append(a)
        gauge = []
        self.service.factory.metrics.gauge = lambda *a: gauge.append(a)

        # startup another client for a different user.
        # this user is on the same shard
        client.kill()
        connector.disconnect()
        client, connector = yield self._get_client_helper("usr3")
        root = yield client.get_root()

        mkfile_req = yield client.make_file(request.ROOT, root, 'chau')
        upload_req = self._put_content(client, mkfile_req.new_id, hash_value,
                                       crc32_value, size, deflated_data,
                                       mhash_value)
        resp = yield upload_req.deferred

        #the response should have the new_generation
        self.assertEquals(mkfile_req.new_generation + 1, resp.new_generation)

        # the BEGIN_CONTENT should be from the end
        message = [m for m in client.messages
                   if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
        self.assertEqual(message.begin_content.offset, deflated_size)

        # check all went ok by getting the content
        get_req = yield client.get_content(request.ROOT,
                                           mkfile_req.new_id, hash_value)
        self.assertEqual(get_req.data, deflated_data)
        # check reused content stat
        self.assertTrue(('MagicUploadJob.upload', deflated_size) in gauge)
        self.assertTrue(('MagicUploadJob.upload.begin', 1) in meter)
        client.kill()
        connector.disconnect()

    @defer.inlineCallbacks
    def test_putcontent_reuse_content_same_user_no_magic(self):
        """Same user doesn't upload everything even with no hash."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def auth(client):
            """Start authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            # first file, it should ask for all the content not magic here
            mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
            upload_req = self._put_content(client, mkfile_req.new_id,
                                           hash_value, crc32_value, size,
                                           deflated_data, None)
            yield upload_req.deferred

            # the BEGIN_CONTENT should be from 0
            message = [m for m in client.messages
                       if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
            self.assertEqual(message.begin_content.offset, 0)

            # hook to test stats (don't have graphite in dev)
            meter = []
            self.service.factory.metrics.meter = lambda *a: meter.append(a)
            gauge = []
            self.service.factory.metrics.gauge = lambda *a: gauge.append(a)
            client.messages = []

            # other file but same content, still no magic
            mkfile_req = yield client.make_file(request.ROOT, root, 'chau')
            upload_req = self._put_content(client, mkfile_req.new_id,
                                           hash_value, crc32_value, size,
                                           deflated_data, None)
            resp = yield upload_req.deferred

            # response has the new generation in it
            self.assertEqual(
                resp.new_generation, mkfile_req.new_generation + 1)

            # the BEGIN_CONTENT should be from the end
            message = [m for m in client.messages
                       if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
            self.assertEqual(message.begin_content.offset, deflated_size)

            # check all went ok by getting the content
            get_req = yield client.get_content(request.ROOT,
                                               mkfile_req.new_id, hash_value)
            self.assertEqual(get_req.data, deflated_data)
            # check reused content stat
            self.assertTrue(('MagicUploadJob.upload', deflated_size) in gauge)
            self.assertTrue(('MagicUploadJob.upload.begin', 1) in meter)

        yield self.callback_test(auth, add_default_callbacks=True)

    def _put_content(self, client, new_id, hash_value, crc32_value,
                     size, deflated_data, magic_hash):
        """Put content to a file."""
        return client.put_content_request(
            request.ROOT, new_id, NO_CONTENT_HASH, hash_value, crc32_value,
            size, len(deflated_data), StringIO(deflated_data),
            magic_hash=magic_hash)

    @defer.inlineCallbacks
    def test_putcontent_reuse_content_same_user_with_magic(self):
        """Same user with magic hash: of course no new upload is needed."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        mhash_object = magic_hash_factory()
        mhash_object.update(data)
        mhash_value = mhash_object.content_hash()._magic_hash

        @defer.inlineCallbacks
        def auth(client):
            """Start authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            # first file, it should ask for all the content not magic here
            mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
            upload_req = self._put_content(client, mkfile_req.new_id,
                                           hash_value, crc32_value, size,
                                           deflated_data, mhash_value)
            yield upload_req.deferred

            # the BEGIN_CONTENT should be from 0
            message = [m for m in client.messages
                       if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
            self.assertEqual(message.begin_content.offset, 0)

            meter = []
            self.service.factory.metrics.meter = lambda *a: meter.append(a)
            gauge = []
            self.service.factory.metrics.gauge = lambda *a: gauge.append(a)
            client.messages = []

            # another file but same content, still no upload
            mkfile_req = yield client.make_file(request.ROOT, root, 'chau')
            upload_req = self._put_content(client, mkfile_req.new_id,
                                           hash_value, crc32_value, size,
                                           deflated_data, mhash_value)
            resp = yield upload_req.deferred

            # response has the new generation in it
            self.assertEqual(
                resp.new_generation, mkfile_req.new_generation + 1)

            # the BEGIN_CONTENT should be from the end
            message = [m for m in client.messages
                       if m.type == protocol_pb2.Message.BEGIN_CONTENT][0]
            self.assertEqual(message.begin_content.offset, deflated_size)

            # check all went ok by getting the content
            get_req = yield client.get_content(request.ROOT,
                                               mkfile_req.new_id, hash_value)
            self.assertEqual(get_req.data, deflated_data)
            # check reused content stat
            self.assertTrue(('MagicUploadJob.upload', deflated_size) in gauge)
            self.assertTrue(('MagicUploadJob.upload.begin', 1) in meter)

        yield self.callback_test(auth, add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_putcontent_magic_hash(self):
        """Test that it calculated and stored the magic hash on put content."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        magic_hash_value = get_magic_hash(data)

        @defer.inlineCallbacks
        def auth(client):
            """Start authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
            yield client.put_content(request.ROOT, mkfile_req.new_id,
                                     NO_CONTENT_HASH, hash_value, crc32_value,
                                     size, deflated_size,
                                     StringIO(deflated_data))

            content_blob = self.usr0.volume().get_content(hash_value)
            self.assertEqual(content_blob.magic_hash, magic_hash_value)

        yield self.callback_test(auth, add_default_callbacks=True)

    def test_putcontent_blob_exists(self):
        """Test putting content with an existing blob (no magic)."""
        data = "*" * 100
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        # create the content blob without a magic hash in a different
        # user.
        self.obj_factory.make_user(
            100, u'my_user', u'', 2 ** 20, shard_id=u'shard0')
        self.usr3.make_filepath_with_content(
            u"~/Ubuntu One/file.txt", hash_value, crc32_value, size,
            deflated_size, uuid.uuid4())

        # overwrite UploadJob method to detect if it connected and
        # uploaded stuff to S3 (it shouldn't)
        self.patch(content.BaseUploadJob, '_start_receiving',
                   lambda s: defer.fail(Exception("This shouldn't be called")))

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()
            req = yield client.make_file(request.ROOT, root_id, "hola")
            yield client.put_content(request.ROOT, req.new_id, NO_CONTENT_HASH,
                                     hash_value, crc32_value, size,
                                     deflated_size, StringIO(deflated_data))

            # check it has content ok
            self.usr0.volume().get_content(hash_value)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_putcontent_buffer_limit(self):
        """Put content hitting the buffer limit."""
        self.patch(config.api_server, 'multipart_threshold', 1024 * 1024)
        self.patch(config.api_server, 'upload_buffer_max_size', 1024 * 10)
        size = config.api_server.multipart_threshold // 2
        self.usr0.update(max_storage_bytes=size * 2)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        # patch the S3Producer to never resume and buffer everything
        self.patch(S3Producer, 'resumeProducing', lambda s: None)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            # create the dir
            root_id = yield client.get_root()
            make_req = yield client.make_file(request.ROOT, root_id, "hola")

            # put content and check
            args = (request.ROOT, make_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            try:
                yield client.put_content(*args)
            except protoerrors.TryAgainError:
                pass
            else:
                self.fail('Should fail with TryAgainError.')
        return self.callback_test(test, add_default_callbacks=True)

    def test_put_content_on_a_dir_normal(self):
        """Test putting content in a dir."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        file_obj = StringIO(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()
            make_req = yield client.make_dir(request.ROOT, root_id, "hola")
            d = client.put_content(request.ROOT, make_req.new_id,
                                   NO_CONTENT_HASH, hash_value, crc32_value,
                                   size, deflated_size, file_obj)
            yield self.assertFailure(d, protoerrors.NoPermissionError)
        return self.callback_test(test, add_default_callbacks=True)

    def test_put_content_on_a_dir_magic(self):
        """Test putting content in a dir."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        file_obj = StringIO(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()

            # create a normal file
            make_req = yield client.make_file(request.ROOT, root_id, "hola")
            yield client.put_content(request.ROOT, make_req.new_id,
                                     NO_CONTENT_HASH, hash_value, crc32_value,
                                     size, deflated_size, file_obj)

            # create a dir and trigger a putcontent that will use 'magic'
            make_req = yield client.make_dir(request.ROOT, root_id, "chau")
            d = client.put_content(request.ROOT, make_req.new_id,
                                   NO_CONTENT_HASH, hash_value, crc32_value,
                                   size, deflated_size, file_obj)
            yield self.assertFailure(d, protoerrors.NoPermissionError)
        return self.callback_test(test, add_default_callbacks=True)


class TestMultipartPutContent(TestWithDatabase):
    """Test put_content using multipart command."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set up."""
        self.handler = MementoHandler()
        self.handler.setLevel(0)
        self._logger = logging.getLogger('storage.server')
        self._logger.addHandler(self.handler)
        yield super(TestMultipartPutContent, self).setUp()
        # override defaults set by TestWithDatabase.setUp.
        self.patch(config.api_server, 'multipart_threshold', 1024 * 10)
        self.patch(config.api_server, 'storage_chunk_size', 1024)

    @defer.inlineCallbacks
    def tearDown(self):
        """Tear down."""
        self._logger.removeHandler(self.handler)
        yield super(TestMultipartPutContent, self).tearDown()

    def get_data(self, size):
        """Return random data of the specified size."""
        return os.urandom(size)

    @defer.inlineCallbacks
    def _test_putcontent(self, num_files=1, size=1024 * 1024):
        """Test putting content to a file."""
        data = self.get_data(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def auth(client):
            """Authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            # hook to test stats (don't have graphite in dev)
            meter = []
            self.service.factory.metrics.meter = lambda *a: meter.append(a)
            gauge = []
            self.service.factory.metrics.gauge = lambda *a: gauge.append(a)

            for i in range(num_files):
                fname = 'hola_%d' % i
                mkfile_req = yield client.make_file(request.ROOT, root, fname)
                yield client.put_content(request.ROOT, mkfile_req.new_id,
                                         NO_CONTENT_HASH, hash_value,
                                         crc32_value, size, deflated_size,
                                         StringIO(deflated_data))

                try:
                    self.usr0.volume().get_content(hash_value)
                except errors.DoesNotExist:
                    raise ValueError("content blob is not there")
                # check upload stat and log, with the offset sent
                self.assertIn(('MultipartUploadJob.upload', 0), gauge)
                self.assertIn(('MultipartUploadJob.upload.begin', 1), meter)
                self.assertTrue(self.handler.check_debug(
                    "MultipartUploadJob begin content from offset 0"))

        yield self.callback_test(auth, timeout=self.timeout,
                                 add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_putcontent_multipart(self):
        """Test putcontent with multipart upload job."""
        # tune the config for this tests
        size = int(config.api_server.multipart_threshold * 1.5)
        self.usr0.update(max_storage_bytes=size * 2)
        yield self._test_putcontent(size=size)

    @defer.inlineCallbacks
    def test_resume_putcontent(self):
        """Test that the client can resume a putcontent request."""
        self.patch(config.api_server, 'multipart_threshold', 1024 * 512)
        self.patch(config.api_server, 'storage_chunk_size', 1024 * 64)
        chunk_size = config.api_server.storage_chunk_size
        size = config.api_server.multipart_threshold * 2
        self.usr0.update(max_storage_bytes=size * 2)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        # setup
        connect_d = defer.Deferred()
        factory = FactoryHelper(connect_d.callback, caps=server.PREFERRED_CAP)
        connector = reactor.connectTCP("localhost", self.port, factory)
        client = yield connect_d
        yield client.dummy_authenticate("open sesame")

        # hook to test stats (don't have graphite in dev)
        meter = []
        self.service.factory.metrics.meter = lambda *a: meter.append(a)
        gauge = []
        self.service.factory.metrics.gauge = lambda *a: gauge.append(a)

        called = []
        # patch part_done callback to know when the parts are done
        orig_part_done = content.MultipartUploadJob.part_done_callback

        @defer.inlineCallbacks
        def part_done(*args, **kwargs):
            if len(called) == 0:
                yield orig_part_done(*args, **kwargs)
            called.append(1)

        self.patch(content.MultipartUploadJob, 'part_done_callback', part_done)

        # patch BytesMessageProducer in order to avoid sending the whole file
        orig_go = sp_client.BytesMessageProducer.go

        def my_go(myself):
            data = myself.fh.read(request.MAX_PAYLOAD_SIZE)
            if len(called) >= 1:
                myself.request.error(EOFError("finish!"))
                myself.producing = False
                myself.finished = True
                return
            if data:
                response = protocol_pb2.Message()
                response.type = protocol_pb2.Message.BYTES
                response.bytes.bytes = data
                myself.request.sendMessage(response)
                reactor.callLater(0.1, myself.go)

        self.patch(sp_client.BytesMessageProducer, 'go', my_go)
        # we are authenticated
        root = yield client.get_root()
        filename = 'hola_12'
        mkfile_req = yield client.make_file(request.ROOT, root, filename)
        upload_id = []
        try:
            yield client.put_content(
                request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                hash_value, crc32_value, size, deflated_size,
                StringIO(deflated_data),
                upload_id_cb=upload_id.append)
        except EOFError:
            # check upload stat and log, with the offset sent,
            # first time tarts from beginning.
            self.assertTrue(('MultipartUploadJob.upload', 0) in gauge)
            self.assertTrue(('MultipartUploadJob.upload.begin', 1) in meter)
            self.assertTrue(self.handler.check_debug("MultipartUploadJob "
                            "begin content from offset 0"))
        else:
            self.fail("Should raise EOFError.")
        yield client.kill()
        yield connector.disconnect()

        # restore the BytesMessageProducer.go method
        self.patch(sp_client.BytesMessageProducer, 'go', orig_go)

        # connect a new client and try to upload again
        connect_d = defer.Deferred()
        factory = FactoryHelper(connect_d.callback, caps=server.PREFERRED_CAP)
        connector = reactor.connectTCP("localhost", self.port, factory)
        client = yield connect_d
        yield client.dummy_authenticate("open sesame")

        # restore patched client
        self.patch(sp_client.BytesMessageProducer, 'go', orig_go)

        processMessage = sp_client.PutContent.processMessage

        begin_content_d = defer.Deferred()

        def new_processMessage(myself, message):
            if message.type == protocol_pb2.Message.BEGIN_CONTENT:
                begin_content_d.callback(message)
            # call the original processMessage method
            return processMessage(myself, message)

        self.patch(sp_client.PutContent, 'processMessage', new_processMessage)
        req = sp_client.PutContent(
            client, request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
            hash_value, crc32_value, size, deflated_size,
            StringIO(deflated_data), upload_id=str(upload_id[0]))
        req.start()
        yield req.deferred

        message = yield begin_content_d
        # check that the offset is one chunk
        self.assertEqual(chunk_size, message.begin_content.offset)
        self.assertEqual(message.begin_content.upload_id, '')
        offset_sent = message.begin_content.offset
        try:
            node_content = self.usr0.volume().get_content(hash_value)
        except errors.DoesNotExist:
            raise ValueError("content blob is not there")
        self.assertEqual(node_content.crc32, crc32_value)
        self.assertEqual(node_content.size, size)
        self.assertEqual(node_content.deflated_size, deflated_size)
        self.assertEqual(node_content.hash, hash_value)
        self.assertTrue(node_content.storage_key)

        # check upload stat and log, with the offset sent, second time it
        # resumes from the first chunk
        self.assertTrue(('MultipartUploadJob.upload', offset_sent) in gauge)
        self.assertTrue(self.handler.check_debug("MultipartUploadJob "
                        "begin content from offset %d" % offset_sent))

        yield client.kill()
        yield connector.disconnect()

    @defer.inlineCallbacks
    def test_resume_putcontent_invalid_upload_id(self):
        """Client try to resume with an invalid upload_id.

        It receives a new upload_id.
        """
        self.patch(config.api_server, 'multipart_threshold', 1024 * 128)
        self.patch(config.api_server, 'storage_chunk_size', 1024 * 32)
        size = config.api_server.multipart_threshold * 2
        self.usr0.update(max_storage_bytes=size * 2)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        # hook to test stats (don't have graphite in dev)
        meter = []
        self.service.factory.metrics.meter = lambda *a: meter.append(a)
        gauge = []
        self.service.factory.metrics.gauge = lambda *a: gauge.append(a)

        @defer.inlineCallbacks
        def auth(client):
            """Make authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
            upload_id = []
            req = client.put_content_request(
                request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                hash_value, crc32_value, size, deflated_size,
                StringIO(deflated_data), upload_id="invalid id",
                upload_id_cb=upload_id.append)
            yield req.deferred
            self.assertTrue(('MultipartUploadJob.upload', 0) in gauge)
            self.assertTrue(('MultipartUploadJob.upload.begin', 1) in meter)
            self.assertTrue(self.handler.check_debug(
                "MultipartUploadJob begin content from offset 0"))
            self.assertEqual(len(upload_id), 1)
            self.assertIsInstance(uuid.UUID(upload_id[0]), uuid.UUID)

        yield self.callback_test(auth, add_default_callbacks=True)

    @defer.inlineCallbacks
    def test_putcontent_magic_hash(self):
        """Test that it calculated and stored the magic hash on put content."""
        data = os.urandom(30000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        magic_hash_value = get_magic_hash(data)

        @defer.inlineCallbacks
        def auth(client):
            """Make authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            mkfile_req = yield client.make_file(request.ROOT, root, 'hola')
            yield client.put_content(request.ROOT, mkfile_req.new_id,
                                     NO_CONTENT_HASH, hash_value, crc32_value,
                                     size, deflated_size,
                                     StringIO(deflated_data))

            content_blob = self.usr0.volume().get_content(hash_value)
            self.assertEqual(content_blob.magic_hash, magic_hash_value)
        yield self.callback_test(auth, add_default_callbacks=True)

    def test_putcontent_corrupt(self):
        """Put content on a file with corrupt data."""
        self.patch(config.api_server, 'multipart_threshold', 1024 * 512)
        self.patch(config.api_server, 'storage_chunk_size', 1024 * 64)
        size = config.api_server.multipart_threshold * 2
        self.usr0.update(max_storage_bytes=size * 2)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data) + 10
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")

            # create the dir
            root_id = yield client.get_root()
            make_req = yield client.make_file(request.ROOT, root_id, "hola")

            # put content and check
            args = (request.ROOT, make_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            try:
                putc_req = client.put_content_request(*args)
                yield putc_req.deferred
            except Exception as ex:
                self.assertTrue(isinstance(ex, protoerrors.UploadCorruptError))
            self.assertTrue(self.handler.check_debug('UploadCorrupt',
                                                     str(size)))
             # check that the uploadjob was deleted.
            node = self.usr0.volume(None).get_node(make_req.new_id)
            self.assertRaises(errors.DoesNotExist,
                              node.get_multipart_uploadjob, putc_req.upload_id,
                              hash_value, crc32_value, size, deflated_size)

        return self.callback_test(test, add_default_callbacks=True)

    def test_putcontent_blob_exists(self):
        """Test putting content with an existing blob (no magic)."""
        data = self.get_data(1024 * 20)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        # create the content blob without a magic hash in a different
        # user.
        self.obj_factory.make_user(
            100, u'my_user', u'', 2 ** 20, shard_id=u'shard0')
        self.usr3.make_filepath_with_content(
            u"~/Ubuntu One/file.txt",
            hash_value, crc32_value, size, deflated_size, uuid.uuid4())

        def auth(client):

            def check_file(result):
                return threads.deferToThread(
                    lambda: self.usr0.volume().get_content(hash_value))

            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.get_root())
            d.addCallback(lambda root_id:
                          client.make_file(request.ROOT, root_id, "hola"))
            d.addCallback(self.save_req, "file")
            d.addCallback(lambda req: client.put_content(
                request.ROOT, req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_putcontent_pause_client_transport(self):
        """Put content hitting the buffer limit pause client transport."""
        self.patch(config.api_server, 'multipart_threshold', 1024 * 512)
        self.patch(config.api_server, 'storage_chunk_size', 1024 * 64)
        self.patch(config.api_server, 'upload_buffer_max_size', 1024 * 10)
        size = config.api_server.multipart_threshold * 2
        self.usr0.update(max_storage_bytes=size * 2)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        # patch MultipartUploadJob.registerProducer to intercept
        # calls to the transport
        called = []

        def collect(f):
            """Decorator to collect calls."""

            def wrapper():
                """The wrapper."""
                called.append(f.im_func.func_name)
                return f()

            return wrapper

        orig_registerProducer = content.MultipartUploadJob.registerProducer

        def registerProducer(s, p):
            """registerProducer that also decorates the transport."""
            p.pauseProducing = collect(p.pauseProducing)
            p.resumeProducing = collect(p.resumeProducing)
            return orig_registerProducer(s, p)

        self.patch(content.MultipartUploadJob, 'registerProducer',
                   registerProducer)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            # create the dir
            root_id = yield client.get_root()
            make_req = yield client.make_file(request.ROOT, root_id, "hola")

            # put content and check
            args = (request.ROOT, make_req.new_id, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data))
            yield client.put_content(*args)
            self.assertIn('pauseProducing', called)
            self.assertIn('resumeProducing', called)
            paused = len([i for i in called if 'pause' in i])
            resumed = len([i for i in called if 'resume' in i])
            self.assertTrue(
                paused <= resumed,
                "We should have equal or more calls to resumeProducing")
        return self.callback_test(test, add_default_callbacks=True)

    def test_put_content_on_a_dir(self):
        """Test putting content in a dir."""
        data = os.urandom(300000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)
        file_obj = StringIO(deflated_data)

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()
            make_req = yield client.make_dir(request.ROOT, root_id, "hola")
            d = client.put_content(request.ROOT, make_req.new_id,
                                   NO_CONTENT_HASH, hash_value, crc32_value,
                                   size, deflated_size, file_obj)
            yield self.assertFailure(d, protoerrors.NoPermissionError)
        return self.callback_test(test, add_default_callbacks=True)


class TestMultipartPutContentGoodCompression(TestMultipartPutContent):
    """TestMultipartPutContent using data with a good compression ratio."""

    def get_data(self, size):
        """Return zero data of the specified size."""
        with open('/dev/zero', 'r') as source:
            return source.read(size) + os.urandom(size)


class TestPutContentInternalError(TestWithDatabase):
    """Test put_content command."""

    createOOPSFiles = True

    @defer.inlineCallbacks
    def test_putcontent_handle_internal_error_in_uploadjob_deferred(self):
        """PutContent should handle errors in upload_job.deferred.

        Test that a PutContent fails and is terminated as soon we get an
        error, instead of wait until the full upload is done.
        """
        chunk_size = config.api_server.storage_chunk_size
        user = self.create_user(max_storage_bytes=chunk_size ** 2)
        content_user = content.User(self.service.factory.content, user.id,
                                    user.root_volume_id, user.username,
                                    user.visible_name)
        # create the file
        a_file = user.root.make_file(u"A new file")
        # build the upload data
        data = os.urandom(int(chunk_size * 1.5))
        size = len(data)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)

        # get a server instance
        storage_server = self.service.factory.buildProtocol('addr')
        storage_server.transport = StringTransport()
        # twisted 10.0.0 (lucid) returns an invalid peer in transport.getPeer()
        peerAddr = address.IPv4Address('TCP', '192.168.1.1', 54321)
        storage_server.transport.peerAddr = peerAddr
        storage_server.user = content_user
        storage_server.working_caps = server.PREFERRED_CAP

        message = protocol_pb2.Message()
        message.put_content.share = ''
        message.put_content.node = str(a_file.id)
        message.put_content.previous_hash = ''
        message.put_content.hash = hash_value
        message.put_content.crc32 = crc32_value
        message.put_content.size = size
        message.put_content.deflated_size = deflated_size
        message.id = 10
        message.type = protocol_pb2.Message.PUT_CONTENT

        begin_d = defer.Deferred()
        self.patch(server.PutContentResponse, 'sendMessage',
                   lambda *r: begin_d.callback(None))
        error_d = defer.Deferred()
        self.patch(
            server.PutContentResponse, 'sendError',
            lambda _, error, comment: error_d.callback((error, comment)))
        pc = server.PutContentResponse(storage_server, message)
        pc.id = 123

        # s3 put should fail early
        s3_class = self.service.factory.s3_class

        def s3_put(*args, **kwargs):
            """s3lib put that always fails."""
            factory = s3lib.contrib.http_client.HTTPStreamingClientFactory(
                'http://example.com', method='GET', postdata={}, headers={},
                agent="S3lib", streaming=True)
            factory.connect_deferred.callback(None)
            factory.deferred.errback(ValueError("Fail!"))
            return factory

        self.patch(s3_class, 'put', s3_put)

        # start uploading
        pc.start()
        # only one packet, in order to trigger the _start_receiving code
        # path.
        yield begin_d
        msg = protocol_pb2.Message()
        msg.type = protocol_pb2.Message.BYTES
        msg.bytes.bytes = deflated_data[:65536]
        pc._processMessage(msg)
        # check the error
        error_type, comment = yield error_d
        self.assertEquals(error_type, protocol_pb2.Error.INTERNAL_ERROR)
        self.assertEquals(comment, "Fail!")
        # check that the put_content response is properly termintated
        # and the server is shuttdown.
        yield storage_server.wait_for_shutdown()
        self.assertTrue(pc.finished)
        self.assertTrue(storage_server.shutting_down)
        # check the generated oops
        oopses = list(self.get_oops())
        self.assert_(1, len(oopses) > 0)
        with open(oopses[0], 'r') as f:
            oops_data = serializer.read(f)
        self.assertEqual("ValueError", oops_data["type"])
        self.assertEqual("Fail!", oops_data["value"])

    @defer.inlineCallbacks
    def test_putcontent_handle_error_in_sendok(self):
        """PutContent should handle errors in send_ok."""
        data = os.urandom(1000)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        @defer.inlineCallbacks
        def auth(client):
            """Authenticated test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()

            mkfile_req = yield client.make_file(request.ROOT, root, "hola")

            def breakit(*a):
                """Raise an exception to simulate the method call failed."""
                raise MemoryError("Simulated ME")

            self.patch(server.PutContentResponse, "_commit_uploadjob", breakit)

            d = client.put_content(
                request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data))
            yield self.assertFailure(d, protoerrors.InternalError)

        yield self.callback_test(auth, add_default_callbacks=True)


class TestChunkedContent(TestWithDatabase):
    """ Tests operation on large data that requires multiple chunks """

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestChunkedContent, self).setUp()
        # tune the config for this tests
        self.patch(config.api_server, 'multipart_threshold', 1024 * 1024 * 5)
        self.patch(config.api_server, 'storage_chunk_size', 1024 * 1024)

    @defer.inlineCallbacks
    def tearDown(self):
        yield super(TestChunkedContent, self).tearDown()

    def test_putcontent_chunked(self, put_fail=False, get_fail=False):
        """Checks a chunked putcontent."""
        size = int(config.api_server.storage_chunk_size * 1.5)
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)

        def auth(client):

            def raise_quota(_):

                # we need a bigger quota for this test...
                def _raise_quota(new_size):
                    self.usr0.update(max_storage_bytes=new_size)
                d = threads.deferToThread(_raise_quota, size * 2)
                return d

            def check_content(content):
                self.assertEqual(content.data, deflated_data)

            def _put_fail(result):
                # this will allow the server to split the data into chunks but
                # fail to put it back together in a single blob
                if put_fail == 400:
                    self.s4_site.resource.fail_next_put(
                        error=http.BAD_REQUEST, message="RequestTimeout")
                elif put_fail:
                    self.s4_site.resource.fail_next_put()
                return result

            def _get_fail(result):
                # this will allow the server to split the data into chunks but
                # fail to put it back together in a single blob
                if get_fail:
                    self.s4_site.resource.fail_next_get()
                return result

            d = client.dummy_authenticate("open sesame")
            d.addCallback(raise_quota)
            d.addCallback(lambda _: client.get_root())
            d.addCallback(lambda root_id: client.make_file(request.ROOT,
                                                           root_id, "hola"))
            d.addCallback(self.save_req, 'req')
            d.addCallback(_put_fail)
            d.addCallback(_get_fail)
            d.addCallback(lambda mkfile_req: client.put_content(
                request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH, hash_value,
                crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallback(lambda _: client.get_content(request.ROOT,
                                                       self._state.req.new_id,
                                                       hash_value))
            if not put_fail and not get_fail:
                d.addCallback(check_content)
            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(auth, timeout=10)

    def test_putcontent_chunked_putfail_500(self):
        '''Assures that chunked putcontent fails with "try again".'''
        d = self.test_putcontent_chunked(put_fail=True)
        self.assertFails(d, 'TRY_AGAIN')
        return d

    def test_putcontent_chunked_getfail_500(self):
        '''Assures that chunked putcontent fails with "try again".'''
        d = self.test_putcontent_chunked(get_fail=True)
        self.assertFails(d, 'TRY_AGAIN')
        return d

    @defer.inlineCallbacks
    def test_putcontent_chunked_putfail_400(self):
        '''Assures that chunked putcontent fails with "try again".'''
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.WARNING)
        logger.addHandler(hdlr)
        try:
            yield self.test_putcontent_chunked(put_fail=400)
        except protoerrors.TryAgainError:
            # check that we have all the expected info in the logs
            log_msg = hdlr.records[-1].message
            uuid_regexp = "[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+"
            self.assertTrue(re.match(uuid_regexp, log_msg))
        else:
            self.fail("Should fail with TryAgain")
        finally:
            logger.removeHandler(hdlr)


class UserTest(TestWithDatabase):
    """Test User functionality."""

    @defer.inlineCallbacks
    def setUp(self):
        yield super(UserTest, self).setUp()

        # user and root to use in the tests
        u = self.suser = self.create_user(max_storage_bytes=64 ** 2)
        self.user = content.User(self.service.factory.content, u.id,
                                 u.root_volume_id, u.username, u.visible_name)
        self.patch(config.api_server, 'multipart_threshold', 32 ** 2)

    @defer.inlineCallbacks
    def test_make_file_node_with_gen(self):
        """Test that make_file returns a node with generation in it."""
        root_id, root_gen = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        _, generation, _ = yield self.user.make_file(volume_id, root_id,
                                                     u"name", True)
        self.assertEqual(generation, root_gen + 1)

    @defer.inlineCallbacks
    def test_make_dir_node_with_gen(self):
        """Test that make_dir returns a node with generation in it."""
        root_id, root_gen = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        _, generation, _ = yield self.user.make_dir(volume_id, root_id,
                                                    u"name", True)
        self.assertEqual(generation, root_gen + 1)

    @defer.inlineCallbacks
    def test_unlink_node_with_gen(self):
        """Test that unlink returns a node with generation in it."""
        root_id, root_gen = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, generation, _ = yield self.user.make_dir(volume_id, root_id,
                                                          u"name", True)
        new_gen, kind, name, _ = yield self.user.unlink_node(
            volume_id, node_id)
        self.assertEqual(new_gen, generation + 1)
        self.assertEqual(kind, "Directory")
        self.assertEqual(name, u"name")

    @defer.inlineCallbacks
    def test_move_node_with_gen(self):
        """Test that move returns a node with generation in it."""
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        yield self.user.make_dir(volume_id, root_id, u"name", True)
        node_id, generation, _ = yield self.user.make_dir(volume_id, root_id,
                                                          u"name", True)
        new_generation, _ = yield self.user.move(volume_id, node_id,
                                                 root_id, u"new_name")
        self.assertEqual(new_generation, generation + 1)

    @defer.inlineCallbacks
    def test_get_upload_job(self):
        """Test for _get_upload_job."""
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, _, _ = yield self.user.make_file(volume_id, root_id,
                                                  u"name", True)
        size = config.api_server.multipart_threshold
        # this will create a new uploadjob
        upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size / 2, size / 4,
            True)
        self.assertTrue(isinstance(upload_job, content.UploadJob),
                        upload_job.__class__)

    @defer.inlineCallbacks
    def test_get_upload_job_multipart(self):
        """Test for _get_multipart_upload_job."""
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, _, _ = yield self.user.make_file(volume_id, root_id,
                                                  u"name", True)
        size = config.api_server.multipart_threshold
        # this will create a new uploadjob
        upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size * 2,
            True)
        self.assertTrue(isinstance(upload_job, content.MultipartUploadJob),
                        upload_job.__class__)
        # set the multipart_id so we resume it
        yield upload_job.uploadjob.set_multipart_id('foobar')
        # get the same uploadjob using the upload_id
        same_upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size * 2,
            True, upload_id=str(upload_job.upload_id))
        self.assertEqual(same_upload_job.uploadjob.uploadjob_id,
                         upload_job.uploadjob.uploadjob_id)
        self.assertTrue(same_upload_job.uploadjob.when_last_active
                        > upload_job.uploadjob.when_last_active)

    @defer.inlineCallbacks
    def test_get_upload_job_multipart_invalid_upload_id(self):
        """Test for _get_multipart_upload_job with an invalid upload_id."""
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, _, _ = yield self.user.make_file(volume_id, root_id,
                                                  u"name", True)
        size = config.api_server.multipart_threshold
        # this will create a new uploadjob
        upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size * 2,
            True, upload_id="hola")
        self.assertTrue(isinstance(upload_job, content.MultipartUploadJob),
                        upload_job.__class__)
        # set the multipart_id so we resume it
        yield upload_job.uploadjob.set_multipart_id('foobar')
        # get the same uploadjob using the upload_id
        same_upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size * 2,
            True, upload_id=str(upload_job.upload_id))
        self.assertEqual(same_upload_job.uploadjob.uploadjob_id,
                         upload_job.uploadjob.uploadjob_id)
        self.assertTrue(same_upload_job.uploadjob.when_last_active >
                        upload_job.uploadjob.when_last_active)

    @defer.inlineCallbacks
    def test_get_upload_job_big_file(self):
        """Test for get_upload_job.

        This is with a file size >= multipart_threshold but with a
        deflated_size < multipart_threshold.
        """
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, _, _ = yield self.user.make_file(volume_id, root_id,
                                                  u"name", True)
        size = config.api_server.multipart_threshold
        # this will create a new uploadjob
        upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size / 2,
            True)
        self.assertTrue(isinstance(upload_job, content.UploadJob),
                        upload_job.__class__)

    @defer.inlineCallbacks
    def test_resumable_upload_disabled(self):
        """Test that we can disable resumable uploads."""
        # disable multipart/resumable uploads
        self.patch(config.api_server, 'multipart_threshold', 0)
        root_id, _ = yield self.user.get_root()
        volume_id = yield self.user.get_volume_id(root_id)
        node_id, _, _ = yield self.user.make_file(volume_id, root_id,
                                                  u"name", True)
        size = 1024 * 512
        self.suser.update(max_storage_bytes=size * 5)
        # this will create a new uploadjob
        upload_job = yield self.user.get_upload_job(
            None, node_id, NO_CONTENT_HASH, 'foo', 10, size * 4, size, True)
        self.assertTrue(isinstance(upload_job, content.UploadJob),
                        upload_job.__class__)

    @defer.inlineCallbacks
    def test_get_free_bytes_root(self):
        """Get the user free bytes, normal case."""
        self.suser.update(max_storage_bytes=1000)
        fb = yield self.user.get_free_bytes()
        self.assertEqual(fb, 1000)

    @defer.inlineCallbacks
    def test_get_free_bytes_own_share(self):
        """Get the user free bytes asking for same user's share."""
        other_user = self.create_user(id=2, username=u'user2')
        share = self.suser.root.share(other_user.id, u"sharename")
        self.suser.update(max_storage_bytes=1000)
        fb = yield self.user.get_free_bytes(share.id)
        self.assertEqual(fb, 1000)

    @defer.inlineCallbacks
    def test_get_free_bytes_othershare_ok(self):
        """Get the user free bytes for other user's share."""
        other_user = self.create_user(id=2, username=u'user2',
                                      max_storage_bytes=500)
        share = other_user.root.share(self.suser.id, u"sharename")
        fb = yield self.user.get_free_bytes(share.id)
        self.assertEqual(fb, 500)

    @defer.inlineCallbacks
    def test_get_free_bytes_othershare_bad(self):
        """Get the user free bytes for a share of a user that is not valid."""
        other_user = self.create_user(id=2, username=u'user2',
                                      max_storage_bytes=500)
        share = other_user.root.share(self.suser.id, u"sharename")
        other_user.update(subscription=False)
        d = self.user.get_free_bytes(share.id)
        yield self.assertFailure(d, errors.DoesNotExist)


class TestUploadJob(TestWithDatabase):
    """Tests for UploadJob class."""

    upload_class = content.UploadJob

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestUploadJob, self).setUp()
        self.chunk_size = config.api_server.storage_chunk_size
        self.half_size = self.chunk_size / 2
        self.double_size = self.chunk_size * 2
        self.user = self.create_user(max_storage_bytes=self.chunk_size ** 2)
        self.content_user = content.User(self.service.factory.content,
                                         self.user.id,
                                         self.user.root_volume_id,
                                         self.user.username,
                                         self.user.visible_name)

        def slowScheduler(x):
            """A slower scheduler for our cooperator."""
            return reactor.callLater(0.1, x)

        self._cooperator = task.Cooperator(scheduler=slowScheduler)

    @defer.inlineCallbacks
    def tearDown(self):
        """Tear down."""
        self._cooperator.stop()
        yield super(TestUploadJob, self).tearDown()

    @defer.inlineCallbacks
    def make_upload(self, size):
        """Create the storage UploadJob object.

        @param size: the size of the upload
        @return: a tuple (deflated_data, hash_value, upload_job)
        """
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        magic_hash_value = get_magic_hash(data)
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)
        root, _ = yield self.content_user.get_root()
        c_user = self.content_user
        rpcdal = self.service.factory.content.rpcdal_client
        r = yield rpcdal.call(
            'make_file_with_content',
            user_id=c_user.id, volume_id=self.user.root_volume_id,
            parent_id=root, name=u"A new file",
            node_hash=model.EMPTY_CONTENT_HASH, crc32=0,
            size=0, deflated_size=0, storage_key=None)
        node_id = r['node_id']
        node = yield c_user.get_node(self.user.root_volume_id, node_id, None)
        upload_job = self.upload_class(c_user, node, node.content_hash,
                                       hash_value, crc32_value, size,
                                       deflated_size, None, False,
                                       magic_hash_value)
        defer.returnValue((deflated_data, hash_value, upload_job))

    @defer.inlineCallbacks
    def test_simple_upload(self):
        """Test UploadJob without scatter/gather."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        yield upload_job.commit(result)
        node_id = upload_job.file_node.id
        node = yield self.content_user.get_node(self.user.root_volume_id,
                                                node_id, None)
        self.assertEqual(node.content_hash, hash_value)

    @defer.inlineCallbacks
    def test_chunked_upload(self):
        """Test UploadJob with chunks."""
        size = self.double_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()

        # now let's upload some data
        def data_iter(chunk_size=request.MAX_MESSAGE_SIZE):
            """Iterate over chunks."""
            for part in xrange(0, len(deflated_data), chunk_size):
                yield upload_job.add_data(
                    deflated_data[part:part + chunk_size])

        self._cooperator.coiterate(data_iter())
        result = yield upload_job.deferred
        yield upload_job.commit(result)
        node_id = upload_job.file_node.id
        node = yield self.content_user.get_node(self.user.root_volume_id,
                                                node_id, None)
        self.assertEqual(node.content_hash, hash_value)
        f = upload_job.s3.get(config.api_server.s3_bucket,
                              upload_job._storage_key)
        s3_data = yield f.deferred
        self.assertEqual(s3_data, deflated_data)

    @defer.inlineCallbacks
    def test_upload_fails_with_400(self):
        """Test UploadJob failing with error 400."""
        size = self.double_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)

        def data_iter(data, chunk_size=request.MAX_MESSAGE_SIZE):
            """Iterate over chunks."""
            for part in xrange(0, len(data), chunk_size):
                yield upload_job.add_data(data[part:part + chunk_size])

        # add a log handler
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.WARNING)
        logger.addHandler(hdlr)
        yield upload_job.connect()
        # make the first put fail
        coop_task = self._cooperator.cooperate(data_iter(deflated_data))
        # inject a 400 error
        self.s4_site.resource.fail_next_put(error=http.BAD_REQUEST,
                                            message="RequestTimeout")
        try:
            yield upload_job.deferred
        except server.errors.TryAgain as e:
            self.assertEquals(
                str(e), 'Web error (status 400) while uploading data.')
            logger.removeHandler(hdlr)
            log_msg = hdlr.records[-1].message
            context_msg = (
                '%(session_ids)s - %(username)s (%(user_id)s) - '
                'node: %(volume_id)s::%(node_id)s - recv: %(recv)s - '
                'sent: %(sent)s')

            upload_context = dict(
                user_id=self.user.id,
                username=self.user.username.replace('%', '%%'),
                session_ids='No sessions?',
                volume_id=upload_job.file_node.volume_id,
                node_id=str(upload_job.file_node.id),
                recv=upload_job.producer.bytes_received,
                sent=upload_job.producer.bytes_sent)
            self.assertIn(context_msg % upload_context, log_msg)
            self.assertIn('<head><title>400 - RequestTimeout</title>',
                          log_msg)
            yield defer.succeed(None)
        else:
            yield defer.fail("Should fail with TryAgain")
        finally:
            # stop the CooperativeTask
            try:
                coop_task.stop()
            except task.TaskFinished:
                # ignore the error of the task finished
                pass

    @defer.inlineCallbacks
    def test_upload_s3_500(self):
        """Test UploadJob with a S3 failure."""
        size = int(self.chunk_size * 1.5)
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data[:self.chunk_size])
        self.s4_site.resource.fail_next_put()
        upload_job.add_data(deflated_data[self.chunk_size:])
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.WARNING)
        logger.addHandler(hdlr)
        try:
            yield upload_job.deferred
        except server.errors.TryAgain as e:
            self.assertEquals(
                str(e), 'Web error (status 500) while uploading data.')
            logger.removeHandler(hdlr)
            log_msg = hdlr.records[-1].message
            context_msg = (
                '%(session_ids)s - %(username)s (%(user_id)s) - '
                'node: %(volume_id)s::%(node_id)s - recv: %(recv)s - '
                'sent: %(sent)s')
            upload_context = dict(
                user_id=self.user.id,
                username=self.user.username.replace('%', '%%'),
                session_ids='No sessions?',
                volume_id=upload_job.file_node.volume_id,
                node_id=str(upload_job.file_node.id),
                recv=upload_job.producer.bytes_received,
                sent=upload_job.producer.bytes_sent)
            self.assertIn(context_msg % upload_context, log_msg)
            self.assertIn('<head><title>500 - Internal Server Error</title>',
                          log_msg)
            yield defer.succeed(None)
        else:
            yield defer.fail("Should fail with TryAgain")

    def test_upload_fail_with_conflict(self):
        """Test UploadJob conflict."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        # poison the upload
        upload_job.original_file_hash = "sha1:fakehash"
        try:
            yield upload_job.commit(result)
        except server.errors.ConflictError as e:
            self.assertEquals(str(e), 'The File changed while uploading.')
        else:
            yield defer.fail("Should fail with ConflictError")

    @defer.inlineCallbacks
    def test_upload_corrupted_deflated(self):
        """Test corruption of deflated data in UploadJob."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        # change the deflated data to trigger a UploadCorrupt error
        upload_job.add_data(deflated_data + '10')
        result = yield upload_job.deferred
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._deflated_size_hint_mismatch)
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    def test_upload_corrupted_inflated(self):
        """Test corruption of inflated data in UploadJob."""
        # now test corruption of the inflated data
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        # change the inflated size hint to trigger the error
        upload_job._set_inflated_size(10)
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._inflated_size_hint_mismatch)
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    def test_upload_corrupted_hash(self):
        """Test corruption of hash in UploadJob."""
        # now test corruption of the content hash hint
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        orig_hash_hint = self.upload_class.hash_hint
        yield upload_job.connect()
        self.upload_class.hash_hint = 'sha1:fakehash'
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._content_hash_hint_mismatch)
            self.upload_class.hash_hint = orig_hash_hint
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    def test_upload_corrupted_magic_hash(self):
        """Test corruption of magic hash in UploadJob."""
        # now test corruption of the content hash hint
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        orig_hash_hint = self.upload_class.hash_hint
        yield upload_job.connect()
        self.upload_class.magic_hash = 'sha1:fakehash'
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._content_hash_hint_mismatch)
            self.upload_class.hash_hint = orig_hash_hint
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    def test_upload_corrupted_crc32(self):
        """Test corruption of crc32 in UploadJob."""
        # now test corruption of the crc32 hint
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        orig_crc32_hint = self.upload_class.crc32_hint
        self.upload_class.crc32_hint = 'bad crc32'
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._crc32_hint_mismatch)
            self.upload_class.crc32_hint = orig_crc32_hint
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    @defer.inlineCallbacks
    def test_commit_return_node_with_gen(self):
        """Commit return the node with the updated generation."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        previous_generation = upload_job.file_node.generation
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        new_generation = yield upload_job.commit(result)
        self.assertEqual(new_generation, previous_generation + 1)

    @defer.inlineCallbacks
    def test_add_bad_data(self):
        """Test UploadJob.add_data with invalid data."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        self.assertRaises(server.errors.UploadCorrupt, upload_job.add_data,
                          'Neque porro quisquam est qui dolorem ipsum...')
        yield upload_job.cancel()

    def test_upload_id(self):
        """Test the upload_id generation."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        # regular upload job always has '' as the upload_id
        self.assertEqual(upload_job.upload_id, '')

    @defer.inlineCallbacks
    def test_buffers_size(self):
        """Test the buffers_size property."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        deflated_size = len(deflated_data)
        upload_job.add_data(deflated_data[:deflated_size / 2])
        upload_job.add_data(deflated_data[deflated_size / 2:])
        self.assertEqual(deflated_size, upload_job.buffers_size)
        self.assertEqual(deflated_size, upload_job.producer.buffer_size)
        result = yield upload_job.deferred
        yield upload_job.commit(result)
        node_id = upload_job.file_node.id
        node = yield self.content_user.get_node(self.user.root_volume_id,
                                                node_id, None)
        self.assertEqual(node.content_hash, hash_value)

    def test_stop_sets_canceling(self):
        """Set canceling on stop."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        assert not upload_job.canceling
        upload_job.stop()
        self.assertTrue(upload_job.canceling)

    @defer.inlineCallbacks
    def test_unregisterProducer_on_cancel(self):
        """unregisterProducer is never called"""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        mocker = Mocker()
        producer = mocker.mock()
        self.patch(upload_job, 'producer', producer)
        expect(producer.stopProducing())
        with mocker:
            yield upload_job.cancel()

    @defer.inlineCallbacks
    def test_unregisterProducer_on_stop(self):
        """unregisterProducer isn't called."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        mocker = Mocker()
        producer = mocker.mock()
        expect(producer.stopProducing())
        self.patch(upload_job, 'producer', producer)
        with mocker:
            yield upload_job.stop()

    @defer.inlineCallbacks
    def test_wait_for_s3_connection(self):
        """UploadJob.connect waits for s3 connection."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        client = upload_job.factory.http_client
        self.assertEqual(client.transport.connected, 1)
        self.assertTrue(upload_job.factory.connect_deferred.called)
        self.assertTrue(client.factory.connect_deferred.called)
        yield upload_job.cancel()


class TestMultipartUploadJob(TestUploadJob):
    """Tests for MultipartUploadJob class."""

    upload_class = content.MultipartUploadJob

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestMultipartUploadJob, self).setUp()
        # tune the config for this tests
        self.patch(config.api_server, 'multipart_threshold', 1024)
        self.patch(config.api_server, 'storage_chunk_size', 100)
        size = config.api_server.multipart_threshold * 2
        self.half_size = size
        self.double_size = size

    @defer.inlineCallbacks
    def tearDown(self):
        yield super(TestMultipartUploadJob, self).tearDown()

    @defer.inlineCallbacks
    def make_upload(self, size):
        """Create the storage UploadJob object.

        @param size: the size of the upload
        @return: a tuple (deflated_data, hash_value, upload_job)
        """
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        magic_hash_value = get_magic_hash(data)
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)
        root, _ = yield self.content_user.get_root()
        c_user = self.content_user
        rpcdal = self.service.factory.content.rpcdal_client
        r = yield rpcdal.call(
            'make_file_with_content', user_id=c_user.id,
            volume_id=self.user.root_volume_id, parent_id=root,
            name=u"A new file", node_hash=model.EMPTY_CONTENT_HASH, crc32=0,
            size=0, deflated_size=0, storage_key=None)
        node_id = r['node_id']
        node = yield c_user.get_node(self.user.root_volume_id, node_id, None)

        args = (c_user, self.user.root_volume_id, node_id, node.content_hash,
                hash_value, crc32_value, size,
                deflated_size, str(uuid.uuid4()))
        upload = yield content.DBUploadJob.make(*args)
        upload_job = self.upload_class(c_user, node, node.content_hash,
                                       hash_value, crc32_value, size,
                                       deflated_size, upload, None, False,
                                       magic_hash_value)
        defer.returnValue((deflated_data, hash_value, upload_job))

    @defer.inlineCallbacks
    def test_add_bad_data(self):
        """Test MultipartUploadJob.add_data with invalid data."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()

        # upload the whole thing, we don't check the data on multipart uploads
        # during the upload
        chunk_size = 512
        msg = 'Neque porro quisquam est qui dolorem ipsum...'
        for part in xrange(0, len(deflated_data), chunk_size):
            data = deflated_data[part:part + chunk_size]
            if part == chunk_size * 0:
                yield upload_job.add_data(msg * 100)
            else:
                yield upload_job.add_data(data)
        try:
            yield upload_job.deferred
        except server.errors.UploadCorrupt as e:
            self.assertIn("-3 while decompressing", str(e))
        else:
            self.fail("Should fail with UploadCorrupt")
        finally:
            yield upload_job.cancel()

    @defer.inlineCallbacks
    def test_upload_corrupted_deflated(self):
        """Test corruption of deflated data in UploadJob."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        deflated_data += '10'
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        # change the deflated size to force a failure in upload_job.commit
        upload_job.factory.deflated_size += 1
        try:
            yield upload_job.commit(result)
        except server.errors.UploadCorrupt as e:
            self.assertEquals(str(e), upload_job._deflated_size_hint_mismatch)
        else:
            yield defer.fail("Should fail with UploadCorrupt")

    def test_upload_id(self):
        """Test the upload_id generation."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        self.assertEqual(upload_job.upload_id, upload_job.multipart_key_name)

    @defer.inlineCallbacks
    def test_buffers_size(self):
        """Test the buffers_size property."""
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        deflated_size = len(deflated_data)
        upload_job.add_data(deflated_data[:deflated_size / 2])
        self.assertEqual(deflated_size / 2, upload_job.buffers_size)
        self.assertEqual(deflated_size / 2, upload_job.factory.buffer_size)
        # pause the producer again
        upload_job.producer.pauseProducing()
        upload_job.add_data(deflated_data[deflated_size / 2:])
        self.assertEqual(deflated_size, upload_job.buffers_size)
        self.assertApproximates(deflated_size,
                                upload_job.factory.buffer_size, 1)
        result = yield upload_job.deferred
        yield upload_job.commit(result)
        node = yield self.content_user.get_node(self.user.root_volume_id,
                                                upload_job.file_node.id, None)
        self.assertEqual(node.content_hash, hash_value)

    @defer.inlineCallbacks
    def test_unregisterProducer_on_cancel(self):
        """unregisterProducer is called on cancel."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        mocker = Mocker()
        producer = mocker.mock()
        self.patch(upload_job, 'producer', producer)
        # cancel
        expect(producer.unregisterProducer())
        expect(producer.stopProducing())
        with mocker:
            yield upload_job.cancel()

    @defer.inlineCallbacks
    def test_unregisterProducer_on_stop(self):
        """unregisterProducer is called on stop too."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        mocker = Mocker()
        producer = mocker.mock()
        self.patch(upload_job, 'producer', producer)
        # stop
        expect(producer.stopProducing())
        expect(producer.unregisterProducer())
        with mocker:
            yield upload_job.stop()

    @defer.inlineCallbacks
    def test_stopProducing_no_ProducerStopped_on_stop(self):
        """If stopProducing raises ProducerSopped, we catch it on stop."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        mocker = Mocker()
        producer = mocker.mock()
        self.patch(upload_job, 'producer', producer)
        # stop
        expect(producer.stopProducing()).throw(ProducerStopped())
        expect(producer.unregisterProducer())
        with mocker:
            yield upload_job.stop()

    @defer.inlineCallbacks
    def test_unregisterProducer_on_commit(self):
        """unregisterProducer is called on commit too."""
        mocker = Mocker()
        size = self.half_size
        deflated_data, hash_value, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        upload_job.producer = mocker.patch(upload_job.producer)
        result = yield upload_job.deferred
        with mocker:
            yield upload_job.commit(result)
        node_id = upload_job.file_node.id
        node = yield self.content_user.get_node(self.user.root_volume_id,
                                                node_id, None)
        self.assertEqual(node.content_hash, hash_value)

    @defer.inlineCallbacks
    def test_wait_for_s3_connection(self):
        """UploadJob.connect waits for s3 connection."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        client = upload_job.factory._current_client_factory.http_client
        self.assertEqual(client.transport.connected, 1)
        self.assertTrue(upload_job.factory.connect_deferred.called)
        self.assertTrue(client.factory.connect_deferred.called)
        yield upload_job.cancel()

    @defer.inlineCallbacks
    def test_s3_connection_failure_doesnt_blow_up(self):
        """Test that failure to connect to to S3 reports a reasonable error."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        # Make sure the S3 connection fails before we actually setup a producer
        fake_s3_deferred = defer.Deferred()
        reactor.callLater(
            0, fake_s3_deferred.errback, web_error.Error("Boom!"))

        class FakeS3:
            """Creates upload that is destined to fail."""

            def create_multipart_upload(self, *args):
                """Just return the fake deferred."""
                return fake_s3_deferred

        upload_job.s3 = FakeS3()
        try:
            yield upload_job.connect()
            self.fail("Expected Exception to be thrown!")
        except storage_errors.TryAgain as e:
            self.assertTrue("Boom!" in e.message)

    @defer.inlineCallbacks
    def test_mp_upload_missing_from_s3(self):
        """Client tries to resume, but the upload isn't in S3.

        It receives a new upload_id.
        """
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        # set a fake multipart_id
        upload_job.uploadjob.multipart_id = str(uuid.uuid4())
        # Make sure the S3 connection fails before we actually setup a producer
        fake_s3_deferred = defer.Deferred()
        reactor.callLater(0, fake_s3_deferred.errback, web_error.Error("404"))
        mp_upload = MultipartUpload(None, "bucket", "key", "upload_id")

        class FakeS3:
            """Creates upload that is destined to fail."""

            def get_multipart_upload(self, *args):
                """Just return a deferred that fails with 404."""
                return fake_s3_deferred

            def create_multipart_upload(self, *args):
                """Just return the fake deferred."""
                return defer.succeed(mp_upload)

        class FakeConsumer(object):
            """A fake consumer."""
            deferred = defer.Deferred()
            connect_deferred = defer.succeed(None)

        upload_job.s3 = FakeS3()
        self.patch(mp_upload, "_upload_part", lambda *a: FakeConsumer())
        yield upload_job.connect()
        self.assertIdentical(mp_upload, upload_job.mp_upload)
        # check that we have the new id
        self.assertEqual(upload_job.multipart_id, "upload_id")

    @defer.inlineCallbacks
    def test_commit_and_delete_fails(self):
        """Commit and delete fails, log in warning."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        # make commit fail
        self.patch(upload_job, "_commit",
                   lambda _: defer.fail(ValueError("boom")))
        # also delete
        self.patch(upload_job, "delete",
                   lambda: defer.fail(ValueError("delete boom")))
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.WARNING)
        logger.addHandler(hdlr)
        self.addCleanup(logger.removeHandler, hdlr)
        hdlr.debug = True
        try:
            yield upload_job.commit(result)
        except ValueError as e:
            self.assertEqual(str(e), "boom")
            self.assertTrue(hdlr.check_warning("delete boom"))
        else:
            self.fail("Should fail and log a warning.")

    @defer.inlineCallbacks
    def test_delete_after_commit_ok(self):
        """Delete the UploadJob after succesful commit."""
        size = self.half_size
        deflated_data, _, upload_job = yield self.make_upload(size)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        logger = logging.getLogger('storage.server')
        hdlr = MementoHandler()
        hdlr.setLevel(logging.WARNING)
        logger.addHandler(hdlr)
        self.addCleanup(logger.removeHandler, hdlr)
        hdlr.debug = True
        yield upload_job.commit(result)
        node = upload_job.file_node
        # check that the upload is no more
        d = content.DBUploadJob.get(self.content_user, node.volume_id,
                                    node.id, upload_job.upload_id,
                                    upload_job.hash_hint,
                                    upload_job.crc32_hint,
                                    upload_job.inflated_size_hint,
                                    upload_job.deflated_size_hint)
        yield self.assertFailure(d, errors.DoesNotExist)


class TestNode(TestWithDatabase):
    """Tests for Node class."""

    upload_class = content.UploadJob

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestNode, self).setUp()
        self.chunk_size = config.api_server.storage_chunk_size
        self.half_size = self.chunk_size / 2
        self.double_size = self.chunk_size * 2
        self.user = self.create_user(max_storage_bytes=self.chunk_size ** 2)
        self.suser = content.User(self.service.factory.content, self.user.id,
                                  self.user.root_volume_id, self.user.username,
                                  self.user.visible_name)

        # add a memento handler, to check we log ok
        self.logger = logging.getLogger('storage.server')
        self.handler = MementoHandler()
        self.handler.setLevel(logging.WARNING)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        """Tear down."""
        self.logger.removeHandler(self.handler)
        return super(TestNode, self).tearDown()

    @defer.inlineCallbacks
    def _upload_a_file(self, user, content_user):
        """Upload a file to s3.

        @param user: the storage user
        @param content: the content.User
        @return: a tuple (upload, deflated_data)
        """
        size = self.chunk_size / 2
        data = os.urandom(size)
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        magic_hash_value = get_magic_hash(data)
        crc32_value = crc32(data)
        deflated_size = len(deflated_data)
        root, _ = yield content_user.get_root()
        rpcdal = self.service.factory.content.rpcdal_client
        r = yield rpcdal.call(
            'make_file_with_content', user_id=content_user.id,
            volume_id=self.user.root_volume_id, parent_id=root,
            name=u"A new file", node_hash=model.EMPTY_CONTENT_HASH, crc32=0,
            size=0, deflated_size=0, storage_key=None)
        node_id = r['node_id']
        node = yield content_user.get_node(user.root_volume_id, node_id, None)
        upload_job = content.UploadJob(content_user, node, node.content_hash,
                                       hash_value, crc32_value, size,
                                       deflated_size, None, False,
                                       magic_hash_value)
        yield upload_job.connect()
        upload_job.add_data(deflated_data)
        result = yield upload_job.deferred
        yield upload_job.commit(result)
        node = yield content_user.get_node(user.root_volume_id, node_id, None)
        self.assertEqual(hash_value, node.content_hash)
        defer.returnValue((node, deflated_data))

    @defer.inlineCallbacks
    def test_get_content(self):
        """Test for Node.get_content 'all good' code path."""
        node, deflated_data = yield self._upload_a_file(self.user, self.suser)
        producer = yield node.get_content(previous_hash=node.content_hash)
        consumer = BufferedConsumer(producer)
        # resume producing
        consumer.resumeProducing()
        yield producer.deferred
        self.assertEqual(len(consumer.buffer.getvalue()), len(deflated_data))
        self.assertEqual(consumer.buffer.getvalue(), deflated_data)

    @defer.inlineCallbacks
    def test_get_content_fail_with_timeout(self):
        """Test for Node.get_content failing with TimeoutError."""
        node, deflated_data = yield self._upload_a_file(self.user, self.suser)
        # replace the factory.s3 in order to inject the error

        def get_from_s3(*args, **kwargs):
            """Just create the Factory and make it fail."""
            factory = s3lib.contrib.http_client.HTTPStreamingClientFactory(
                'http://example.com', method='GET',
                postdata={}, headers={},
                agent="S3lib", streaming=True)
            factory.deferred.errback(error.TimeoutError())
            return factory

        node._get_from_s3 = get_from_s3
        # change the retrier parameters of this Node instance
        node.s3_retries = 3
        node.s3_retry_wait = 0.0001
        try:
            producer = yield node.get_content(previous_hash=node.content_hash)
            yield producer.deferred
        except errors.RetryLimitReached as e:
            # check that the message is the expected and also check that
            # the number of retries match with node.s3_retries
            msg = ("Maximum retries (%d) reached. Please try again.\n"
                   "Original exception: TimeoutError: User timeout caused "
                   "connection failure.")
            self.assertEquals(str(e), msg % (node.s3_retries,))
            self.assertTrue(self.handler.check_warning(
                            "Retrying: <bound method Node._get_producer"))
            for i in range(2):
                self.assertTrue(self.handler.check_warning(
                                "failed with: TimeoutError('User timeout "
                                "caused connection failure.') - "
                                "retry_count: %d" % i))
            yield defer.succeed(None)
        else:
            self.fail("Should fail with TryAgain")

    @defer.inlineCallbacks
    def test_get_producer_fail_with_timeout(self):
        """Test for get_content producer that fails with TimeoutError."""
        node, deflated_data = yield self._upload_a_file(self.user, self.suser)

        # replace the factory.s3 in order to inject the error
        old_get_from_s3 = node._get_from_s3

        def get_from_s3(*args, **kwargs):
            """Intercept the factory create by _get_from_s3."""
            factory = old_get_from_s3(*args, **kwargs)

            def break_the_producer(producer):
                """Inject a failure in the callback chain."""
                producer.deferred.addCallback(
                    lambda _: defer.fail(error.TimeoutError()))
                return producer

            factory.deferred.addCallback(break_the_producer)
            return factory

        node._get_from_s3 = get_from_s3
        producer = yield node.get_content(previous_hash=node.content_hash,
                                          user=self.suser)
        consumer = BufferedConsumer(producer)
        try:
            # resume producing
            consumer.resumeProducing()
            yield producer.deferred
        except server.errors.TryAgain as e:
            self.assertEqual(str(e), 'TimeoutError while downloading data.')
        else:
            raise Exception("Should fail with TryAgain")

    @defer.inlineCallbacks
    def _get_user_node(self):
        """Get a user and a node."""
        node, deflated_data = yield self._upload_a_file(self.user, self.suser)
        defer.returnValue((self.suser, node))

    @defer.inlineCallbacks
    def test_get_producer_call_handles3error_on_normalbucket_error(self):
        """_handle_s3_errors is called if the first bucket raises != 404."""
        user, node = yield self._get_user_node()

        # make it fail when asking for the producer
        err = web_error.Error('500')

        class FakeResponse(object):
            """Fake Response."""
            deferred = defer.fail(err)

        node._get_from_s3 = lambda *a: FakeResponse

        # flag if was called, storing the failure and user, and
        # returning the failure
        called = []
        node._handle_s3_errors = lambda f, u: called.extend((f, u)) or f

        # check and test
        d = node._get_producer('bucket', 'storage_key', 'headers', user)
        # consume the exc, no matter which one
        self.assertFailure(d, Exception)
        self.assertEqual(called[0].value, err)
        self.assertEqual(called[1], user)

    @defer.inlineCallbacks
    def test_get_producer_call_handles3error_on_configuredbucket_error(self):
        """_handle_s3_errors is called if the configured bucket fails."""
        user, node = yield self._get_user_node()

        # setup a fake configured fallback bucket
        config.api_server.s3_fallback_bucket = 'something'
        self.addCleanup(setattr, config.api_server, 's3_fallback_bucket', '')

        # make it fail when asking for the producer
        err = web_error.Error('404')

        class FakeResponse(object):
            """Fake Response."""

            def __init__(self):
                self.deferred = defer.fail(err)

        node._get_from_s3 = lambda *a: FakeResponse()

        # flag if was called, storing the failure and user, and
        # returning the failure
        called = []
        node._handle_s3_errors = lambda f, u: called.extend((f, u)) or f

        # check and test
        d = node._get_producer('bucket', 'storage_key', 'headers', user)
        # consume the exception, no matter which one
        self.assertFailure(d, Exception)
        self.assertEqual(called[0].value, err)
        self.assertEqual(called[1], user)

    @defer.inlineCallbacks
    def test_get_producer_adds_handles3error_on_producer_errback(self):
        """_handle_s3_errors is called if the producer has a problem."""
        user, node = yield self._get_user_node()

        # make it fail when asking for the producer
        err = web_error.Error('500')

        class DeferredContainer(object):
            """Just something that has a deferred in it."""

            def __init__(self):
                self.deferred = defer.Deferred()

        fake_response = DeferredContainer()
        fake_producer = DeferredContainer()
        fake_response.deferred.callback(fake_producer)
        node._get_from_s3 = lambda *a: fake_response

        # flag if was called, storing the failure and user, and
        # returning the failure
        called = []
        node._handle_s3_errors = lambda *a: called.extend(a)

        # get the producer, check it still wasn't called
        yield node._get_producer('bucket', 'storage_key', 'headers', user)
        self.assertFalse(called)

        # errback the deferred and check
        fake_producer.deferred.errback(err)
        self.assertEqual(called[0].value, err)
        self.assertEqual(called[1], user)

    @defer.inlineCallbacks
    def test_handles3errors_no_web(self):
        """Only handle web errors."""
        user, node = yield self._get_user_node()

        err = ValueError('foo')
        failure = node._handle_s3_errors(Failure(err), user)
        self.assertEqual(failure.value, err)

    @defer.inlineCallbacks
    def test_handles3errors_special_416(self):
        """Handle status 416 error specifically."""
        user, node = yield self._get_user_node()

        err = web_error.Error('416')
        self.assertRaises(server.errors.NotAvailable,
                          node._handle_s3_errors, Failure(err), user)
        self.assertTrue(self.handler.check_warning(
                        "s3 returned a status code of 416", str(node.id)))

    @defer.inlineCallbacks
    def test_handles3errors_generic(self):
        """Generic web errors end in TRY_AGAIN."""
        user, node = yield self._get_user_node()

        err = web_error.Error('404')
        self.assertRaises(server.errors.NotAvailable,
                          node._handle_s3_errors, Failure(err), user)
        self.assertTrue(self.handler.check_warning(
                        "s3 returned a status code of 404."))

    @defer.inlineCallbacks
    def test_handles3errors_timeouterror(self):
        """TimeoutError ends in TRY_AGAIN."""
        user, node = yield self._get_user_node()

        err = error.TimeoutError()
        self.assertRaises(server.errors.TryAgain,
                          node._handle_s3_errors, Failure(err), user)
        self.assertTrue(self.handler.check_warning(
                        "s3 threw TimeoutError", "TRY_AGAIN", str(node.id)))


class TestGenerations(TestWithDatabase):
    """Tests for generations related methods."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestGenerations, self).setUp()
        self.suser = u = self.create_user(max_storage_bytes=64 ** 2)
        self.user = content.User(self.service.factory.content, u.id,
                                 u.root_volume_id, u.username, u.visible_name)

    @defer.inlineCallbacks
    def test_get_delta_empty(self):
        """Test that User.get_delta works as expected."""
        delta = yield self.user.get_delta(None, 0)
        free_bytes = self.suser.get_quota().free_bytes
        self.assertEquals(delta, ([], 0, free_bytes))

    @defer.inlineCallbacks
    def test_get_delta_from_0(self):
        """Test that User.get_delta works as expected."""
        nodes = [self.suser.root.make_file(u"name%s" % i) for i in range(5)]
        delta, end_gen, free_bytes = yield self.user.get_delta(None, 0)
        self.assertEquals(len(delta), len(nodes))
        self.assertEquals(end_gen, nodes[-1].generation)
        self.assertEquals(free_bytes, self.suser.get_quota().free_bytes)

    @defer.inlineCallbacks
    def test_get_delta_from_middle(self):
        """Test that User.get_delta works as expected."""
        # create some nodes
        root = self.suser.root
        nodes = [root.make_file(u"name%s" % i) for i in range(5)]
        nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
        from_generation = nodes[5].generation
        delta, end_gen, free_bytes = yield self.user.get_delta(None,
                                                               from_generation)
        self.assertEquals(len(delta), len(nodes[6:]))
        self.assertEquals(end_gen, nodes[-1].generation)
        self.assertEquals(free_bytes, self.suser.get_quota().free_bytes)

    @defer.inlineCallbacks
    def test_get_delta_from_last(self):
        """Test that User.get_delta works as expected."""
        # create some nodes
        root = self.suser.root
        nodes = [root.make_file(u"name%s" % i) for i in range(5)]
        nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
        from_generation = nodes[-1].generation
        delta, end_gen, free_bytes = yield self.user.get_delta(None,
                                                               from_generation)
        self.assertEquals(len(delta), 0)
        self.assertEquals(end_gen, nodes[-1].generation)
        self.assertEquals(free_bytes, self.suser.get_quota().free_bytes)

    @defer.inlineCallbacks
    def test_get_delta_partial(self):
        """Test User.get_delta with partial delta."""
        # create some nodes
        root = self.suser.root
        nodes = [root.make_file(u"name%s" % i) for i in range(10)]
        nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(10)]
        limit = 5
        delta, vol_gen, free_bytes = yield self.user.get_delta(None, 10,
                                                               limit=limit)
        self.assertEqual(len(delta), limit)
        self.assertEqual(vol_gen, 20)

    @defer.inlineCallbacks
    def test_rescan_from_scratch(self):
        """Test User.rescan_from_scratch."""
        root = self.suser.root
        nodes = [root.make_file(u"name%s" % i) for i in range(5)]
        nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
        for f in [root.make_file(u"name%s" % i) for i in range(5, 10)]:
            f.delete()
        for d in [root.make_subdirectory(u"dir%s" % i) for i in range(5, 10)]:
            d.delete()
        live_nodes, gen, free_bytes = yield self.user.get_from_scratch(None)
        # nodes + root
        self.assertEqual(len(nodes) + 1, len(live_nodes))
        self.assertEqual(30, gen)


class TestContentManagerTests(TestWithDatabase):
    """Test ContentManger class."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestContentManagerTests, self).setUp()
        self.suser = self.create_user(max_storage_bytes=64 ** 2)
        self.cm = content.ContentManager(self.service.factory)
        self.cm.rpcdal_client = self.service.rpcdal_client

    @defer.inlineCallbacks
    def test_get_user_by_id(self):
        """Test get_user_by_id."""
        # user isn't cached yet.
        u = yield self.cm.get_user_by_id(self.suser.id)
        self.assertEqual(u, None)
        u = yield self.cm.get_user_by_id(self.suser.id, required=True)
        self.assertTrue(isinstance(u, content.User))
        # make sure it's in the cache
        self.assertEqual(u, self.cm.users[self.suser.id])
        # get it from the cache
        u = yield self.cm.get_user_by_id(self.suser.id)
        self.assertTrue(isinstance(u, content.User))

    @defer.inlineCallbacks
    def test_get_user_by_id_race_condition(self):
        """Two requests both try to fetch and cache the user."""
        # Has to fire before first call to rpcdal client returns
        d = defer.Deferred()
        rpc_call = self.cm.rpcdal_client.call

        @defer.inlineCallbacks
        def delayed_rpc_call(funcname, **kwargs):
            """Wait for the deferred, then make the real client call."""
            yield d
            val = yield rpc_call(funcname, **kwargs)
            defer.returnValue(val)

        self.cm.rpcdal_client.call = delayed_rpc_call

        # Start the first call
        u1_deferred = self.cm.get_user_by_id(self.suser.id, required=True)
        # Start the second call
        u2_deferred = self.cm.get_user_by_id(self.suser.id, required=True)
        # Let the first continue
        d.callback(None)
        # Get the results
        u1 = yield u1_deferred
        u2 = yield u2_deferred

        self.assertIdentical(u1, u2)
        self.assertIdentical(u1, self.cm.users[self.suser.id])


class TestContentWithSSL(TestWithDatabase):
    """Test the upload and download using ssl with s3."""

    s4_use_ssl = True

    def test_getcontent(self):
        """Get the content from a file."""
        data = "*" * 100000
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
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(lambda _: client.get_content(
                          request.ROOT, self._state.req.new_id, hash_value))
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_putcontent(self):
        """Test putting content to a file."""
        data = os.urandom(100000)
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
                    try:
                        self.usr0.volume().get_content(hash_value)
                    except errors.DoesNotExist:
                        raise ValueError("content blob is not there")
                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            filename = 'hola'
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root: client.make_file(request.ROOT, root, filename),
                client.test_fail)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
            return d

        return self.callback_test(auth)


class DBUploadJobTestCase(TestCase):
    """Tests for the DBUploadJob."""

    class FakeUser(object):
        """Fake object that simulates a rpc_dal call."""

        def __init__(self, to_return):
            self.to_return = to_return
            self.recorded = None
            self.id = 'fake_user_id'

        def call(self, method, **attribs):
            """Record the call."""
            self.recorded = (method, attribs)
            return defer.succeed(self.to_return)

        rpc_dal = property(lambda self: self)

    def setUp(self):
        """Set up."""
        d = dict(uploadjob_id='uploadjob_id', uploaded_bytes='uploaded_bytes',
                 multipart_id='multipart_id', multipart_key='multipart_key',
                 chunk_count='chunk_count', hash_context='hash_context',
                 magic_hash_context='magic_hash_context',
                 decompress_context='decompress_context',
                 inflated_size='inflated_size', crc32='crc32',
                 when_last_active='when_last_active')
        self.user = self.FakeUser(to_return=d)
        return super(DBUploadJobTestCase, self).setUp()

    @defer.inlineCallbacks
    def test_get(self):
        """Test the getter."""
        args = (self.user, 'volume_id', 'node_id', 'uploadjob_id',
                'hash_value', 'crc32', 'inflated_size', 'deflated_size')
        dbuj = yield content.DBUploadJob.get(*args)

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'get_uploadjob')
        should = dict(user_id='fake_user_id', volume_id='volume_id',
                      node_id='node_id', uploadjob_id='uploadjob_id',
                      hash_value='hash_value', crc32='crc32',
                      inflated_size='inflated_size',
                      deflated_size='deflated_size')
        self.assertEqual(attribs, should)

        # check it built the instance correctly
        self.assertTrue(isinstance(dbuj, content.DBUploadJob))
        self.assertEqual(dbuj.user, self.user)
        self.assertEqual(dbuj.volume_id, 'volume_id')
        self.assertEqual(dbuj.node_id, 'node_id')
        self.assertEqual(dbuj.uploadjob_id, 'uploadjob_id')
        self.assertEqual(dbuj.uploaded_bytes, 'uploaded_bytes')
        self.assertEqual(dbuj.multipart_id, 'multipart_id')
        self.assertEqual(dbuj.multipart_key, 'multipart_key')
        self.assertEqual(dbuj.chunk_count, 'chunk_count')
        self.assertEqual(dbuj.inflated_size, 'inflated_size')
        self.assertEqual(dbuj.crc32, 'crc32')
        self.assertEqual(dbuj.hash_context, 'hash_context')
        self.assertEqual(dbuj.magic_hash_context, 'magic_hash_context')
        self.assertEqual(dbuj.decompress_context, 'decompress_context')
        self.assertEqual(dbuj.when_last_active, 'when_last_active')

    @defer.inlineCallbacks
    def test_make(self):
        """Test the builder."""
        args = (self.user, 'volume_id', 'node_id', 'previous_hash',
                'hash_value', 'crc32', 'inflated_size', 'deflated_size',
                'multipart_key')
        dbuj = yield content.DBUploadJob.make(*args)

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'make_uploadjob')
        should = dict(user_id='fake_user_id', volume_id='volume_id',
                      node_id='node_id', previous_hash='previous_hash',
                      hash_value='hash_value', crc32='crc32',
                      inflated_size='inflated_size',
                      deflated_size='deflated_size',
                      multipart_key='multipart_key')
        self.assertEqual(attribs, should)

        # check it built the instance correctly
        self.assertTrue(isinstance(dbuj, content.DBUploadJob))
        self.assertEqual(dbuj.user, self.user)
        self.assertEqual(dbuj.volume_id, 'volume_id')
        self.assertEqual(dbuj.node_id, 'node_id')
        self.assertEqual(dbuj.uploadjob_id, 'uploadjob_id')
        self.assertEqual(dbuj.uploaded_bytes, 'uploaded_bytes')
        self.assertEqual(dbuj.multipart_id, 'multipart_id')
        self.assertEqual(dbuj.multipart_key, 'multipart_key')
        self.assertEqual(dbuj.chunk_count, 'chunk_count')
        self.assertEqual(dbuj.inflated_size, 'inflated_size')
        self.assertEqual(dbuj.crc32, 'crc32')
        self.assertEqual(dbuj.hash_context, 'hash_context')
        self.assertEqual(dbuj.magic_hash_context, 'magic_hash_context')
        self.assertEqual(dbuj.decompress_context, 'decompress_context')
        self.assertEqual(dbuj.when_last_active, 'when_last_active')

    def _make_uj(self):
        """Helper to create the upload job."""
        args = (self.user, 'volume_id', 'node_id', 'previous_hash',
                'hash_value', 'crc32', 'inflated_size', 'deflated_size',
                'multipart_key')
        return content.DBUploadJob.make(*args)

    @defer.inlineCallbacks
    def test_set_multipart_id(self):
        """Test the multipart_id setter."""
        dbuj = yield self._make_uj()
        yield dbuj.set_multipart_id('multipart_id')

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'set_uploadjob_multipart_id')
        should = dict(user_id='fake_user_id', uploadjob_id='uploadjob_id',
                      multipart_id='multipart_id')
        self.assertEqual(attribs, should)

    @defer.inlineCallbacks
    def test_add_part(self):
        """Test add_part method."""
        dbuj = yield self._make_uj()
        yield dbuj.add_part('chunk_size', 'inflated_size', 'crc32',
                            'hash_context', 'magic_hash_context',
                            'decompress_context')

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'add_part_to_uploadjob')
        should = dict(user_id='fake_user_id', uploadjob_id='uploadjob_id',
                      chunk_size='chunk_size', inflated_size='inflated_size',
                      crc32='crc32', hash_context='hash_context',
                      magic_hash_context='magic_hash_context',
                      decompress_context='decompress_context')
        self.assertEqual(attribs, should)

    @defer.inlineCallbacks
    def test_delete(self):
        """Test delete method."""
        dbuj = yield self._make_uj()
        yield dbuj.delete()

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'delete_uploadjob')
        should = dict(user_id='fake_user_id', uploadjob_id='uploadjob_id')
        self.assertEqual(attribs, should)

    @defer.inlineCallbacks
    def test_touch(self):
        """Test the touch method."""
        dbuj = yield self._make_uj()
        self.user.to_return = dict(when_last_active='new_when_last_active')
        yield dbuj.touch()

        # check it called rpcdal correctly
        method, attribs = self.user.recorded
        self.assertEqual(method, 'touch_uploadjob')
        should = dict(user_id='fake_user_id', uploadjob_id='uploadjob_id')
        self.assertEqual(attribs, should)

        # check updated attrib
        self.assertEqual(dbuj.when_last_active, 'new_when_last_active')
