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

"""Tests for ubuntuone.storage.server.upload."""

import hashlib
import os
import zlib

from mocker import Mocker, expect, ANY
from twisted.internet import defer, reactor, task, error as txerror
from twisted.trial.unittest import TestCase

from s3lib import producers, s3lib
from s4 import s4

from ubuntuone.storage import rhashlib, rzlib
from ubuntuone.storage.server import upload, errors
from ubuntuone.storage.server.auth import DummyAuthProvider
from ubuntuone.storage.server.testing import testcase
from ubuntuone.storageprotocol.content_hash import (
    content_hash_factory,
    crc32,
    magic_hash_factory,
)


class UploadTestCase(testcase.BaseProtocolTestCase):
    """Base test case for upload stuff."""
    auth_provider_class = DummyAuthProvider

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(UploadTestCase, self).setUp()
        self.s4_site.resource._add_bucket("test")
        self.s3 = s3lib.S3("localhost", self.s4_port,
                           s4.AWS_DEFAULT_ACCESS_KEY_ID,
                           s4.AWS_DEFAULT_SECRET_ACCESS_KEY)

        def slowScheduler(x):
            """A slower scheduler for our cooperator."""
            return reactor.callLater(0.1, x)
        self._cooperator = task.Cooperator(scheduler=slowScheduler)

    @defer.inlineCallbacks
    def tearDown(self):
        """Tear down."""
        self._cooperator.stop()
        yield super(UploadTestCase, self).tearDown()


class MultipartUploadFactoryMockTest(TestCase):
    """Tests for MultipartUploadFactory."""

    def tearDown(self):
        # reset class level buffers size counter
        upload.MultipartUploadFactory.buffers_size = 0
        return super(MultipartUploadFactoryMockTest, self).tearDown()

    def test_init(self):
        """Test initialization."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        hash_object = mocker.mock()
        decompressor = mocker.mock()
        part_done_cb = mocker.mock()
        with mocker:
            factory = upload.MultipartUploadFactory(
                mp_upload, chunk_size, len(zmessage), offset, len(message),
                crc32_value, part_done_cb=part_done_cb,
                hash_factory=lambda: hash_object,
                decompressobj_factory=lambda: decompressor)
        self.assertEqual(factory.finished, False)
        self.assertEqual(factory.cancelled, False)
        self.assertEqual(factory.paused, True)
        self.assertEqual(factory.deflated_size, 0)
        self.assertEqual(factory.inflated_size, len(message))
        self.assertEqual(factory.crc32, 0)
        self.assertEqual(factory.offset, offset)
        self.assertEqual(factory._chunk_count, 0)
        self.assertIsInstance(factory.connect_deferred, defer.Deferred)
        self.assertEqual(factory.chunk_size, chunk_size)
        self.assertEqual(factory.multipart_upload, mp_upload)
        self.assertEqual(factory.part_done_cb, part_done_cb)

    def test_init_resume(self):
        """Test initialization with extra values."""
        message = "101" * 1024
        zmessage = zlib.compress(message)
        chunk_size = 100
        offset = len(zmessage) / 2
        crc32_value = crc32(zmessage[:offset])
        chunk_count = offset // chunk_size
        mocker = Mocker()
        mp_upload = mocker.mock()
        hash_object = mocker.mock()
        magic_hash_object = mocker.mock()
        decompressor = mocker.mock()
        part_done_cb = mocker.mock()
        hasher = rhashlib.sha1()
        hasher.update("foo")
        hash_context = hasher.get_context()
        magic_hasher = rhashlib.resumable_magic_hash_factory()
        magic_hasher.update("bar")
        magic_hash_context = magic_hasher.get_context()
        decompress_context = rzlib.decompressobj().get_state()
        # mock expectations
        expect(hash_object.set_context(hash_context))
        expect(magic_hash_object.set_context(magic_hash_context))
        expect(decompressor.set_state(decompress_context))
        with mocker:
            factory = upload.MultipartUploadFactory(
                mp_upload, chunk_size, len(zmessage), offset, len(message),
                crc32_value, chunk_count=chunk_count,
                hash_context=hash_context,
                magic_hash_context=magic_hash_context,
                decompress_context=decompress_context,
                part_done_cb=part_done_cb, hash_factory=lambda: hash_object,
                magic_hash_factory=lambda: magic_hash_object,
                decompressobj_factory=lambda: decompressor)
        self.assertEqual(factory.finished, False)
        self.assertEqual(factory.cancelled, False)
        self.assertEqual(factory.paused, True)
        self.assertEqual(factory.deflated_size, offset)
        self.assertEqual(factory.inflated_size, len(message))
        self.assertEqual(factory.crc32, crc32_value)
        self.assertEqual(factory.offset, offset)
        self.assertEqual(factory._chunk_count, chunk_count)
        self.assertEqual(factory.chunk_size, chunk_size)
        self.assertEqual(factory.multipart_upload, mp_upload)
        self.assertEqual(factory.part_done_cb, part_done_cb)

    def test_flush_buffer(self):
        """Test flush_buffer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        # setup some fake data
        data = "some data"
        data_len = len(data)
        factory._buffer.append(data)
        factory.__class__.buffers_size += data_len
        factory.buffer_size += data_len
        # mock expetactions
        factory = mocker.patch(factory)
        expect(factory._send(data)).result(True)
        # consumer calls
        with mocker:
            factory.flush_buffer()
            self.assertEqual(upload.MultipartUploadFactory.buffers_size, 0)
            self.assertEqual(factory.buffer_size, 0)

    def test_flush_buffer_empty(self):
        """Test flush_buffer method with an empty buffer."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory._buffer).result([])
        with mocker:
            send_more = factory.flush_buffer()
            self.assertTrue(send_more)

    def test_flush_buffer_paused(self):
        """Test flush_buffer method with the factory paused."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        offset = 0
        crc32_value = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = True
        factory._buffer.append("some data")
        factory = mocker.patch(factory)
        expect(factory.paused).result(True).count(2)
        with mocker:
            send_more = factory.flush_buffer()
            self.assertFalse(send_more)

    # IPushProducer interface tests

    def test_resumeProducing(self):
        """Test resumeProducing method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = True
        factory = mocker.patch(factory)
        expect(factory.flush_buffer()).result(True)
        with mocker:
            factory.resumeProducing()
            self.assertFalse(factory.paused)

    def test_resumeProducing_upload_corrupt(self):
        """Test resumeProducing UploadCorrupt handling."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = True
        factory = mocker.patch(factory)
        exc = errors.UploadCorrupt()
        expect(factory.flush_buffer()).throw(exc)
        expect(factory.deferred.errback(exc))
        with mocker:
            factory.resumeProducing()
            self.assertFalse(factory.paused)

    def test_pauseProducing(self):
        """Test pauseProducing method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        with mocker:
            factory.pauseProducing()
            self.assertTrue(factory.paused)

    def test_stopProducing(self):
        """Test stopProducing method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        # setup some fake data
        data = "some data"
        data_len = len(data)
        factory._buffer.append(data)
        factory.__class__.buffers_size += data_len
        factory.buffer_size += data_len
        factory.paused = False
        # mock stuff
        factory = mocker.patch(factory)
        expect(factory.pauseProducing())
        expect(factory.flush_decompressor())
        with mocker:
            factory.stopProducing()
            self.assertEqual(upload.MultipartUploadFactory.buffers_size, 0)
            self.assertEqual(factory.buffer_size, 0)
            self.assertEqual(factory._buffer, None)

    def test_stopProducing_empty_buffer(self):
        """Test pauseProducing method with an empty buffer."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        # mock stuff
        factory = mocker.patch(factory)
        expect(factory.pauseProducing())
        expect(factory._buffer).result([])
        expect(factory.flush_decompressor())
        with mocker:
            factory.stopProducing()
            self.assertEqual(upload.MultipartUploadFactory.buffers_size, 0)
            self.assertEqual(factory.buffer_size, 0)

    # end IPushProducer interface tests

    def test__finishConsumer(self):
        """Test _finishConsumer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory.consumer = consumer
        # mock stuff
        factory = mocker.patch(factory)
        expect(consumer.unregisterProducer())
        expect(factory.flush_decompressor())
        with mocker:
            factory._finishConsumer()
            self.assertEqual(factory.consumer, None)

    def test__finishConsumer_None(self):
        """Test _finishConsumer method with consumer = None."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        # mock stuff
        factory = mocker.patch(factory)
        expect(factory.consumer).result(None).count(1)
        with mocker:
            self.assertRaises(RuntimeError, factory._finishConsumer)

    def test_quitProducing(self):
        """Test quitProducing method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory.consumer = consumer
        # mock stuff
        factory = mocker.patch(factory)
        expect(factory.stopProducing())
        expect(consumer.unregisterProducer())
        expect(consumer.stopProducing())
        with mocker:
            factory.quitProducing()
            self.assertEqual(factory.consumer, None)

    def test_registerProducer(self):
        """Test for registerProducer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        producer = mocker.mock()
        factory.registerProducer(producer)
        self.assertEqual(factory.producer, producer)
        self.assertIdentical(factory.producer, producer)

    def test_unregisterProducer(self):
        """Test for unregisterProducer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        producer = mocker.mock()
        factory.registerProducer(producer)
        factory.unregisterProducer()
        self.assertEqual(factory.producer, None)

    def test_dataReceived(self):
        """Test dataReceived method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory.paused).result(False).count(2)
        expect(factory.flush_buffer()).result(True).count(1)
        expect(factory._send("some data")).count(1)
        with mocker:
            factory.dataReceived("some data")
            self.assertEqual(factory.bytes_received, len("some data"))

    def test_dataReceived_paused(self):
        """Test dataReceived while paused."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory.paused).result(True).count(1)
        expect(factory._buffer.append("some data")).count(1)
        with mocker:
            factory.dataReceived("some data")
            self.assertEqual(upload.MultipartUploadFactory.buffers_size,
                             len("some data"))
            self.assertEqual(factory.buffer_size, len("some data"))
            self.assertEqual(factory.bytes_received, len("some data"))

    def test_dataReceived_after_stopped(self):
        """Test dataReceived after stopProducing is ignored."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory._send(ANY)).count(0)
        with mocker:
            factory.pauseProducing()
            factory.dataReceived("first")
            factory.stopProducing()
            self.assertIdentical(factory._buffer, None)

            factory.dataReceived("some data")
            # We didn't blow up!
            # No data was buffered
            self.assertIdentical(factory._buffer, None)
        # factory._send was not called with any additional data

    def test_dataReceived_paused_after_flush_buffer(self):
        """Test dataReceived and factory being paused after flush_buffer."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory.paused).result(False).count(1)
        expect(factory.flush_buffer()).result(True).count(1)
        # don't know other way to change an attribute of a patched instance
        expect(factory.paused).result(True).count(1)
        expect(factory._buffer.append("some data")).count(1)
        with mocker:
            factory.dataReceived("some data")
            self.assertEqual(upload.MultipartUploadFactory.buffers_size,
                             len("some data"))
            self.assertEqual(factory.buffer_size, len("some data"))
            self.assertEqual(factory.bytes_received, len("some data"))

    def test__send(self):
        """Test _send method."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory.current_chunk_size = chunk_size
        factory = mocker.patch(factory)
        expect(factory.consumer).result(consumer).count(1)
        expect(factory.paused).result(False).count(1)
        expect(factory.writeToConsumer("some data")).count(1)
        with mocker:
            send_more = factory._send("some data")
            self.assertTrue(send_more)
            self.assertEqual(factory._already_sent, len("some data"))

    def test__send_no_data(self):
        """Test _send method."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory.current_chunk_size = chunk_size
        factory = mocker.patch(factory)
        expect(factory.consumer).result(consumer).count(1)
        expect(factory.paused).result(False).count(1)
        with mocker:
            send_more = factory._send("")
            self.assertTrue(send_more)

    def test__send_paused(self):
        """Test _send method while paused."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory.consumer).result(consumer).count(1)
        expect(factory.paused).result(True).count(1)
        with mocker:
            self.assertRaises(RuntimeError, factory._send, "some data")

    def test__send_None_consumer(self):
        """Test _send method with a None consumer."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory.paused = False
        factory = mocker.patch(factory)
        expect(factory.consumer).result(None).count(1)
        with mocker:
            self.assertRaises(RuntimeError, factory._send, "some data")

    def test__send_finish_chunk(self):
        """Test _send method and finish the in-progress chunk."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        data = zmessage[:10]
        factory.paused = False
        factory.current_chunk_size = chunk_size
        factory._already_sent = chunk_size - len(data)
        factory = mocker.patch(factory)
        expect(factory.consumer).result(consumer).count(1)
        expect(factory.paused).result(False).count(1)
        expect(factory.writeToConsumer(data)).count(1)
        expect(factory._finishConsumer()).count(1)
        with mocker:
            send_more = factory._send(data)
            self.assertFalse(send_more)
            self.assertEqual(factory._already_sent, 0)
            self.assertEqual(len(factory._buffer), 0)

    def test__send_finish_oversized_chunk(self):
        """Test _send method with more data than a chunk size."""
        message = os.urandom(1024)
        zmessage = zlib.compress(message)
        chunk_size = len(zmessage) / 5
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        data = zmessage[:10]
        factory.paused = False
        factory.current_chunk_size = chunk_size
        factory._already_sent = chunk_size - 5
        factory = mocker.patch(factory)
        expect(factory.consumer).result(consumer).count(1)
        expect(factory.paused).result(False).count(1)
        expect(factory.writeToConsumer(data[:5])).count(1)
        expect(factory._finishConsumer()).count(1)
        with mocker:
            send_more = factory._send(data)
            self.assertFalse(send_more)
            self.assertEqual(factory._already_sent, 0)
            self.assertEqual(factory._buffer[0], data[5:])
            self.assertEqual(factory.buffer_size, 5)
            self.assertEqual(upload.MultipartUploadFactory.buffers_size, 5)

    def test_writeToConsumer(self):
        """Test writeToConsumer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        hash_object = mocker.mock()
        decompressor = mocker.mock()
        part_done_cb = mocker.mock()
        consumer = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb,
            hash_factory=lambda: hash_object,
            decompressobj_factory=lambda: decompressor)
        factory.consumer = consumer
        factory = mocker.patch(factory)
        expect(consumer.write("some data"))
        expect(factory.add_deflated_data("some data")).count(1)
        with mocker:
            factory.writeToConsumer("some data")
            self.assertEqual(factory.deflated_size, len("some data"))
            self.assertEqual(factory.bytes_sent, len("some data"))

    def test_flush_decompressor(self):
        """Test fluah_Decompressor """
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        mp_upload = mocker.mock()
        hash_object = mocker.mock()
        decompressor = mocker.mock()
        part_done_cb = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb,
            hash_factory=lambda: hash_object,
            decompressobj_factory=lambda: decompressor)
        factory = mocker.patch(factory)
        decompressor_copy = mocker.mock()
        expect(decompressor.copy()).result(decompressor_copy).count(1)
        expect(decompressor_copy.flush()).result("extra data").count(1)
        expect(factory.add_inflated_data("extra data")).count(1)
        with mocker:
            factory.flush_decompressor()

    @defer.inlineCallbacks
    def test_chunk_iter(self):
        """Test the _chunk_iter method."""
        message = zlib.compress(os.urandom(1024 * 1024))
        mocker = Mocker()
        mp_upload = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, 1024, len(message), 0, 1024 * 1024,
            crc32(message), part_done_cb=lambda *a: None)
        with mocker:
            chunks = list(factory._chunk_iter())
        self.assertEqual(len(chunks), 1024)
        for i, chunk in enumerate(chunks):
            if i == len(chunks) - 1:
                self.assertEqual(chunk, len(message) - 1024 * 1024 + 1024)
            else:
                self.assertEqual(chunk, 1024)
        yield factory.cancel()

    @defer.inlineCallbacks
    def test_chunk_iter_remainder(self):
        """Test the _chunk_iter with a remainder"""
        message = zlib.compress(os.urandom(1024 * 1024))
        len(message)
        mocker = Mocker()
        mp_upload = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, 1000, len(message), 0, 1024 * 1024,
            crc32(message), part_done_cb=lambda *a: None)
        with mocker:
            chunks = list(factory._chunk_iter())
        self.assertEqual(len(chunks), 1048)
        for i, chunk in enumerate(chunks):
            if i == len(chunks) - 1:
                self.assertEqual(chunk, len(message) - 1048 * 1000 + 1000)
            else:
                self.assertEqual(chunk, 1000)
        yield factory.cancel()

    @defer.inlineCallbacks
    def test_chunk_iter_offset(self):
        """Test the _chunk_iter with a non 0 offset."""
        message = zlib.compress(os.urandom(1024 * 1024))
        mocker = Mocker()
        mp_upload = mocker.mock()
        factory = upload.MultipartUploadFactory(
            mp_upload, 1024, len(message), 1024 * 512, 1024 * 1024,
            crc32(message), part_done_cb=lambda *a: None)
        with mocker:
            chunks = list(factory._chunk_iter())
        self.assertEqual(len(chunks), 512)
        for i, chunk in enumerate(chunks):
            if i == len(chunks) - 1:
                self.assertEqual(chunk, len(message) - 1024 * 1024 + 1024)
            else:
                self.assertEqual(chunk, 1024)
        yield factory.cancel()

    def test_new_consumer(self):
        """Basic test of _new_consumer method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = lambda *a: None
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        connect_deferred = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        _factory.connect_deferred = mocker.mock()
        factory = mocker.patch(_factory)
        expect(mp_upload._upload_part(
            _factory, str(chunk_size), 1)).result(consumer)
        # expect the call to the connect_deferred
        expect(consumer.connect_deferred).result(connect_deferred)
        # and the chain with factory connect_deferred
        expect(_factory.connect_deferred.called)
        expect(connect_deferred.chainDeferred(_factory.connect_deferred))
        with mocker:
            c = factory.new_consumer(chunk_size)
            self.assertIdentical(c, consumer)
            self.assertEqual(factory._chunk_count, 1)
            self.assertEqual(factory.current_chunk_size, chunk_size)

    @defer.inlineCallbacks
    def test_startFactory(self):
        """Test startFactory method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = lambda *a: None
        mp_upload = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        part_d = defer.succeed(None)
        expect(factory._chunk_iter()).result([chunk_size]).count(1)
        expect(factory._upload_part(chunk_size)).result(part_d).count(1)
        with mocker:
            yield factory.startFactory()
            self.assertEqual(factory.finished, True)
            self.assertEqual(factory._current_client_factory, None)
            self.assertTrue(factory.deferred.called)

    @defer.inlineCallbacks
    def test_startFactory_connectionDone(self):
        """Test startFactory handling ConnectionDone."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = lambda *a: None
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        part_d = defer.fail(txerror.ConnectionDone())
        factory = mocker.patch(_factory)
        expect(factory._chunk_iter()).result([chunk_size]).count(1)
        expect(factory.new_consumer(chunk_size)).result(consumer).count(1)
        expect(consumer.deferred).result(part_d).count(2)
        expect(consumer.cancel()).result(defer.succeed(None)).count(1)
        with mocker:
            yield factory.startFactory()

            self.assertEqual(factory.finished, False)
            self.assertEqual(factory._current_client_factory, None)
            self.assertTrue(factory.deferred.called)
            try:
                yield factory.deferred
            except txerror.ConnectionDone:
                # all good
                pass
            else:
                self.fail("Should fail with ConnectionDone?")

    @defer.inlineCallbacks
    def test_startFactory_connectionDone_cancelled(self):
        """Test startFactory handling ConnectionDone."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = lambda *a: None
        mp_upload = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        _factory.cancelled = True
        part_d = defer.fail(txerror.ConnectionDone())
        factory = mocker.patch(_factory)
        expect(factory._chunk_iter()).result([chunk_size]).count(1)
        expect(factory._upload_part(chunk_size)).result(part_d).count(1)
        with mocker:
            yield factory.startFactory()
            self.assertEqual(factory.finished, True)
            self.assertEqual(factory.cancelled, True)
            self.assertTrue(factory.deferred.called)
            result = yield factory.deferred
            self.assertIsInstance(result, upload.MultipartUploadFactory)

    @defer.inlineCallbacks
    def test_startFactory_error(self):
        """Test startFactory error handling."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = lambda *a: None
        mp_upload = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        part_d = defer.fail(RuntimeError("boom!"))
        factory = mocker.patch(_factory)
        expect(factory._chunk_iter()).result([chunk_size]).count(1)
        expect(factory._upload_part(chunk_size)).result(part_d).count(1)
        with mocker:
            yield factory.startFactory()
            self.assertEqual(factory.finished, False)
            self.assertEqual(factory._current_client_factory, None)
            self.assertTrue(factory.deferred.called)
            try:
                yield factory.deferred
            except RuntimeError:
                # all good.
                pass
            else:
                self.fail("Should fail with RuntimeError.")

    @defer.inlineCallbacks
    def test__upload_part(self):
        """Test _upload_part method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        http_producer = mocker.mock()
        c2b = mocker.mock()
        # patch the ConsumerToBuffer class
        ConsumerToBuffer = mocker.replace("s3lib.producers.ConsumerToBuffer")
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.new_consumer(chunk_size)).result(consumer).count(1)
        # consumer result is a s3lib.contrib.http_client.HTTPProducer instance
        expect(consumer.deferred).result(defer.succeed(http_producer)).count(1)
        # create the ConsumerToBuffer(http_producer)
        expect(ConsumerToBuffer(http_producer)).result(c2b).count(1)
        # wait for the http_producer deferred
        expect(http_producer.deferred).result(defer.succeed(True)).count(1)
        # wait for the ConsumerToBuffer deferred
        expect(c2b.deferred).result(defer.succeed(""))
        # part_done_cb gets called
        expect(part_done_cb(_factory._chunk_count, chunk_size,
                            _factory.inflated_size, _factory.crc32,
                            _factory.hash_object.get_context(),
                            _factory.magic_hash_object.get_context(),
                            _factory.decompressor.get_state())).count(1)
        with mocker:
            d = factory._upload_part(chunk_size)
            self.assertEqual(_factory._current_client_factory, consumer)
            yield d

    @defer.inlineCallbacks
    def test__upload_part_cancelled(self):
        """Test _upload_part whith a cancelled factory"""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        http_producer = mocker.mock()
        c2b = mocker.mock()
        # patch the ConsumerToBuffer class
        ConsumerToBuffer = mocker.replace("s3lib.producers.ConsumerToBuffer")
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.new_consumer(chunk_size)).result(consumer).count(1)
        # consumer result is a s3lib.contrib.http_client.HTTPProducer instance
        expect(consumer.deferred).result(defer.succeed(http_producer)).count(1)
        expect(factory.cancelled).result(True).count(1)
        expect(http_producer.stopProducing()).count(1)
        # create the ConsumerToBuffer(http_producer)
        expect(ConsumerToBuffer(http_producer)).result(c2b).count(0)
        # wait for the http_producer deferred
        expect(http_producer.deferred).result(defer.succeed(True)).count(0)
        # wait for the ConsumerToBuffer deferred
        expect(c2b.deferred).result(defer.succeed("")).count(0)
        # part_done_cb gets called
        expect(part_done_cb(_factory._chunk_count, chunk_size,
                            _factory.inflated_size, _factory.crc32,
                            _factory.hash_object.get_context(),
                            _factory.magic_hash_object.get_context(),
                            _factory.decompressor.get_state())).count(0)
        with mocker:
            d = factory._upload_part(chunk_size)
            self.assertEqual(_factory._current_client_factory, consumer)
            yield d

    @defer.inlineCallbacks
    def test__upload_part_http_producer_error(self):
        """Test _upload_part with an error in the HTTPProducer deferred."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        http_producer = mocker.mock()
        c2b = mocker.mock()
        # patch the ConsumerToBuffer class
        ConsumerToBuffer = mocker.replace("s3lib.producers.ConsumerToBuffer")
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.new_consumer(chunk_size)).result(consumer).count(1)
        # consumer result is a s3lib.contrib.http_client.HTTPProducer instance
        expect(consumer.deferred).result(defer.succeed(http_producer)).count(1)
        # create the ConsumerToBuffer(http_producer)
        expect(ConsumerToBuffer(http_producer)).result(c2b).count(1)
        # wait for the http_producer deferred
        expect(http_producer.deferred).result(defer.succeed(False)).count(1)
        expect(c2b.deferred).count(0)
        # part_done_cb gets called
        expect(part_done_cb(_factory._chunk_count, chunk_size,
                            _factory.inflated_size, _factory.crc32,
                            _factory.hash_object.get_context(),
                            _factory.magic_hash_object.get_context(),
                            _factory.decompressor.get_state())).count(0)
        with mocker:
            try:
                yield factory._upload_part(chunk_size)
            except ValueError, e:
                self.assertEqual(str(e), "Got ret from rp: False")
            else:
                self.fail("Should fail with ValueError.")

    @defer.inlineCallbacks
    def test__upload_part_consumer2buffer_error(self):
        """Test _upload_part error handling of ConsumerToBuffer deferred."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        http_producer = mocker.mock()
        c2b = mocker.mock()
        # patch the ConsumerToBuffer class
        ConsumerToBuffer = mocker.replace("s3lib.producers.ConsumerToBuffer")
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.new_consumer(chunk_size)).result(consumer).count(1)
        # consumer result is a s3lib.contrib.http_client.HTTPProducer instance
        expect(consumer.deferred).result(defer.succeed(http_producer)).count(1)
        # create the ConsumerToBuffer(http_producer)
        expect(ConsumerToBuffer(http_producer)).result(c2b).count(1)
        # wait for the http_producer deferred
        expect(http_producer.deferred).result(defer.succeed(True)).count(1)
        # wait for the ConsumerToBuffer deferred
        expect(c2b.deferred).result(defer.succeed("boom!"))
        # part_done_cb gets called
        expect(part_done_cb(_factory._chunk_count, chunk_size,
                            _factory.inflated_size, _factory.crc32,
                            _factory.hash_object.get_context(),
                            _factory.decompressor.get_state())).count(0)
        with mocker:
            try:
                yield factory._upload_part(chunk_size)
            except ValueError, e:
                self.assertEqual(str(e), "Got ret from rc: 'boom!'")
            else:
                self.fail("Should fail with ValueError.")

    @defer.inlineCallbacks
    def test_cancel(self):
        """Test cancel method."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        consumer = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.quitProducing()).count(1)
        expect(factory._current_client_factory).result(consumer).count(3)
        expect(consumer.cancel()).count(1)
        expect(consumer.deferred).result(defer.succeed(None)).count(1)
        factory._current_client_factory = None
        with mocker:
            yield factory.cancel()
            self.assertTrue(factory.cancelled)

    @defer.inlineCallbacks
    def test_cancel_cancelled(self):
        """Test cancel method with an already cancelled factory."""
        message = "101" * 10
        zmessage = zlib.compress(message)
        chunk_size = 1024
        crc32_value = offset = 0
        mocker = Mocker()
        part_done_cb = mocker.mock()
        mp_upload = mocker.mock()
        _factory = upload.MultipartUploadFactory(
            mp_upload, chunk_size, len(zmessage), offset, len(message),
            crc32_value, part_done_cb=part_done_cb)
        factory = mocker.patch(_factory)
        expect(factory.cancelled).result(True).count(1)
        # check that nothing else is called after the check of cancelled
        expect(factory._current_client_factory).count(0)
        with mocker:
            yield factory.cancel()


class MultipartUploadFactoryTest(UploadTestCase):
    """Tests for MultipartUploadFactory."""

    @defer.inlineCallbacks
    def test_dont_write_while_paused(self):
        """Test the _chunk_iter method."""
        # patch FileDescriptor bufferSize
        from twisted.internet import abstract
        self.patch(abstract.FileDescriptor, 'bufferSize', 1024)
        message = zlib.compress(os.urandom(1024 * 20))
        key_name = "test_dont_write_while_paused"
        mp_upload = yield self.s3.create_multipart_upload("test", key_name)

        def data_iter():
            """Iterate over chunks."""
            chunk_sz = 256
            for part in xrange(0, len(message), chunk_sz):
                yield factory.dataReceived(message[part:part + chunk_sz])

        factory = upload.MultipartUploadFactory(
            mp_upload, 1024, len(message), 0, 1024 * 20,
            crc32(message), part_done_cb=lambda *a: None)

        test_deferred = defer.Deferred()
        # patch factory.resumeProducing to catch the error
        orig_resumeProducing = factory.resumeProducing

        def resumeProducing():
            try:
                orig_resumeProducing()
            except RuntimeError, e:
                test_deferred.errback(e)
        self.patch(factory, 'resumeProducing', resumeProducing)
        factory.startFactory()
        # wire the factory deferred
        factory.deferred.chainDeferred(test_deferred)

        self._cooperator.coiterate(data_iter())
        try:
            yield test_deferred
        except Exception, e:
            yield factory.multipart_upload.cancel()
            self.fail(e)

    @defer.inlineCallbacks
    def test_stopproducing_reset_buffers_size(self):
        """Reset the buffer_size when stopProducing is called."""
        upload.MultipartUploadFactory.buffers_size = 0
        message = zlib.compress(os.urandom(1024))
        key_name = "multipart_factory_put"
        mp_upload = yield self.s3.create_multipart_upload("test", key_name)
        factory = upload.MultipartUploadFactory(
            mp_upload, 1024, len(message), 512, 1024,
            crc32(message), part_done_cb=lambda *a: None)
        factory.startFactory()
        # we are paused, so lets buffer some bytes
        factory.dataReceived('a' * 100)
        self.assertTrue(factory._buffer)
        self.assertEqual(upload.MultipartUploadFactory.buffers_size, 100)
        yield factory.cancel()
        factory.stopProducing()
        self.assertFalse(factory._buffer)
        self.assertEqual(upload.MultipartUploadFactory.buffers_size, 0)

    @defer.inlineCallbacks
    def test_multipart_put(self):
        """Put an object using multipart and streaming, get it back."""
        message = zlib.compress(os.urandom(1024 * 20))
        key_name = "multipart_factory_put"

        def data_iter():
            """Iterate over chunks."""
            chunk_sz = 64
            for part in xrange(0, len(message), chunk_sz):
                yield factory.dataReceived(message[part:part + chunk_sz])

        mp_upload = yield self.s3.create_multipart_upload("test", key_name)

        called = []

        def part_done_callback(part_num, size, inflated_size, crc32,
                               hash_ctx, magic_hash_ctx, zlib_ctx):
            called.append((part_num, size, inflated_size, crc32,
                           hash_ctx, magic_hash_ctx, zlib_ctx))
            return None

        factory = upload.MultipartUploadFactory(
            mp_upload, 1024, len(message), 0, 1024 * 20,
            crc32(message), part_done_cb=part_done_callback)
        factory.startFactory()

        self._cooperator.coiterate(data_iter())
        try:
            yield factory.deferred
        except Exception:
            yield factory.multipart_upload.cancel()
            raise
        else:
            upload_info = yield factory.multipart_upload.complete()

        self.assertEqual(upload_info["bucket"], "test")
        self.assertEqual(upload_info["key"], key_name)
        self.assertEqual(upload_info["etag"],
                         '"%s"' % hashlib.md5(message).hexdigest())
        message_2 = yield self.s3.get("test", upload_info["key"]).deferred
        self.assertEqual(message, message_2)
        self.assertEqual(len(message) // 1024, len(called))
        for num, size, _, _, _, _, _ in called:
            # we could have a part with size at most 99
            self.assertApproximates(size, 1024, 1023)


class ProxyHashingProducerTest(UploadTestCase):
    """Tests for ProxyHashingProducer."""

    @defer.inlineCallbacks
    def test_proxy_producer(self):
        """Test ProxyHashingProducer."""
        data = os.urandom(1024 * 10)
        message = zlib.compress(data)
        s3producer = producers.S3Producer(len(message))
        producer = upload.ProxyHashingProducer(s3producer)
        consumer = testcase.BufferedConsumer(producer)
        producer.resumeProducing()

        chunk_sz = 10
        for part in xrange(0, len(message), chunk_sz):
            yield producer.dataReceived(message[part:part + chunk_sz])
        producer.flush_decompressor()

        self.assertEqual(consumer.buffer.getvalue(), message)
        hasher = content_hash_factory()
        hasher.update(data)
        self.assertEqual(producer.hash_object.content_hash(),
                         hasher.content_hash())
        magic_hasher = magic_hash_factory()
        magic_hasher.update(data)
        self.assertEqual(producer.magic_hash_object.content_hash()._magic_hash,
                         magic_hasher.content_hash()._magic_hash)
        self.assertEqual(producer.inflated_size, len(data))
        self.assertEqual(producer.crc32, crc32(data))


class HashingMixinTestCase(UploadTestCase):
    """Tests for the HashingMixin."""

    def test_add_deflated_data(self):
        """Test that add_deflated_data decompress in chunks."""
        raw_data = os.urandom(1000)
        data = zlib.compress(raw_data)
        hm = upload.HashingMixin()
        called = []
        # patch add_inflated_data to check the chunks
        self.patch(hm, 'add_inflated_data', called.append)
        hm.add_deflated_data(data)
        # check that we have all the chunks
        self.assertEqual(10 + len(data) % 10, len(called))
        # check that the inflated data is equal to the raw data.
        self.assertEqual(raw_data, ''.join(called))

    def test_add_deflated_data_odd(self):
        """Test that add_deflated_data decompress in chunks."""
        raw_data = os.urandom(1333)
        data = zlib.compress(raw_data)
        hm = upload.HashingMixin()
        called = []
        # patch add_inflated_data to check the chunks
        self.patch(hm, 'add_inflated_data', called.append)
        hm.add_deflated_data(data)
        self.assertEqual(raw_data, ''.join(called))
        # check that we have all the chunks
        if len(data) % 10:
            self.assertEqual(11, len(called))
        else:
            self.assertEqual(10, len(called))
        # check that the inflated data is equal to the raw data.
        self.assertEqual(raw_data, ''.join(called))

    def test_add_deflated_data_zero(self):
        """Test that add_deflated_data decompress in chunks."""
        hm = upload.HashingMixin()
        called = []
        # patch add_inflated_data to check the chunks
        self.patch(hm, 'add_inflated_data', called.append)
        hm.add_deflated_data('')
        # check that we have all the chunks
        self.assertEqual(0, len(called))
