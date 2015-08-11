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

"""Provides Producers and Consumers for uploading stuff to S3.

There are two implementations ProxyHashingProducer and
ProxyConsumerHashingSizedProducer and a base class: HashingMixin

ProxyHashingProducer is a producer proxy that calculates the hash of the entire
produced data.
"""

import logging
import collections
import zlib

from cStringIO import StringIO

from twisted.internet import defer, interfaces
from twisted.python.failure import Failure
import twisted.internet.error
from zope.interface import implements

from s3lib.producers import ConsumerToBuffer
from config import config
from ubuntuone.storageprotocol.content_hash import (
    content_hash_factory,
    crc32,
    magic_hash_factory,
)
from ubuntuone.storage import rhashlib, rzlib
from ubuntuone.storage.server import errors

logger = logging.getLogger("storage.server.upload")


class HashingMixin(object):
    """A mixin with hashing helpers.

    Calculates the hash, crc32 and inflated_size.
    """
    chunks = 10

    def __init__(self, hash_factory=content_hash_factory,
                 decompressobj_factory=zlib.decompressobj,
                 magic_hash_factory=magic_hash_factory):
        self.decompressor = decompressobj_factory()
        self.hash_object = hash_factory()
        self.magic_hash_object = magic_hash_factory()
        self.crc32 = 0
        self.inflated_size = 0

    def decompress(self, data):
        """Inflate the raw data."""
        try:
            return self.decompressor.decompress(data)
        except zlib.error, e:
            # bad data makes zlib cry
            raise errors.UploadCorrupt(str(e))

    def add_inflated_data(self, data):
        """Process inflated data to make sure checksums match."""
        self.hash_object.update(data)
        self.magic_hash_object.update(data)
        self.crc32 = crc32(data, self.crc32)
        self.inflated_size += len(data)

    def add_deflated_data(self, data):
        """Helper to decompress, hash, etc. in smaller chunks."""
        if not data:
            # avoid any extra work, there is nothing to see here.
            return
        chunk_size = len(data) // self.chunks
        buf = StringIO(data)
        for i in xrange(self.chunks):  # split the data in 10 chunks
            self.add_inflated_data(self.decompress(buf.read(chunk_size)))
        if buf.tell() < len(data):
            self.add_inflated_data(self.decompress(buf.read()))

    def flush_decompressor(self):
        """Flush the decompressor object and handle pending bytes."""
        final_data = self.decompressor.flush()
        self.add_inflated_data(final_data)


class ProxyHashingProducer(HashingMixin):
    """A producer that hash and streams stuff off to another producer.

    This producer calculates the hash, crc32 and inflated size.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, producer):
        super(ProxyHashingProducer, self).__init__()
        self.producer = producer

    def __getattr__(self, name):
        """Override getattr to get the attr from self.producer."""
        return getattr(self.producer, name)

    def set_consumer(self, consumer):
        """consumer setter."""
        self.producer.consumer = consumer

    def get_consumer(self):
        """consumer getter."""
        return self.producer.consumer

    consumer = property(get_consumer, set_consumer)

    @property
    def finished(self):
        """Returns self.producer.finished."""
        return self.producer.finished

    def resumeProducing(self):
        """See IPushProducer."""
        self.producer.resumeProducing()

    def stopProducing(self):
        """See IPushProducer."""
        self.producer.stopProducing()

    def pauseProducing(self):
        """See IPushProducer."""
        self.producer.pauseProducing()

    def dataReceived(self, data):
        """Handle data from client."""
        self.producer.dataReceived(data)
        self.add_deflated_data(data)


class MultipartUploadFactory(HashingMixin):
    """A Producer that's also a consumer factory.

    Fires a new consumer for each chunk.
    Also hash the data and keep the hash context for each chunk.
    """
    implements(interfaces.IPushProducer)

    consumer = None
    buffers_size = 0
    producerStreaming = True

    def __init__(self, multipart_upload, chunk_size,
                 total_size, offset, inflated_size, crc32, chunk_count=None,
                 hash_context=None, magic_hash_context=None,
                 decompress_context=None, part_done_cb=None,
                 hash_factory=rhashlib.sha1,
                 magic_hash_factory=rhashlib.resumable_magic_hash_factory,
                 decompressobj_factory=rzlib.decompressobj):
        super(MultipartUploadFactory, self).__init__(
            hash_factory=hash_factory, magic_hash_factory=magic_hash_factory,
            decompressobj_factory=decompressobj_factory)
        if total_size < 0:
            raise ValueError("Total size must be positive: %r" % total_size)
        self.total_size = total_size
        if chunk_size < 0:
            raise ValueError("Chunk size must be positive: %r" % chunk_size)
        # internal housekeeping
        self.finished = False
        self.cancelled = False
        self.paused = True
        self.producer = None  # holds the client transport
        self.bytes_received = 0
        self.bytes_sent = 0
        self._already_sent = 0
        self.deflated_size = 0
        self.buffer_size = 0
        self.current_chunk_size = 0
        if offset:
            self.deflated_size += offset
        if inflated_size:
            self.inflated_size = inflated_size
        if crc32:
            self.crc32 = crc32
        if hash_context:
            self.hash_object.set_context(hash_context)
        if magic_hash_context:
            self.magic_hash_object.set_context(magic_hash_context)
        if decompress_context:
            self.decompressor.set_state(decompress_context)
        self._buffer = collections.deque()
        self.multipart_upload = multipart_upload
        self.chunk_size = chunk_size
        self.offset = offset
        self.part_done_cb = part_done_cb
        self._chunk_count = chunk_count or 0
        # this is the current streamingfactory
        self._current_client_factory = None
        # this is the deferred we fire when we're done
        self.deferred = defer.Deferred()
        # add a call/errback to resume the client transport no matter what

        def resume_producer(r):
            """call resumeProducing on the client transport."""
            if self.producer:
                self.producer.resumeProducing()
            return r

        self.deferred.addBoth(resume_producer)
        # initial connect deferred
        self.connect_deferred = defer.Deferred()

    def flush_buffer(self):
        """Flush the buffer."""
        send_more = True
        while self._buffer and send_more and not self.paused:
            data = self._buffer.popleft()
            data_len = len(data)
            self.__class__.buffers_size -= data_len
            self.buffer_size -= data_len
            send_more = self._send(data)
        # if we got paused while flushing, return False
        if self.paused:
            return False
        return send_more

    # IProducer interface
    def resumeProducing(self):
        """see IPushProducer"""
        # send any buffered data while paused
        self.paused = False
        try:
            self.flush_buffer()
        except errors.UploadCorrupt, e:
            self.deferred.errback(e)
        # resumeProducing on the client transport
        if self.producer:
            self.producer.resumeProducing()

    def pauseProducing(self):
        """see IPushProducer"""
        self.paused = True

    def stopProducing(self):
        """ ISizedProducer stopProducing interface """
        self.pauseProducing()
        # resume client transport
        if self.producer:
            self.producer.resumeProducing()
        if self._buffer is not None:
            # cleanup buffers_size, as we are never going to flush the buffer.
            self.__class__.buffers_size -= self.buffer_size
            self.buffer_size = 0
            # cleanup the buffer
            self._buffer = None
        if self.current_chunk_size > self._already_sent:
            logger.warning(
                "%s forced to stop producing - already sent: %s",
                self.__class__.__name__, self._already_sent)
        self.flush_decompressor()

    def _finishConsumer(self):
        """ finish off with the current consumer """
        if self.consumer is None:
            raise RuntimeError("need a consumer to finish!")
        self.pauseProducing()
        self.consumer.unregisterProducer()
        self.consumer = None
        self.flush_decompressor()

    def quitProducing(self):
        """ ISizedProducer quitProducing interface """
        self.stopProducing()
        if self.consumer is not None:
            self.consumer.unregisterProducer()
            self.consumer.stopProducing()
            self.consumer = None

    def registerProducer(self, producer):
        """Register a producer, should be the client connection."""
        self.producer = producer

    def unregisterProducer(self):
        """Unregister the producer."""
        self.producer = None

    def dataReceived(self, data):
        """Push data to the consumer or buffer it."""
        if self._buffer is None:
            # stopProducing() was already called, but client is still sending
            # data.  Ignore the data, since an error response will eventually
            # be propagated to the client.
            return
        self.bytes_received += len(data)
        if self.paused:
            # increase the counter
            data_len = len(data)
            self.__class__.buffers_size += data_len
            self.buffer_size += data_len
            self._buffer.append(data)
        else:
            send_more = self.flush_buffer()
            # if we just finished a consumer, buffer the data
            if not send_more or self.paused:
                # increase the counter
                data_len = len(data)
                self.__class__.buffers_size += data_len
                self.buffer_size += data_len
                self._buffer.append(data)
            else:
                self._send(data)
        # check buffer size
        if self.buffer_size >= config.api_server.upload_buffer_max_size:
            # we should pause the client transport/conn
            self.producer.pauseProducing()

    def _send(self, data):
        """Actually write data to the consumer.

        Return a bool, True if more data can be sent, else False.
        """
        if self.consumer is None:
            raise RuntimeError("write called while no consumer is active")
        if self.paused:
            raise RuntimeError("Asked to write to consumer while paused")
        if not data:
            return True

        data_size = len(data)
        if self._already_sent + data_size >= self.current_chunk_size:
            # we have more buffered that we can send to this consumer, so
            # send enough to comply with the chunk size...
            len_to_send = self.current_chunk_size - self._already_sent
            self.writeToConsumer(data[:len_to_send])
            # ...and save the rest for later, at the left of the deque
            extra_data = data[len_to_send:]
            if extra_data:
                # increase the counter and add back to the buffer, but only if
                # there is any extra_data
                extra_data_len = len(extra_data)
                self.__class__.buffers_size += extra_data_len
                self.buffer_size += extra_data_len
                self._buffer.appendleft(extra_data)
            # and of course, finish the current consumer
            self._finishConsumer()
            self._already_sent = 0
            return False
        else:
            # we can send all the data
            self.writeToConsumer(data)
            self._already_sent += data_size
            # only finish this consumer if our internal buffer is empty
            if self.finished and not self._buffer:
                self._finishConsumer()
            return True

    def writeToConsumer(self, data):
        """Write data to consumer and hash it."""
        data_len = len(data)
        self.bytes_sent += data_len
        self.consumer.write(data)
        self.deflated_size += data_len
        self.add_deflated_data(data)

    def flush_decompressor(self):
        """Flush the decompressor object and handle pending bytes."""
        decompressor = self.decompressor.copy()
        final_data = decompressor.flush()
        self.add_inflated_data(final_data)

    def _chunk_iter(self):
        """Generator for chunk sizes.

        This will generate chunks of the specified size, and if there are extra
        bytes, those will be appended to the last chunk. Because we have
        minimun size in S3 parts of 5MB.
        """
        chunk_size = self.chunk_size
        total_size = self.total_size - self.offset
        for i in xrange(total_size // chunk_size - 1):
            yield chunk_size
        yield total_size % chunk_size + chunk_size

    def new_consumer(self, chunk_size):
        """Instantiate a new consumer for a chunk_size."""
        # create a consumer that will write to S3 the contents fo the current
        # chunk
        self._chunk_count += 1
        consumer = self.multipart_upload._upload_part(
            self, str(chunk_size), self._chunk_count)
        # if it's the first connection, hook the connect_deferred
        if not self.connect_deferred.called:
            consumer.connect_deferred.chainDeferred(self.connect_deferred)
        # set how much we'll write this time around
        self.current_chunk_size = chunk_size
        return consumer

    @defer.inlineCallbacks
    def startFactory(self):
        """Start running the circus.

        Start a new consumer for each chunk and call self.part_done_cb when
        each chunk finish.
        """
        for chunk_size in self._chunk_iter():
            try:
                yield self._upload_part(chunk_size)
            except twisted.internet.error.ConnectionDone:
                # if we catch this as a result of being cancelled, it's ok,
                # whomever cancelled us doesn't expect us to go ka-boom
                if self.cancelled:
                    # stop the loop, but finish the factory
                    break
                failure = Failure()
                yield self.cancel()
                self.deferred.errback(failure)
                return
            except Exception:
                # catch everything else, and bailout
                failure = Failure()
                yield self.cancel()
                self.deferred.errback(failure)
                return

        self.finished = True
        self._current_client_factory = None
        if not self.deferred.called:
            self.deferred.callback(self)

    @defer.inlineCallbacks
    def _upload_part(self, chunk_size):
        """Upload a single part/chunk."""
        consumer = self.new_consumer(chunk_size)
        self._current_client_factory = consumer
        rp = yield consumer.deferred
        # a put operation has finished
        if self.cancelled:
            # and we got cancelled jut before getting here
            rp.stopProducing()
            return
        rc = ConsumerToBuffer(rp)
        ret = yield rp.deferred
        # check the status of the result producer

        if not ret:
            raise ValueError("Got ret from rp: %r" % (ret,))
        # get the actual status result and check it as well
        ret = yield rc.deferred
        if ret != '':
            # should be '' on a transfer that finished ok...
            raise ValueError("Got ret from rc: %r" % (ret,))
        hash_context = self.hash_object.get_context()
        magic_hash_context = self.magic_hash_object.get_context()
        decompress_context = self.decompressor.get_state()
        yield self.part_done_cb(self._chunk_count, chunk_size,
                                self.inflated_size, self.crc32,
                                hash_context, magic_hash_context,
                                decompress_context)

    @defer.inlineCallbacks
    def cancel(self):
        """Cancel this factory's work."""
        if self.cancelled:
            # already cancelled, ignore this call.
            return
        # try to stop the deferred chain from working
        self.cancelled = True
        # shut down the proxy producer-consumer operation
        self.quitProducing()
        # if we were writing to a blob, abort that as well
        if self._current_client_factory:
            self._current_client_factory.cancel()
            d = self._current_client_factory.deferred
            self._current_client_factory = None
            yield d
