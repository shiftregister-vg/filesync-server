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

"""Provides a layer to handle all the database objects from twisted.

This layer is the main interface to the RPC DAL.
"""

import calendar
import logging
import posixpath as pypath
import sys
import uuid
import weakref
import zlib

import twisted.internet.error
import twisted.web.error

from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.web import http

from s3lib.s3lib import ProducerStopped
from s3lib.producers import S3Producer, NullConsumer
from backends.filesync.data import errors as dataerrors
from config import config
from ubuntuone import txutils
from ubuntuone.storage.server import errors, upload
from ubuntuone.storageprotocol import protocol_pb2

ZERO_LENGTH_CONTENT_KEY = ""


class NetworkRetry(txutils.NetworkRetry):
    """NetworkRetry subclass that raise RetryLimitReached."""

    def __init__(self, *args, **kwargs):
        super(NetworkRetry, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('storage.server')

    def _handle_retried_exc(self, exc, retry_count, func, args, kwargs):
        """Handle a exception inside the retry loop. (do nothing by default)"""
        self.logger.warning("Retrying: %s with: %s, %s - failed with: "
                            "%s('%s') - retry_count: %d", func, args, kwargs,
                            exc.__class__.__name__, exc, retry_count)

    def _handle_exc(self, exc):
        """Handle a exception raised by the last retry.

        This subclass returns a RetryLimitReached failure.
        """
        msg = ("Maximum retries (%i) reached. Please try again.\n"
               "Original exception: %s: %s" % (self.retries,
                                               exc.__class__.__name__,
                                               str(exc)))
        return defer.fail(Failure(dataerrors.RetryLimitReached(msg),
                                  dataerrors.RetryLimitReached,
                                  sys.exc_info()[2]))


class MultipartRetry(NetworkRetry):
    """NetworkRetry subclass that handle S3 errors."""

    def _handle_retried_exc(self, exc, retry_count, func, args, kwargs):
        """Handle a exception inside the retry loop.

        if it's a 404 error, bail out.

        This method is always called from a try/except.
        """
        if isinstance(exc, twisted.web.error.Error):
            # propagate 404 errors
            if exc.status == '404':
                raise
        super(MultipartRetry, self)._handle_retried_exc(
            exc, retry_count, func, args, kwargs)


class FalseProducer(object):
    """Not really a producer, just deliver all the data when asked to.

    It has all the methods to comply the Push or Pull Producer Interface,
    but the only one implemented is resumeProducing: sends all the data.
    """

    def __init__(self, data):
        self.data = data
        self.deferred = defer.Deferred()
        self.consumer = None

    def resumeProducing(self):
        """Resume producing, just send all the data."""
        if self.consumer:
            self.consumer.write(self.data)
        self.deferred.callback(True)

    def stopProducing(self):
        """Stop producing."""

    def pauseProducing(self):
        """Pause producing."""


class Node(object):
    """StorageObject proxy."""

    s3_retries = config.api_server.s3_retries
    s3_retry_wait = config.api_server.s3_retry_wait

    def __init__(self, manager, node):
        """Create a Node.

        @param manager: the ContentManager which created this object
        @param node: a dao.StorageNode
        """
        self.manager = manager
        self.id = node['id']
        self.volume_id = node['volume_id']
        self.path = node['path']
        self.name = node['name']
        self.parent_id = node['parent_id']
        self.is_file = node['is_file']
        self.content_hash = node['content_hash']
        self.size = node['size'] or 0
        self.crc32 = node['crc32'] or 0
        self.deflated_size = node['deflated_size'] or 0
        self.is_live = node['is_live']
        self.generation = node['generation']
        self.is_public = node['is_public']
        last_modif = node['last_modified']

        # special cases for no content
        if node['storage_key'] is None:
            self.has_content = False
            self.storage_key = ZERO_LENGTH_CONTENT_KEY
        else:
            self.has_content = node['has_content']
            self.storage_key = node['storage_key']

        self.last_modified = calendar.timegm(last_modif.timetuple())
        self.node = node
        self.logger = logging.getLogger('storage.server')

    @defer.inlineCallbacks
    def get_content(self, start=None, end=None, previous_hash=None, user=None):
        """Get the content for this node.

        @param start: the start offset
        @param end: the end offset
        @param previous_hash: not used for FileNode.
        @param user: the user doing the request, useful for logging.
        """
        if not self.is_file:
            raise TypeError("Content can be retrieved only on Files.")
        storage_key = self.storage_key
        size = self.deflated_size
        if storage_key == ZERO_LENGTH_CONTENT_KEY:
            # we send the compressed empty string
            producer = FalseProducer(zlib.compress(""))
            defer.returnValue(producer)
            return
        if start is not None or end is not None:
            headers = {'Range': 'bytes=%d-%d' % (
                start if start is not None else 0,
                end if end is not None else size)}
        else:
            headers = None

        retrier = NetworkRetry(catch=twisted.internet.error.TimeoutError,
                               retries=self.s3_retries,
                               retry_wait=self.s3_retry_wait)
        producer = yield retrier(self._get_producer,
                                 config.api_server.s3_bucket,
                                 str(storage_key), headers, user)
        defer.returnValue(producer)

    def _get_from_s3(self, bucket, key, headers, streaming):
        """Inner function to handle s3.get."""
        s3 = self.manager.factory.s3()
        return s3.get(bucket, key, headers=headers, streaming=streaming)

    @defer.inlineCallbacks
    def _get_producer(self, bucket, storage_key,
                      headers, user, streaming=True):
        """Return the content Producer."""
        try:
            get_response = self._get_from_s3(bucket, str(storage_key),
                                             headers, streaming)
            producer = yield get_response.deferred
        except twisted.web.error.Error, exc:
            # if fallback bucket is configured, and the file was
            # not found, try that bucket.
            if config.api_server.s3_fallback_bucket and exc.status == '404':
                get_response = self._get_from_s3(
                    config.api_server.s3_fallback_bucket,
                    str(storage_key), headers, streaming)
                try:
                    producer = yield get_response.deferred
                except twisted.web.error.Error, exc:
                    yield self._handle_s3_errors(Failure(exc), user)
            else:
                yield self._handle_s3_errors(Failure(exc), user)

        producer.deferred.addErrback(self._handle_s3_errors, user)
        defer.returnValue(producer)

    def _context_msg(self, user):
        """Return a string with the context."""
        if user.protocols:
            session_ids = ','.join([str(p.session_id) for p in user.protocols])
        else:
            session_ids = 'No sessions?'
        context = dict(user_id=user.id,
                       username=user.username.replace('%', '%%'),
                       session_ids=session_ids, node_id=str(self.id))
        return ("%(session_ids)s - %(username)s (%(user_id)s) - "
                "node_id=%(node_id)s" % context)

    def _handle_s3_errors(self, failure, user):
        """Transform s3 errors in something more appropiate.

        s3 errors include all the errors in twisted.web.error, and also
        specifically twisted.internet.error.TimeoutError.

        Always return a failure or raise an exception.
        """
        if not failure.check(twisted.web.error.Error,
                             twisted.internet.error.TimeoutError):
            # not an s3 error
            return failure

        ctx = self._context_msg(user)
        if failure.check(twisted.internet.error.TimeoutError):
            self.logger.warning("%s - s3 threw TimeoutError while sending data"
                                " (returning TRY_AGAIN)", ctx)
            raise errors.S3DownloadError(
                failure.value, "TimeoutError while downloading data.")

        # ok, a web error
        status = failure.value.status
        if status == str(http.REQUESTED_RANGE_NOT_SATISFIABLE):
            self.logger.warning("%s - s3 returned a status code of 416.", ctx)
            raise errors.NotAvailable(failure.getErrorMessage())
        if status == str(http.NOT_FOUND):
            self.logger.warning("%s - s3 returned a status code of 404.", ctx)
            raise errors.NotAvailable(failure.getErrorMessage())
        else:
            self.logger.warning("%s - s3 threw %s while sending data "
                                "(returning TRY_AGAIN) with response: %r",
                                ctx, status, failure.value.response)
            msg = "Web error (status %s) while downloading data."
            raise errors.S3DownloadError(
                failure.value, msg % failure.value.status)


class DBUploadJob(object):
    """A proxy for Upload model objects."""

    def __init__(self, user, volume_id, node_id, uploadjob_id, uploaded_bytes,
                 multipart_id, multipart_key, chunk_count, inflated_size,
                 crc32, hash_context, magic_hash_context, decompress_context,
                 when_last_active):
        self.__dict__ = locals()

    @classmethod
    def get(cls, user, volume_id, node_id, uploadjob_id, hash_value, crc32,
            inflated_size, deflated_size):
        """Get a multipart upload job."""
        data = dict(user=user, volume_id=volume_id,
                    node_id=node_id)
        kwargs = dict(user_id=user.id, volume_id=volume_id, node_id=node_id,
                      uploadjob_id=uploadjob_id,
                      hash_value=hash_value, crc32=crc32,
                      inflated_size=inflated_size, deflated_size=deflated_size)
        d = user.rpc_dal.call('get_uploadjob', **kwargs)
        d.addCallback(lambda r: r.update(data) or r)
        d.addCallback(lambda r: cls(**r))
        return d

    @classmethod
    def make(cls, user, volume_id, node_id, previous_hash,
             hash_value, crc32, inflated_size, deflated_size, multipart_key):
        """Make an upload job."""
        data = dict(user=user, volume_id=volume_id,
                    node_id=node_id, multipart_key=multipart_key)
        kwargs = dict(user_id=user.id, volume_id=volume_id, node_id=node_id,
                      previous_hash=previous_hash,
                      hash_value=hash_value, crc32=crc32,
                      inflated_size=inflated_size,
                      deflated_size=deflated_size, multipart_key=multipart_key)
        d = user.rpc_dal.call('make_uploadjob', **kwargs)
        d.addCallback(lambda r: r.update(data) or r)
        d.addCallback(lambda r: cls(**r))
        return d

    def set_multipart_id(self, multipart_id):
        """Set the multipart id for the upload job."""
        self.multipart_id = multipart_id
        return self.user.rpc_dal.call('set_uploadjob_multipart_id',
                                      user_id=self.user.id,
                                      uploadjob_id=self.uploadjob_id,
                                      multipart_id=multipart_id)

    def add_part(self, chunk_size, inflated_size, crc32,
                 hash_context, magic_hash_context, decompress_context):
        """Add a part to an upload job."""
        kwargs = dict(user_id=self.user.id, uploadjob_id=self.uploadjob_id,
                      chunk_size=chunk_size, inflated_size=inflated_size,
                      crc32=crc32, hash_context=hash_context,
                      magic_hash_context=magic_hash_context,
                      decompress_context=decompress_context)
        return self.user.rpc_dal.call('add_part_to_uploadjob', **kwargs)

    def delete(self):
        """Delete an upload job."""
        return self.user.rpc_dal.call('delete_uploadjob', user_id=self.user.id,
                                      uploadjob_id=self.uploadjob_id)

    @defer.inlineCallbacks
    def touch(self):
        """Touch an upload job."""
        r = yield self.user.rpc_dal.call('touch_uploadjob',
                                         user_id=self.user.id,
                                         uploadjob_id=self.uploadjob_id)
        self.when_last_active = r['when_last_active']


class BaseUploadJob(object):
    """Main interface for Uploads."""

    _inflated_size_hint_mismatch = "Inflated size does not match hint."
    _deflated_size_hint_mismatch = "Deflated size does not match hint."
    _content_hash_hint_mismatch = "Content hash does not match hint."
    _magic_hash_hint_mismatch = "Magic hash does not match hint."
    _crc32_hint_mismatch = "Crc32 does not match hint."

    def __init__(self, user, file_node, previous_hash, hash_hint, crc32_hint,
                 inflated_size_hint, deflated_size_hint, session_id,
                 blob_exists, magic_hash):
        node_hash = file_node.content_hash
        if (node_hash or previous_hash) and \
                node_hash != previous_hash and node_hash != hash_hint:
            raise errors.ConflictError("Previous hash does not match.")

        self.user = user
        self.session_id = session_id
        self.magic_hash = magic_hash
        self.producer = None
        self.factory = None
        self.deferred = defer.Deferred()
        self.s3 = None
        self._initial_data = True
        # this are used to track keys uploaded into s3
        self._storage_key = None
        self.canceling = False
        self.logger = logging.getLogger('storage.server')

        self.original_file_hash = node_hash
        self.hash_hint = hash_hint
        self.crc32_hint = crc32_hint
        self.inflated_size_hint = inflated_size_hint
        self.deflated_size_hint = deflated_size_hint
        self.file_node = file_node
        self.blob_exists = blob_exists

    @property
    def buffers_size(self):
        """The total size of the buffer in this upload."""
        raise NotImplementedError("subclass responsability.")

    @property
    def upload_id(self):
        """Return the upload_id for this upload job."""
        raise NotImplementedError("subclass responsability.")

    @property
    def offset(self):
        """The offset of this upload."""
        raise NotImplementedError("subclass responsability.")

    @property
    def inflated_size(self):
        """The inflated size of this upload."""
        return self.producer.inflated_size

    @property
    def crc32(self):
        """The crc32 of this upload."""
        return self.producer.crc32

    @property
    def hash_object(self):
        """The hash_object of this upload."""
        return self.producer.hash_object

    @property
    def magic_hash_object(self):
        """The magic_hash_object of this upload."""
        return self.producer.magic_hash_object

    @defer.inlineCallbacks
    def connect(self):
        """Setup the producer and consumer (S3 connection)."""
        if self.s3 is None:
            self.s3 = self.user.manager.factory.s3()
        if self.blob_exists:
            # we have a storage object like this already
            # wrap the S3Producer to only hash and discard the bytes
            self.producer = upload.ProxyHashingProducer(self.producer)
            self.factory = NullConsumer()
            self.factory.registerProducer(self.producer)
            self.producer.resumeProducing()
            self.deferred.callback(None)
        else:
            # we need to upload this content, get ready for it
            d = self._start_receiving()
            d.addErrback(self._handle_connection_done)
            d.addErrback(self._handle_s3_errors)
            yield d
            # at this point, self.factory is set...if it's not it's ok to
            # blowup
            self.factory.deferred.addErrback(self._handle_connection_done)
            self.factory.deferred.addErrback(self._handle_s3_errors)
            self.factory.deferred.chainDeferred(self.deferred)

    def _start_receiving(self):
        """Prepare the upload job to start receiving streaming bytes."""
        # generate a new storage_key for this upload
        self._storage_key = uuid.uuid4()
        # replace self.producer with a hashing proxy
        self.producer = upload.ProxyHashingProducer(self.producer)
        self.factory = self.s3.put(
            config.api_server.s3_bucket,
            str(self._storage_key), self.producer,
            headers={'Content-Length': str(self.deflated_size_hint)},
            streaming=True)
        return self.factory.connect_deferred

    def add_data(self, data):
        """Add data to this upload."""
        # add data is called by the server with the bytes that arrive in a
        # packet. This is at most MAX_MESSAGE_SIZE bytes (2**16, 65k at the
        # moment).
        # zlib has a theoretical limit of compression of 1032:1, so this
        # means that at most we will get a 1032*2**16 ~= 64MB, meaning that
        # the memory usage for this has a maximum.
        # """the theoretical limit for the zlib format (as opposed to its
        # implementation in the currently available sources) is 1032:1."""
        # http://zlib.net/zlib_tech.html
        self.producer.dataReceived(data)
        if (not self.blob_exists and
                self.buffers_size >= config.api_server.upload_buffer_max_size):
            raise errors.BufferLimit("Buffer limit reached.")

    def registerProducer(self, producer):
        """Register a producer, this is the client connection."""
        # by default do nothing, only MultipartUploadJob care about this.
        pass

    def unregisterProducer(self):
        """Unregister the producer."""
        # by default do nothing, only MultipartUploadJob care about this.
        pass

    def cancel(self):
        """Cancel this upload job."""
        return self._stop_producer_and_factory()

    @defer.inlineCallbacks
    def _stop_producer_and_factory(self):
        """Cancel this upload job.

        - Unregister producer.
        - Stop the producer if not yet stopped.
        - Cancel the factory if one exists.
        """
        self.unregisterProducer()
        self.canceling = True
        if self.producer is not None:
            # upload already started
            try:
                self.producer.stopProducing()
            except ProducerStopped:
                # dont't care if stopped it in the middle, we're
                # canceling!
                pass
        if self.factory is not None:
            yield self.factory.cancel()

    def stop(self):
        """Stop the upload and cleanup."""
        return self._stop_producer_and_factory()

    def _handle_connection_done(self, failure):
        """Process error states encountered by producers and consumers """
        if failure.check(twisted.internet.error.ConnectionDone):
            # if we're on the canceling pathway, we expect this
            if self.canceling:
                return
            raise errors.UploadCanceled("Connection closed prematurely.")
        return failure

    def _handle_s3_errors(self, failure):
        """Handle s3lib twisted.web.error.Error and TimeoutError."""

        def context_msg():
            """Return a str with the context for this upload."""
            session_ids = ''
            if self.user.protocols:
                session_ids = ','.join([str(p.session_id)
                                        for p in self.user.protocols])
            upload_context = dict(
                user_id=self.user.id,
                username=self.user.username.replace('%', '%%'),
                session_ids=session_ids or 'No sessions?',
                volume_id=self.file_node.volume_id,
                node_id=self.file_node.id,
                bytes_received=(self.producer.bytes_received
                                if self.producer else 0),
                bytes_sent=(self.producer.bytes_sent
                            if self.producer else 0))
            context_msg = (
                '%(session_ids)s - %(username)s (%(user_id)s) - ' +
                'node: %(volume_id)s::%(node_id)s - ' +
                'recv: %(bytes_received)s - sent: %(bytes_sent)s - ')
            return context_msg % upload_context
        if failure.check(twisted.web.error.Error):
            self.logger.warning(context_msg() + "s3 threw %s while receiving "
                                "data (returning TRY_AGAIN) with response: %r",
                                failure.value.status, failure.value.response)
            raise errors.S3UploadError(failure.value,
                                       "Web error (status %s) while uploading"
                                       " data." % (failure.value.status,))
        elif failure.check(twisted.internet.error.TimeoutError,
                           twisted.internet.error.ConnectionLost):
            self.logger.warning(context_msg() + "S3 connection threw a "
                                "TimeoutError while receiving data (returning "
                                "TRY_AGAIN) ")
            raise errors.S3UploadError(failure.value)

        return failure

    def flush_decompressor(self):
        """Flush the decompresor and handle the data."""
        # by default the producer do the hashing
        self.producer.flush_decompressor()

    def commit(self, put_result):
        """Simple commit, overwrite for more detailed behaviour."""
        return self._commit(put_result)

    @defer.inlineCallbacks
    def _commit(self, put_result):
        """Make this upload the current content for the node."""
        if self.producer is not None:
            assert self.producer.finished, "producer hasn't finished"
        self.flush_decompressor()
        # size matches hint
        if self.deflated_size != self.deflated_size_hint:
            raise errors.UploadCorrupt(self._deflated_size_hint_mismatch)
        if self.inflated_size != self.inflated_size_hint:
            raise errors.UploadCorrupt(self._inflated_size_hint_mismatch)

        # get the magic hash value here, don't log it, don't save it
        magic_hash = self.magic_hash_object.content_hash()
        magic_hash_value = magic_hash._magic_hash
        # magic hash should match the one sent by the client
        if self.magic_hash is not None and magic_hash_value != self.magic_hash:
            raise errors.UploadCorrupt(self._magic_hash_hint_mismatch)

        # hash matches hint
        hash = self.hash_object.content_hash()
        if hash != self.hash_hint:
            raise errors.UploadCorrupt(self._content_hash_hint_mismatch)

        # crc matches hint
        if self.crc32 != self.crc32_hint:
            raise errors.UploadCorrupt(self._crc32_hint_mismatch)

        storage_key = self._storage_key
        if storage_key is None:
            storage_key = self.file_node.storage_key
        if storage_key is None and self.inflated_size == 0:
            storage_key = ZERO_LENGTH_CONTENT_KEY

        new_gen = yield self._commit_content(storage_key, magic_hash_value)
        defer.returnValue(new_gen)

    @defer.inlineCallbacks
    def _commit_content(self, storage_key, magic_hash):
        """Commit the content in the DAL."""
        kwargs = dict(user_id=self.user.id, node_id=self.file_node.id,
                      volume_id=self.file_node.volume_id,
                      original_hash=self.original_file_hash,
                      hash_hint=self.hash_hint, crc32_hint=self.crc32_hint,
                      inflated_size_hint=self.inflated_size_hint,
                      deflated_size_hint=self.deflated_size_hint,
                      storage_key=storage_key, magic_hash=magic_hash,
                      session_id=self.session_id)
        try:
            r = yield self.user.rpc_dal.call('make_content', **kwargs)
        except dataerrors.ContentMissing:
            raise errors.TryAgain("Content missing on commit content.")
        except dataerrors.HashMismatch:
            raise errors.ConflictError("The File changed while uploading.")
        defer.returnValue(r['generation'])


class UploadJob(BaseUploadJob):
    """A non-resumable upload job."""

    def __init__(self, user, file_node, previous_hash, hash_hint, crc32_hint,
                 inflated_size_hint, deflated_size_hint,
                 session_id, blob_exists, magic_hash):
        super(UploadJob, self).__init__(user, file_node, previous_hash,
                                        hash_hint, crc32_hint,
                                        inflated_size_hint, deflated_size_hint,
                                        session_id, blob_exists,
                                        magic_hash)
        self.deflated_size = 0
        self.producer = S3Producer(self.deflated_size_hint)

    @property
    def upload_id(self):
        """Return the upload_id for this upload job."""
        return ''

    @property
    def offset(self):
        return 0

    @property
    def buffers_size(self):
        """The size of the producer buffer in this upload."""
        if self.producer:
            return self.producer.buffer_size
        else:
            return 0

    def add_data(self, data):
        super(UploadJob, self).add_data(data)
        self.deflated_size += len(data)


class MagicUploadJob(BaseUploadJob):
    """The magic upload job.

    Its initial offset is the size itself (no data should be added), all
    that is required for the upload is known at the beginning.  The only
    real action here is the commit.
    """

    def __init__(self, user, file_node, previous_hash, hash_hint, crc32_hint,
                 inflated_size_hint, deflated_size_hint,
                 storage_key, magic_hash, session_id, blob_exists):
        super(MagicUploadJob, self).__init__(user, file_node, previous_hash,
                                             hash_hint, crc32_hint,
                                             inflated_size_hint,
                                             deflated_size_hint,
                                             session_id, blob_exists,
                                             magic_hash)
        self.storage_key = storage_key
        # all already done!
        self.deferred.callback(None)

    @property
    def upload_id(self):
        """Return the upload_id for this upload job."""
        return ''

    @property
    def offset(self):
        """The initial offset is all the file."""
        return self.deflated_size_hint

    @property
    def buffers_size(self):
        """The size of the producer buffer in this upload."""
        return 0

    def add_data(self, data):
        """No data should be added!"""
        raise RuntimeError("No data should be added to the MagicUploadJob!")

    def connect(self):
        """Nothing to do."""

    def commit(self, put_result):
        """Make this upload the current content for the node."""
        return self._commit_content(self.storage_key, self.magic_hash)


class MultipartUploadJob(BaseUploadJob):
    """A multipart/resumable upload job."""

    def __init__(self, user, file_node, previous_hash, hash_hint, crc32_hint,
                 inflated_size_hint, deflated_size_hint, uploadjob, session_id,
                 blob_exists, magic_hash):
        super(MultipartUploadJob, self).__init__(
            user, file_node, previous_hash, hash_hint, crc32_hint,
            inflated_size_hint, deflated_size_hint, session_id, blob_exists,
            magic_hash)
        self.mp_upload = None
        self.uploadjob = uploadjob

    @property
    def buffers_size(self):
        """The size of the producer buffer in this upload."""
        size = 0
        if self.factory:
            size = self.factory.buffer_size
        return size

    @property
    def upload_id(self):
        """Return the upload_id for this upload job."""
        return self.multipart_key_name

    @property
    def offset(self):
        """Return the offset."""
        return self.uploadjob.uploaded_bytes

    @property
    def deflated_size(self):
        """Return the deflated_size."""
        return self.factory.deflated_size

    @property
    def inflated_size(self):
        """Return the inflated_size."""
        return self.factory.inflated_size

    @property
    def crc32(self):
        """Return the crc32."""
        return self.factory.crc32

    @property
    def hash_object(self):
        """Return the hash_object."""
        return self.factory.hash_object

    @property
    def magic_hash_object(self):
        """Return the magic_hash_object."""
        return self.factory.magic_hash_object

    @property
    def multipart_id(self):
        """Return the multipart_id."""
        return self.uploadjob.multipart_id

    @property
    def multipart_key_name(self):
        """Return the multipart_key_name."""
        return self.uploadjob.multipart_key

    @property
    def chunk_count(self):
        """Return the chunk_count."""
        return self.uploadjob.chunk_count

    @defer.inlineCallbacks
    def load_s3_multipart_upload(self):
        """Fetch or create a multipart upload in S3."""
        if self.s3 is None:
            self.s3 = self.user.manager.factory.s3()
        if self.multipart_id and self.multipart_key_name:
            try:
                self.mp_upload = yield self.s3.get_multipart_upload(
                    config.api_server.s3_bucket, self.multipart_key_name,
                    self.multipart_id)
            except twisted.web.error.Error, e:
                # log in warning just to be sure everything is working in
                # production
                upload_context = dict(
                    user_id=self.user.id,
                    username=self.user.username.replace('%', '%%'),
                    volume_id=self.uploadjob.volume_id,
                    node_id=str(self.uploadjob.node_id))
                context_msg = (
                    '%(username)s - (%(user_id)s) - '
                    'node: %(volume_id)s::%(node_id)s - ') % upload_context
                self.logger.warning("%s - Multipart upload doesn't exist "
                                    "(Got %r from S3)", context_msg, e.status)
                # if we get 404, this means that the upload was cancelled or
                # completed, but isn't there any more.
                if e.status == '404':
                    # create a new one and reset self.multipart_id
                    self.uploadjob.multipart_id = None
                    self.mp_upload = yield self.s3.create_multipart_upload(
                        config.api_server.s3_bucket, self.multipart_key_name)
                else:
                    raise
        else:
            self.mp_upload = yield self.s3.create_multipart_upload(
                config.api_server.s3_bucket, self.multipart_key_name)

    @defer.inlineCallbacks
    def part_done_callback(self, chunk_count, chunk_size, inflated_size, crc32,
                           hash_context, magic_hash_context,
                           decompress_context):
        """Process the info for each uploaded part"""
        yield self.uploadjob.add_part(chunk_size, inflated_size, crc32,
                                      hash_context, magic_hash_context,
                                      decompress_context)

    @defer.inlineCallbacks
    def _start_receiving(self):
        """Prepare the upload job to start receiving streaming bytes."""
        # get/create the mp_upload info from/in S3
        yield self.load_s3_multipart_upload()
        # persist the multipart upload id
        if self.multipart_id is None:
            yield self.uploadjob.set_multipart_id(self.mp_upload.id)
        # generate a new storage_key for this upload
        self._storage_key = self.multipart_key_name
        self.factory = upload.MultipartUploadFactory(
            self.mp_upload, config.api_server.storage_chunk_size,
            total_size=self.deflated_size_hint,
            offset=self.offset,
            inflated_size=self.uploadjob.inflated_size,
            crc32=self.uploadjob.crc32,
            chunk_count=self.chunk_count,
            hash_context=self.uploadjob.hash_context,
            magic_hash_context=self.uploadjob.magic_hash_context,
            decompress_context=self.uploadjob.decompress_context,
            part_done_cb=self.part_done_callback)
        # the factory is also the producer
        self.producer = self.factory
        self.factory.startFactory()
        yield self.factory.connect_deferred

    def flush_decompressor(self):
        """Flush the decompressor object and handle pending bytes."""
        self.factory.flush_decompressor()

    @defer.inlineCallbacks
    def _complete_upload(self):
        """Complete the multipart upload."""
        try:
            # complete the S3 multipart upload
            yield self._retry(self.mp_upload.complete)
        except twisted.web.error.Error, e:
            # if we get 404, this means that the upload was cancelled or
            # completed, but isn't there any more.
            if e.status == '404' and self.canceling:
                raise dataerrors.DoesNotExist("The multipart upload "
                                              "doesn't exists")
            # propagate the error
            raise

    @defer.inlineCallbacks
    def commit(self, put_result):
        """Make this upload the current content for the node."""
        # unregister the client transport
        self.unregisterProducer()
        # if this is a real multipart upload to S3, call complete
        # this can be none if we already have the content blob and we are just
        # checking the hash
        if self.mp_upload is not None:
            try:
                yield self._complete_upload()
            except twisted.web.error.Error, e:
                # if we get 404, this means that the upload was cancelled or
                # completed, but isn't there any more.
                if e.status == '404' and self.canceling:
                    raise dataerrors.DoesNotExist("The multipart upload "
                                                  "doesn't exists")
                # propagate the error
                raise

        try:
            new_gen = yield super(MultipartUploadJob, self).commit(put_result)
        except Exception as ex1:
            try:
                yield self.delete()
            except Exception as ex2:
                self.logger.warning("%s: while deleting uploadjob after "
                                    "an error, %s", ex2.__class__.__name__,
                                    ex2)
            raise ex1
        else:
            try:
                yield self.delete()
            except Exception as delete_exc:
                self.logger.warning("%s: while deleting uploadjob after "
                                    "commit, %s",
                                    delete_exc.__class__.__name__, delete_exc)
        defer.returnValue(new_gen)

    @defer.inlineCallbacks
    def delete(self):
        """ Cancel and clean up after the current upload job."""
        try:
            yield self.uploadjob.delete()
        except dataerrors.DoesNotExist:
            pass

    @defer.inlineCallbacks
    def cancel(self):
        """Cancel this upload."""
        # unregister the client transport
        yield super(MultipartUploadJob, self).cancel()

        # delete the upload_job
        d = self.delete()
        d.addErrback(self._handle_connection_done)
        yield d

        # abort/cancel the mp upload in s3
        if self.mp_upload:
            try:
                yield self._retry(self.mp_upload.cancel)
            except twisted.web.error.Error, e:
                # check if the upload isn't in S3 ignore the error
                if e.status != '404':
                    raise

    def _retry(self, func, *args, **kwargs):
        """Retry func using MultipartRetry."""
        s3_retries = config.api_server.s3_retries
        s3_retry_wait = config.api_server.s3_retry_wait
        retrier = MultipartRetry(
            # catch timeout, tcp timeout.
            catch=(twisted.internet.error.TimeoutError,
                   twisted.internet.error.TCPTimedOutError,
                   twisted.web.error.Error),
            retries=s3_retries, retry_wait=s3_retry_wait)
        return retrier(func, *args, **kwargs)

    def add_data(self, data):
        """Add data to this upload."""
        # override default add_data to never raise BufferLimit
        self.producer.dataReceived(data)

    def registerProducer(self, producer):
        """Register a producer, this is the client connection."""
        self.producer.registerProducer(producer)

    def unregisterProducer(self):
        """Unregister the producer."""
        if self.producer:
            self.producer.unregisterProducer()


class User(object):
    """A proxy for model.User objects."""

    def __init__(self, manager, user_id,
                 root_volume_id, username, visible_name):
        self.manager = manager
        self.id = user_id
        self.root_volume_id = root_volume_id
        self.username = username
        self.visible_name = visible_name
        self.protocols = []
        self.rpc_dal = self.manager.rpcdal_client

    def register_protocol(self, protocol):
        """Register protocol as a connection authenticated for this user.

        @param protocol: the Server protocol.
        """
        self.protocols.append(protocol)

    def unregister_protocol(self, protocol, cleanup=None):
        """Unregister protocol.

        @param protocol: the Server protocol.
        """
        self.protocols.remove(protocol)

    def broadcast(self, message, filter=lambda _: True):
        """Send message to all connections from this user."""
        for protocol in self.protocols:
            if not filter(protocol):
                continue
            new_message = protocol_pb2.Message()
            new_message.CopyFrom(message)
            new_message.id = protocol.get_new_request_id()
            protocol.sendMessage(new_message)
            protocol.log.trace_message("NOTIFICATION:", new_message)

    @defer.inlineCallbacks
    def get_root(self):
        """Get the root node for this user."""
        r = yield self.rpc_dal.call('get_root', user_id=self.id)
        defer.returnValue((r['root_id'], r['generation']))

    @defer.inlineCallbacks
    def get_free_bytes(self, share_id=None):
        """Returns free space for the given share or the user volume.

        @param share_id: if provided, the id of an accepted share to the user
        """
        if share_id:
            try:
                share = yield self.rpc_dal.call(
                    'get_share', user_id=self.id, share_id=share_id)
                owner_id = share['shared_by_id']
            except dataerrors.DoesNotExist:
                # There is currently a bug in the client which
                # will allow volume_id to be passed to this method. And it
                # will default to the free_bytes of the user. However, this
                # method should not accept a volume_id and share_id should
                # always be valid
                owner_id = self.id
        else:
            owner_id = self.id
        r = yield self.rpc_dal.call('get_user_quota', user_id=owner_id)
        defer.returnValue(r['free_bytes'])

    @defer.inlineCallbacks
    def get_storage_byte_quota(self):
        """Returns purchased and available space for the user."""
        r = yield self.rpc_dal.call('get_user_quota', user_id=self.id)
        defer.returnValue((r['max_storage_bytes'], r['used_storage_bytes']))

    @defer.inlineCallbacks
    def get_node(self, volume_id, node_id, content_hash):
        """Get a content.Node for this node_id.

        @param: volume_id: None for the root volume, or uuid of udf or share id
        @param node_id: an uuid object or string representing the id of the
            we are looking for
        @param content_hash: The current content hash of the node.
        """
        node = yield self.rpc_dal.call('get_node', user_id=self.id,
                                       volume_id=volume_id, node_id=node_id)
        if content_hash and content_hash != node['content_hash']:
            msg = "Node is not available due to hash mismatch."
            raise errors.NotAvailable(msg)

        if node['is_file'] and node['crc32'] is None:
            msg = "Node does not exist since it has no content."
            raise dataerrors.DoesNotExist(msg)

        defer.returnValue(Node(self.manager, node))

    @defer.inlineCallbacks
    def move(self, volume_id, node_id, new_parent_id,
             new_name, session_id=None):
        """Move a node.

        Returns a list of modified nodes.

        @param volume_id: the id of the udf or share, None for root.
        @param node_id: the id of the node to move.
        @param new_parent_id: the node id of the new parent.
        @param new_name: the new name for node_id.
        """
        args = dict(user_id=self.id, volume_id=volume_id, node_id=node_id,
                    new_name=new_name, new_parent_id=new_parent_id,
                    session_id=session_id)
        r = yield self.rpc_dal.call('move', **args)
        defer.returnValue((r['generation'], r['mimetype']))

    @defer.inlineCallbacks
    def make_dir(self, volume_id, parent_id, name, session_id=None):
        """Create a directory.

        @param: volume_id: None for the root volume, or uuid of udf or share id
        @param parent: the parent content.Node.
        @param name: the name for the directory.
        """
        args = dict(user_id=self.id, volume_id=volume_id, parent_id=parent_id,
                    name=name, session_id=session_id)
        r = yield self.rpc_dal.call('make_dir', **args)
        defer.returnValue((r['node_id'], r['generation'], r['mimetype']))

    @defer.inlineCallbacks
    def make_file(self, volume_id, parent_id, name,
                  session_id=None):
        """Create a file.

        @param: volume_id: None for the root volume, or uuid of udf or share id
        @param parent: the parent content.Node.
        @param name: the name for the file.
        """
        args = dict(user_id=self.id, volume_id=volume_id, parent_id=parent_id,
                    name=name, session_id=session_id)
        r = yield self.rpc_dal.call('make_file', **args)
        defer.returnValue((r['node_id'], r['generation'], r['mimetype']))

    @defer.inlineCallbacks
    def create_udf(self, path, name, session_id=None):
        """Creates an UDF.

        @param path: the directory of where the UDF is
        @param name: the name of the UDF
        @param session_id: id of the session where the event was generated
        """
        fullpath = pypath.join(path, name)
        r = yield self.rpc_dal.call('create_udf', user_id=self.id,
                                    path=fullpath, session_id=session_id)
        defer.returnValue((r['udf_id'], r['udf_root_id'], r['udf_path']))

    @defer.inlineCallbacks
    def delete_volume(self, volume_id, session_id=None):
        """Deletes a volume.

        @param volume_id: the id of the share or udf.
        @param session_id: id of the session where the event was generated.
        """
        yield self.rpc_dal.call('delete_volume', user_id=self.id,
                                volume_id=volume_id, session_id=session_id)

    @defer.inlineCallbacks
    def list_volumes(self):
        """List all the volumes the user is involved.

        This includes the real Root, the UDFs, and the shares that were shared.
        to her and she already accepted.
        """
        r = yield self.rpc_dal.call('list_volumes', user_id=self.id)
        root_info = r['root']
        shares = r['shares']
        udfs = r['udfs']
        free_bytes = r['free_bytes']
        defer.returnValue((root_info, shares, udfs, free_bytes))

    @defer.inlineCallbacks
    def list_shares(self):
        """List all the shares the user is involved.

        This only returns the "from me" shares, and the "to me" shares that I
        still didn't accept.
        """
        r = yield self.rpc_dal.call('list_shares', user_id=self.id,
                                    accepted=False)
        defer.returnValue((r['shared_by'], r['shared_to']))

    @defer.inlineCallbacks
    def create_share(self, node_id, shared_to_username, name, access_level):
        """Creates a share.

        @param node_id: the id of the node that will be root of the share.
        @param shared_to_username: the username of the receiving user.
        @param name: the name of the share.
        @param access_level: the permissions on the share.
        """
        readonly = access_level == "View"
        r = yield self.rpc_dal.call('create_share', user_id=self.id,
                                    node_id=node_id, share_name=name,
                                    to_username=shared_to_username,
                                    readonly=readonly)
        defer.returnValue(r['share_id'])

    @defer.inlineCallbacks
    def delete_share(self, share_id):
        """Deletes a share.

        @param share_id: the share id.
        """
        yield self.rpc_dal.call('delete_share',
                                user_id=self.id, share_id=share_id)

    @defer.inlineCallbacks
    def share_accepted(self, share_id, answer):
        """Accepts (or not) the share.

        @param share_id: the share id.
        @param answer: if it was accepted ("Yes") or not ("No").
        """
        if answer == "Yes":
            call = 'accept_share'
        elif answer == "No":
            call = 'decline_share'
        else:
            raise ValueError("Received invalid answer: %r" % answer)
        yield self.rpc_dal.call(call, user_id=self.id, share_id=share_id)

    @defer.inlineCallbacks
    def unlink_node(self, volume_id, node_id, session_id=None):
        """Unlink a node.

        @param volume_id: the id of the volume of the node.
        @param node_id: the id of the node.
        """
        r = yield self.rpc_dal.call('unlink_node', user_id=self.id,
                                    volume_id=volume_id, node_id=node_id,
                                    session_id=session_id)
        defer.returnValue((r['generation'], r['kind'],
                           r['name'], r['mimetype']))

    @defer.inlineCallbacks
    def get_upload_job(self, vol_id, node_id, previous_hash, hash_value, crc32,
                       inflated_size, deflated_size, session_id=None,
                       magic_hash=None, upload_id=None):
        """Create an upload reservation for a node.

        @param vol_id: the volume id this node belongs to.
        @param node_id: the node to upload to.
        @param previous_hash: the current hash of the node.
        @param hash_value: the hash of the new content.
        @param crc32: the crc32 of the new content.
        @param size: the uncompressed size of the new content.
        @param deflated_size: the compressed size of the new content.
        """
        if previous_hash == "":
            previous_hash = None

        # reuse the content if we can
        r = yield self.rpc_dal.call('get_reusable_content', user_id=self.id,
                                    hash_value=hash_value,
                                    magic_hash=magic_hash)
        blob_exists, storage_key = r['blob_exists'], r['storage_key']

        if storage_key is not None:
            upload_job = yield self._get_magic_upload_job(
                vol_id, node_id, previous_hash, hash_value,
                crc32, inflated_size, deflated_size,
                storage_key, magic_hash, session_id,
                blob_exists)
            defer.returnValue(upload_job)

        # only use multipart upload if the content does not exist and we're
        # above the minimum file size (if configured)
        multipart_threshold = config.api_server.multipart_threshold
        if (not blob_exists and
                multipart_threshold > 0 and
                deflated_size >= multipart_threshold):
            # this is a multipart upload
            multipart_key = uuid.uuid4()
            upload_job = yield self._get_multipart_upload_job(
                vol_id, node_id, previous_hash, hash_value, crc32,
                inflated_size, deflated_size, multipart_key,
                session_id, upload_id, blob_exists, magic_hash)
        else:
            upload_job = yield self._get_upload_job(
                vol_id, node_id,
                previous_hash, hash_value, crc32, inflated_size,
                deflated_size, session_id, blob_exists, magic_hash)
        defer.returnValue(upload_job)

    @defer.inlineCallbacks
    def _get_upload_job(self, vol_id, node_id, previous_hash, hash_value,
                        crc32, inflated_size, deflated_size,
                        session_id, blob_exists, magic_hash):
        """Create an upload reservation for a node.

        @param vol_id: the volume id this node belongs to.
        @param node_id: the node to upload to.
        @param previous_hash: the current hash of the node.
        @param hash_value: the hash of the new content.
        @param crc32: the crc32 of the new content.
        @param size: the uncompressed size of the new content.
        @param deflated_size: the compressed size of the new content.
        """
        node = yield self.rpc_dal.call('get_node', user_id=self.id,
                                       volume_id=vol_id, node_id=node_id)
        if not node["is_file"]:
            raise dataerrors.NoPermission("Can only put content on files.")
        file_node = Node(self.manager, node)
        uj = UploadJob(self, file_node, previous_hash, hash_value,
                       crc32, inflated_size, deflated_size,
                       session_id, blob_exists, magic_hash)
        defer.returnValue(uj)

    @defer.inlineCallbacks
    def _get_magic_upload_job(self, vol_id, node_id, previous_hash, hash_value,
                              crc32, inflated_size, deflated_size, storage_key,
                              magic_hash, session_id, blob_exists):
        """Create a magic upload reservation for a node.

        @param vol_id: the volume id this node belongs to.
        @param node_id: the node to upload to.
        @param previous_hash: the current hash of the node.
        @param hash_value: the hash of the new content.
        @param crc32: the crc32 of the new content.
        @param size: the uncompressed size of the new content.
        @param deflated_size: the compressed size of the new content.
        @param storage_key: the content's storage key
        @param magic_hash: the magic_hash from client
        """
        node = yield self.rpc_dal.call('get_node', user_id=self.id,
                                       volume_id=vol_id, node_id=node_id)
        if not node["is_file"]:
            raise dataerrors.NoPermission("Can only put content on files.")
        file_node = Node(self.manager, node)
        uj = MagicUploadJob(self, file_node, previous_hash, hash_value,
                            crc32, inflated_size, deflated_size,
                            storage_key, magic_hash, session_id, blob_exists)
        defer.returnValue(uj)

    @defer.inlineCallbacks
    def _get_multipart_upload_job(self, vol_id, node_id, previous_hash,
                                  hash_value, crc32, inflated_size,
                                  deflated_size, multipart_key,
                                  session_id, upload_id, blob_exists,
                                  magic_hash):
        """Create an multipart upload reservation for a node.

        @param vol_id: the volume id this node belongs to.
        @param node_id: the node to upload to.
        @param previous_hash: the current hash of the node.
        @param hash_value: the hash of the new content.
        @param crc32: the crc32 of the new content.
        @param size: the uncompressed size of the new content.
        @param deflated_size: the compressed size of the new content.
        @param multipart_key: the key name of the upload.
        @param upload_id: the upload_id sent by the client.
        """
        node = yield self.rpc_dal.call('get_node', user_id=self.id,
                                       volume_id=vol_id, node_id=node_id)
        if not node["is_file"]:
            raise dataerrors.NoPermission("Can only put content on files.")
        file_node = Node(self.manager, node)

        upload = None
        if upload_id:
            # check if there is already a job.
            try:
                uploadid = uuid.UUID(upload_id)
            except ValueError:
                # invalid upload_id, just ignore it a create a new upload.
                upload = None
            else:
                try:
                    upload = yield DBUploadJob.get(self, vol_id, node_id,
                                                   uploadid, hash_value, crc32,
                                                   inflated_size,
                                                   deflated_size)
                except dataerrors.DoesNotExist:
                    # there is no uploadjob with the specified id
                    upload = None

        if upload is None:
            # no uploadjob found, create a new one.
            try:
                upload = yield DBUploadJob.make(self, vol_id, node_id,
                                                previous_hash, hash_value,
                                                crc32, inflated_size,
                                                deflated_size, multipart_key)
            except dataerrors.HashMismatch:
                raise errors.ConflictError("Previous hash does not match.")
        else:
            # update the when_last_active value.
            yield upload.touch()

        uj = MultipartUploadJob(self, file_node, previous_hash, hash_value,
                                crc32, inflated_size, deflated_size,
                                upload, session_id, blob_exists, magic_hash)
        defer.returnValue(uj)

    @defer.inlineCallbacks
    def get_delta(self, volume_id, from_generation, limit=None):
        """Get the delta form generation for volume_id."""
        r = yield self.rpc_dal.call('get_delta', user_id=self.id,
                                    volume_id=volume_id, limit=limit,
                                    from_generation=from_generation)
        nodes = [Node(self.manager, n) for n in r['nodes']]
        defer.returnValue((nodes, r['vol_generation'], r['free_bytes']))

    @defer.inlineCallbacks
    def get_from_scratch(self, volume_id, start_from_path=None, limit=None,
                         max_generation=None):
        """Get the list of live nodes in volume_id."""
        r = yield self.rpc_dal.call('get_from_scratch', user_id=self.id,
                                    volume_id=volume_id,
                                    start_from_path=start_from_path,
                                    limit=limit, max_generation=max_generation)
        nodes = [Node(self.manager, n) for n in r['nodes']]
        defer.returnValue((nodes, r['vol_generation'], r['free_bytes']))

    @defer.inlineCallbacks
    def get_volume_id(self, node_id):
        """Get the (client) volume_id (UDF id or root) of this node_id.

        @param node_id: an uuid object or string representing the id of the
            we are looking for
        """
        r = yield self.rpc_dal.call('get_volume_id', user_id=self.id,
                                    node_id=node_id)
        defer.returnValue(r['volume_id'])


class ContentManager(object):
    """Manages Users."""

    def __init__(self, factory):
        """Create a ContentManager."""
        self.factory = factory
        self.users = weakref.WeakValueDictionary()

    @defer.inlineCallbacks
    def get_user_by_id(self, user_id, session_id=None, required=False):
        """Return a user by id and session id if its connected.

        If it's not cached and required, it's retrieved from the DB.
        """
        user = self.users.get(user_id, None)
        if user is None and required:
            r = yield self.rpcdal_client.call('get_user_data', user_id=user_id,
                                              session_id=session_id)
            # Another task may have already updated the cache, so check again
            user = self.users.get(user_id, None)
            if user is None:
                user = User(self, user_id, r['root_volume_id'],
                            r['username'], r['visible_name'])
                self.users[user_id] = user
        defer.returnValue(user)
