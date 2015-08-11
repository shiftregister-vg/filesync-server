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

from ubuntuone.storageprotocol import request
from ubuntuone.storageprotocol import protocol_pb2


class StorageServerError(request.StorageProtocolError):
    """The base class for all server exceptions."""


class ProtocolError(StorageServerError):
    errno = protocol_pb2.Error.PROTOCOL_ERROR


class InternalError(StorageServerError):
    errno = protocol_pb2.Error.INTERNAL_ERROR


class UnsupportedProtocolVersion(StorageServerError):
    errno = protocol_pb2.Error.UNSUPPORTED_VERSION


class AuthenticationRequired(StorageServerError):
    errno = protocol_pb2.Error.AUTHENTICATION_REQUIRED


class AuthenticationError(StorageServerError):
    errno = protocol_pb2.Error.AUTHENTICATION_FAILED


class UploadInProgress(StorageServerError):
    errno = protocol_pb2.Error.UPLOAD_IN_PROGRESS


class UploadCanceled(StorageServerError):
    errno = protocol_pb2.Error.UPLOAD_CANCELED


class UploadCorrupt(StorageServerError):
    errno = protocol_pb2.Error.UPLOAD_CORRUPT


class ConflictError(StorageServerError):
    errno = protocol_pb2.Error.CONFLICT


class TryAgain(StorageServerError):
    """Error that results in a client TRY_AGAIN."""
    errno = protocol_pb2.Error.TRY_AGAIN

    def __init__(self, orig_error, msg=None):
        """Save off the original exception, if present."""
        if not msg:
            msg = str(orig_error)
        super(TryAgain, self).__init__(msg)
        self.orig_error = orig_error


class S3Error(TryAgain):
    """There was an error accessing the s3 external service."""


class S3UploadError(S3Error):
    """S3 error while sending data."""


class S3DownloadError(S3Error):
    """S3 error while receiving data."""


class NotAvailable(StorageServerError):
    errno = protocol_pb2.Error.NOT_AVAILABLE


class ProtocolReferenceError(ReferenceError):
    pass


class BufferLimit(Exception):
    pass
