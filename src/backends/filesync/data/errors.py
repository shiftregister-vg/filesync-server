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

"""Exceptions raised by the data access layer."""

from backends.db.errors import RetryLimitReached, IntegrityError
_ = (RetryLimitReached, IntegrityError)  # Silence lint.


class StorageError(Exception):
    """Base Error for Data Errors."""


class InvalidFilename(StorageError):
    """The file name is invalid."""


class DoesNotExist(StorageError):
    """The object does not exist."""


class NoPermission(StorageError):
    """The current user doesn't have permission."""


class AlreadyExists(StorageError):
    """An attempt was made to create something that already exists."""


class NotEmpty(StorageError):
    """An attempt was made to delete a directory that is not empty."""


class NotADirectory(StorageError):
    """The object is not a Directory."""


class QuotaExceeded(StorageError):
    """This action would exceed the user's quota."""

    def __init__(self, msg, volume_id, free_bytes):
        super(QuotaExceeded, self).__init__(msg)
        self.volume_id = volume_id
        self.free_bytes = free_bytes


class ContentMissing(StorageError):
    """This action expected a contentblob but can't find it."""


class DirectoriesHaveNoContent(StorageError):
    """Directory objects do not have content blobs."""


class HashMismatch(StorageError):
    """A Hash match has failed."""


class ShareAlreadyAccepted(StorageError):
    """An attempt was made to accept and already accepted Share."""


class InvalidVolumePath(InvalidFilename):
    """An attempt was made to create a UserVolume with the wrong path."""


class LockedUserError(StorageError):
    """An attemp to get a locked user."""

    def __str__(self):
        return str(self.args) if self.args else "User Locked"
