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

"""Services for handling downloads."""

from backends.filesync.data import model, errors
from backends.filesync.data.gateway import SystemGateway
from backends.filesync.data.dbmanager import (
    get_shard_ids,
    retryable_transaction,
    fsync_commit,
    fsync_readonly,
)

# states
UNKNOWN = "Unknown"
DOWNLOADED_NOT_PRESENT = "Downloaded But Not Present"
QUEUED = model.DOWNLOAD_STATUS_QUEUED
DOWNLOADING = model.DOWNLOAD_STATUS_DOWNLOADING
DOWNLOADED = model.DOWNLOAD_STATUS_COMPLETE
ERROR = model.DOWNLOAD_STATUS_ERROR


@retryable_transaction()
@fsync_commit
def download_start(user_id, download_id):
    """Start the download."""
    SystemGateway().update_download(user_id, download_id,
                                    status=model.DOWNLOAD_STATUS_DOWNLOADING)


@retryable_transaction()
@fsync_commit
def download_error(user_id, download_id, message):
    """Mark the download as in error."""
    return SystemGateway().update_download(
        user_id, download_id,
        status=model.DOWNLOAD_STATUS_ERROR, error_message=message)


@retryable_transaction()
@fsync_commit
def download_complete(user_id, download_id, hash, crc32, size,
                      deflated_size, mimetype, storage_key):
    """Complete the download."""
    gw = SystemGateway()
    return gw.download_complete(user_id, download_id, hash, crc32, size,
                                deflated_size, mimetype, storage_key)


@retryable_transaction()
@fsync_commit
def get_or_make_download(user_id, volume_id, path, download_url, dl_key):
    """Get or make a download if it doesn't already exist."""
    gw = SystemGateway()
    try:
        download = gw.get_download(
            user_id, volume_id, path, download_url, dl_key)
    except errors.DoesNotExist:
        download = gw.make_download(
            user_id, volume_id, path, download_url, dl_key)
    return download


@retryable_transaction()
@fsync_commit
def download_update(user_id, download_id, status=None,
                    node_id=None, error_message=None):
    """Update a download directly.

    Typically this isn't used.
    """
    gw = SystemGateway()
    return gw.update_download(user_id, download_id, status=status,
                              node_id=node_id, error_message=error_message)


@fsync_readonly
def get_status_from_download(user_id, download):
    """Gets the status from a download object."""
    gw = SystemGateway()
    if download.status == model.DOWNLOAD_STATUS_COMPLETE:
        # check if the file is actually present
        user = gw.get_user(user_id)
        try:
            gw.get_node(download.node_id, user.shard_id)
        except errors.DoesNotExist:
            return DOWNLOADED_NOT_PRESENT
    return download.status


@fsync_readonly
def get_status(user_id, volume_id, path, download_url, dl_key):
    """Get the status of the download."""
    gw = SystemGateway()
    try:
        download = gw.get_download(
            user_id, volume_id, path, download_url, dl_key)
    except errors.DoesNotExist:
        return UNKNOWN
    return get_status_from_download(user_id, download)


@fsync_readonly
def get_status_by_id(user_id, dl_id):
    """Get the status of the download."""
    gw = SystemGateway()
    try:
        download = gw.get_download_by_id(user_id, dl_id)
    except errors.DoesNotExist:
        return UNKNOWN
    return get_status_from_download(user_id, download)


@retryable_transaction()
@fsync_commit
def make_download(user_id, udf_id, file_path, download_url, download_key=None):
    """Create a new download object."""
    gw = SystemGateway()
    return gw.make_download(
        user_id, udf_id, file_path, download_url, download_key)


@fsync_readonly
def get_download(user_id, udf_id, file_path, download_url, download_key=None):
    """Get a download by its UDF, file path and download URL and key."""
    gw = SystemGateway()
    return gw.get_download(
        user_id, udf_id, file_path, download_url, download_key)


@fsync_readonly
def get_download_by_id(user_id, download_id):
    """Get a download by its ID."""
    gw = SystemGateway()
    return gw.get_download_by_id(user_id, download_id)


@fsync_readonly
def get_failed_downloads(start_date, end_date):
    """Get the failed downloads between start_date and end_date."""
    gw = SystemGateway()
    downloads = []
    for shard_id in get_shard_ids():
        downloads.extend(list(gw.get_failed_downloads(
            shard_id, start_date, end_date)))
    return downloads
