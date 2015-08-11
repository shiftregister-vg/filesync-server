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

"""Services are the only public interface to the Data Access Layer.

This may provice Data Access Objects (DAO) which can also be used to access
the data layer. Some objects, may be provided that do not have database access
"""

from backends.filesync.data import errors
from backends.filesync.data.dbmanager import (
    retryable_transaction,
    fsync_commit,
    fsync_readonly,
)
from backends.filesync.data.gateway import SystemGateway
from backends.filesync.data.dbmanager import get_new_user_shard_id


@retryable_transaction()
@fsync_commit
def make_storage_user(user_id, username, visible_name, max_storage_bytes,
                      shard_id=None):
    """Create or update a StorageUser."""
    gw = SystemGateway()
    if not shard_id:
        shard_id = get_new_user_shard_id(user_id)
    return gw.create_or_update_user(
        user_id, username, visible_name, max_storage_bytes, shard_id=shard_id)


@fsync_readonly
def get_storage_user(user_id=None, username=None, session_id=None,
                     active_only=True, readonly=False):
    """Get a storage user.

    readonly kwarg is just to not raise LockedUserError in case the user is
    locked.
    """
    gw = SystemGateway()
    user = gw.get_user(user_id=user_id, username=username,
                       session_id=session_id, ignore_lock=readonly)
    if active_only and (user is None or not user.is_active):
        raise errors.DoesNotExist("User does not exist.")
    return user


@fsync_readonly
def get_shareoffer(shareoffer_id):
    """Get a Share Offer."""
    gw = SystemGateway()
    return gw.get_shareoffer(shareoffer_id)


@retryable_transaction()
@fsync_commit
def claim_shareoffer(user_id, username, visible_name, share_offer_id):
    """Claim a shared folder offer sent to an email.

    Used when a user has been offered a shared folder, but has not
    subscribed yet and they are not a storageuser. This will create a
    storageuser in an inactive state so they will have the share once they
    subscribe.
    """
    gw = SystemGateway()
    return gw.claim_shareoffer(user_id, username, visible_name, share_offer_id)


@fsync_readonly
def get_public_file(public_key, use_uuid=False):
    """Get a public file."""
    gw = SystemGateway()
    return gw.get_public_file(public_key, use_uuid=use_uuid)


@fsync_readonly
def get_public_directory(public_key):
    """Get a public directory."""
    gw = SystemGateway()
    return gw.get_public_directory(public_key)


@fsync_readonly
def get_node_for_shard(node_id, shard_id):
    """Get the StorageNode for the specified node_id, shard_id.

    raise DoesNotExist if the node isn't there.
    """
    gw = SystemGateway()
    return gw.get_node(node_id, shard_id)


@fsync_readonly
def get_user_info_for_shard(user_id, shard_id):
    """Get the UserInfo dao (read only) for the user_id, shard_id."""
    gw = SystemGateway()
    return gw.get_user_info(user_id, shard_id)


@fsync_readonly
def get_abandoned_uploadjobs(shard_id, last_active, limit=1000):
    """Return the live resumable uploadjobs.

    @param shard_id: the shard to use for the query.
    @param last_active_before: datetime, a filter of the when_started field.
    @param limit: the limit on the number of results
    """
    gw = SystemGateway()
    return gw.get_abandoned_uploadjobs(shard_id, last_active, limit)


@fsync_commit
def cleanup_uploadjobs(shard_id, uploadjob_ids):
    """Delete UploadJobs

    @param shard_id: the shard to use for the query.
    @param uploadjobs_ids: the list of id of jobs to delete
    """
    gw = SystemGateway()
    return gw.cleanup_uploadjobs(shard_id, uploadjob_ids)
