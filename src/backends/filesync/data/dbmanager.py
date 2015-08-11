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

"""Manage database connections and stores to the storage database."""

from backends.db.store import get_filesync_store
from backends.db.dbtransaction import (
    get_storm_commit,
    get_storm_readonly,
    storage_tm,
)
from backends.db.dbtransaction import retryable_transaction  # NOQA
from config import config

fsync_commit = get_storm_commit(storage_tm)
fsync_readonly = get_storm_readonly(storage_tm)
fsync_readonly_slave = get_storm_readonly(storage_tm, use_ro_store=True)


class InvalidShardId(Exception):
    """Raised when an invalid shard ID is passed"""


def get_shard_store(shard_id):
    """Return the Storm.Store for the given shard_id"""
    try:
        return get_filesync_store(shard_id)
    except KeyError:
        raise InvalidShardId("Invalid Shard ID: %s" % shard_id)


def get_user_store():
    """Get the main storage store."""
    return get_filesync_store('storage')


def get_storage_store():
    """Return the default storage store.

    This is primarily for legacy tests while transaction handling is migrated
    """
    return get_filesync_store('storage')


def get_rotated_shardid(id, shard_ids):
    """Return a shard_id."""
    return unicode(shard_ids[id % len(shard_ids)])


def get_new_user_shard_id(user_id):
    """Get the shard id for new users."""
    new_user_shard_ids = config.database.shards.new_user_shards
    #if there is nothing configured, return the first shard.
    if new_user_shard_ids:
        return get_rotated_shardid(user_id, get_shard_ids())
    return get_rotated_shardid(user_id, new_user_shard_ids)


def get_shard_ids():
    """Get a list of configured shard ids."""
    return config.database.shards.shard_ids
