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

"""List the stores used and provide utilities to get the store for each db."""

import contextlib

from storm.database import register_scheme
from storm.databases.postgres import Postgres

from backends.db.dbconfig import (
    get_postgres_uri,
    get_connection_settings,
)

from backends.db.dbtransaction import (
    account_tm,
    account_zstorm,
    storage_zstorm,
    get_storm_commit,
    get_storm_readonly,
)


class FilesyncDatabase(Postgres):

    def __init__(self, uri):
        # A unique name for this database connection. Used by storm's
        # TimelineTracer to identify the connection used for a specific query.
        self.name = uri.database
        super(FilesyncDatabase, self).__init__(uri)

    def connect(self, event=None):
        """Create a connection and set it's name to the database name.

        The connection name is used by storm's TimelineTracer to
        identify the connection used for a specific query.
        """
        connection = super(FilesyncDatabase, self).connect(event=event)
        connection.name = self.name
        return connection


# We set the override the registered uri scheme for 'postgres' so that the
# custom Postgres-based database factory above is used, setting a 'name'
# attribute on the connection that matches the requested database, causing the
# TimelineTracer to then include that database name as part of the actions
# traced.
register_scheme("postgres", FilesyncDatabase)


def get_django_stores():
    """Return a dictionary of configured stores for django."""
    settings = get_connection_settings()
    storm_stores = {}
    for key, cfg in settings.items():
        storm_stores[key] = get_postgres_uri(cfg)
    return storm_stores


def get_store(store_name, zstorm=None):
    """Return the Storm.Store for the given schema"""
    if not zstorm:
        zstorm = account_zstorm
    settings = get_connection_settings()
    # get the current transaction and see if it has the ro_store flag set
    txn = zstorm.transaction_manager.get()
    if getattr(txn, 'use_ro_store', False):
        # only use it if there is a config for it.
        ro_store_name = 'ro-%s' % store_name
        if ro_store_name in settings:
            store_name = ro_store_name
    store_settings = settings[store_name]
    uri = get_postgres_uri(store_settings)
    return zstorm.get(store_name, default_uri=uri)


def get_filesync_store(store_name):
    """get a store using the storage_tm."""
    return get_store(store_name, zstorm=storage_zstorm)


def get_user_store():
    """Get the main store used for users and common dataobjects"""
    return get_filesync_store('storage')


@contextlib.contextmanager
def implicit_flushes_blocked_on(store):
    try:
        store.block_implicit_flushes()
        yield
    finally:
        store.unblock_implicit_flushes()


#
# The default decorators use the account_tm
#
account_commit = get_storm_commit(account_tm)
account_readonly = get_storm_readonly(account_tm)
account_readonly_slave = get_storm_readonly(account_tm, use_ro_store=True)
