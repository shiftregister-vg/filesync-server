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

"""Resources for use by tests."""

from __future__ import absolute_import

__metaclass__ = type
__all__ = ["StorageDatabaseResource"]

import os

import psycopg2
import testresources
import transaction

from backends.db.schemas import account as account_schema
from backends.db.schemas import auth as auth_schema
from backends.db.schemas import fsync_main as main_schema
from backends.db.schemas import fsync_shard as shard_schema
from backends.db.schemas import txlog as txlog_schema
from backends.db.dbwatcher import DatabaseWatcher
from backends.db.db_admin_store import get_admin_store

from backends.filesync.data.dbmanager import get_shard_ids, storage_tm
from backends.db.store import account_tm

DEBUG_RESOURCES = bool(os.environ.get("DEBUG_RESOURCES"))


class DatabaseResource(testresources.TestResource):
    """A resource that resets a database to a known state for each test."""
    _watcher = None

    def __init__(self, dbname, schema_modules, store_name, autocommit=False,
                 tx_manager=transaction):
        super(DatabaseResource, self).__init__()
        self.dbname = dbname
        self.schema_modules = schema_modules
        self.store_name = store_name
        self.autocommit = autocommit
        self.saw_commit = False
        self.schemas = None
        self.tx_manager = tx_manager

    def __repr__(self):
        return "<DatabaseResource %s>" % self.dbname

    @staticmethod
    def get_watcher():
        """Get the `DatabaseWatcher` instance for the `psycopg2` adapter."""
        watcher = DatabaseResource._watcher
        if watcher is None:
            DatabaseResource._watcher = watcher = (DatabaseWatcher(psycopg2))
            watcher.install()
        return watcher

    def make(self, dependent_resources=None):
        """See `TestResource`"""
        if DEBUG_RESOURCES:
            print "*** Make %s ***" % self.dbname
        watcher = self.get_watcher()
        watcher.enable(self.dbname)
        if self.schemas is None:
            self.schemas = [s.create_schema() for s in self.schema_modules]
        store = get_admin_store(self.store_name)
        transaction.abort()
        for s in self.schemas:
            s.upgrade(store)
        transaction.commit()
        transaction.begin()
        for s in reversed(self.schemas):
            s.delete(store)
        transaction.commit()
        self.saw_commit = False
        watcher.hook(self.dbname, self._notify_change)
        watcher.reset(self.dbname)
        return self

    def clean(self, resource):
        """See `TestResource`"""
        assert self is resource, "Unknown resource passed to clean()"
        if DEBUG_RESOURCES:
            print "*** Clean %s ***" % self.dbname
        self.tx_manager.abort()
        # Someone committed to the database: clean it up.
        if self.saw_commit:
            store = get_admin_store(self.store_name)
            for s in reversed(self.schemas):
                s.delete(store)
            transaction.commit()
        watcher = self.get_watcher()
        watcher.unhook(self.dbname, self._notify_change)
        watcher.reset(self.dbname)
        watcher.disable(self.dbname)

    def _notify_change(self, dbname, commit=False):
        """Dirty the resource if the database is accessed."""
        if DEBUG_RESOURCES:
            print "*** Change %s, commit=%r ***" % (dbname, commit)
        self.dirtied(self)
        # If this is an autocommit database, then any use of the
        # connection should be treated as a commit.
        if commit or self.autocommit:
            self.saw_commit = True


AccountDatabaseResource = DatabaseResource(
    dbname='account',
    schema_modules=[auth_schema, account_schema],
    store_name='account',
    tx_manager=account_tm)


StorageDatabaseResource = DatabaseResource(
    dbname='storage',
    schema_modules=[main_schema],
    store_name='storage',
    tx_manager=storage_tm)


def get_shard_resources():
    """Get testresources for shard databases"""
    resources = []
    for shard in get_shard_ids():
        r = DatabaseResource(
            dbname=shard,
            schema_modules=[shard_schema, txlog_schema],
            store_name=shard,
            tx_manager=storage_tm)
        resources.append((shard, r))
    return resources

shard_resources = get_shard_resources()


def get_all_db_resources():
    """Get a list of all db test resources."""
    all_db_resources = [
        ('account_db', AccountDatabaseResource),
        ('storage_db', StorageDatabaseResource),
    ]
    all_db_resources.extend(shard_resources)
    return all_db_resources
