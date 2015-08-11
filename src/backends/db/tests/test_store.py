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

"""Tests for the 'store' module"""

import unittest

from backends.db import store
from backends.testing.testcase import DatabaseResourceTestCase


class StoreTestCase(DatabaseResourceTestCase):
    """Getting a store returns one configured with FilesyncDatabase."""

    def test_get_user_store(self):
        """Test get_user_store returns the expected store."""
        db = store.get_user_store().get_database()
        self.assertTrue(isinstance(db, store.FilesyncDatabase))
        self.assertEqual('storage', db.name)

    def test_get_filesync_store(self):
        """Test get_filesync_store returns the expected store."""
        db = store.get_filesync_store('shard0').get_database()
        self.assertTrue(isinstance(db, store.FilesyncDatabase))
        self.assertEqual('shard0', db.name)


class FilesyncDatabaseTestCase(unittest.TestCase):
    """Test custom FilesyncDatabase factory."""

    def test_connection_has_name(self):
        """Inherit the name from the database when a connection is created."""

        class FakeConnection(object):
            """A fake connection object for testing."""
            def __init__(self, database, event):
                pass

        class FakeFilesyncDatabase(store.FilesyncDatabase):
            """A FilesyncDatabase that createse FakeConnection."""
            connection_factory = FakeConnection

        class FakeURI(object):
            """A helper URI object."""
            host = None
            port = None
            username = None
            password = None
            options = {}
            database = "a-known-database"

        db = FakeFilesyncDatabase(FakeURI())
        self.assertEqual("a-known-database", db.connect().name)
