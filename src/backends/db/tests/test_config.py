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

"""Test DB config."""

import os
import unittest

from utilities.utils import get_tmpdir

from backends.db import dbconfig
from backends.db.dbconfig import (
    _format_postgres_uri,
    db_dir,
    get_connection_settings,
)
from config import config


# This example tests all the features of the YML config
# defaults: define connection settings that will be used if not overriden
# stores: is a list of all the configured stores and the connection settings
#         these will override defaults if set
# shards: define specific shard configuration for filesync
#         this is used for selecting which shard to put a user on.
TEST_DB = {
    'user': 'webapp',
    'defaults': {
        'database': 'account',
        'host': 'fake.server.net',
        'port': 5433,
        'options': {
            'isolation': 'serializable',
        },
    },
    'shards': {
        'new_user_shards': ['shard0', 'shard1', 'shard2'],
        'shard_ids': ['shard0', 'shard1', 'shard2'],
    },
    'stores': {
        'storage': {
            'database': 'storage',
            'new_user_shards': ['shard0', 'shard1', 'shard2'],
            'shard_ids': ['shard0', 'shard1', 'shard2'],
        },
        'shard0': {
            'port': 5434,
            'database': 'shard0',
        },
        'shard1': {
            'database': 'shard1',
        },
    },
}

URI = "postgres://%(user)s@%(host)s:%(port)s/%(db)s?isolation=%(iso)s"


class DbConfigTestCase(unittest.TestCase):
    """Test postgres uri generation"""

    def setUp(self):
        old_db = config.database
        config.database = TEST_DB
        self.addCleanup(setattr, config, 'database', old_db)

    def tearDown(self):
        dbconfig.conn_settings = None

    def test_db_dir(self):
        """Test db_dir points to tmp inside config.root."""
        expected = os.path.join(get_tmpdir(), 'db')
        self.assertTrue(db_dir.startswith(expected),
                        'db_dir (%r) must start with %r' % (db_dir, expected))

    def test_format_postgres_uri(self):
        """Test _format_postgres_uri."""
        uri = _format_postgres_uri('storage')
        expected = URI % dict(host='fake.server.net', port=5433, db='storage',
                              user='webapp', iso='serializable')
        self.assertEqual(uri, expected)

    def test_get_connection_settings(self):
        """Test get_connection_settings."""
        self.assertEqual(dbconfig.conn_settings, None)
        conn = get_connection_settings()
        # the global gets set by this.
        self.assertEqual(dbconfig.conn_settings, conn)
        # some sanity checks:
        self.assertEqual(conn['storage']['database'], 'storage')
        self.assertEqual(conn['storage']['port'], 5433)
        self.assertEqual(conn['storage']['password'], '')
        self.assertEqual(conn['storage']['host'], 'fake.server.net')
        self.assertEqual(conn['shard0']['database'], 'shard0')
        self.assertEqual(conn['shard0']['port'], 5434)
        self.assertEqual(conn['shard0']['options'], 'isolation=serializable')
