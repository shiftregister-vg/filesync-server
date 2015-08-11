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

"""Test the datamanager."""

import backends.filesync.data.dbmanager as dbm
from backends.filesync.data.testing.testcase import StorageDALTestCase


class DataManagerTestCase(StorageDALTestCase):
    """Run some simple tests on the datamanager"""

    def test_invalid_shard(self):
        """Make sure an exception is thrown when an invalid shard is passed"""
        self.assertRaises(dbm.InvalidShardId, dbm.get_shard_store, "XXX")

    def test_stores(self):
        """Make sure we get the stores work"""
        #run through all the shards and try to talk to them
        for k in dbm.get_shard_ids():
            s = dbm.get_shard_store(k)
            s.execute('SELECT 1')

        #make sure get_shard_store is working as designed
        s1 = dbm.get_shard_store('shard0')
        s2 = dbm.get_shard_store('shard0')
        #even though they are different, they are the same
        self.assertTrue(s1 is s2)
        s3 = dbm.get_shard_store('shard1')
        self.assertFalse(s1 is s3)
