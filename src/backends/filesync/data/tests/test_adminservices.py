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

"""Tests for the adminservices features."""

from backends.filesync.data.testing.testcase import StorageDALTestCase
from backends.filesync.data import dao, services
from backends.filesync.data import adminservices as admin


class AdminServicesTestCase(StorageDALTestCase):
    """Tests the adminservices module features."""

    def _make_users(self):
        """Create users for tests."""
        usernames = [u'bob', u'bobby', u'inez', u'juan', u'tim']
        for i, name in zip(range(5), usernames):
            services.make_storage_user(i, name, name, 2 ** 30)

    def test_StorageUserFinder(self):
        """Test the StorageUserFinder."""
        users = admin.StorageUserFinder()
        self.assertEquals(users.all(), [])
        self.assertEquals(users.count(), 0)
        self.assertEquals(users.is_empty(), True)
        self._make_users()
        #the returning object can be reused
        self.assertEquals(len(users.all()), 5)
        self.assertEquals(users.count(), 5)
        self.assertEquals(users.is_empty(), False)
        self.assertEquals(users[4].username, "tim")
        users.filter = "BOB"
        self.assertEquals(len(users.all()), 2)
        self.assertEquals(users[0].username, "bob")
        self.assertEquals(users[1].username, "bobby")
        users.filter = "juan"
        self.assertEquals(len(users.all()), 1)
        self.assertTrue(isinstance(users[0], dao.StorageUser))
        self.assertEquals(users[0].username, "juan")
        #test slicing
        users.filter = None
        subset = users[2:4]
        self.assertEquals(len(subset), 2)
        self.assertEquals(subset[0].username, "inez")
        self.assertEquals(subset[1].username, "juan")
