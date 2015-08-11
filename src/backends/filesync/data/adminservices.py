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

"""Services provided for administrative access to storage data."""

from backends.filesync.data import get_storage_store, model, dao
from backends.filesync.data.gateway import StorageUserGateway
from backends.filesync.data.dbmanager import fsync_readonly


class StorageUserFinder(object):
    """A special class for getting or finding StorageUsers.

    This becomes an indexable sequence of StorageUser objects based on the
    optional filter. If no filter is provided, all users will be returned.
    """

    def __init__(self, filter=None):
        self.filter = filter

    def _find_users(self):
        """Perform storm query based on current filter."""
        store = get_storage_store()
        conditions = []
        if self.filter is not None:
            filter = unicode("%" + self.filter + "%")
            conditions.append(
                model.StorageUser.username.like(filter, case_sensitive=False))
        return store.find(model.StorageUser,
                          *conditions).order_by(model.StorageUser.username)

    def _get_dao_from_result(self, result):
        """Convert the result to a StorageUser DAO."""
        user_dao = dao.StorageUser(result)
        user_dao._gateway = StorageUserGateway(user_dao)
        return user_dao

    @fsync_readonly
    def all(self):
        """Return all the results of the find.

        Be careful with this as it will not constrain the query or result size.
        """
        return [self._get_dao_from_result(r) for r in self._find_users()]

    @fsync_readonly
    def count(self):
        """Return the count of the results."""
        return self._find_users().count()

    @fsync_readonly
    def is_empty(self):
        """Return True if there are any results."""
        return self._find_users().is_empty()

    @fsync_readonly
    def __getitem__(self, index):
        """Get a StorageUser or list of StorageUsers."""
        result = self._find_users()[index]
        #if index is an integer, a tuple will be returned.
        if isinstance(result, model.StorageUser):
            return self._get_dao_from_result(result)
        return [self._get_dao_from_result(r) for r in result]
