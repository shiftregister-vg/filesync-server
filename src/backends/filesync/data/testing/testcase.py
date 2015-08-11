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

"""Base Test class for testing the data access layer"""

import uuid

from itertools import count

from backends.filesync.data import utils, storage_tm
from backends.filesync.data.gateway import SystemGateway
from backends.filesync.data.dbmanager import get_user_store, get_shard_store
from backends.filesync.data.testing.testdata import (
    default_shard_id, get_fake_hash)
from backends.testing.testcase import DatabaseResourceTestCase


class StorageDALTestCase(DatabaseResourceTestCase):
    """A base TestCase with an object factory and other helper methods."""

    def setUp(self):
        """Set up."""
        super(StorageDALTestCase, self).setUp()
        self.obj_factory = DAOObjectFactory()
        self.user_store = get_user_store()
        self.get_shard_store = get_shard_store
        self.save_utils_set_public_uuid = utils.set_public_uuid

    def tearDown(self):
        """Tear down."""
        utils.set_public_uuid = self.save_utils_set_public_uuid
        super(StorageDALTestCase, self).tearDown()

    def patch(self, obj, attr_name, new_val):
        """Patch!"""
        old_val = getattr(obj, attr_name)
        setattr(obj, attr_name, new_val)
        self.addCleanup(setattr, obj, attr_name, old_val)

    def create_user(self, id=1, username=u"username", visible_name=u"vname",
                    max_storage_bytes=2 * (2 ** 30),
                    shard_id=default_shard_id):
        """Deprecated; This is only a compatibility shim for tests that
        haven't been updated to use the object factory directly."""
        return self.obj_factory.make_user(
            user_id=id, username=username,
            visible_name=visible_name, max_storage_bytes=max_storage_bytes,
            shard_id=shard_id)


class DAOObjectFactory(object):
    """An anonymous object factory that creates DAO objects."""

    _unique_int_counter = count(100000)

    def get_unique_integer(self):
        """Return an integer unique to this factory.

        For each thread, this will be a series of increasing numbers, but the
        starting point will be unique per thread.
        """
        return DAOObjectFactory._unique_int_counter.next()

    def get_unique_unicode(self):
        return u'unique-string-%d' % self.get_unique_integer()

    def make_user(self, user_id=None, username=None, visible_name=None,
                  max_storage_bytes=2 ** 20, shard_id=default_shard_id):
        if username is None:
            username = self.get_unique_unicode()
        if visible_name is None:
            visible_name = self.get_unique_unicode()
        if user_id is None:
            user_id = self.get_unique_integer()
        user = SystemGateway().create_or_update_user(
            user_id, username, visible_name, max_storage_bytes,
            shard_id=shard_id)
        storage_tm.commit()
        return user

    def make_file(self, user=None, parent=None, name=None,
                  mimetype=u'text/plain'):
        if user is None:
            user = self.make_user()
        if name is None:
            name = self.get_unique_unicode()
        if parent is None:
            parent = user.root
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = self.get_unique_integer()
        size = 100
        deflated_size = 10000
        f = parent.make_file_with_content(
            name, hash, crc, size, deflated_size, storage_key, mimetype)
        f.load(with_content=True)
        return f
