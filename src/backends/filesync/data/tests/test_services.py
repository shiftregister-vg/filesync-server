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

"""Test the Data services."""

import uuid
import datetime

from backends.filesync.data.testing.testdata import get_fake_hash
from backends.filesync.data.testing.testcase import StorageDALTestCase
from backends.filesync.data.services import (
    get_abandoned_uploadjobs,
    get_node_for_shard,
    get_user_info_for_shard,
    get_public_file,
    get_public_directory,
    get_storage_user,
    make_storage_user,
)
from backends.filesync.data import dao, errors, utils, model


MAX_STORAGE_BYTES = 10 * 23


class DataServicesTestCase(StorageDALTestCase):
    """Test the DataServices.

    Since all the logic is in lower level tests, these tests are kept
    to a minimum
    """

    def assert_storage_user(
            self, storage_user, user_id, visible_name, max_storage_bytes):
        self.assertIsInstance(storage_user, dao.StorageUser)
        self.assertEqual(storage_user.id, user_id)
        self.assertEqual(storage_user.visible_name, visible_name)
        quota = storage_user.get_quota()
        self.assertEqual(quota.max_storage_bytes, max_storage_bytes)

    def test_make_storage_user(self):
        """Test the make_storage_user function."""
        storage_user = make_storage_user(
            1, u"Cool UserName", u"Visible Name", MAX_STORAGE_BYTES)
        self.assert_storage_user(
            storage_user, 1, u"Visible Name", MAX_STORAGE_BYTES)

    def test_get_storage_user(self):
        """Test the get_storage_user function."""
        user = make_storage_user(
            1, u"Cool UserName", u"Visible Name", MAX_STORAGE_BYTES)
        user = get_storage_user(1)
        self.assertTrue(isinstance(user, dao.StorageUser))
        user.update(subscription=False)
        self.assertRaises(errors.DoesNotExist, get_storage_user, 1)
        user = get_storage_user(1, active_only=False)
        user.update(subscription=True)
        # now check a locked user.
        suser = self.user_store.get(model.StorageUser, user.id)
        suser.locked = True
        self.user_store.commit()
        self.assertRaises(errors.LockedUserError, get_storage_user, user.id)
        # and ignore the lock too
        user = get_storage_user(user.id, readonly=True)
        self.assertTrue(isinstance(user, dao.StorageUser))

    def test_get_node_for_shard(self):
        """Test the get_node_for_shard function."""
        user1 = self.obj_factory.make_user(
            1, u"User 1", u"User 1", MAX_STORAGE_BYTES, shard_id=u"shard1")
        node = user1.volume().root.make_file(u"test file")
        new_node = get_node_for_shard(node.id, u'shard1')
        self.assertEquals(node.id, new_node.id)
        self.assertEquals(node.parent_id, new_node.parent_id)
        self.assertEquals(node.name, new_node.name)
        self.assertEquals(node.path, new_node.path)

    def test_get_user_info_for_shard(self):
        """Test the get_user_info_for_shard function."""
        user = self.obj_factory.make_user(
            1, u"User 1", u"User 1", MAX_STORAGE_BYTES, shard_id=u"shard1")
        user_info = get_user_info_for_shard(user.id, user.shard_id)
        quota = user.get_quota()
        self.assertEquals(quota.max_storage_bytes, user_info.max_storage_bytes)
        self.assertEquals(quota.used_storage_bytes,
                          user_info.used_storage_bytes)
        self.assertEquals(quota.free_bytes, user_info.free_bytes)
        self.assertRaises(errors.DoesNotExist, get_user_info_for_shard, 41,
                          user.shard_id)

    def test_get_abandoned_uploadjobs(self):
        """Test the get_abandoned_uploadjobs function."""
        self.assertRaises(TypeError, get_abandoned_uploadjobs, 'shard1')
        jobs = get_abandoned_uploadjobs('shard1', datetime.datetime.now(), 100)
        self.assertTrue(isinstance(jobs, list))

    def test_get_public_file(self):
        """Test the get_public_file function."""
        save_setting = utils.set_public_uuid
        utils.set_public_uuid = False
        user = self.obj_factory.make_user(
            1, u"Cool UserName", u"Visible Name", 10)
        a_file = user.volume().root.make_file_with_content(
            u"file.txt", get_fake_hash(), 123, 1, 1, uuid.uuid4())
        a_file.change_public_access(True)
        public_key = a_file.public_key
        f1 = get_public_file(public_key)
        self.assertEqual(f1, a_file)
        a_file.change_public_access(False)
        self.assertRaises(errors.DoesNotExist,
                          get_public_file, public_key, use_uuid=False)
        utils.set_public_uuid = save_setting

    def test_get_public_directory(self):
        """Test the get_public_directory function."""
        user = self.obj_factory.make_user(
            1, u"Cool UserName", u"Visible Name", 10)
        a_dir = user.volume().root.make_subdirectory(u'test_dir')
        a_dir.make_file_with_content(
            u"file.txt", get_fake_hash(), 123, 1, 1, uuid.uuid4())
        a_dir.change_public_access(True, allow_directory=True)
        public_key = a_dir.public_key
        pub_dir = get_public_directory(public_key)
        self.assertEqual(pub_dir, a_dir)
        a_dir.change_public_access(False, allow_directory=True)
        self.assertRaises(errors.DoesNotExist,
                          get_public_directory, public_key)

    def test_get_public_file_public_uuid(self):
        """Test the get_public_file function."""
        save_setting = utils.set_public_uuid
        utils.set_public_uuid = True
        user = self.obj_factory.make_user(
            1, u"Cool UserName", u"Visible Name", 10)
        a_file = user.volume().root.make_file_with_content(
            u"file.txt", get_fake_hash(), 123, 1, 1, uuid.uuid4())
        a_file.change_public_access(True)
        public_key = a_file.public_key
        # get the file using the public uuid
        f1 = get_public_file(public_key, use_uuid=True)
        self.assertEqual(f1, a_file)
        # can't get the file using the old id
        self.assertRaises(errors.DoesNotExist,
                          get_public_file, public_key)
        a_file.change_public_access(False)
        self.assertRaises(errors.DoesNotExist,
                          get_public_file, public_key, use_uuid=True)
        utils.set_public_uuid = save_setting
