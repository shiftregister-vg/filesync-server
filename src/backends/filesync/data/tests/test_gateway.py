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

"""Test the data gateways."""

import datetime
import logging
import os
import types
import uuid

from operator import attrgetter

import posixpath as pypath
from mock import patch
from psycopg2 import IntegrityError
from storm.locals import Store

from backends.filesync.data.testing.ormtestcase import ORMTestCase
from backends.filesync.data.testing.testcase import StorageDALTestCase
from backends.filesync.data.gateway import (
    GatewayBase,
    ReadWriteVolumeGateway,
    StorageUserGateway,
    SystemGateway,
    fix_all_udfs_with_generation_out_of_sync,
    fix_udfs_with_generation_out_of_sync,
    timing_metric,
)
from backends.filesync.data.dbmanager import (
    get_shard_store, storage_tm as transaction)
from backends.filesync.data import dao, errors, model, utils
from backends.filesync.data.testing.testdata import (
    get_fake_hash, get_test_contentblob, default_shard_id)
from backends.filesync.notifier.notifier import (
    ShareAccepted,
    ShareCreated,
    ShareDeclined,
    ShareDeleted,
    UDFCreate,
    UDFDelete,
    VolumeNewGeneration,
)


class FakeShare(object):
    """A share-like object."""

    def __init__(self, id, name, root_id, shared_by_id, shared_to_id,
                 access, accepted):
        """Initialize an instance."""
        self.id = id
        self.name = name
        self.root_id = root_id
        self.shared_by_id = shared_by_id
        self.shared_to_id = shared_to_id
        self.access = access
        self.accepted = accepted


class FakeUDF(object):
    """A udf-like object."""

    def __init__(self, owner_id, id, root_id, path):
        """Initialize an instance."""
        self.owner_id = owner_id
        self.id = id
        self.root_id = root_id
        self.path = path


class GatewayBaseTestCase(StorageDALTestCase):
    """Test the System level gateway"""

    def setUp(self):
        super(GatewayBaseTestCase, self).setUp()
        self.gw = GatewayBase()
        self.notifications = []
        self.patch(self.gw._notifier, 'on_event',
                   lambda e: self.notifications.append(e))

    def test_get_user(self):
        """Test get_user"""
        user = self.create_user()
        user2 = self.gw.get_user(user.id, session_id="QWERTY")
        self.assertEqual(user.id, user2.id)
        self.assertEqual(user.username, user2.username)
        self.assertEqual(user2._gateway.session_id, "QWERTY")

    def test_get_user_username(self):
        """Test get_user with username"""
        user = self.create_user()
        user2 = self.gw.get_user(username=user.username, session_id="QWERTY")
        self.assertEqual(user.id, user2.id)
        self.assertEqual(user.username, user2.username)
        self.assertEqual(user2._gateway.session_id, "QWERTY")

    def test_get_user_locked(self):
        """Test get_user with a locked user."""
        user = self.create_user()
        suser = self.user_store.get(model.StorageUser, user.id)
        suser.locked = True
        self.user_store.commit()
        self.assertRaises(
            errors.LockedUserError,
            self.gw.get_user, user.id, session_id="QWERTY")

    def test_get_user_locked_ignore_lock(self):
        """Test get_user with a locked user, but ignore the lock."""
        user = self.create_user()
        suser = self.user_store.get(model.StorageUser, user.id)
        suser.locked = True
        self.user_store.commit()
        nuser = self.gw.get_user(
            user.id, session_id="QWERTY", ignore_lock=True)
        self.assertEqual(user.id, nuser.id)

    def test_split_in_list(self):
        """Test the utils.split_in_list function."""
        biglist = [i for i in range(1000)]
        # test with various max sizes
        for i in range(3, 210):
            sublists = utils.split_in_list(biglist, max=i)
            items = 0
            for s in sublists:
                items += len(s)
                self.assertTrue(len(s) <= i)
            # make sure we have the same number of items
            self.assertEqual(items, 1000)

    def test_queue_new_generation(self):
        """When a new volume generation."""
        vol_id = uuid.uuid4()
        self.gw.queue_new_generation(1, vol_id, 3)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(1, vol_id, 3, self.gw.session_id)
        ])

    def _test_queue_share_event(self, event_class, method_name):
        """Test share events (generic form)."""
        share_id = uuid.uuid4()
        name = u'foobar'
        root_id = uuid.uuid4()
        shared_by_id = 10
        shared_to_id = 11
        access = 'Read'
        accepted = 'False'
        share = FakeShare(share_id, name, root_id, shared_by_id, shared_to_id,
                          access, accepted)
        getattr(self.gw, method_name)(share)
        transaction.commit()
        self.assertEqual(self.notifications, [
            event_class(share_id, name, root_id, shared_by_id, shared_to_id,
                        access, accepted)
        ])

    def test_queue_share_created(self):
        """Test event generated when a share is created."""
        self._test_queue_share_event(ShareCreated, 'queue_share_created')

    def test_queue_share_deleted(self):
        """Test event generated when a share is deleted."""
        self._test_queue_share_event(ShareDeleted, 'queue_share_deleted')

    def test_queue_share_accepted(self):
        """Test event generated when a share is accepted."""
        self._test_queue_share_event(ShareAccepted, 'queue_share_accepted')

    def test_queue_share_declined(self):
        """Test event generated when a share is declined."""
        self._test_queue_share_event(ShareDeclined, 'queue_share_declined')

    def test_queue_udf_create(self):
        """Test event generated when a UDF has been created."""
        owner_id = 10
        udf_id = uuid.uuid4()
        root_id = uuid.uuid4()
        suggested_path = u'foo/bar/baz'
        udf = FakeUDF(owner_id, root_id, udf_id, suggested_path)
        self.gw.queue_udf_create(udf)
        transaction.commit()
        self.assertEqual(self.notifications, [
            UDFCreate(owner_id, root_id, udf_id, suggested_path,
                      self.gw.session_id)
        ])

    def test_queue_udf_delete(self):
        """Test event generated when a UDF is deleted."""
        owner_id = 10
        udf_id = uuid.uuid4()
        root_id = uuid.uuid4()
        suggested_path = u'foo/bar/baz'
        udf = FakeUDF(owner_id, udf_id, root_id, suggested_path)
        self.gw.queue_udf_delete(udf)
        transaction.commit()
        self.assertEqual(self.notifications, [UDFDelete(owner_id, udf_id)])


class EventNotificationTest(StorageDALTestCase):
    """Tests to make sure the events trigger notifications."""

    def setUp(self):
        super(EventNotificationTest, self).setUp()
        self.user = self.create_user(id=1, username=u"user1")
        self.gw = GatewayBase(session_id=uuid.uuid4())
        self.vgw = ReadWriteVolumeGateway(self.user)
        self.root = self.vgw.get_root()

        self.notifications = []
        self.patch(self.vgw._notifier, 'on_event',
                   lambda e: self.notifications.append(e))

    def setup_shares(self):
        """Create shares for tests"""
        self.user1 = self.create_user(id=2, username=u"sharee1")
        self.user2 = self.create_user(id=3, username=u"sharee2")
        self.user3 = self.create_user(id=4, username=u"sharee3")
        self.user4 = self.create_user(id=5, username=u"sharee4")
        root = ReadWriteVolumeGateway(self.user).get_root()
        # make some children to share
        self.d1 = self.vgw.make_subdirectory(root.id, u"dir1")
        self.d2 = self.vgw.make_subdirectory(self.d1.id, u"dir1")
        self.d3 = self.vgw.make_subdirectory(self.d2.id, u"dir1")
        self.share1 = self.user_store.add(model.Share(
            self.user.id, root.id, self.user1.id, u"Share1", "Modify"))
        self.share2 = self.user_store.add(model.Share(
            self.user.id, root.id, self.user2.id, u"Share2", "Modify"))
        self.share3 = self.user_store.add(model.Share(
            self.user.id, self.d1.id, self.user3.id, u"Share3", "View"))
        self.share4 = self.user_store.add(model.Share(
            self.user.id, self.d2.id, self.user4.id, u"Share4", "Modify"))
        # Note that only share1-3 are accepted
        self.share1.accept()
        self.share2.accept()
        self.share3.accept()
        transaction.commit()
        self.notifications = []

    def test_queue_new_generation_generation_None(self):
        """When a new volume generation is None."""
        vol_id = uuid.uuid4()
        self.gw.queue_new_generation(1, vol_id, None)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(1, vol_id, 0, self.gw.session_id)
        ])

    def test_handle_node_change_with_shares(self):
        """Test the handle_node_change."""
        self.setup_shares()
        node = get_shard_store(self.user.shard_id).get(
            model.StorageObject, self.d3.id)
        self.vgw.handle_node_change(node)
        transaction.commit()
        self.assertIn(VolumeNewGeneration(self.user.id, None, node.
                                          generation, self.vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user1.id, self.share1.id, node.
                                          generation, self.vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user2.id, self.share2.id, node.
                                          generation, self.vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user3.id, self.share3.id, node.
                                          generation, self.vgw.session_id),
                      self.notifications)

    def test_handle_node_change_from_share(self):
        """Test the handle_node_change."""
        self.setup_shares()
        node = get_shard_store(self.user.shard_id).get(
            model.StorageObject, self.d3.id)
        share = self.user1.get_share(self.share1.id)
        vgw = ReadWriteVolumeGateway(self.user1, share=share)
        vgw.handle_node_change(node)
        transaction.commit()

        self.assertIn(VolumeNewGeneration(self.user.id, None,
                                          node.generation, vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user3.id, self.share3.id,
                                          node.generation, vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user2.id, self.share2.id,
                                          node.generation, vgw.session_id),
                      self.notifications)
        self.assertIn(VolumeNewGeneration(self.user1.id, self.share1.id,
                                          node.generation, vgw.session_id),
                      self.notifications)

    def test_make_file(self):
        """Make sure make_file sends a notification."""
        # the root node gets a notification as a directory content change
        f = self.vgw.make_file(self.root.id, u"filename")
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, f.generation,
                                self.vgw.session_id)
        ])

    def test_make_file_with_magic_content(self):
        """Make sure make_file with magic content sends a notification."""
        cb = get_test_contentblob("FakeContent")
        cb.magic_hash = 'magic'
        get_shard_store(self.user.shard_id).add(cb)
        f = self.vgw.make_file(self.root.id, u"filename", hash=cb.hash,
                               magic_hash='magic')
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, f.generation,
                                self.vgw.session_id)
        ])

    def test_make_file_twice_notification(self):
        """Make sure make_file doesn't sends notifications.

        A second make_file will just return the existing one.
        """
        # the root node gets a notification as a directory content change
        self.vgw.make_file(self.root.id, u"dirname")
        transaction.commit()
        self.notifications = []
        self.vgw.make_file(self.root.id, u"dirname")
        transaction.commit()

    def test_delete_file_notifications(self):
        """Make sure delete_file sends notifications."""
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        f = self.vgw.make_file_with_content(
            self.root.id, name, hash, crc, size, deflated_size, storage_key)
        transaction.commit()
        self.notifications = []
        f = self.vgw.delete_node(f.id)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, f.generation,
                                self.vgw.session_id)
        ])

    def test_restore_file_notifications(self):
        """Make sure restore_file sends notifications."""
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        a_file = self.vgw.make_file_with_content(
            self.root.id, name, hash, crc, size, deflated_size, storage_key)
        transaction.commit()
        self.notifications = []
        self.vgw.delete_node(a_file.id)
        transaction.commit()
        self.notifications = []
        node = self.vgw.restore_node(a_file.id)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, node.generation,
                                self.vgw.session_id)
        ])

    def test_make_subdirectory_notification(self):
        """Make sure make_subdirectory sends notifications."""
        # the root node gets a notification as a directory content change
        d = self.vgw.make_subdirectory(self.root.id, u"dirname")
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, d.generation,
                                self.vgw.session_id)
        ])

    def test_make_subdirectory_twice_notification(self):
        """Make sure make_subdirectory doesn't sends notifications.

        A second make_subdirectory will just return the existing one.
        """
        # the root node gets a notification as a directory content change
        self.vgw.make_subdirectory(self.root.id, u"dirname")
        transaction.commit()
        self.notifications = []
        self.vgw.make_subdirectory(self.root.id, u"dirname")
        transaction.commit()

    def test_delete_directory_notification(self):
        """Make sure make_subdirectory sends notifications."""
        d = self.vgw.make_subdirectory(self.root.id, u"dir")
        transaction.commit()
        self.notifications = []
        d = self.vgw.delete_node(d.id)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, d.generation,
                                self.vgw.session_id)
        ])

    def test_move_notification_no_move(self):
        """Make sure no notifications are sent with noop moves."""
        root_id = self.vgw.get_root().id
        d1 = self.vgw.make_subdirectory(root_id, u"dira1")
        transaction.commit()
        self.notifications = []
        self.vgw.move_node(d1.id, d1.parent_id, d1.name)
        transaction.commit()
        # no expected events...

    def test_move_notification_with_overwrite(self):
        """Make sure moves send the corrent notifications."""
        root_id = self.vgw.get_root().id
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 111111111
        deflated_size = 10000
        dira1 = self.vgw.make_subdirectory(root_id, u"dira1")
        dira2 = self.vgw.make_subdirectory(dira1.id, u"dira2")
        self.vgw.make_file_with_content(dira1.id, name, hash, crc,
                                        size, deflated_size, storage_key)
        transaction.commit()
        self.notifications = []
        n = self.vgw.move_node(dira2.id, root_id, u"dira1")
        transaction.commit()
        # we get two generations, one for the delete and one for the move
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, n.generation - 1,
                                self.vgw.session_id),
            VolumeNewGeneration(self.user.id, None, n.generation,
                                self.vgw.session_id),
        ])

    def test_move_notification(self):
        """Make sure moves send the corrent notifications."""
        root_id = self.vgw.get_root().id
        dira1 = self.vgw.make_subdirectory(root_id, u"dira1")
        dira2 = self.vgw.make_subdirectory(dira1.id, u"dira2")
        transaction.commit()
        self.notifications = []
        n = self.vgw.move_node(dira2.id, root_id, u"dirc1")
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, n.generation,
                                self.vgw.session_id)
        ])

    def test_make_file_with_content(self):
        """Ensure file creation with content sends corrent notifications."""
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 111111111
        deflated_size = 10000
        magic_hash = "magic_hash"
        n = self.vgw.make_file_with_content(self.root.id, name, hash, crc,
                                            size, deflated_size, storage_key,
                                            magic_hash=magic_hash)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, n.generation,
                                self.vgw.session_id)
        ])

    def test_make_file_with_content_overwrite(self):
        """Make file with contentblob and overwite its existing content."""
        name = u"filename.tif"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        f = self.vgw.make_file_with_content
        f(self.root.id, name, hash, crc, size, deflated_size,
          storage_key, mimetype=u'image/tif')
        transaction.commit()
        self.notifications = []
        size = 101
        newhash = get_fake_hash("CXXYYY")
        newstorage_key = uuid.uuid4()
        n = f(self.root.id, name, newhash, crc, size, deflated_size,
              newstorage_key)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, n.generation,
                                self.vgw.session_id)
        ])

    def test_make_content(self):
        """Make sure make_content sends correct notifications."""
        filenode = self.vgw.make_file(self.root.id, u"the file name")
        transaction.commit()
        self.notifications = []
        new_hash = get_fake_hash()
        new_storage_key = uuid.uuid4()
        crc = 12345
        size = 11111
        def_size = 10000
        newnode = self.vgw.make_content(filenode.id, filenode.content_hash,
                                        new_hash, crc, size, def_size,
                                        new_storage_key)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, newnode.generation,
                                self.vgw.session_id)
        ])

    def test_change_public_access_no_node_update(self):
        """Test that change_public_access don't send a node_update event."""
        f = self.vgw.make_file(self.root.id, u"filename")
        transaction.commit()
        self.notifications = []
        f = self.vgw.change_public_access(f.id, True)
        transaction.commit()
        self.assertEqual(self.notifications, [
            VolumeNewGeneration(self.user.id, None, f.generation,
                                self.vgw.session_id)
        ])


class SystemGatewayTestCase(StorageDALTestCase):
    """Test the System level gateway"""

    def setUp(self):
        super(SystemGatewayTestCase, self).setUp()
        self.gw = SystemGateway()

    def test_create_or_update_user(self):
        """Test the basic make user method"""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        self.assertEqual(user.username, u"username")
        self.assertEqual(user.visible_name, u"Visible Name")
        self.assertEqual(user._subscription_status, model.STATUS_LIVE)
        store = get_shard_store("shard1")
        info = store.get(model.StorageUserInfo, 1)
        self.assertEqual(info.max_storage_bytes, 1)
        root = model.StorageObject.get_root(store, user.id)
        self.assertEqual(root.volume_id, user.root_volume_id)
        self.assertEqual(root.owner_id, user.id)
        self.assertTrue(isinstance(user, dao.StorageUser))

    def test_create_or_update_user_update(self):
        """Test the basic make user method"""
        self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                      1, shard_id=u"shard1")
        # update the user info.
        usr = get_shard_store('storage').get(model.StorageUser, 1)
        usr.status = model.STATUS_DEAD
        usr.subscription_status = model.STATUS_DEAD
        transaction.commit()
        # creates for existing users, will not change their shard
        user = self.gw.create_or_update_user(1, u"username2", u"Visible Name2",
                                             2, shard_id=u"shard2")
        self.assertEqual(user.shard_id, u"shard1")
        quota = user._gateway.get_quota()
        # their quota and other infor should get updated
        self.assertEqual(user._status, model.STATUS_LIVE)
        self.assertEqual(user._subscription_status, model.STATUS_LIVE)
        self.assertEqual(user.username, u"username2")
        self.assertEqual(user.visible_name, u"Visible Name2")
        self.assertEqual(quota.max_storage_bytes, 2)

    def test_get_shareoffer(self):
        """Test get_shareoffer."""
        user1 = self.create_user(id=1, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(user1.id, root.id, None, u"Share", "View",
                            email="fake@example.com")
        self.user_store.add(share)
        transaction.commit()
        share = self.gw.get_shareoffer(share.id)
        # test with already accepted share_offer
        user2 = self.create_user(id=2, username=u"sharee")
        self.gw.claim_shareoffer(
            user2.id, user2.username, user2.visible_name, share.id)
        self.assertRaises(errors.ShareAlreadyAccepted,
                          self.gw.get_shareoffer, share.id)
        # non existent offer
        self.assertRaises(errors.DoesNotExist, self.gw.get_shareoffer,
                          uuid.uuid4())
        # test with Dead (rescinded) share_offer
        share = model.Share(user1.id, root.id, None, u"Share3", "View",
                            email="fake3@example.com")
        share.status = model.STATUS_DEAD
        self.user_store.add(share)
        self.assertRaises(
            errors.DoesNotExist, self.gw.get_shareoffer, share.id)

    def test_claim_shareoffer(self):
        """Test that the claim_shareoffer function works properly.

        A share_offer isn't a direct share to a user. Instead, its offered in
        that an share is created but only sent to an email address. However,
        any user can accept the share as long as it hasn't been accepted or the
        offer hasn't been rescended.

        """
        # setup the share_offer
        user1 = self.create_user(id=1, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(user1.id, root.id, None, u"Share", "View",
                            email="fake@example.com")
        self.user_store.add(share)
        transaction.commit()
        # user can't accept their own share offer
        self.assertRaises(
            errors.DoesNotExist, self.gw.claim_shareoffer, user1.id,
            user1.username, user1.visible_name, share.id)
        # basic test success...user already exist
        user2 = self.create_user(id=2, username=u"sharee")
        transaction.commit()
        self.gw.claim_shareoffer(
            user2.id, user2.username, user2.visible_name, share.id)
        user2 = self.gw.get_user(user2.id)
        self.assertEqual(user2.is_active, True)
        share = self.user_store.get(model.Share, share.id)
        self.assertEqual(share.shared_to, user2.id)
        self.assertEqual(share.accepted, True)
        # Exception raised when accepted twice
        self.assertRaises(
            errors.ShareAlreadyAccepted, self.gw.claim_shareoffer, user2.id,
            user2.username, user2.visible_name, share.id)
        # test with new user
        share = model.Share(user1.id, root.id, None, u"Share2", "View",
                            email="fake2@example.com")
        self.user_store.add(share)
        self.gw.claim_shareoffer(3, u"user3", u"user 3", share.id)
        user3 = self.gw.get_user(3)
        # the user is created, but is inactive
        # subscribe
        self.assertEqual(user3.is_active, False)
        # subscribing will call something like this:
        self.gw.create_or_update_user(3, u"user3", u"user 3", 2 * (2 ** 30),
                                      shard_id=default_shard_id)
        transaction.commit()
        user3 = self.gw.get_user(3)
        self.assertEqual(user3.is_active, True)
        share = self.user_store.get(model.Share, share.id)
        self.assertEqual(share.shared_to, user3.id)
        self.assertEqual(share.accepted, True)

    def test_claim_shareoffer_without_user(self):
        """Test that the claim_shareoffer function works properly."""
        # setup the share_offer
        user1 = self.create_user(id=1, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(user1.id, root.id, None, u"Share", "View",
                            email="fake@example.com")
        self.user_store.add(share)
        transaction.commit()
        # user 2 does not exist
        self.gw.claim_shareoffer(2, u"sharee", u"Sharee", share.id)
        user2 = self.gw.get_user(2)
        store = get_shard_store(user2.shard_id)
        root2 = model.StorageObject.get_root(store, user2.id)
        self.assertTrue(root2 is not None)
        self.assertEqual(user2.is_active, False)
        share = self.user_store.get(model.Share, share.id)
        self.assertEqual(share.shared_to, user2.id)
        self.assertEqual(share.accepted, True)

    def test_make_download(self):
        """The make_download() method creates a Download object."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        dl_url = u"http://download/url"
        download = self.gw.make_download(
            user.id, udf.id, u"path", dl_url)
        self.assertTrue(isinstance(download, dao.Download))
        self.assertEqual(download.owner_id, user.id)
        self.assertEqual(download.volume_id, udf.id)
        self.assertEqual(download.file_path, u"path")
        self.assertEqual(download.download_url, dl_url)
        self.assertEqual(download.download_key, dl_url)

    def test_make_download_with_key(self):
        """The make_download_with_key() makes a Download object with a key."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url", ["key"])
        self.assertTrue(isinstance(download, dao.Download))
        self.assertEqual(download.owner_id, user.id)
        self.assertEqual(download.volume_id, udf.id)
        self.assertEqual(download.file_path, u"path")
        self.assertEqual(download.download_url, u"http://download/url")
        self.assertEqual(download.download_key, unicode(repr(["key"])))

    def test_get_download(self):
        """The get_download() method can find downloads by user, volume,
        path and URL or key.
        """
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url")

        other_download = self.gw.get_download(
            user.id, udf.id, u"path", u"http://download/url")
        self.assertEqual(other_download.id, download.id)

    def test_get_old_download_with_null_key(self):
        """Old Download records may have no download key."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download_url = u"http://download/url"
        file_path = u"path"
        download_id = uuid.uuid4()

        # force the insert of an old download (with the key set to NULL)
        # this is done manually because the updated model always sets the key
        SQL = """INSERT INTO Download (id, owner_id, file_path, download_url,
                                       volume_id, status, status_change_date)
                        VALUES (?, ?, ?, ?, ?, 'Complete', now())"""
        get_shard_store(user.shard_id).execute(
            SQL, (download_id, user.id, file_path, download_url, udf.id))

        download = self.gw.get_download(
            user.id, udf.id, file_path, download_url)
        self.assertEqual(download.id, download_id)
        self.assertEqual(download.download_key, None)

    def test_get_old_song_with_multiple_download_records(self):
        """Old songs may have multiple download records. Get the last."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")

        file_path = u"path"
        download_key = u"mydownloadkey"
        download1_url = u"http://download/url/1"
        download1 = self.gw.make_download(
            user.id, udf.id, file_path, download1_url, download_key)

        download2_url = u"http://download/url/2"
        download2 = self.gw.make_download(
            user.id, udf.id, file_path, download2_url, download_key)

        download = self.gw.get_download(
            user.id, udf.id, file_path, download1_url, download_key)
        self.assertNotEqual(download.id, download1.id)
        self.assertEqual(download.id, download2.id)

    def test_get_download_with_key(self):
        """The get_download() method can find downloads by user, volume,
        path and URL or key.
        """
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        key = ["some", "key"]
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url", key)

        other_download = self.gw.get_download(
            user.id, udf.id, u"path", u"http://download/url", key)
        self.assertEqual(other_download.id, download.id)

    def test_get_download_with_same_key_and_different_url(self):
        """The get_download() method can find downloads by user, volume,
        path and URL or key.
        """
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        key = ["some", "key"]
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url/1", key)

        # request with same key and different URL
        other_download = self.gw.get_download(
            user.id, udf.id, u"path", u"http://download/url/2", key)
        self.assertEqual(other_download.id, download.id)

    def test_get_download_by_id(self):
        """The get_download_by_id() method can find a download by its
        owner ID and download ID.
        """
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url")

        other_download = self.gw.get_download_by_id(user.id, download.id)
        self.assertEqual(other_download.id, download.id)

    def test_update_download_status(self):
        """The update_download() method can update a download's status."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/spath/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url")
        new_download = self.gw.update_download(
            download.owner_id, download.id,
            status=model.DOWNLOAD_STATUS_DOWNLOADING)
        self.assertEqual(
            new_download.status, model.DOWNLOAD_STATUS_DOWNLOADING)

    def test_update_download_node_id(self):
        """The update_download() method can update a download's node_id."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url")
        a_file = udf.root_node.make_file(u"TheName")
        new_download = self.gw.update_download(
            download.owner_id, download.id,
            node_id=a_file.id)
        self.assertEqual(new_download.node_id, a_file.id)

    def test_update_download_error_message(self):
        """The update_download() method can update a download's error
        message."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        udf = model.UserVolume.create(
            get_shard_store(user.shard_id), user.id, u"~/path/name")
        download = self.gw.make_download(
            user.id, udf.id, u"path", u"http://download/url")
        new_download = self.gw.update_download(
            download.owner_id, download.id, error_message=u"error")
        self.assertEqual(new_download.error_message, u"error")

    def test_get_failed_download_ids_for_shard(self):
        """Test get_failed_download_ids_for_shard()"""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        download1 = self.gw.make_download(
            user.id, user.root_volume_id, u"path", u"http://download/url")
        download2 = self.gw.make_download(
            user.id, user.root_volume_id, u"path2", u"http://download/url2")
        transaction.commit()
        download2 = self.gw.update_download(
            user.id, download2.id, status=model.DOWNLOAD_STATUS_ERROR,
            error_message=u"something died")
        failed_downloads = self.gw.get_failed_downloads(
            u"shard1", start_date=download1.status_change_date,
            end_date=download2.status_change_date)
        self.assertTrue(download2.id in [d.id for d in failed_downloads])

    def test_get_node(self):
        """Test the get_node method."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        sgw = SystemGateway()
        shard_store = get_shard_store(user.shard_id)
        root = model.StorageObject.get_root(shard_store, user.id)
        node = root.make_file(u"TheName")
        node._content_hash = model.EMPTY_CONTENT_HASH
        node.mimetype = u"fakemime"
        new_node = sgw.get_node(node.id, user.shard_id)
        self.assertEqual(node.id, new_node.id)
        self.assertEqual(node.parent_id, new_node.parent_id)
        self.assertEqual(node.name, new_node.name)
        self.assertEqual(node.path, new_node.path)
        self.assertEqual(node.content_hash, new_node.content_hash)
        # check that DoesNotExist is raised
        self.assertRaises(errors.DoesNotExist, sgw.get_node,
                          uuid.uuid4(), user.shard_id)

    def test_get_user_info(self):
        """Test the get_user_info method."""
        user = self.gw.create_or_update_user(1, u"username", u"Visible Name",
                                             1, shard_id=u"shard1")
        quota = user._gateway.get_quota()
        sgw = SystemGateway()
        user_info = sgw.get_user_info(user.id, user.shard_id)
        self.assertEqual(quota.max_storage_bytes,
                         user_info.max_storage_bytes)
        self.assertEqual(quota.used_storage_bytes,
                         user_info.used_storage_bytes)
        self.assertEqual(quota.free_bytes, user_info.free_bytes)
        # check that DoesNotExist is raised
        self.assertRaises(errors.DoesNotExist, sgw.get_user_info,
                          10, user.shard_id)

    def test_get_abandoned_uploadjobs(self):
        """Test the get_abandoned_uploadjobs method."""
        crc = 12345
        size = 100
        def_size = 100
        for uid in range(0, 10):
            suser = self.create_user(id=uid, username=u"testuser_%d" % uid,
                                     shard_id=u"shard1",
                                     max_storage_bytes=1024)
            user = self.gw.get_user(suser.id)
            vgw = user._gateway.get_root_gateway()
            root = vgw.get_root()
            new_hash = get_fake_hash(os.urandom(100))
            file1 = vgw.make_file(root.id, u"file1-%d" % uid)
            up1 = vgw.make_uploadjob(file1.id, file1.content_hash, new_hash,
                                     crc, size, def_size,
                                     multipart_id=str(uuid.uuid4()),
                                     multipart_key=uuid.uuid4())
            # change the when_started date for the test.
            store = get_shard_store(u"shard1")
            uploadjob = store.get(model.UploadJob, up1.id)
            uploadjob.when_last_active = (
                datetime.datetime.now() - datetime.timedelta(uid))
        transaction.commit()
        # check that filtering by date works as expected.
        for idx in range(0, 10):
            date = datetime.datetime.now() - datetime.timedelta(idx)
            jobs = list(self.gw.get_abandoned_uploadjobs(
                'shard1', date))
            self.assertEqual(len(jobs), 10 - idx)

    def test_cleanup_abandoned_uploadjobs(self):
        """Test the cleanup_uploadjobs method."""
        crc = 12345
        size = 100
        def_size = 100
        for uid in range(0, 10):
            suser = self.create_user(id=uid, username=u"testuser_%d" % uid,
                                     shard_id=u"shard1",
                                     max_storage_bytes=1024)
            user = self.gw.get_user(suser.id)
            vgw = user._gateway.get_root_gateway()
            root = vgw.get_root()
            new_hash = get_fake_hash(os.urandom(100))
            file1 = vgw.make_file(root.id, u"file1-%d" % uid)
            up1 = vgw.make_uploadjob(file1.id, file1.content_hash, new_hash,
                                     crc, size, def_size,
                                     multipart_id=str(uuid.uuid4()),
                                     multipart_key=uuid.uuid4())
            # change the when_started date for the test.
            store = get_shard_store(u"shard1")
            uploadjob = store.get(model.UploadJob, up1.id)
            uploadjob.when_last_active = (
                datetime.datetime.now() - datetime.timedelta(10))
        transaction.commit()
        # check that filtering by date works as expected.
        date = datetime.datetime.now() - datetime.timedelta(9)
        jobs = self.gw.get_abandoned_uploadjobs('shard1', date)
        self.assertEqual(len(jobs), 10)
        self.gw.cleanup_uploadjobs('shard1', jobs)
        jobs = self.gw.get_abandoned_uploadjobs('shard1', date)
        self.assertEqual(len(jobs), 0)


class SystemGatewayPublicFileTestCase(StorageDALTestCase):
    """Test public files operations."""

    def setUp(self):
        super(SystemGatewayPublicFileTestCase, self).setUp()
        self.gw = SystemGateway()
        self.user1 = self.create_user(id=1, username=u"sharer")
        self.vgw = ReadWriteVolumeGateway(self.user1)
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = deflated_size = 10
        self.root = self.vgw.get_root()
        file1 = self.vgw.make_file_with_content(
            self.root.id, name, hash, crc, size, deflated_size, storage_key,
            mimetype=u"fakemime")
        self.file = self.vgw._get_node_simple(file1.id)
        self.save_flag = utils.set_public_uuid

    def tearDown(self):
        utils.set_public_uuid = self.save_flag
        super(SystemGatewayPublicFileTestCase, self).tearDown()

    def get_get_public_file_DoesNotExist(self):
        """Get get_public_file with unknown key."""
        # DoesNotExist is raised for bad file IDs.
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, 'xyz')

    def test_get_public_file_no_content(self):
        """Test get_public_file when file has no content."""
        utils.set_public_uuid = False
        file_dao = self.vgw.change_public_access(self.file.id, True)
        self.file._content_hash = None
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, file_dao.public_key)

    def test_get_public_file_no_storage_key(self):
        """Test get_public_file when file has no storage_key."""
        utils.set_public_uuid = False
        file_dao = self.vgw.change_public_access(self.file.id, True)
        self.file.content.storage_key = None
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, file_dao.public_key)

    def test_get_public_file(self):
        """Tests for get_public_file."""
        utils.set_public_uuid = False
        file_dao = self.vgw.change_public_access(self.file.id, True)
        self.assertNotEquals(file_dao.public_id, None)
        self.assertEqual(file_dao.public_uuid, None)
        public_id = file_dao.public_id
        public_key = file_dao.public_key
        # Once a file has been made public, it can be looked up by its ID.
        file2 = self.gw.get_public_file(public_key)
        self.assertEqual(file2.id, self.file.id)
        self.assertEqual(file2.mimetype, u"fakemime")
        # this file was created with content, the content must be returned
        self.assertNotEquals(file2.content, None)

        # DoesNotExist is raised if that file is made private.
        file_dao = self.vgw.change_public_access(self.file.id, False)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, public_key)

        # public_id stays the same when set back to public
        self.assertEqual(file_dao.public_id, None)
        file_dao = self.vgw.change_public_access(file_dao.id, True)
        self.assertEqual(file_dao.public_id, public_id)

        # DoesNotExist is raised if the underlying file is deleted.
        self.vgw.delete_node(self.file.id)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, public_key)

    def test_get_public_file_public_uuid(self):
        """Tests for get_public_file."""
        utils.set_public_uuid = True
        file_dao = self.vgw.change_public_access(self.file.id, True)
        self.assertNotEquals(file_dao.public_id, None)
        self.assertNotEquals(file_dao.public_uuid, None)
        public_id = file_dao.public_id
        public_uuid = file_dao.public_uuid
        public_key = file_dao.public_key
        # Once a file has been made public, it can be looked up by its UUID.
        file2 = self.gw.get_public_file(public_key, use_uuid=True)
        self.assertEqual(file2.id, self.file.id)
        self.assertEqual(file2.mimetype, self.file.mimetype)
        # this file was created with content, the content must be returned
        self.assertNotEquals(file2.content, None)
        # but not it's public_id since the config is set to use uuid
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, public_key)

        # DoesNotExist is raised if that file is made private.
        file_dao = self.vgw.change_public_access(self.file.id, False)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, public_key, True)

        # public_id stays the same when set back to public
        self.assertEqual(file_dao.public_id, None)
        file_dao = self.vgw.change_public_access(file_dao.id, True)
        self.assertEqual(file_dao.public_id, public_id)
        self.assertEqual(file_dao.public_uuid, public_uuid)

        # DoesNotExist is raised if the underlying file is deleted.
        self.vgw.delete_node(self.file.id)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_file, public_key, use_uuid=True)

    def test_get_public_file_user_locked(self):
        """get_public_file works with a locked user."""
        utils.set_public_uuid = False
        file_dao = self.vgw.change_public_access(self.file.id, True)
        self.assertNotEquals(file_dao.public_id, None)
        self.assertEqual(file_dao.public_uuid, None)
        public_key = file_dao.public_key
        # lock the user
        suser = self.user_store.get(model.StorageUser, self.user1.id)
        suser.locked = True
        self.user_store.commit()
        # Once a file has been made public, it can be looked up by its ID.
        file2 = self.gw.get_public_file(public_key)
        self.assertEqual(file2.id, self.file.id)


class SystemGatewayPublicDirectoryTestCase(StorageDALTestCase):
    """Test public directory operations."""

    def setUp(self):
        super(SystemGatewayPublicDirectoryTestCase, self).setUp()
        self.gw = SystemGateway()
        self.user1 = self.create_user(id=1, username=u"sharer")
        self.vgw = ReadWriteVolumeGateway(self.user1)
        self.root = self.vgw.get_root()
        dir1 = self.vgw.make_subdirectory(self.root.id, u"a-folder")
        self.dir = self.vgw._get_node_simple(dir1.id)
        self.save_flag = utils.set_public_uuid
        utils.set_public_uuid = True

    def tearDown(self):
        utils.set_public_uuid = self.save_flag
        super(SystemGatewayPublicDirectoryTestCase, self).tearDown()

    def get_get_public_directory_DoesNotExist(self):
        """Get get_public_file with unknown key."""
        # DoesNotExist is raised for bad IDs
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_directory, 'xyz')

    def test_get_public_directory(self):
        """Tests for get_public_directory."""
        dir_dao = self.vgw.change_public_access(
            self.dir.id, True, allow_directory=True)
        self.assertNotEquals(dir_dao.public_id, None)
        self.assertNotEquals(dir_dao.public_uuid, None)
        public_key = dir_dao.public_key
        # If I don't do this, the test fails
        transaction.commit()
        # lock the user
        suser = self.user_store.get(model.StorageUser, self.user1.id)
        suser.locked = True
        self.user_store.commit()
        # Once a directory has been made public, it can be accessed
        dir2 = self.gw.get_public_directory(public_key)
        self.assertEqual(dir2.id, self.dir.id)

    def test_get_public_directory_user_locked(self):
        """get_public_directory also works with locked user."""
        dir_dao = self.vgw.change_public_access(
            self.dir.id, True, allow_directory=True)
        self.assertNotEquals(dir_dao.public_id, None)
        self.assertNotEquals(dir_dao.public_uuid, None)
        public_id = dir_dao.public_id
        public_uuid = dir_dao.public_uuid
        public_key = dir_dao.public_key
        # If I don't do this, the test fails
        transaction.commit()
        # Once a directory has been made public, it can be accessed
        dir2 = self.gw.get_public_directory(public_key)
        self.assertEqual(dir2.id, self.dir.id)
        # DoesNotExist is raised if that directory is made private
        self.vgw.change_public_access(self.dir.id, False, allow_directory=True)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_directory, public_key)
        dir_dao = self.vgw.change_public_access(
            dir_dao.id, True, allow_directory=True)
        # public_id stays the same when set back to public
        self.assertEqual(dir_dao.public_id, public_id)
        self.assertEqual(dir_dao.public_uuid, public_uuid)
        # DoesNotExist is raised if the underlying directory is deleted
        self.vgw.delete_node(self.dir.id)
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_public_directory, public_key)


class StorageUserGatewayTestCase(StorageDALTestCase):
    """Test the StorageUserGateway."""

    def setUp(self):
        super(StorageUserGatewayTestCase, self).setUp()
        self.user = self.create_user(username=u"testuser", shard_id=u"shard0")
        self.gw = StorageUserGateway(self.user)

    def test_update(self):
        """Test updating a user."""
        quota1 = self.gw.get_quota()
        self.gw.update(max_storage_bytes=None, subscription=None)
        user2 = self.gw.get_user(self.user.id, session_id="QWERTY")
        quota2 = user2._gateway.get_quota()
        # in this example, nothing should change
        self.assertEqual(self.user.username, user2.username)
        self.assertEqual(self.user.visible_name, user2.visible_name)
        self.assertEqual(quota1.max_storage_bytes, quota2.max_storage_bytes)
        self.assertEqual(self.user._subscription_status,
                         user2._subscription_status)
        self.assertEqual(self.user._status, user2._status)

        self.gw.update(max_storage_bytes=2)
        user2 = self.gw.get_user(self.user.id)
        quota = self.gw.get_quota()
        self.assertEqual(quota.max_storage_bytes, 2)
        self.assertEqual(user2._subscription_status, model.STATUS_LIVE)
        # make sure the StorageUserInfo is updated as well
        store = get_shard_store(user2.shard_id)
        info = store.get(model.StorageUserInfo, user2.id)
        self.assertEqual(info.max_storage_bytes, 2)

        # subscription
        self.gw.update(subscription=False)
        user2 = self.gw.get_user(self.user.id)
        self.assertEqual(user2._subscription_status, model.STATUS_DEAD)
        self.gw.update(subscription=True)
        user2 = self.gw.get_user(self.user.id)
        self.assertEqual(user2._subscription_status, model.STATUS_LIVE)

    def test_get_udf_gateway_None(self):
        """Make sure method returns None and no Error."""
        vgw = self.gw.get_udf_gateway(uuid.uuid4())
        self.assertEqual(vgw, None)

    def test_get_share_gateway_None(self):
        """Make sure method returns None and no Error."""
        vgw = self.gw.get_share_gateway(uuid.uuid4())
        self.assertEqual(vgw, None)

    def test_accept_share(self):
        """Test accepting a direct share."""
        user1 = self.create_user(id=2, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(user1.id, root.id, self.user.id, u"Share", "View")
        self.user_store.add(share)
        transaction.commit()
        # if it's dead, it can't be accepted
        share.status = model.STATUS_DEAD
        self.assertRaises(errors.DoesNotExist, self.gw.accept_share, share.id)
        # this should succeed
        share.status = model.STATUS_LIVE
        self.assertEqual(share.status, model.STATUS_LIVE)
        self.assertEqual(share.accepted, False)
        self.gw.accept_share(share.id)
        self.assertEqual(share.accepted, True)
        # cant be accepted twice
        self.assertRaises(errors.DoesNotExist, self.gw.accept_share, share.id)

    def test_decline_share(self):
        """Test declinet a direct share."""
        user1 = self.create_user(id=2, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(user1.id, root.id, self.user.id, u"Share", "View")
        self.user_store.add(share)
        transaction.commit()
        # if it's dead, it can't be accepted
        share.status = model.STATUS_DEAD
        self.assertRaises(errors.DoesNotExist, self.gw.decline_share, share.id)
        # this should succeed
        share.status = model.STATUS_LIVE
        self.assertEqual(share.status, model.STATUS_LIVE)
        self.assertEqual(share.accepted, False)
        self.gw.decline_share(share.id)
        self.assertEqual(share.accepted, False)
        # when a share is declined, it is also deleted
        self.assertEqual(share.status, model.STATUS_DEAD)
        # cant be accepted twice
        self.assertRaises(errors.DoesNotExist, self.gw.decline_share, share.id)

    def test_delete_share(self):
        """Test delete shares from share-er and share-ee"""
        user1 = self.create_user(id=2, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(self.user.id, root.id, user1.id,
                            u"Share", "View")
        self.user_store.add(share)
        transaction.commit()
        share = self.gw.get_share(share.id, accepted_only=False)
        self.assertEqual(share.status, model.STATUS_LIVE)
        self.gw.delete_share(share.id)
        # share is now inaccessible
        self.assertRaises(errors.DoesNotExist,
                          self.gw.get_share, share.id, accepted_only=False)
        # make it live again so we can test it with the share-ee
        share = self.user_store.get(model.Share, share.id)
        share.status = model.STATUS_LIVE
        # get user1's gateway and delete it
        user1._gateway.delete_share(share.id)

    def test_get_shares_of_nodes(self):
        """Test get_shares_of_nodes."""
        fake_nodeids = [uuid.uuid4(), uuid.uuid4()]
        shares = self.gw.get_shares_of_nodes(fake_nodeids)
        self.assertEqual(list(shares), [])
        usera = self.create_user(id=2, username=u"sharee1", shard_id=u"shard0")
        userb = self.create_user(id=3, username=u"sharee2", shard_id=u"shard1")
        userc = self.create_user(id=4, username=u"sharee3")
        vgw = self.gw.get_root_gateway()
        dir1 = vgw.make_subdirectory(vgw.get_root().id, u"shared1")
        dir2 = vgw.make_subdirectory(vgw.get_root().id, u"shared2")
        # make 4 shares
        sharez = vgw.make_share(dir1.id, u"sharex", user_id=usera.id)
        sharea = vgw.make_share(dir2.id, u"sharea", user_id=usera.id)
        shareb = vgw.make_share(dir2.id, u"shareb", user_id=userb.id)
        sharec = vgw.make_share(dir2.id, u"sharec", user_id=userc.id)
        # userc will not accept the share in this test
        usera._gateway.accept_share(sharez.id)
        usera._gateway.accept_share(sharea.id)
        userb._gateway.accept_share(shareb.id)
        # get the shares of the two dirs and some fake uuids
        shares = self.gw.get_shares_of_nodes([dir2.id, dir1.id] + fake_nodeids)
        # only three shares are active
        shares = list(shares)
        self.assertEqual(len(shares), 3)
        share_info = dict([(share.id, share) for share in shares])
        # the shared_to property is also populated
        self.assertEqual(share_info[sharez.id].root_id, dir1.id)
        self.assertEqual(share_info[sharez.id].shared_to.id, usera.id)
        self.assertEqual(share_info[sharez.id].shared_to.username,
                         usera.username)
        self.assertEqual(share_info[sharea.id].root_id, dir2.id)
        self.assertEqual(share_info[sharea.id].shared_to.id, usera.id)
        self.assertEqual(share_info[sharea.id].shared_to.username,
                         usera.username)
        self.assertEqual(share_info[shareb.id].root_id, dir2.id)
        self.assertEqual(share_info[shareb.id].shared_to.id, userb.id)
        self.assertEqual(share_info[shareb.id].shared_to.username,
                         userb.username)
        # can also get the unnaccepted shares
        shares = self.gw.get_shares_of_nodes(
            [dir2.id, dir1.id] + fake_nodeids, accepted_only=False)
        shares = list(shares)
        self.assertEqual(len(shares), 4)
        share_info = dict([(share.id, share) for share in shares])
        self.assertTrue(sharec.id in share_info)

    def test_make_udf(self):
        """Test make_udf method."""
        udf = self.gw.make_udf(u"~/path/name")
        self.assertEqual(udf.owner_id, self.user.id)
        self.assertEqual(udf.path, u"~/path/name")
        self.assertNotEqual(udf.when_created, None)
        # make sure the same UDF is created
        udf2 = self.gw.make_udf(u"~/path/name")
        self.assertEqual(udf.id, udf2.id)

    def test_make_udf_fails(self):
        """Test failures when making UDF"""
        self.gw.make_udf(u"~/a/b/c")
        self.gw.make_udf(u"~/a/b/cc")
        self.assertRaises(errors.NoPermission,
                          self.gw.make_udf, u"~/a/b/c/d/e")
        self.assertRaises(errors.NoPermission,
                          self.gw.make_udf, u"~/a/b")

    def test_get_udf(self):
        """Test make and get udf methods."""
        udf = self.gw.make_udf(u"~/path/name")
        udf2 = self.gw.get_udf(udf.id)
        self.assertEqual(udf.id, udf2.id)
        self.assertEqual(udf.path, udf2.path)

    def test_get_udf_by_path(self):
        """Test different ways to get udf by path."""
        udf = self.gw.make_udf(u"~/a/b/c")
        udf1 = self.gw.get_udf_by_path(u"~/a/b/c")
        self.assertEqual(udf.id, udf1.id)
        self.assertEqual(udf.path, udf1.path)
        udf1 = self.gw.get_udf_by_path(u"~/a/b/c", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        self.assertEqual(udf.path, udf1.path)
        udf1 = self.gw.get_udf_by_path(u"~/a/b/c/", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        self.assertEqual(udf.path, udf1.path)
        udf1 = self.gw.get_udf_by_path(u"~/a/b/c/d/e", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        self.assertEqual(udf.path, udf1.path)
        udf1 = self.gw.get_udf_by_path(u"~/a/b/c/d/e/", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        self.assertEqual(udf.path, udf1.path)

    def test_get_udfs(self):
        """Test get_udfs method."""
        for i in range(10):
            self.gw.make_udf(u"~/path/to/file/name %s" % i)
        udfs = self.gw.get_udfs()
        udfs = list(udfs)
        self.assertEqual(len(udfs), 10)
        for udf in udfs:
            self.gw.delete_udf(udf.id)
        udfs2 = self.gw.get_udfs()
        self.assertEqual(len(list(udfs2)), 0)

    def test_delete_udf(self):
        """Test delete_udf method."""
        udf = self.gw.make_udf(u"~/path/name")
        self.gw.delete_udf(udf.id)
        transaction.commit()

    def test_delete_udf_with_children(self):
        """Test delete_udf method."""
        udf = self.gw.make_udf(u"~/path/name")
        vgw = ReadWriteVolumeGateway(self.user, udf=udf)
        a_dir = vgw.make_subdirectory(vgw.root_id, u"subdir")
        vgw.make_file(a_dir.id, u"file")
        self.gw.delete_udf(udf.id)
        self.assertRaises(errors.DoesNotExist, self.gw.get_udf, udf.id)

    def test_delete_udf_with_shares(self):
        """Test udf deletion with related shares."""
        udf = self.gw.make_udf(u"~/path/name")
        vgw = ReadWriteVolumeGateway(self.user, udf=udf)
        dir1 = vgw.make_subdirectory(vgw.root_id, u"subdir")
        usera = self.create_user(id=2, username=u"sharee1", shard_id=u"shard0")
        sharea = vgw.make_share(dir1.id, u"sharea", user_id=usera.id)
        usera._gateway.accept_share(sharea.id)
        self.gw.delete_udf(udf.id)

    def test_noaccess_inactive_user(self):
        """Make sure NoAccess is rasied when the user is inactive."""
        self.user.update(subscription=False)
        self.assertRaises(errors.NoPermission, self.gw.make_udf, u"~/p/n")
        self.assertRaises(errors.NoPermission, self.gw.accept_share, 1)
        self.assertRaises(errors.NoPermission, self.gw.get_share, 1)
        self.assertRaises(errors.NoPermission, self.gw.delete_share, 1)
        self.assertRaises(errors.NoPermission, self.gw.get_udf, 1)
        self.assertRaises(errors.NoPermission, self.gw.get_volume_gateway)
        # the generators behave a little differently
        self.assertRaises(errors.NoPermission, list, self.gw.get_shared_by())
        self.assertRaises(errors.NoPermission, list, self.gw.get_udfs())

    def test_delete_related_shares(self):
        """Test _delete_related_shares."""
        usera = self.create_user(id=2, username=u"sharee1", shard_id=u"shard0")
        userb = self.create_user(id=3, username=u"sharee2", shard_id=u"shard1")
        userc = self.create_user(id=4, username=u"sharee3")
        store = get_shard_store(self.user.shard_id)
        vgw = self.gw.get_root_gateway()
        dir1 = vgw.make_subdirectory(vgw.get_root().id, u"shared1")
        dir2 = vgw.make_subdirectory(dir1.id, u"shared2")
        # make 4 shares
        sharez = vgw.make_share(dir1.id, u"sharex", user_id=usera.id)
        sharea = vgw.make_share(dir2.id, u"sharea", user_id=usera.id)
        shareb = vgw.make_share(dir2.id, u"shareb", user_id=userb.id)
        sharec = vgw.make_share(dir2.id, u"sharec", user_id=userc.id)
        # userc will not accept the share in this test
        usera._gateway.accept_share(sharez.id)
        usera._gateway.accept_share(sharea.id)
        userb._gateway.accept_share(shareb.id)
        for s in [sharea, sharea, shareb, sharec]:
            share = self.user._gateway.get_share(s.id, accepted_only=False)
            self.assertEqual(share.status, 'Live')
        transaction.commit()
        dir1 = store.get(model.StorageObject, dir1.id)
        self.user._gateway.delete_related_shares(dir1)
        # the shares no longer exist
        for s in [sharez, sharea, shareb, sharec]:
            self.assertRaises(
                errors.DoesNotExist,
                self.user._gateway.get_share, s.id, accepted_only=False)

    def test_delete_related_shares_lotsofchildren(self):
        """Make sure the query splitting does it's job."""
        vgw = self.gw.get_root_gateway()
        dir1 = vgw.make_subdirectory(vgw.get_root().id, u"shared1")
        # make lots of children
        for i in xrange(300):
            vgw.make_subdirectory(dir1.id, u"d%s" % i)
        usera = self.create_user(id=2, username=u"sharee1", shard_id=u"shard0")
        sharea = vgw.make_share(dir1.id, u"sharea", user_id=usera.id)
        usera._gateway.accept_share(sharea.id)
        store = get_shard_store(self.user.shard_id)
        dir1 = store.get(model.StorageObject, dir1.id)
        self.user._gateway.delete_related_shares(dir1)
        self.assertRaises(
            errors.DoesNotExist,
            self.user._gateway.get_share, sharea.id, accepted_only=False)

    def test_get_downloads(self):
        """Test get_downlods method."""
        dls = self.gw.get_downloads()
        self.assertEqual(dls, [])
        sysgw = SystemGateway()
        udf = model.UserVolume.create(
            get_shard_store(self.user.shard_id),
            self.user.id, u"~/path/name")
        dl_url = u"http://download/url"
        found_urls = {}
        for i in range(5):
            url = dl_url + str(i)
            sysgw.make_download(self.user.id, udf.id, u"path%s" % i, url)
            found_urls[url] = False
        dls = self.gw.get_downloads()
        self.assertEqual(len(dls), 5)
        for dl in dls:
            if dl.download_url in found_urls:
                found_urls[dl.download_url] = True
        for k, v in found_urls.items():
            self.assertTrue(v)

    def test_get_public_files(self):
        """Test get_public_files method."""
        vgw = self.gw.get_root_gateway()
        shard_store = get_shard_store(self.user.shard_id)
        root = model.StorageObject.get_root(shard_store, self.user.id)
        node = root.make_file(u"TheName")
        node._content_hash = model.EMPTY_CONTENT_HASH
        node.mimetype = u"fakemime"
        vgw.change_public_access(node.id, True)
        nodes = list(self.gw.get_public_files())
        self.assertEqual(1, len(nodes))
        self.assertEqual(node.id, nodes[0].id)
        self.assertEqual(node.volume_id, nodes[0].volume_id)
        self.assertNotEquals(None, node.publicfile_id)
        # now test it with more than one file (and some dirs too)

        def create_files(root):
            """Create a 5 dirs with 5 files each in the specified root."""
            for dname in [u'dir_%s' % i for i in xrange(5)]:
                d = root.make_subdirectory(dname)
                for fname, j in [(u'file_%s' % j, j) for j in xrange(10)]:
                    f = d.make_file(fname)
                    f._content_hash = model.EMPTY_CONTENT_HASH
                    f.mimetype = u"fakemime"
                    if j % 2:
                        continue
                    vgw.change_public_access(f.id, True)
                    vgw.make_file(d.id, fname)

        create_files(root)
        file_cnt = 5 * 5 + 1
        nodes = list(self.gw.get_public_files())
        self.assertEqual(file_cnt, len(nodes))
        # and test it with public files inside a UDF
        udf = self.gw.make_udf(u"~/path/name")
        udf_root = shard_store.get(model.StorageObject, udf.root_id)
        create_files(udf_root)
        file_cnt = file_cnt + 5 * 5
        nodes = list(self.gw.get_public_files())
        self.assertEqual(file_cnt, len(nodes))

    def test_get_public_folders(self):
        """Test get_public_folders method."""
        vgw = self.gw.get_root_gateway()
        shard_store = get_shard_store(self.user.shard_id)
        root = model.StorageObject.get_root(shard_store, self.user.id)
        node = root.make_subdirectory(u'test_dir')
        vgw.change_public_access(node.id, True, allow_directory=True)
        nodes = list(self.gw.get_public_folders())
        self.assertEqual(1, len(nodes))
        self.assertEqual(node.id, nodes[0].id)
        self.assertEqual(node.volume_id, nodes[0].volume_id)
        self.assertNotEquals(None, node.publicfile_id)
        for dname in [u'dir_%s' % i for i in xrange(5)]:
            d = root.make_subdirectory(dname)
            vgw.change_public_access(d.id, True, allow_directory=True)
        nodes = list(self.gw.get_public_folders())
        # 5 + 1
        self.assertEqual(6, len(nodes))

    def test_get_share_generation(self):
        """Test the get_share_generation method."""
        user1 = self.create_user(id=2, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(self.user.id, root.id, user1.id,
                            u"Share", "View")
        self.user_store.add(share)
        transaction.commit()
        share = self.gw.get_share(share.id, accepted_only=False)
        self.assertEqual(0, self.gw.get_share_generation(share))
        # increase the generation
        user1.root.make_subdirectory(u"a dir")
        # check we get the correct generation
        self.assertEqual(1, self.gw.get_share_generation(share))

    def test_get_share_generation_None(self):
        """Test the get_share_generation method."""
        user1 = self.create_user(id=2, username=u"sharer")
        store = get_shard_store(user1.shard_id)
        root = model.StorageObject.get_root(store, user1.id)
        share = model.Share(self.user.id, root.id, user1.id,
                            u"Share", "View")
        self.user_store.add(share)
        transaction.commit()
        vol = store.find(
            model.UserVolume,
            model.UserVolume.id == model.StorageObject.volume_id,
            model.StorageObject.id == root.id).one()
        # force generation = None
        vol.generation = None
        share = self.gw.get_share(share.id, accepted_only=False)
        self.assertEqual(0, self.gw.get_share_generation(share))
        # increase the generation
        user1.root.make_subdirectory(u"a dir")
        # check we get the correct generation
        self.assertEqual(1, self.gw.get_share_generation(share))

    def test_reusable_content_no_blob(self):
        """No blob at all, can not reuse."""
        blobexists, storage_key = self.gw._get_reusable_content('hash_value',
                                                                'magic')
        self.assertFalse(blobexists)
        self.assertEqual(storage_key, None)

    def _make_file_with_content(self, hash_value, gw=None):
        """Make a file with some content."""
        name = u"filename"
        storage_key = uuid.uuid4()
        crc = 12345
        size = deflated_size = 10

        if gw is None:
            gw = self.gw
        vgw = gw.get_root_gateway()
        root = vgw.get_root()
        n = vgw.make_file_with_content(root.id, name, hash_value, crc, size,
                                       deflated_size, storage_key)
        return n

    def test_reusable_content_same_owner_no_magic(self):
        """Test update_content will reuse owned content, even with no magic."""
        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value)
        blobexists, storage_key = self.gw.is_reusable_content(hash_value, None)
        self.assertTrue(blobexists)
        self.assertEqual(storage_key, node.content.storage_key)

    def test_reusable_content_same_owner_with_magic(self):
        """Test update_content will reuse owned content."""
        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value)
        get_shard_store(self.user.shard_id).find(
            model.ContentBlob,
            model.ContentBlob.hash == node.content_hash
        ).set(magic_hash='magic')
        blobexists, storage_key = self.gw.is_reusable_content(hash_value,
                                                              'magic')
        self.assertTrue(blobexists)
        self.assertEqual(storage_key, node.content.storage_key)

    def test_reusable_content_different_owner_no_magic(self):
        """Test update_content will not reuse someone elses content."""
        user2 = self.create_user(id=66, username=u"testuserX",
                                 shard_id=self.user.shard_id,
                                 max_storage_bytes=2 ** 32)
        assert user2.id != self.user.id

        hash_value = get_fake_hash()
        self._make_file_with_content(hash_value, gw=user2._gateway)
        blobexists, storage_key = self.gw.is_reusable_content(hash_value, None)
        self.assertTrue(blobexists)
        self.assertEqual(storage_key, None)

    def test_reusable_content_different_owner_with_magic(self):
        """Test update_content will reuse someone elses content with magic."""
        user2 = self.create_user(id=66, username=u"testuserX",
                                 shard_id=self.user.shard_id,
                                 max_storage_bytes=2 ** 32)
        assert user2.id != self.user.id

        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value, gw=user2._gateway)
        get_shard_store(self.user.shard_id).find(
            model.ContentBlob,
            model.ContentBlob.hash == node.content_hash
        ).set(magic_hash='magic')
        self.assertTrue(self.gw.is_reusable_content(hash_value, 'magic'))
        blobexists, storage_key = self.gw.is_reusable_content(hash_value,
                                                              'magic')
        self.assertTrue(blobexists)
        self.assertEqual(storage_key, node.content.storage_key)

    def test__get_reusable_content_no_blob(self):
        """No blob at all, can not reuse."""
        blobexists, blob = self.gw._get_reusable_content('hash_value', 'magic')
        self.assertFalse(blobexists)
        self.assertEqual(blob, None)

    def test__get_reusable_content_same_owner_no_magic(self):
        """Test update_content will reuse owned content, even with no magic."""
        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value)
        blobexists, blob = self.gw._get_reusable_content(hash_value, None)
        self.assertTrue(blobexists)
        self.assertEqual(blob.hash, node.content_hash)

    def test__get_reusable_content_same_owner_with_magic(self):
        """Test update_content will reuse owned content."""
        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value)
        get_shard_store(self.user.shard_id).find(
            model.ContentBlob,
            model.ContentBlob.hash == node.content_hash
        ).set(magic_hash='magic')
        blobexists, blob = self.gw._get_reusable_content(hash_value, 'magic')
        self.assertTrue(blobexists)
        self.assertEqual(blob.hash, node.content_hash)

    def test__get_reusable_content_different_owner_no_magic(self):
        """Test update_content will not reuse someone elses content."""
        user2 = self.create_user(id=66, username=u"testuserX",
                                 shard_id=self.user.shard_id,
                                 max_storage_bytes=2 ** 32)
        assert user2.id != self.user.id

        hash_value = get_fake_hash()
        self._make_file_with_content(hash_value, gw=user2._gateway)
        blobexists, blob = self.gw._get_reusable_content(hash_value, None)
        self.assertTrue(blobexists)
        self.assertEqual(blob, None)

    def test__get_reusable_content_different_owner_with_magic(self):
        """Test update_content will reuse someone elses content with magic."""
        user2 = self.create_user(id=66, username=u"testuserX",
                                 shard_id=self.user.shard_id,
                                 max_storage_bytes=2 ** 32)
        assert user2.id != self.user.id

        hash_value = get_fake_hash()
        node = self._make_file_with_content(hash_value, gw=user2._gateway)
        get_shard_store(self.user.shard_id).find(
            model.ContentBlob,
            model.ContentBlob.hash == node.content_hash
        ).set(magic_hash='magic')
        self.assertTrue(self.gw._get_reusable_content(hash_value, 'magic'))
        blobexists, blob = self.gw._get_reusable_content(hash_value, 'magic')
        self.assertTrue(blobexists)
        self.assertEqual(blob.hash, node.content_hash)

    def test_get_photo_directories(self):
        """Get the get_photo_directories method."""
        udf1 = self.gw.make_udf(u"~/Photos/from/party")
        udf2 = self.gw.make_udf(u"~/Photos/from/party2")
        udf3 = self.gw.make_udf(u"~/Photos/from/party3")
        vgw1 = self.gw.get_root_gateway()
        vgw2 = self.gw.get_udf_gateway(udf1.id)
        vgw3 = self.gw.get_udf_gateway(udf2.id)
        vgw4 = self.gw.get_udf_gateway(udf3.id)
        hash_value = get_fake_hash()
        expected_dirs = []
        for vgw in [vgw1, vgw2, vgw3, vgw4]:
            root = vgw.get_root()
            d1 = vgw.make_tree(root.id, u"/a/path/without/pics1")
            vgw.make_file_with_content(
                d1.id, u"picture.txt", hash_value, 100, 100, 1000,
                uuid.uuid4())
            vgw.make_tree(root.id, u"/a/path/with/pics2")
            d2 = vgw.make_tree(root.id, u"/a/path/with/pics")
            if d2.volume_id != udf3.id:
                expected_dirs.append(d2)
            vgw.make_file_with_content(
                d2.id, u"picture1.jpg", hash_value, 100, 100, 100,
                uuid.uuid4())
            vgw.make_file_with_content(
                d2.id, u"picture2.jpg", hash_value, 100, 100, 100,
                uuid.uuid4())
            vgw.make_file_with_content(
                d2.id, u"picture3.jpg", hash_value, 100, 100, 100,
                uuid.uuid4())
            d3 = vgw.make_tree(root.id, u"/a/path/with/pics4")
            vgw.make_file_with_content(
                d3.id, u"picture.jpg",
                model.EMPTY_CONTENT_HASH, 0, 0, 0, uuid.uuid4())
        # this udf's results should not show up
        self.gw.delete_udf(udf3.id)
        dirs = list(self.gw.get_photo_directories())
        self.assertEqual(len(dirs), 3)
        self.assertEqual(set([(d.id, d.vol_type) for d in dirs]),
                         set([(d1.id, d1.vol_type) for d1 in expected_dirs]))


class ReadWriteVolumeGatewayUtilityTests(StorageDALTestCase):
    """Test ReadWriteVolumeGateway utility functions."""

    def setUp(self):
        super(ReadWriteVolumeGatewayUtilityTests, self).setUp()
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1",
                                max_storage_bytes=200)
        self.user = self.gw.get_user(user.id)
        self.user_quota = self.user._gateway.get_quota()
        self.shard_store = get_shard_store(self.user.shard_id)
        self.vgw = self.user._gateway.get_root_gateway()
        self.root = self.vgw.get_root()

    def test_get_all_nodes(self):
        """Test get_all_nodes."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        all_nodes = self.vgw.get_all_nodes()
        self.assertEqual(len(all_nodes), 21)
        # the lists are not sorted so not equal
        self.assertNotEquals(all_nodes, nodes)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        self.assertEqual(all_nodes, nodes)

        # with a max_generation
        nodes_gen_10 = self.vgw.get_all_nodes(max_generation=10)
        self.assertEqual(
            nodes_gen_10, [n for n in nodes if n.generation <= 10])
        nodes_gen_20 = self.vgw.get_all_nodes(max_generation=20)
        self.assertEqual(nodes_gen_20, nodes)

        # with max_generation and limit
        nodes_limit_5 = self.vgw.get_all_nodes(max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:10])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10)
        # same but with the last gen.
        nodes_limit_10 = self.vgw.get_all_nodes(max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_only_from_volume(self):
        """Test get_all_nodes."""
        # make some files on a UDF...
        udf = model.UserVolume.create(
            self.shard_store, self.user.id, u"~/thepath/thename")
        udf_dao = dao.UserVolume(udf, self.user)
        udf_vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        udf_root_id = udf_vgw.get_root().id
        for i in range(10):
            d = udf_vgw.make_subdirectory(udf_root_id, u"%sd" % i)
            f = udf_vgw.make_file(d.id, u"%sfile.txt" % i)
        # make files on the root and make sure only they are returned
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        all_nodes = self.vgw.get_all_nodes()
        self.assertEqual(len(all_nodes), 21)
        # the lists are not sorted so not equal
        self.assertNotEquals(all_nodes, nodes)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        self.assertEqual(all_nodes, nodes)

        # with a max_generation
        nodes_gen_10 = self.vgw.get_all_nodes(max_generation=10)
        self.assertEqual(
            nodes_gen_10, [n for n in nodes if n.generation <= 10])
        nodes_gen_20 = self.vgw.get_all_nodes(max_generation=20)
        self.assertEqual(nodes_gen_20, nodes)

        # with max_generation and limit
        nodes_limit_5 = self.vgw.get_all_nodes(max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:10])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10)
        # same but with the last gen.
        nodes_limit_10 = self.vgw.get_all_nodes(max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_kind(self):
        """Test get_all_nodes."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"d%s" % i)
            f = self.vgw.make_file(d.id, u"file%s.txt" % i)
            dirs.append(d)
            files.append(f)
        dirs.append(root)
        # sort them in the right order
        files.sort(key=attrgetter('path', 'name'))
        dirs.sort(key=attrgetter('path', 'name'))
        all_nodes = self.vgw.get_all_nodes(kind='File')
        self.assertEqual(len(all_nodes), 10)
        self.assertEqual(all_nodes, files)
        all_nodes = self.vgw.get_all_nodes(kind='Directory')
        self.assertEqual(len(all_nodes), 11)
        self.assertEqual(all_nodes, dirs)

    def test_get_all_with_mimetype(self):
        """Test get_all_nodes with mimetype filter."""
        hash = get_fake_hash()
        key = uuid.uuid4()
        root_id = self.vgw.get_root().id
        # make a bunch of files with content
        mkfile = lambda i, m: self.vgw.make_file_with_content(
            root_id, u"file%s" % i, hash,
            1, 1, 1, key, mimetype=m)
        nodes = [mkfile(i, u'image/tif') for i in range(10)]
        # make a bunch of files with content and the wrong mimetype
        other_nodes = [mkfile(i, u'fake') for i in range(100, 110)]
        # an unknown mimetype will return nothing
        all_nodes = self.vgw.get_all_nodes(mimetypes=[u'mmm'])
        self.assertEqual(len(all_nodes), 0)
        # this will only return the ones with the matching mimetype
        all_nodes = self.vgw.get_all_nodes(mimetypes=[u'image/tif'])
        self.assertEqual(len(all_nodes), 10)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        self.assertEqual(all_nodes, nodes)
        # get both mimetypes
        nodes.extend(other_nodes)
        nodes.sort(key=attrgetter('path', 'name'))
        all_nodes = self.vgw.get_all_nodes(mimetypes=[u'image/tif', u'fake'])
        self.assertEqual(len(all_nodes), 20)
        self.assertEqual(all_nodes, nodes)

    def test_get_all_with_content(self):
        """Test get_all_nodes with mimetype filter."""
        hash = get_fake_hash()
        key = uuid.uuid4()
        root_id = self.vgw.get_root().id
        # make a bunch of files with content
        mkfile = lambda i, m: self.vgw.make_file_with_content(
            root_id, u"file%s.tif" % i, hash,
            1, 1, 1, key, mimetype=m)
        [mkfile(i, u'image/tif') for i in range(10)]
        all_nodes = self.vgw.get_all_nodes(kind='File', with_content=True)
        self.assertEqual(len(all_nodes), 10)
        for n in all_nodes:
            self.assertEqual(n.content.hash, hash)

    def test_get_all_with_content_and_mimetype(self):
        """Test get_all_nodes with mimetype filter."""
        hash = get_fake_hash()
        key = uuid.uuid4()
        root_id = self.vgw.get_root().id
        # make a bunch of files with content
        mkfile = lambda i, m: self.vgw.make_file_with_content(
            root_id, u"file%s.tif" % i, hash,
            1, 1, 1, key, mimetype=m)
        [mkfile(i, u'image/tif') for i in range(10)]
        all_nodes = self.vgw.get_all_nodes(with_content=True,
                                           mimetypes=[u'image/tif'])
        self.assertEqual(len(all_nodes), 10)
        for n in all_nodes:
            self.assertTrue(n.content.hash, hash)

    def test_get_all_nodes_with_max_generation(self):
        """Test get_all_nodes with max_generation."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        # with a max_generation in the middle
        nodes_gen_10 = self.vgw.get_all_nodes(max_generation=10)
        self.assertEqual(nodes_gen_10,
                         [n for n in nodes if n.generation <= 10])
        # with the last generation
        nodes_gen_20 = self.vgw.get_all_nodes(
            max_generation=nodes[-1].generation)
        self.assertEqual(nodes_gen_20, nodes)

    def test_get_all_nodes_with_limit(self):
        """Test get_all_nodes with a limit."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))

        # get the nodes by chunks of 10 top
        nodes_limit_10 = self.vgw.get_all_nodes(limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_with_max_generation_and_limit(self):
        """Test get_all_nodes with max_generation and limit."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))

        # with max_generation and limit
        # first get all the nodes at gen_10
        nodes_gen_10 = self.vgw.get_all_nodes(max_generation=10)
        # now get them in chunks
        nodes_limit_5 = self.vgw.get_all_nodes(max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:10])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10)

        # same but with the last generation
        nodes_limit_10 = self.vgw.get_all_nodes(max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_with_max_generation_and_limit_only_from_volume(
            self):
        """Test get_all_nodes wiht max_generation and limit for a UDF."""
        # make some files on a UDF...
        udf = model.UserVolume.create(
            self.shard_store, self.user.id, u"~/thepath/thename")
        udf_dao = dao.UserVolume(udf, self.user)
        udf_vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        udf_root_id = udf_vgw.get_root().id
        # make files on the root and make sure only they are returned
        dirs = []
        files = []
        for i in range(10):
            d = udf_vgw.make_subdirectory(udf_root_id, u"%sd" % i)
            f = udf_vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(udf_vgw.get_root())
        all_nodes = udf_vgw.get_all_nodes()
        self.assertEqual(len(all_nodes), 21)
        # the lists are not sorted so not equal
        self.assertNotEquals(all_nodes, nodes)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        self.assertEqual(all_nodes, nodes)

        # with a max_generation
        nodes_gen_10 = udf_vgw.get_all_nodes(max_generation=10)
        self.assertEqual(
            nodes_gen_10, [n for n in nodes if n.generation <= 10])
        nodes_gen_20 = udf_vgw.get_all_nodes(max_generation=20)
        self.assertEqual(nodes_gen_20, nodes)

        # with max_generation and limit
        nodes_limit_5 = udf_vgw.get_all_nodes(max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += udf_vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:10])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += udf_vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10)
        # same but with the last gen.
        nodes_limit_10 = udf_vgw.get_all_nodes(max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += udf_vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += udf_vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_chunked_with_changes(self):
        """Test chunked get_all_nodes with changes in the middle."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        nodes_gen_19 = self.vgw.get_all_nodes(max_generation=19)
        self.assertEqual(nodes_gen_19, nodes[:20])

        # same but with the last generation
        nodes_limit_5 = self.vgw.get_all_nodes(max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:5])

        # now make some changes,
        # create a new dir.
        new_dir = self.vgw.make_subdirectory(root_id, u"01d")

        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:10])

        # create a new file inside a existing dir.
        self.vgw.make_file(nodes[5].id, u"0_new_file.txt")
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:15])

        # and also make a move
        to_move = nodes[10:15][2]
        self.vgw.move_node(to_move.id, new_dir.id, to_move.name)
        last_node = nodes_limit_5[-1]
        # get the rest of the result.
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        self.assertEqual(nodes_limit_5, nodes_gen_19)

        # now move a node that should have been in the result,
        # but before getting to it.
        to_move = nodes_gen_19[-1]
        self.vgw.move_node(to_move.id, nodes_gen_19[0].id, to_move.name)
        last_node = nodes_gen_19[-5]
        result = self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        # check that it wasn't included in the result.
        self.assertEqual(result, nodes_gen_19[-4:-1])

    def test_get_all_nodes_chunked_with_move_middle(self):
        """Test chunked get_all_nodes with a move in the middle."""
        root = self.vgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = self.vgw.make_subdirectory(root_id, u"%sd" % i)
            f = self.vgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        nodes_gen_19 = self.vgw.get_all_nodes(max_generation=19)
        self.assertEqual(nodes_gen_19, nodes[:20])

        # same but with the last generation
        nodes_limit_5 = self.vgw.get_all_nodes(max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:10])

        # move a node that's already in the result
        to_move = nodes[10:15][2]
        # now move a node that it's in the result,
        self.vgw.move_node(to_move.id, nodes[0].id, to_move.name)
        last_node = nodes_limit_5[-1]
        # get the rest of the result.
        nodes_limit_5 += self.vgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        # remove the moved node from the nodes_gen_19 list
        del nodes_gen_19[12]
        self.assertEqual(nodes_limit_5, nodes_gen_19)

    def test_get_all_nodes_chunked_only_root(self):
        """Test chunked get_all_nodes with only one node, the root."""
        self.vgw.get_root()
        # sort them in the right order
        nodes = self.vgw.get_all_nodes()
        self.assertEqual(len(nodes), 1)

        # same but with the last generation
        nodes_limit_5 = self.vgw.get_all_nodes(
            start_from_path=(nodes[-1].path, nodes[-1].name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, [])

    def test_get_all_nodes_chunked_only_root_in_volume(self):
        """Test chunked get_all_nodes with only one node, the root."""
        udf = model.UserVolume.create(
            self.shard_store, self.user.id, u"~/thepath/thename")
        udf_dao = dao.UserVolume(udf, self.user)
        udf_vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        # sort them in the right order
        nodes = udf_vgw.get_all_nodes()
        self.assertEqual(len(nodes), 1)

        # same but with the last generation
        nodes_limit_5 = udf_vgw.get_all_nodes(
            start_from_path=(nodes[-1].path, nodes[-1].name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, [])

    def test_get_deleted_files(self):
        """Test get_deleted_files."""
        root_id = self.root.id
        # make some directories
        dir1 = self.vgw.make_subdirectory(root_id, u"dir2")
        dir2 = self.vgw.make_subdirectory(dir1.id, u"dir1")
        mkfile = lambda p, i: self.vgw.make_file(p, u"file%s.txt" % i)
        # make some files
        [mkfile(root_id, i) for i in range(10)]
        files1 = [mkfile(dir1.id, i) for i in range(10)]
        files2 = [mkfile(dir2.id, i) for i in range(10)]
        nodes = self.vgw.get_deleted_files()
        self.assertEqual(nodes, [])
        self.vgw.delete_node(dir1.id, cascade=True)
        nodes = self.vgw.get_deleted_files()
        # although there are 22 dead nodes with the directories,
        # only the files are deleted
        self.assertEqual(len(nodes), 20)
        dead_nodes = files1 + files2
        dead_nodes.sort(key=attrgetter('when_last_modified', 'path', 'name'))
        self.assertEqual(dead_nodes, nodes)
        # test start and limit
        nodes = self.vgw.get_deleted_files(limit=2)
        self.assertEqual(dead_nodes[:2], nodes)
        nodes = self.vgw.get_deleted_files(start=3, limit=5)
        self.assertEqual(dead_nodes[3:8], nodes)

    def setup_shares(self):
        """Setup some shares for moves from shares."""
        d = self.vgw.make_tree(self.root.id, u"/a/b/c/d")
        y = self.vgw.make_tree(self.root.id, u"/x/y")

        usera = self.create_user(id=2, username=u"sharee1")
        userb = self.create_user(id=3, username=u"sharee2")
        userc = self.create_user(id=4, username=u"sharee3")
        # make 4 shares
        sharea = self.vgw.make_share(d.id, u"sharea", user_id=usera.id)
        shareb = self.vgw.make_share(d.id, u"shareb", user_id=userb.id)
        sharec = self.vgw.make_share(d.id, u"sharec", user_id=userc.id)
        # userc will not accept the share in this test
        usera._gateway.accept_share(sharea.id)
        userb._gateway.accept_share(shareb.id)
        transaction.commit()
        return d, y, sharea, shareb, sharec

    def test_move_from_share_1(self):
        """Test _make_move_from_share method.

        Basic obvious move from a share.
        """
        d, y, sa, sb, sc = self.setup_shares()
        f1 = self.vgw.make_file(d.id, u"file.txt")
        self.vgw.move_node(f1.id, y.id, f1.name)
        mfs = self.shard_store.find(
            model.MoveFromShare,
            model.MoveFromShare.id == f1.id,
            model.MoveFromShare.status == model.STATUS_DEAD)
        # should have 2 MoveFromShare
        shares = [n.share_id for n in mfs]
        self.assertEqual(len(shares), 2)
        self.assertTrue(sa.id in shares)
        self.assertTrue(sb.id in shares)
        # non accepted shares
        self.assertTrue(sc.id not in shares)

    def test_move_from_share_2(self):
        """Test _make_move_from_share method.

        Move into a child of the node's folder.
        """
        d, y, sa, sb, sc = self.setup_shares()
        f1 = self.vgw.make_file(d.id, u"file.txt")
        np = self.vgw.make_tree(d.id, u'/q/w/e/r/t/y')
        self.assertTrue(np.full_path.startswith(f1.path))
        self.vgw.move_node(f1.id, np.id, f1.name)
        mfs = self.shard_store.find(model.MoveFromShare)
        # should have no MoveFromShare
        self.assertEqual(mfs.count(), 0)

    def test_move_from_share_3(self):
        """Test _make_move_from_share method.

        Move into a parent of the share.
        """
        d, y, sa, sb, sc = self.setup_shares()
        f1 = self.vgw.make_file(d.id, u"file.txt")
        np = self.vgw.get_node_by_path(u'/a/b/c/')
        self.vgw.move_node(f1.id, np.id, f1.name)
        mfs = self.shard_store.find(model.MoveFromShare)
        # should have two shares listed with the node
        shares = [n.share_id for n in mfs]
        self.assertEqual(len(shares), 2)
        self.assertTrue(sa.id in shares)
        self.assertTrue(sb.id in shares)
        # non accepted shares
        self.assertTrue(sc.id not in shares)

    def test_change_public_access_file(self):
        """Test the basics of changing public access to a file."""
        a_file = self.vgw.make_file(self.root.id, u"a-file.txt")
        # It has no public ID
        self.assertEqual(a_file.public_id, None)
        # It now has a public ID
        self.assertEqual(
            self.vgw.change_public_access(a_file.id, True).public_id, 1)
        # Disabled it, back to None
        self.assertEqual(
            self.vgw.change_public_access(a_file.id, False).public_id, None)

    def test_change_public_access_directory_nopermission(self):
        """Test that by default you can't make a directory public."""
        a_dir = self.vgw.make_tree(self.root.id, u"/x")
        self.assertRaises(errors.NoPermission,
                          self.vgw.change_public_access, a_dir.id, True)

    def test_change_public_access_directory(self):
        """Test that directories can be made public if explicitly requested."""
        a_dir = self.vgw.make_tree(self.root.id, u"/x")
        # It has no public ID
        self.assertEqual(a_dir.public_id, None)
        # It now has a public ID
        self.assertEqual(
            self.vgw.change_public_access(a_dir.id, True, True).public_id, 1)


#
# The following test test the volume gateway API for all types of volumes
#
class CommonReadWriteVolumeGatewayApiTest(StorageDALTestCase):
    """Test the common API ReadWriteVolumeGateway against each type of volume.

    For each type of volume, override setup_volume.
    This class tests the user's root volume
    """
    def setUp(self):
        super(CommonReadWriteVolumeGatewayApiTest, self).setUp()
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1",
                                max_storage_bytes=200)
        self.user = self.gw.get_user(user.id)
        self.user_quota = self.user._gateway.get_quota()
        self.shard_store = get_shard_store(self.user.shard_id)
        self.setup_volume()

    def setup_volume(self):
        """Setup the volume used for this test case."""
        # make a test file using storm
        self.owner = self.user
        self.owner_quota = self.owner.get_quota()
        self.vgw = self.user._gateway.get_root_gateway()
        self.root = self.shard_store.get(model.StorageObject,
                                         self.vgw.get_root().id)
        self.file = self.root.make_file(u"TheName")
        self.file._content_hash = model.EMPTY_CONTENT_HASH
        self.file.mimetype = u"fakemime"

    def tweak_users_quota(self, user_id, max_bytes, used_bytes=0):
        """Utility to toy with the user's quota."""
        user = self.gw.get_user(user_id)
        store = get_shard_store(user.shard_id)
        store.find(
            model.StorageUserInfo,
            model.StorageUserInfo.id == user_id
        ).set(max_storage_bytes=max_bytes, used_storage_bytes=used_bytes)
        store.commit()

    def test__get_quota(self):
        """Test _get_quota."""
        self.owner_quota.load()
        quota = self.vgw._get_quota()
        self.assertTrue(isinstance(quota, model.StorageUserInfo))
        self.assertEqual(self.owner_quota.id, quota.id)
        self.assertEqual(self.owner_quota.free_bytes, quota.free_bytes)

    def test_get_quota(self):
        """Test get_quota."""
        self.owner_quota.load()
        quota = self.vgw.get_quota()
        self.assertTrue(isinstance(quota, dao.UserInfo))
        self.assertEqual(self.owner_quota.id, quota.id)
        self.assertEqual(self.owner_quota.free_bytes, quota.free_bytes)

    def test_get_root(self):
        """Test get_root."""
        node = self.vgw.get_root()
        self.assertTrue(isinstance(node, dao.DirectoryNode))
        self.assertEqual(node.id, self.root.id)

    def test__get_root_node(self):
        """Test _get_root_node method."""
        node = self.vgw._get_root_node()
        self.assertEqual(node, self.root)

    def test__get_user_volume(self):
        """Test _get_user_volume method."""
        vol = self.vgw._get_user_volume()
        self.assertEqual(vol, self.root.volume)

    def test_get_user_volume(self):
        """Test get_user_volume method."""
        vol = self.vgw.get_user_volume()
        self.assertTrue(isinstance(vol, dao.UserVolume))
        self.assertEqual(vol.id, self.root.volume_id)

    def test__get_left_joins(self):
        """Some basic tests to make sure the method returns what is expected.
        """
        # create a file to test with
        tables, origin = self.vgw._get_left_joins(
            with_content=False, with_parent=False)
        self.assertEqual(tables, (model.StorageObject,))
        self.assertEqual(origin, [model.StorageObject])
        result = self.shard_store.using(*origin).find(
            tables, model.StorageObject.id == self.file.id).one()
        object, content, parent = self.vgw._split_result(result)
        self.assertTrue(isinstance(object, model.StorageObject))
        self.assertTrue(content is None)
        self.assertTrue(parent is None)

    def test__get_left_joins_with_content(self):
        tables, origin = self.vgw._get_left_joins(
            with_content=True, with_parent=False)
        self.assertEqual(tables, (model.StorageObject, model.ContentBlob))
        # make sure it doesn't blow up..other tests will happen later
        result = self.shard_store.using(*origin).find(
            tables, model.StorageObject.id == self.file.id).one()
        object, content, parent = self.vgw._split_result(result)
        self.assertTrue(isinstance(object, model.StorageObject))
        self.assertTrue(isinstance(content, model.ContentBlob))
        self.assertTrue(parent is None)

    def test__get_left_joins_with_parent(self):
        tables, origin = self.vgw._get_left_joins(
            with_content=False, with_parent=True)
        self.assertEqual(tables[0], model.StorageObject)
        # The second element in tables will be a ClassAlias, so we check
        # its base class here.
        self.assertEqual(tables[1].__base__, model.StorageObject)
        # make sure it doesn't blow up..other tests will happen later
        result = self.shard_store.using(*origin).find(
            tables, model.StorageObject.id == self.file.id).one()
        object, content, parent = self.vgw._split_result(result)
        self.assertTrue(isinstance(object, model.StorageObject))
        self.assertTrue(content is None)
        self.assertTrue(parent.id, object.parent_id)

    def test__get_kind_conditions(self):
        """Test _get_kind_conditions."""
        self.assertRaises(errors.StorageError,
                          self.vgw._get_kind_conditions, u"YYY")
        cond = self.vgw._get_kind_conditions(None)
        self.assertEqual(cond, [])
        cond = self.vgw._get_kind_conditions(u"File")
        self.assertEqual(cond, [model.StorageObject.kind == u"File"])
        cond = self.vgw._get_kind_conditions(u"Directory")
        self.assertEqual(cond, [model.StorageObject.kind == u"Directory"])

    def test__get_node_simple(self):
        """Test the _get_node_simple method."""
        node = self.vgw._get_node_simple(self.file.id)
        self.assertEqual(node, self.file)
        self.file.status = 'Dead'
        node = self.vgw._get_node_simple(self.file.id, live_only=True)
        self.assertEqual(node, None)
        node = self.vgw._get_node_simple(self.file.id, live_only=False)
        self.assertEqual(node, self.file)

    def test__get_node(self):
        """Test the _get_node method."""
        result = self.vgw._get_node(self.file.id)
        object, content, parent = self.vgw._split_result(result)
        self.assertEqual(object.id, self.file.id)
        self.assertEqual(content, None)
        self.assertEqual(parent, None)
        node = self.vgw._get_node_from_result(result)
        self.assertEqual(node.id, self.file.id)
        self.assertEqual(node.content, None)

    def test__get_node_with_content(self):
        result = self.vgw._get_node(self.file.id, with_content=True)
        object, content, parent = self.vgw._split_result(result)
        self.assertEqual(object.id, self.file.id)
        self.assertEqual(content.hash, self.file.content.hash)
        node = self.vgw._get_node_from_result(result)
        self.assertEqual(node.id, self.file.id)
        self.assertEqual(node.content.hash, self.file.content.hash)
        self.file.status = model.STATUS_DEAD
        result = self.vgw._get_node(self.file.id, with_content=True)
        object, content, parent = self.vgw._split_result(result)
        self.assertEqual(object, None)

    def test__get_node_with_parent(self):
        result = self.vgw._get_node(self.file.id, with_parent=True)
        object, content, parent = self.vgw._split_result(result)
        self.assertEqual(object.id, self.file.id)
        self.assertEqual(content, None)
        self.assertEqual(parent.id, self.file.parent_id)

    def test__get_children(self):
        """Test the _get_children method."""
        result = self.vgw._get_children(self.file.parent_id, with_content=True)
        object, content, parent = self.vgw._split_result(result[0])
        self.assertEqual(object.id, self.file.id)
        self.assertEqual(content.hash, self.file.content.hash)
        # test some conditions with none:
        # no Directories
        result = self.vgw._get_children(self.file.parent_id, kind='Directory')
        self.assertEqual(result.count(), 0)
        # Filter by mimetypes
        result = self.vgw._get_children(
            self.file.parent_id, mimetypes=[u'bogusmime'])
        self.assertEqual(result.count(), 0)
        result = self.vgw._get_children(
            self.file.parent_id, mimetypes=[u'fakemime'])
        self.assertEqual(result.count(), 1)
        # only dead children
        self.file.status = model.STATUS_DEAD
        result = self.vgw._get_children(self.file.parent_id)
        self.assertEqual(result.count(), 0)

    def test_check_has_children(self):
        """Test the check_has_children function."""
        a_dir = self.vgw.make_subdirectory(self.root.id, u"Directory")
        self.assertTrue(self.vgw.check_has_children(self.root.id, "File"))
        self.assertTrue(self.vgw.check_has_children(self.root.id, "Directory"))
        self.vgw.delete_node(self.file.id)
        self.assertFalse(self.vgw.check_has_children(self.root.id, "File"))
        self.assertTrue(self.vgw.check_has_children(self.root.id, "Directory"))
        self.vgw.delete_node(a_dir.id)
        self.assertFalse(self.vgw.check_has_children(self.root.id, "File"))
        self.assertFalse(
            self.vgw.check_has_children(self.root.id, "Directory"))

    def test_get_child_by_name(self):
        """Test the get_child_by_name method."""
        child = self.vgw.get_child_by_name(self.file.parent_id, self.file.name)
        self.assertEqual(child.id, self.file.id)
        # with content...
        child = self.vgw.get_child_by_name(
            self.file.parent_id, self.file.name, with_content=True)
        self.assertEqual(child.content.hash, self.file.content_hash)
        # invalid parent_id
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_child_by_name, uuid.uuid4(), self.file.name)
        # name not found
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_child_by_name, self.file.parent_id, u"fake")
        # dead file
        self.file.status = model.STATUS_DEAD
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_child_by_name, self.file.parent_id, self.file.name)

    def test_get_node(self):
        """Test get_node method."""
        # make sure it returns the root node when special 'root' is used:
        root_node = self.vgw.get_node('root')
        self.assertEqual(root_node.id, self.vgw.get_root().id)
        node = self.vgw.get_node(self.file.id, with_content=True)
        self.assertEqual(node.id, self.file.id)
        self.assertEqual(node.content.hash, self.file.content_hash)
        self.assertEqual(node.mimetype, self.file.mimetype)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, uuid.uuid4())
        self.assertRaises(errors.HashMismatch, self.vgw.get_node, self.file.id,
                          verify_hash="fake hash")

    def test_get_content(self):
        """Test get_content method."""
        content = self.vgw.get_content(self.file.content_hash)
        self.assertEqual(content.hash, self.file.content.hash)

    def test_make_tree(self):
        """Test make_tree method."""
        node = self.vgw.make_tree(self.root.id, u"/a/b/c")
        self.assertEqual(node.full_path, "/a/b/c")
        node = self.vgw.make_tree(self.root.id, u"/")
        self.assertEqual(node.id, self.root.id)

    def test_make_file(self):
        """Test make_file method."""
        node = self.vgw.make_file(self.root.id, u"the file name")
        self.assertTrue(isinstance(node, dao.FileNode))
        self.assertEqual(node.name, u"the file name")
        self.assertEqual(node.parent_id, self.root.id)
        self.assertEqual(node.volume_id, self.root.volume_id)
        self.assertNotEqual(node.when_created, None)
        self.assertNotEqual(node.when_last_modified, None)
        self.vgw.make_subdirectory(self.root.id, u"duplicatename")
        self.assertRaises(errors.AlreadyExists,
                          self.vgw.make_file, self.root.id, u"duplicatename")

    def test_make_file_with_magic(self):
        """Test make_file method."""
        cb = get_test_contentblob("FakeContent")
        cb.magic_hash = 'magic'
        get_shard_store(self.owner.shard_id).add(cb)
        # make enough room
        self.tweak_users_quota(self.owner.id, cb.deflated_size)
        node = self.vgw.make_file(self.root.id, u"the file name",
                                  hash=cb.hash, magic_hash=cb.magic_hash)
        self.assertEqual(node.content_hash, cb.hash)

    def test_make_file_with_magic_bad_hashes(self):
        """Test make_file method with a bad hash raises an exception."""
        # make a content blob with a magic hash
        cb = get_test_contentblob("FakeContent")
        cb.magic_hash = 'magic'
        get_shard_store(self.owner.shard_id).add(cb)
        self.assertRaises(errors.HashMismatch,
                          self.vgw.make_file, self.root.id, u"name.txt",
                          hash="wronghash")
        self.assertRaises(errors.HashMismatch,
                          self.vgw.make_file, self.root.id, u"name.txt",
                          hash=cb.hash, magic_hash='wrong')

    def test_make_subdirectory(self):
        """Test make_subdirectory method."""
        node = self.vgw.make_subdirectory(self.root.id, u"the file name")
        self.assertTrue(isinstance(node, dao.DirectoryNode))
        self.assertEqual(node.name, u"the file name")
        self.assertEqual(node.parent_id, self.root.id)
        self.assertEqual(node.volume_id, self.root.volume_id)
        self.assertNotEqual(node.when_created, None)
        self.assertNotEqual(node.when_last_modified, None)
        self.vgw.make_file(self.root.id, u"duplicatename")
        self.assertRaises(
            errors.AlreadyExists,
            self.vgw.make_subdirectory, self.root.id, u"duplicatename")

    def test_delete_file_node(self):
        """Test delete_node for files."""
        self.vgw.make_subdirectory(self.root.id, u"the dir name")
        a_file = self.vgw.make_file(self.root.id, u"the file name")
        self.vgw.delete_node(a_file.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, a_file.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.delete_node, a_file.id)

    def test_delete_root_node(self):
        # make sure we can't delete a root node
        self.assertRaises(errors.NoPermission,
                          self.vgw.delete_node, self.root.id)

    def test_delete_directory_node(self):
        """Test delete_node for directories."""
        # test without cascade
        a_dir = self.vgw.make_subdirectory(self.root.id, u"the dir name")
        # the owner of this directory is gonna share it :)
        vgw = ReadWriteVolumeGateway(self.owner)
        usera = self.create_user(id=2, username=u"sharee1", shard_id=u"shard0")
        sharea = vgw.make_share(a_dir.id, u"sharex", user_id=usera.id)
        usera._gateway.accept_share(sharea.id)
        a_file = self.vgw.make_file(a_dir.id, u"the file name")
        self.assertRaises(errors.NotEmpty, self.vgw.delete_node, a_dir.id)
        self.vgw.delete_node(a_file.id)
        self.vgw.delete_node(a_dir.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, a_dir.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.delete_node, a_dir.id)
        # make sure we can't delete a root node
        # test with cascade
        a_dir = self.vgw.make_subdirectory(self.root.id, u"the dir name")
        sharea = vgw.make_share(a_dir.id, u"sharex", user_id=usera.id)
        usera._gateway.accept_share(sharea.id)
        a_file = self.vgw.make_file(self.root.id, u"the file name")
        self.vgw.delete_node(a_dir.id, cascade=True)
        transaction.commit()

    def test_restore_node(self):
        """Test restore_node."""
        self.vgw.make_subdirectory(self.root.id, u"the dir name")
        a_file = self.vgw.make_file(self.root.id, u"file.txt")
        self.vgw.delete_node(a_file.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, a_file.id)
        self.vgw.restore_node(a_file.id)
        node = self.vgw.get_node(a_file.id)
        self.assertEqual(node.id, a_file.id)

    def test_restore_node_conflict(self):
        """Test restore_node when conflict with live file."""
        self.vgw.make_subdirectory(self.root.id, u"the dir name")
        a_file = self.vgw.make_file(self.root.id, u"file.txt")
        self.vgw.delete_node(a_file.id)
        self.vgw.make_file(self.root.id, u"file.txt")
        transaction.commit()
        self.vgw.restore_node(a_file.id)
        node = self.vgw.get_node(a_file.id)
        self.assertEqual(node.name, u"file~1.txt")

    def test_get_uploadjob(self):
        """Test get_uploadjob."""
        a_file = self.vgw.make_file(self.root.id, u"the file name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        # expected failures
        f = self.vgw.make_uploadjob
        self.assertRaises(
            errors.DoesNotExist, f,
            uuid.uuid4(), a_file.content_hash, new_hash, crc, 300, def_size)
        self.assertRaises(
            errors.QuotaExceeded, f,
            a_file.id, a_file.content_hash, new_hash, crc, 300, def_size)
        self.assertRaises(
            errors.HashMismatch, f,
            a_file.id, "WRONG OLD HASH", new_hash, crc, 300, def_size)
        upload_job = f(a_file.id, a_file.content_hash, new_hash, crc,
                       size, def_size)
        self.assertEqual(upload_job.storage_object_id, a_file.id)
        self.assertEqual(upload_job.hash_hint, new_hash)
        self.assertEqual(upload_job.crc32_hint, crc)
        self.assertEqual(upload_job.inflated_size_hint, size)
        self.assertEqual(upload_job.deflated_size_hint, def_size)
        self.assertEqual(upload_job.status, model.STATUS_LIVE)
        self.assertEqual(upload_job.content_exists, False)
        self.assertEqual(upload_job.file.id, a_file.id)
        upload = self.vgw.get_uploadjob(upload_job.id)
        self.assertEqual(upload.id, upload_job.id)
        # make sure delete works
        self.vgw.delete_uploadjob(upload_job.id)
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.get_uploadjob, upload_job.id)

    def test_make_uploadjob_enforces_quota(self):
        """Test make_uploadjob enforces quota check (or not)."""
        a_file = self.vgw.make_file(self.root.id, u"the file name")
        new_hash = get_fake_hash()
        crc = 12345
        size = def_size = 300
        f = self.vgw.make_uploadjob
        self.assertRaises(
            errors.QuotaExceeded, f, a_file.id, a_file.content_hash,
            new_hash, crc, size, def_size, enforce_quota=True)
        upload_job = f(a_file.id, a_file.content_hash, new_hash, crc,
                       size, def_size, enforce_quota=False)
        self.assertTrue(upload_job is not None)

    def test_get_user_uploadjobs(self):
        """Test get_user_uploadjobs."""
        jobs = list(self.vgw.get_user_uploadjobs())
        self.assertEqual(jobs, [])
        file1 = self.vgw.make_file(self.root.id, u"the file1 name")
        file2 = self.vgw.make_file(self.root.id, u"the file2 name")
        file3 = self.vgw.make_file(self.root.id, u"the file3 name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        self.vgw.make_uploadjob(
            file1.id, file1.content_hash, new_hash, crc, size, def_size)
        self.vgw.make_uploadjob(
            file1.id, file1.content_hash, new_hash, crc, size, def_size)
        self.vgw.make_uploadjob(
            file2.id, file2.content_hash, new_hash, crc, size, def_size)
        jobs = list(self.vgw.get_user_uploadjobs())
        self.assertEqual(len(jobs), 3)
        jobs = list(self.vgw.get_user_uploadjobs(node_id=file1.id))
        self.assertEqual(len(jobs), 2)
        jobs = list(self.vgw.get_user_uploadjobs(node_id=file3.id))
        self.assertEqual(jobs, [])

    def test_get_user_multipart_uploadjob(self):
        """Test get_user_multipart_uploadjob."""
        file1 = self.vgw.make_file(self.root.id, u"the file1 name")
        file2 = self.vgw.make_file(self.root.id, u"the file2 name")
        file3 = self.vgw.make_file(self.root.id, u"the file3 name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        upj1 = self.vgw.make_uploadjob(file1.id, file1.content_hash, new_hash,
                                       crc, size, def_size,
                                       multipart_id="foo_1",
                                       multipart_key=uuid.uuid4())
        upj2 = self.vgw.make_uploadjob(file1.id, file1.content_hash, new_hash,
                                       crc, size, def_size,
                                       multipart_id="foo_2",
                                       multipart_key=uuid.uuid4())
        upj3 = self.vgw.make_uploadjob(file2.id, file2.content_hash, new_hash,
                                       crc, size, def_size,
                                       multipart_id="foo_3",
                                       multipart_key=uuid.uuid4())
        job = self.vgw.get_user_multipart_uploadjob(file1.id,
                                                    upj1.multipart_key)
        self.assertEqual(job.id, upj1.id)
        # using extra arguments: hash, crc, size and deflated size
        job = self.vgw.get_user_multipart_uploadjob(
            file1.id, upj2.multipart_key, new_hash, crc, size, def_size)
        self.assertEqual(job.id, upj2.id)
        # with a file_id without any uploadjob
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.get_user_multipart_uploadjob,
                          file3.id, upj1.multipart_key)
        # with a file_id with an uploadjob, but wrong upload_id
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.get_user_multipart_uploadjob,
                          file1.id, upj3.multipart_key)
        # with a wrong content_hash
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.get_user_multipart_uploadjob,
                          file1.id, upj1.multipart_key,
                          hash_hint="sha1:foobar")

    def test_add_uploadjob_part(self):
        """Test add_uploadjob_part."""
        file1 = self.vgw.make_file(self.root.id, u"the file1 name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        job = self.vgw.make_uploadjob(
            file1.id, file1.content_hash, new_hash, crc, size, def_size,
            multipart_id="foo_1", multipart_key=uuid.uuid4())
        job = self.vgw.add_uploadjob_part(
            job.id, 10, 15, 1, "hash context", "magic hash context",
            "zlib context")
        self.assertEqual(job.uploaded_bytes, 10)
        self.assertEqual(job.inflated_size, 15)
        self.assertEqual(job.crc32, 1)
        self.assertEqual(job.chunk_count, 1)
        self.assertEqual(job.hash_context, "hash context")
        self.assertEqual(job.magic_hash_context, "magic hash context")
        self.assertEqual(job.decompress_context, "zlib context")
        job = self.vgw.add_uploadjob_part(
            job.id, 10, 30, 2, "more hash context", "more magic hash context",
            "more zlib context")
        self.assertEqual(job.uploaded_bytes, 20)
        self.assertEqual(job.inflated_size, 30)
        self.assertEqual(job.crc32, 2)
        self.assertEqual(job.chunk_count, 2)
        self.assertEqual(job.hash_context, "more hash context")
        self.assertEqual(job.magic_hash_context, "more magic hash context")
        self.assertEqual(job.decompress_context, "more zlib context")

    def test_set_uploadjob_multipart_id(self):
        """Test set_uploadjob_multpart_id."""
        file1 = self.vgw.make_file(self.root.id, u"the file1 name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        job = self.vgw.make_uploadjob(
            file1.id, file1.content_hash, new_hash, crc, size, def_size,
            multipart_key=uuid.uuid4())
        self.assertEqual(job.multipart_id, None)
        self.vgw.set_uploadjob_multpart_id(job.id, "foo_2")
        self.assertEqual(self.vgw.get_uploadjob(job.id).multipart_id, "foo_2")
        # check that we can set a value again and no exception is raised
        self.vgw.set_uploadjob_multpart_id(job.id, "foo_3")
        self.assertEqual(self.vgw.get_uploadjob(job.id).multipart_id, "foo_3")

    def test_set_uploadjob_when_last_active(self):
        """Test set_uploadjob_when_last_active."""
        file1 = self.vgw.make_file(self.root.id, u"the file1 name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        def_size = 10000
        job = self.vgw.make_uploadjob(
            file1.id, file1.content_hash, new_hash, crc, size, def_size,
            multipart_key=uuid.uuid4())
        new_last_active = datetime.datetime.utcnow() + datetime.timedelta(1)
        self.vgw.set_uploadjob_when_last_active(job.id, new_last_active)
        up_job = self.vgw.get_uploadjob(job.id)
        self.assertEqual(up_job.when_last_active, new_last_active)
        self.assertTrue(up_job.when_last_active > job.when_last_active)

    def test_make_content_simple(self):
        """Test make_content."""
        file_node = self.vgw.make_file(self.root.id, u"the file name")
        old_hash = file_node.content_hash
        file_id = file_node.id
        new_hash = get_fake_hash()
        new_storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        def_size = 10000
        magic_hash = 'magic_hash'

        # if we don't provide a storage_key, a content_blob is expected
        self.assertRaises(errors.ContentMissing, self.vgw.make_content,
                          file_id, old_hash, new_hash,
                          crc, size, def_size, None)

        # the content hash must match
        self.assertRaises(errors.HashMismatch, self.vgw.make_content,
                          file_id, "YYYY", new_hash, crc, size, def_size, None)

        # after the upload job was created/started, the users quota was reduced
        self.tweak_users_quota(self.owner.id, size - 1)
        self.assertRaises(errors.QuotaExceeded, self.vgw.make_content,
                          file_id, old_hash, new_hash,
                          crc, size, def_size, new_storage_key)

        # fix their quota...
        self.tweak_users_quota(self.owner.id, size + 1)
        self.vgw.make_content(file_id, old_hash, new_hash, crc, size, def_size,
                              new_storage_key, magic_hash=magic_hash)

        # reload the file and make sure it looks correct
        file_node._load(with_content=True)
        self.assertEqual(file_node.content_hash, new_hash)
        self.assertEqual(file_node.content.hash, new_hash)
        self.assertEqual(file_node.content.size, size)
        self.assertEqual(file_node.content.deflated_size, def_size)
        self.assertEqual(file_node.content.crc32, crc)

        # make user the user's quota was reduce
        self.owner_quota._load()
        self.assertEqual(self.owner_quota.free_bytes, 1)

        # test deleted node before commiting upload...
        self.vgw.delete_node(file_id)
        self.assertRaises(errors.DoesNotExist, self.vgw.make_content,
                          file_id, old_hash, new_hash,
                          crc, size, def_size, new_storage_key)

    def test_make_content_updates_contentblob(self):
        """Contentblob is updated if needed when making content."""
        filenode = self.vgw.make_file(self.root.id, u"the file name")
        new_hash = get_fake_hash()
        new_storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        def_size = 10000
        magic_hash = 'magic_hash'

        # call it without the magic hash, as before
        n = self.vgw.make_content(filenode.id, filenode.content_hash, new_hash,
                                  crc, size, def_size, new_storage_key)
        assert n.content.magic_hash is None

        # call it with the magic hash
        n = self.vgw.make_content(filenode.id, new_hash, new_hash, crc, size,
                                  def_size, new_storage_key, magic_hash)
        assert n.content.magic_hash == magic_hash

        # reload the file and make sure it was stored ok
        filenode._load(with_content=True)
        self.assertEqual(filenode.content.magic_hash, magic_hash)

    def test_make_content_enforeces_quota(self):
        """Test make_content enforces quota check (or not)."""
        filenode = self.vgw.make_file(self.root.id, u"the file name")
        new_hash = get_fake_hash()
        new_storage_key = uuid.uuid4()
        crc = 12345
        size = def_size = 100
        self.tweak_users_quota(self.owner.id, size - 1)
        self.assertRaises(errors.QuotaExceeded, self.vgw.make_content,
                          filenode.id, filenode.content_hash, new_hash,
                          crc, size, def_size, new_storage_key)

    def test_make_get_content(self):
        """Test the make and get content."""
        hash = get_fake_hash()
        key = uuid.uuid4()
        crc = 12345
        size = 100
        def_size = 10000
        magic_hash = 'magic_hash'
        content = self.vgw._make_content(hash, crc, size, def_size, key,
                                         magic_hash)
        content = self.vgw.get_content(content.hash)
        self.assertEqual(content.hash, hash)
        self.assertEqual(content.crc32, crc)
        self.assertEqual(content.size, size)
        self.assertEqual(content.deflated_size, def_size)
        self.assertEqual(content.status, model.STATUS_LIVE)
        self.assertEqual(content.storage_key, key)
        self.assertEqual(content.magic_hash, magic_hash)

        # make it Dead
        self.shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == hash).set(status=model.STATUS_DEAD)

        # dead content throws exception
        self.assertRaises(errors.DoesNotExist, self.vgw.get_content, hash)

        # IRL we should resurrect dead content? No we throw an exception!
        #    So we should NEVER set a content blob to Dead
        self.assertRaises(
            IntegrityError,
            self.vgw._make_content, hash, crc, size, def_size, key, magic_hash)

    def test_make_file_with_content_enforce_quota(self):
        """Make file with contentblob.

        This is similar to the way the updown server creates a file. But it's
        all handled in one function after the upload.
        """
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        self.assertRaises(
            errors.QuotaExceeded, self.vgw.make_file_with_content,
            self.root.id, name, hash, crc, 3000, 3000, storage_key)

    def test_make_file_with_content(self):
        """Make file with contentblob.

        This is similar to the way the updown server creates a file. But it's
        all handled in one function after the upload.
        """
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 1
        magic_hash = "magic_hash"
        node = self.vgw.make_file_with_content(
            self.root.id, name, hash, crc, size, deflated_size,
            storage_key, mimetype=u'image/tif', magic_hash=magic_hash)
        a_file = self.vgw.get_node(node.id, with_content=True)
        self.assertEqual(a_file.name, u"filename")
        self.assertEqual(a_file.mimetype, u'image/tif')
        self.assertEqual(a_file.status, model.STATUS_LIVE)
        self.assertEqual(a_file.content.hash, hash)
        self.assertEqual(a_file.content.crc32, crc)
        self.assertEqual(a_file.content.size, size)
        self.assertEqual(a_file.content.deflated_size, deflated_size)
        self.assertEqual(a_file.content.storage_key, storage_key)
        self.assertEqual(a_file.content.magic_hash, magic_hash)

    def test_make_file_with_content_public(self):
        """Make file with contentblob and make public."""
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        f = self.vgw.make_file_with_content
        node = f(self.root.id, name, hash, crc, size, deflated_size,
                 storage_key, is_public=True)
        a_file = self.vgw.get_node(node.id, with_content=True)
        self.assertNotEquals(a_file.public_url, None)

    def test_make_file_with_content_overwrite(self):
        """Make file with contentblob and overwite its existing content."""
        name = u"filename.tif"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        magic_hash = "magic_hash"
        f = self.vgw.make_file_with_content
        node1 = f(self.root.id, name, hash, crc, size, deflated_size,
                  storage_key, mimetype=u'image/tif')
        newhash = get_fake_hash("ZZZYYY")
        newstorage_key = uuid.uuid4()
        self.assertNotEqual(newhash, hash)
        self.assertNotEqual(newstorage_key, storage_key)
        f(self.root.id, name, newhash, crc, size,
          deflated_size, newstorage_key, magic_hash=magic_hash)
        node2 = self.vgw.get_node(node1.id, with_content=True)
        self.assertEqual(node2.id, node2.id)
        self.assertEqual(node2.content_hash, newhash)
        self.assertEqual(node2.content.storage_key, newstorage_key)
        self.assertEqual(node2.content.magic_hash, magic_hash)

        # test hashcheck when trying to overwrite with a wrong hash
        self.assertRaises(
            errors.HashMismatch, f, self.root.id, name, newhash, crc, size,
            deflated_size, newstorage_key, previous_hash=get_fake_hash('ABC'))

    def test_make_file_with_same_content_updates_contentblob(self):
        """Make file for the same content update the contentblob if needed."""
        name = u"filename.tif"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        magic_hash = "magic_hash"

        # create it without magic hash, as old times
        n = self.vgw.make_file_with_content(self.root.id, name, hash, crc,
                                            size, deflated_size, storage_key)
        assert n.content.magic_hash is None

        # create with same content, now with magic_hash
        n = self.vgw.make_file_with_content(self.root.id, name, hash, crc,
                                            size, deflated_size, storage_key,
                                            magic_hash=magic_hash)
        assert n.content.magic_hash == magic_hash

        # check that got stored properly
        n = self.vgw.get_node(n.id, with_content=True)
        self.assertEqual(n.content.magic_hash, magic_hash)

    def test_make_file_with_content_enforces_quota(self):
        """Make file with contentblob enforces quota check (or not)."""
        name = u"filename"
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = deflated_size = 10000
        f = self.vgw.make_file_with_content
        self.assertRaises(errors.QuotaExceeded, f, self.root.id, name, hash,
                          crc, size, deflated_size, storage_key,
                          enforce_quota=True)
        node = f(self.root.id, name, hash, crc, size, deflated_size,
                 storage_key, mimetype=u'image/tif', enforce_quota=False)
        self.assertTrue(node is not None)

    def test_move_node_nochange(self):
        """Test move_node method with a no changes."""
        root_id = self.vgw.get_root().id
        d1 = self.vgw.make_subdirectory(root_id, u"dir1")
        d2 = self.vgw.move_node(d1.id, d1.parent_id, d1.name)
        self.assertEqual(d1.generation, d2.generation)

    def test_move_node(self):
        """Test move_node method."""
        root_id = self.vgw.get_root().id
        dira1 = self.vgw.make_subdirectory(root_id, u"dira1")
        dira2 = self.vgw.make_subdirectory(dira1.id, u"dira2")
        dira3 = self.vgw.make_subdirectory(dira2.id, u"dira3")
        dira4 = self.vgw.make_subdirectory(dira3.id, u"dira4")
        dira5 = self.vgw.make_subdirectory(dira4.id, u"dira5")
        dira6 = self.vgw.make_subdirectory(dira5.id, u"dira6")
        dirb1 = self.vgw.make_subdirectory(root_id, u"dirb1")
        dirb2 = self.vgw.make_subdirectory(dirb1.id, u"dirb2")
        dirb3 = self.vgw.make_subdirectory(dirb2.id, u"dirb3")
        dirb4 = self.vgw.make_subdirectory(dirb3.id, u"dirb4")
        dirb5 = self.vgw.make_subdirectory(dirb4.id, u"dirb5")
        self.vgw.make_subdirectory(dirb5.id, u"dirb6")
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.move_node, uuid.uuid4(), dira2.id, u"name")
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.move_node, dira1.id, uuid.uuid4(), u"name")
        self.assertRaises(errors.NoPermission,
                          self.vgw.move_node, dira1.id, dira2.id, u"name")
        dira1 = self.vgw.move_node(dira1.id, dirb1.id, u"newname")
        self.assertEqual(dira1.parent_id, dirb1.id)
        self.assertEqual(dira1.name, u"newname")
        dira6 = self.vgw.get_node(dira6.id)
        path = pypath.join(self.vgw.get_root().full_path,
                           u"dirb1/newname/dira2/dira3/dira4/dira5")
        self.assertEqual(dira6.path, path)
        dirb2 = self.vgw.get_node(dirb2.id)
        # make sure moving with the same name deletes old node and descendants
        dirb4 = self.vgw.move_node(dirb4.id, dirb2.id, u"dirb3")
        transaction.commit()
        self.assertEqual(dirb4.parent_id, dirb2.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, dirb3.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, dirb4.id)
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, dirb5.id)

    def test_move_node_rename(self):
        """Test move_node method with a rename."""
        root_id = self.vgw.get_root().id
        d1 = self.vgw.make_subdirectory(root_id, u"dir1")
        f1 = self.vgw.make_file(root_id, u"file.txt")
        self.assertEqual(f1.mimetype, u"text/plain")
        f2 = self.vgw.move_node(f1.id, root_id, u"newname.mp3")
        self.assertEqual(f1.id, f2.id)
        self.assertEqual(f2.parent_id, root_id)
        self.assertEqual(f2.name, u"newname.mp3")
        self.assertEqual(f2.mimetype, u"audio/mpeg")
        # move into a subdirectory
        f3 = self.vgw.move_node(f2.id, d1.id, u"newname.txt")
        self.assertEqual(f3.id, f2.id)
        self.assertEqual(f3.parent_id, d1.id)
        self.assertEqual(f3.name, u"newname.txt")
        self.assertEqual(f3.mimetype, u"text/plain")

    def test_get_node_by_path(self):
        """Test get_node_by_path."""
        root_id = self.root.id
        dir1 = self.vgw.make_subdirectory(root_id, u"dir1")
        file1 = self.vgw.make_file(root_id, u"file1.txt")
        dir2 = self.vgw.make_subdirectory(dir1.id, u"dir2")
        file2 = self.vgw.make_file(dir1.id, u"file2.mp3")
        dir3 = self.vgw.make_subdirectory(dir2.id, u"dir3")
        file3 = self.vgw.make_file(dir2.id, u"file3.mp3")
        # make sure content and mimetype still work correctly
        hash = get_fake_hash()
        key = uuid.uuid4()
        self.vgw.make_file_with_content(
            dir3.id, u"file4.tif", hash, 123, 100, 1000, key,
            mimetype=u'image/tif')
        # filename with a space in it
        file5 = self.vgw.make_file(root_id, u"space! ")
        transaction.commit()
        # the basic use cases
        r = self.vgw.get_node_by_path(u'/')
        self.assertEqual(r.id, root_id)
        d1 = self.vgw.get_node_by_path(u'/dir1')
        self.assertEqual(d1.id, dir1.id)
        f1 = self.vgw.get_node_by_path(u'/file1.txt')
        self.assertEqual(f1.id, file1.id)
        d2 = self.vgw.get_node_by_path(u'/dir1/dir2')
        self.assertEqual(d2.id, dir2.id)
        f2 = self.vgw.get_node_by_path(u'/dir1/file2.mp3')
        self.assertEqual(f2.id, file2.id)
        d3 = self.vgw.get_node_by_path(u'/dir1/dir2/dir3', kind='Directory')
        self.assertEqual(d3.id, dir3.id)
        f3 = self.vgw.get_node_by_path(u'/dir1/dir2/file3.mp3')
        self.assertEqual(f3.id, file3.id)
        f5 = self.vgw.get_node_by_path(u'/space! ')
        self.assertEqual(f5.id, file5.id)
        # object accessable without inital / path
        d1 = self.vgw.get_node_by_path(u'dir1')
        self.assertEqual(d1.id, dir1.id)
        f1 = self.vgw.get_node_by_path(u'file1.txt', kind='File')
        self.assertEqual(f1.id, file1.id)
        d2 = self.vgw.get_node_by_path(u'dir1/dir2')
        self.assertEqual(d2.id, dir2.id)
        d3 = self.vgw.get_node_by_path(u'dir1/dir2/dir3')
        self.assertEqual(d3.id, dir3.id)
        # the standard node finder options work as well
        f4 = self.vgw.get_node_by_path(u'/dir1/dir2/dir3/file4.tif',
                                       with_content=True)
        self.assertEqual(f4.content.storage_key, key)
        # invalid paths get StorageErrors
        self.assertRaises(errors.StorageError, self.vgw.get_node_by_path, u'')
        self.assertRaises(errors.StorageError, self.vgw.get_node_by_path, u' ')
        # test DoesNotExistCondtions
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_node_by_path, u'/X..Y/')
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_node_by_path, u'/dir1', kind='File')
        self.assertRaises(
            errors.DoesNotExist,
            self.vgw.get_node_by_path, u'/file1.txt', kind='Directory')

    def test_get_generation_delta(self):
        """Test get generation delta limiter."""
        root_id = self.root.id
        # get the generation before we add all the nodes
        last_gen = self.vgw._get_user_volume().generation
        dir1 = self.vgw.make_subdirectory(root_id, u"dir1")
        file1 = self.vgw.make_file(root_id, u"file1.txt")
        dir2 = self.vgw.make_subdirectory(dir1.id, u"dir2")
        file2 = self.vgw.make_file(dir1.id, u"file2.mp3")
        dir3 = self.vgw.make_subdirectory(dir2.id, u"dir3")
        file3 = self.vgw.make_file(dir2.id, u"file3.mp3")
        self.vgw.delete_node(dir3.id)
        self.vgw.delete_node(file3.id)
        delta = list(self.vgw.get_generation_delta(last_gen))
        self.assertTrue(dir1 in delta)
        self.assertTrue(dir2 in delta)
        self.assertTrue(dir3 in delta)
        self.assertTrue(file1 in delta)
        self.assertTrue(file2 in delta)
        self.assertTrue(file3 in delta)

    def test_get_generation_delta_limit(self):
        """Test get generation delta."""
        root_id = self.root.id
        # get the generation before we add all the nodes
        last_gen = self.vgw._get_user_volume().generation
        dir1 = self.vgw.make_subdirectory(root_id, u"dir1")
        file1 = self.vgw.make_file(root_id, u"file1.txt")
        dir2 = self.vgw.make_subdirectory(dir1.id, u"dir2")
        file2 = self.vgw.make_file(dir1.id, u"file2.mp3")
        dir3 = self.vgw.make_subdirectory(dir2.id, u"dir3")
        file3 = self.vgw.make_file(dir2.id, u"file3.mp3")
        self.vgw.delete_node(dir3.id)
        self.vgw.delete_node(file3.id)
        # get the first set (just one)
        delta = list(self.vgw.get_generation_delta(last_gen, 1))
        self.assertEqual(len(delta), 1)
        self.assertTrue(dir1 in delta)
        # get the next set, should (the next 3)
        last_gen = delta[-1].generation
        delta = list(self.vgw.get_generation_delta(last_gen, 3))
        self.assertEqual(len(delta), 3)
        self.assertTrue(file1 in delta)
        self.assertTrue(dir2 in delta)
        self.assertTrue(file2 in delta)
        last_gen = delta[-1].generation
        delta = list(self.vgw.get_generation_delta(last_gen, 100))
        self.assertEqual(len(delta), 2)
        self.assertTrue(dir3 in delta)
        self.assertTrue(file3 in delta)

    def test_get_directories_with_mimetypes(self):
        """Test get_directories_with_mimetype."""
        root_id = self.root.id
        hash = get_fake_hash()
        key = uuid.uuid4()
        # only files with content are returned.
        make_file = lambda id, name: self.vgw.make_file_with_content(
            id, name, hash, 123, 1, 1, key)
        a = self.vgw.make_subdirectory(root_id, u"a")
        ab = self.vgw.make_subdirectory(a.id, u"ab")
        abc = self.vgw.make_subdirectory(ab.id, u"abc")
        make_file(root_id, u"filename1.jpg")
        make_file(a.id, u"filename1.txt")
        make_file(a.id, u"filename1.txt")
        make_file(ab.id, u"filename1.txt")
        make_file(ab.id, u"filename1.txt")
        make_file(ab.id, u"filename1.jpg")
        # these files will be empty and will never be included
        self.vgw.make_file(abc.id, u"filename1.txt")
        self.vgw.make_file(abc.id, u"filename1.txt")
        self.vgw.make_file(abc.id, u"filename1.jpg")
        # get all the mimetypes we have.
        mtypes = [u'text/plain', u'image/jpeg', u'junk']
        dirs = self.vgw.get_directories_with_mimetypes(mtypes)
        self.assertEqual(len(dirs), 3)
        self.assertTrue('/' in [d.full_path for d in dirs])
        self.assertTrue('/a' in [d.full_path for d in dirs])
        self.assertTrue('/a/ab' in [d.full_path for d in dirs])
        # just the text files
        dirs = self.vgw.get_directories_with_mimetypes([u'text/plain'])
        self.assertEqual(len(dirs), 2)
        self.assertTrue('/a' in [d.full_path for d in dirs])
        self.assertTrue('/a/ab' in [d.full_path for d in dirs])
        # just the images
        dirs = self.vgw.get_directories_with_mimetypes(
            [u'image/jpeg', u'junk'])
        self.assertEqual(len(dirs), 2)
        self.assertTrue('/' in [d.full_path for d in dirs])
        self.assertTrue('/a/ab' in [d.full_path for d in dirs])


class UDFReadWriteVolumeGatewayApiTest(CommonReadWriteVolumeGatewayApiTest):
    """Test the ReadWriteVolumeGateway API against for a UDF volume.

    For each type of volume, override setup_volume. This class tests the user's
    root volume
    """
    def setup_volume(self):
        """Setup the volume used for this test case."""
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1",
                                max_storage_bytes=200)
        self.user = self.gw.get_user(user.id, session_id="QWERTY")
        self.user_quota = self.user._gateway.get_quota()
        self.owner = self.user
        self.owner_quota = self.user_quota
        self.shard_store = get_shard_store(self.user.shard_id)
        # make a test file using storm
        udf = model.UserVolume.create(
            self.shard_store, self.user.id, u"~/thepath/thename")
        udf_dao = dao.UserVolume(udf, self.user)
        self.vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        self.root = self.shard_store.get(model.StorageObject,
                                         self.vgw.get_root().id)
        self.file = self.root.make_file(u"TheName")
        self.file._content_hash = model.EMPTY_CONTENT_HASH
        self.file.mimetype = u"fakemime"


class ShareReadWriteVolumeGatewayApiTest(CommonReadWriteVolumeGatewayApiTest):
    """Test the ReadWriteVolumeGateway API against for a UDF volume.

    For each type of volume, override setup_volume. This class tests the user's
    root volume
    """
    def setup_volume(self):
        """Setup the volume used for this test case."""
        # create a user on a different shard and share a folder with self.user
        # shard_store in this test will be the shard the sharer's data is on
        sharer = self.create_user(
            id=2, username=u"sharer", shard_id=u"shard1",
            max_storage_bytes=200)
        self.owner = sharer
        self.owner_quota = sharer._gateway.get_quota()
        self.shard_store = get_shard_store(sharer.shard_id)
        root = model.StorageObject.get_root(self.shard_store, sharer.id)
        rw_node = root.make_subdirectory(u"WriteMe")
        transaction.commit()
        # share a node with the user with modify access
        rw_share = model.Share(
            sharer.id, rw_node.id, self.user.id, u"WriteShare", "Modify")
        self.user_store.add(rw_share)
        rw_share.accept()
        # self.user_store.add(self.wrong_share)
        transaction.commit()
        share_dao = dao.SharedDirectory(rw_share, by_user=sharer)
        self.vgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.root = self.shard_store.get(model.StorageObject,
                                         self.vgw.get_root().id)
        self.file = self.root.make_file(u"TheName")
        self.file._content_hash = model.EMPTY_CONTENT_HASH
        self.file.mimetype = u"fakemime"

#
# Test the features and failures that make the volumes unique
#


class RootReadWriteVolumeGatewayTestCase(StorageDALTestCase):
    """Test the features unique for a ReadWriteVolumeGateway of a default root.

    The root volume is the mother of all user owned objects,
    even nodes that may exist in a udf volume.
    """

    def setUp(self):
        super(RootReadWriteVolumeGatewayTestCase, self).setUp()
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1")
        self.user = self.gw.get_user(user.id, session_id="QWERTY")
        self.shard_store = get_shard_store(self.user.shard_id)
        # make a test file
        vgw = self.user._gateway.get_root_gateway()
        root = self.shard_store.get(model.StorageObject, vgw.get_root().id)
        self.file = root.make_file(u"TheName")
        self.file._content_hash = model.EMPTY_CONTENT_HASH
        self.file.mimetype = u"fakemime"

    def test_root_volume(self):
        """Test the Root Volume."""
        vgw = ReadWriteVolumeGateway(self.user)
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner, self.user)
        self.assertEqual(vgw.root_id, None)
        self.assertEqual(vgw.read_only, False)
        self.assertEqual(vgw.udf, None)
        self.assertEqual(vgw.share, None)
        # make sure we have the correct root
        node = model.StorageObject.get_root(self.shard_store, self.user.id)
        root = vgw.get_root()
        self.assertEqual(root.id, node.id)
        # this technically tests the DirectoryNode DAO properties
        self.assertEqual(root.owner, self.user)
        self.assertEqual(root.can_read, True)
        self.assertEqual(root.can_write, True)
        self.assertEqual(root.can_delete, False)
        # test using the helper method on a user gateway
        # only some basic tests are needed
        vgw = self.user._gateway.get_root_gateway()
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner, self.user)
        self.assertEqual(vgw.root_id, None)
        self.assertEqual(vgw.session_id, "QWERTY")
        # error conditions
        # the only error for a root is if the root doesn't exist or is Dead
        node.status = model.STATUS_DEAD
        self.assertRaises(errors.DoesNotExist, vgw.get_root)

    def test_make_share(self):
        """Test make_share."""
        self.create_user(id=2, username=u"sharer")
        vgw = ReadWriteVolumeGateway(self.user)
        a_dir = vgw.make_subdirectory(vgw.get_root().id, u"the dir")
        a_file = vgw.make_file(vgw.get_root().id, u"the file")
        # test some obvious error conditions
        # can't share files
        self.assertRaises(
            errors.NotADirectory, vgw.make_share, a_file.id, u"hi", user_id=2)
        # user doesn't exist
        self.assertRaises(
            errors.DoesNotExist, vgw.make_share, a_dir.id, u"hi", user_id=3)
        share = vgw.make_share(a_dir.id, u"hi", user_id=2)
        self.assertEqual(share.root_id, a_dir.id)
        self.assertEqual(share.accepted, False)
        self.assertEqual(share.shared_to_id, 2)
        self.assertNotEqual(share.when_shared, None)
        self.assertNotEqual(share.when_last_changed, None)

    def test_make_share_offer(self):
        """Test make_share."""
        vgw = ReadWriteVolumeGateway(self.user)
        a_dir = vgw.make_subdirectory(vgw.get_root().id, u"the dir")
        vgw.make_file(vgw.get_root().id, u"the file")
        transaction.commit()
        # test some obvious error conditions
        share = vgw.make_share(a_dir.id, u"hi", user_id=None,
                               email=u"fake@email.com")
        self.assertEqual(share.root_id, a_dir.id)
        self.assertEqual(share.accepted, False)
        self.assertEqual(share.shared_to_id, None)
        self.assertEqual(share.offered_to_email, u"fake@email.com")

    def test_undelete_volume(self):
        """Test Undelete Volume."""
        vgw = ReadWriteVolumeGateway(self.user)
        d2 = vgw.undelete_volume(u'recovered')
        d = vgw.make_subdirectory(vgw.get_root().id, u"the dir")
        vgw.delete_node(d.id)
        d2 = vgw.undelete_volume(u'recovered')
        vgw.delete_node(d2.id, cascade=True)
        vgw.undelete_volume(u'recovered')
        transaction.commit()


class UDFReadWriteVolumeGatewayTestCase(StorageDALTestCase):
    """Test the features unique for a ReadWriteVolumeGateway of a udf root."""

    def setUp(self):
        super(UDFReadWriteVolumeGatewayTestCase, self).setUp()
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1")
        self.user = self.gw.get_user(user.id, session_id="QWERTY")
        self.shard_store = get_shard_store(self.user.shard_id)
        # make a test file using storm
        self.udf = model.UserVolume.create(
            self.shard_store, self.user.id, u"~/thepath/thename")
        udf_dao = dao.UserVolume(self.udf, self.user)
        self.vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        self.root = self.shard_store.get(model.StorageObject,
                                         self.vgw.get_root().id)

    def test_udf_volume(self):
        """Test a ReadWriteVolumeGateway for a UDF."""
        udf = self.udf
        udf_dao = dao.UserVolume(udf, self.user)
        vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner, self.user)
        self.assertEqual(vgw.root_id, udf.root_id)
        self.assertEqual(vgw.read_only, False)
        self.assertEqual(vgw.udf, udf_dao)
        self.assertEqual(vgw.share, None)
        node = self.shard_store.get(model.StorageObject, udf.root_id)
        root = vgw.get_root()
        self.assertEqual(root.id, node.id)
        self.assertEqual(root.volume_id, udf.id)
        self.assertNotEqual(root.volume_id, model.ROOT_VOLUME)
        self.assertEqual(root.owner, self.user)
        self.assertEqual(root.can_read, True)
        self.assertEqual(root.can_write, True)
        self.assertEqual(root.can_delete, False)
        # test using the helper method on a user gateway
        # only some basic tests are needed
        vgw = self.user._gateway.get_udf_gateway(udf.id)
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner, self.user)
        self.assertEqual(vgw.root_id, udf.root_id)
        # if the status is dead, the root is not acceptable
        udf.status = model.STATUS_DEAD
        udf_dao = dao.UserVolume(udf, self.user)
        self.assertRaises(errors.NoPermission,
                          ReadWriteVolumeGateway, self.user, udf=udf_dao)
        udf.status = model.STATUS_LIVE
        # the owners must match
        udf_dao.owner_id = 1000
        self.assertRaises(errors.NoPermission,
                          ReadWriteVolumeGateway, self.user, udf=udf_dao)
        udf_dao.owner_id = self.user.id
        # the node for the UDF must be live
        node.status = model.STATUS_DEAD
        self.assertRaises(errors.DoesNotExist, vgw.get_root)

    def test_make_share(self):
        """Test make_share."""
        self.create_user(id=2, username=u"sharer")
        a_dir = self.vgw.make_subdirectory(self.root.id, u"the dir")
        share = self.vgw.make_share(a_dir.id, u"hi", user_id=2)
        self.assertEqual(share.root_id, a_dir.id)
        self.assertEqual(share.accepted, False)
        self.assertEqual(share.shared_to_id, 2)

    def test_share_from_dead_udf(self):
        """Test make_share from a udf then set it to Dead."""
        user2 = self.create_user(id=2, username=u"sharer")
        a_dir = self.vgw.make_subdirectory(self.root.id, u"the dir")
        share = self.vgw.make_share(a_dir.id, u"hi", user_id=2, readonly=False)
        share = StorageUserGateway(user2).accept_share(share.id)
        vgw = ReadWriteVolumeGateway(user2, share=share)
        # after the share is deleted, it should be inaccessible
        StorageUserGateway(self.user).delete_share(share.id)
        self.assertRaises(errors.DoesNotExist,
                          vgw.make_file, a_dir.id, u"file.txt")

    def test_make_share_offer(self):
        """Test make_share."""
        a_dir = self.vgw.make_subdirectory(self.root.id, u"the dir")
        transaction.commit()
        share = self.vgw.make_share(a_dir.id, u"hi", user_id=None,
                                    email=u"fake@email.com")
        self.assertEqual(share.root_id, a_dir.id)
        self.assertEqual(share.accepted, False)
        self.assertEqual(share.shared_to_id, None)
        self.assertEqual(share.offered_to_email, u"fake@email.com")

    def test_root_access_fail(self):
        """Unlike a root volume, a UDF Volume can't access nodes on a root"""
        gw = ReadWriteVolumeGateway(self.user)
        a_dir = gw.make_subdirectory(gw.get_root().id, u"dirname")
        a_file = gw.make_file(gw.get_root().id, u"FileName")
        self.assertRaises(errors.DoesNotExist, self.vgw.get_node, a_dir.id)
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.make_file, a_dir.id, u"filename")
        self.assertRaises(errors.DoesNotExist,
                          self.vgw.make_subdirectory, a_dir.id, u"filename")
        self.assertRaises(errors.DoesNotExist, self.vgw.make_uploadjob,
                          a_file.id, a_file.content_hash, get_fake_hash(),
                          1, 1, 1)

    def test_undelete_volume(self):
        """Test Undelete Volume from a UDF."""
        udf = self.udf
        udf_dao = dao.UserVolume(udf, self.user)
        vgw = ReadWriteVolumeGateway(self.user, udf=udf_dao)
        d = vgw.make_file(udf.root_id, u"thefile.txt")
        vgw.delete_node(d.id)
        vgw.undelete_volume(u'recovered')
        transaction.commit()
        rgw = ReadWriteVolumeGateway(self.user)
        node = rgw.get_node(d.id)
        self.assertEqual(d.volume_id, udf.id)
        self.assertEqual(node.volume_id, udf.id)
        self.assertEqual(node.full_path, u"/recovered/thefile.txt")


class ShareGatewayTestCase(StorageDALTestCase):
    """Test the ReadWriteVolumeGateway for shares.

    This test should test all unique features of share volumes as well as
    failures expected on share volumes
    """

    def setUp(self):
        super(ShareGatewayTestCase, self).setUp()
        self.gw = SystemGateway()
        user = self.create_user(username=u"testuser", shard_id=u"shard1")
        self.user = self.gw.get_user(user.id, session_id="QWERTY")
        self.shard_store = get_shard_store(self.user.shard_id)
        self.sharer = self.create_user(id=2, username=u"sharer",
                                       shard_id=u"shard1")
        self.othersharee = self.create_user(id=3, username=u"sharee")
        store = get_shard_store(self.sharer.shard_id)
        root = model.StorageObject.get_root(store, self.sharer.id)
        self.r_node = root.make_subdirectory(u"NoWrite")
        self.file = self.r_node.make_file(u"A File for uploads")
        self.rw_node = root.make_subdirectory(u"WriteMe")
        transaction.commit()
        self.r_share = model.Share(self.sharer.id, self.r_node.id,
                                   self.user.id, u"NoWriteShare", "View")
        self.rw_share = model.Share(self.sharer.id, self.rw_node.id,
                                    self.user.id, u"WriteShare", "Modify")
        self.user_store.add(self.r_share)
        self.user_store.add(self.rw_share)
        self.r_share.accept()
        self.rw_share.accept()
        transaction.commit()

    def test_share_gateway(self):
        """Test basic properties of a share ReadWriteVolumeGateway."""
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        vgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner, self.sharer)
        self.assertEqual(vgw.root_id, self.r_share.subtree)
        self.assertEqual(vgw.udf, None)
        self.assertEqual(vgw.share, share_dao)
        self.assertEqual(vgw.get_root().id, self.r_node.id)
        self.assertEqual(vgw.get_root().owner, self.sharer)
        self.assertEqual(vgw.get_root().can_read, True)
        self.assertEqual(vgw.get_root().can_write, False)
        self.assertEqual(vgw.get_root().can_delete, False)
        root = vgw._get_root_node()
        self.assertEqual(vgw.root_path_mask, root.full_path)

    def test_user_gateway_get_shared_to(self):
        """Test UserGateway get_shared_to methods."""
        shares = self.user._gateway.get_shared_to()
        shares = list(shares)
        self.assertEqual(len(shares), 2)
        # this user only has shares shared to him
        self.assertEqual(shares[0].shared_by.id, self.sharer.id)
        self.assertEqual(shares[1].shared_by.id, self.sharer.id)
        shares = self.sharer._gateway.get_shared_to()
        shares = list(shares)
        self.assertEqual(shares, [])

    def test_user_gateway_get_shared_by(self):
        """Test UserGateway get_shared_by methods."""
        shares = self.sharer._gateway.get_shared_by()
        shares = list(shares)
        self.assertEqual(len(shares), 2)
        # this user only has shares shared to him
        self.assertEqual(shares[0].shared_to.id, self.user.id)
        self.assertEqual(shares[1].shared_to.id, self.user.id)
        shares = self.sharer._gateway.get_shared_by(node_id=self.rw_node.id)
        shares = list(shares)
        self.assertEqual(len(shares), 1)
        self.assertEqual(shares[0].shared_to.id, self.user.id)
        self.assertEqual(shares[0].root_id, self.rw_node.id)
        shares = self.user._gateway.get_shared_by()
        shares = list(shares)
        self.assertEqual(shares, [])

    def test_user_gateway_get_share(self):
        """Test UserGateway get_share methods."""
        wrong_share = model.Share(self.sharer.id, self.rw_node.id,
                                  self.othersharee.id, u"WriteShare", "View")
        # test with a share offer
        shareoffer = model.Share(self.sharer.id, self.r_node.id, None,
                                 u"offer", "View", email=u"fake@example.com")
        self.user_store.add(shareoffer)
        so = self.sharer._gateway.get_share(shareoffer.id, accepted_only=False)
        self.assertEqual(so.id, shareoffer.id)
        self.assertEqual(so.shared_to, None)
        self.assertEqual(so.shared_by.id, self.sharer.id)
        self.assertEqual(so.offered_to_email, shareoffer.email)
        rw_share = self.user._gateway.get_share(self.rw_share.id)
        self.assertEqual(rw_share.id, self.rw_share.id)
        self.assertEqual(rw_share.shared_to.id, self.user.id)
        self.assertEqual(rw_share.shared_by.id, self.sharer.id)
        r_share = self.user._gateway.get_share(self.r_share.id)
        self.assertEqual(r_share.id, self.r_share.id)
        self.assertEqual(r_share.shared_to.id, self.user.id)
        self.assertEqual(r_share.shared_by.id, self.sharer.id)
        self.assertRaises(errors.DoesNotExist,
                          self.user._gateway.get_share, wrong_share.id)

    def test_modify_share_permissions(self):
        """Test permissions for a writable share ReadWriteVolumeGateway."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        vgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.assertEqual(vgw.get_root().can_read, True)
        self.assertEqual(vgw.get_root().can_write, True)
        # still can't delete the root!
        self.assertEqual(vgw.get_root().can_delete, False)

    def test_get_share_gateway(self):
        """Test the get_share_gateway method of a StorageUserGateway."""
        vgw = self.user._gateway.get_share_gateway(self.r_share.id)
        self.assertEqual(vgw.user, self.user)
        self.assertEqual(vgw.owner.id, self.sharer.id)
        self.assertEqual(vgw.root_id, self.r_share.subtree)

    def test_wrong_share(self):
        """Test when share is for another user."""
        wrong_share = model.Share(self.sharer.id, self.rw_node.id,
                                  self.othersharee.id, u"WriteShare", "View")
        share_dao = dao.SharedDirectory(wrong_share, by_user=self.sharer)
        self.assertNotEqual(share_dao.shared_to_id, self.user.id)
        self.assertRaises(errors.NoPermission,
                          ReadWriteVolumeGateway, self.user, share=share_dao)

    def test_share_not_accepted(self):
        """Shares that exist but are not accepted don't work."""
        self.r_share.accepted = False
        transaction.commit()
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        self.assertRaises(errors.NoPermission,
                          ReadWriteVolumeGateway, self.user, share=share_dao)

    def test_dead_share(self):
        """Dead shares share no files."""
        self.r_share.status = model.STATUS_DEAD
        transaction.commit()
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        self.assertRaises(errors.NoPermission, ReadWriteVolumeGateway,
                          self.user, share=share_dao)

    def test__check_share_inactive(self):
        """Shares from inactive users don't work."""
        self.sharer.update(subscription=False)
        transaction.commit()
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        vgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.assertRaises(errors.DoesNotExist, vgw._check_share)

    def test_readonly_share_fail(self):
        """Test make sure updates can't happen on a read only share"""
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        vgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root_id = vgw.get_root().id
        self.assertRaises(errors.NoPermission,
                          vgw.make_file, root_id, u"name")
        self.assertRaises(errors.NoPermission,
                          vgw.make_subdirectory, root_id, u"name")
        a_file = self.r_node.make_file(u"filename")
        dira = self.r_node.make_subdirectory(u"dirname")
        self.assertRaises(errors.NoPermission,
                          vgw.delete_node, dira.id)
        self.assertRaises(errors.NoPermission,
                          vgw.restore_node, dira.id)
        self.assertRaises(errors.NoPermission,
                          vgw.move_node, a_file.id, root_id, u"new name")

    def test_node_get_update_decendant(self):
        """Test to make sure that nodes on shares can be created and accessed
        based on the path checking"""
        # make some children of the shared folder
        vgw = self.sharer._gateway.get_root_gateway()
        # create a nested tree
        a_dir = vgw.make_subdirectory(self.rw_node.id, u"s1")
        a_dir = vgw.make_subdirectory(a_dir.id, u"s1")
        a_dir = vgw.make_subdirectory(a_dir.id, u"s1")
        a_dir = vgw.make_subdirectory(a_dir.id, u"s1")
        # only so we can show what we're doing, not checking
        self.assertEqual(a_dir.path, u"/WriteMe/s1/s1/s1")
        # now go add files and directories via the share
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        d1 = sgw.make_subdirectory(a_dir.id, u"hi")
        self.assertEqual(d1.parent_id, a_dir.id)
        self.assertEqual(d1.path, u"/s1/s1/s1/s1")
        d2 = sgw.get_node(d1.id)
        self.assertEqual(d2.id, d1.id)

    def test_node_not_on_volume(self):
        """Test to make sure nodes can't be retreived if they are not
        on the volume."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_subdirectory, uuid.uuid4(), u"name")
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_subdirectory, uuid.uuid4(), u"name")
        # now create some files from the sharer but are not in the shared
        # directory
        vgw = self.user._gateway.get_root_gateway()
        a_dir = vgw.make_subdirectory(vgw.get_root().id, u"DirName")
        a_file = vgw.make_file(a_dir.id, u"TheName")
        # we shouldn't be able to use these nodes on the share gateway
        self.assertRaises(errors.DoesNotExist,
                          sgw.get_node, a_dir.id)
        self.assertRaises(errors.DoesNotExist,
                          sgw.get_node, a_file.id)
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_subdirectory, a_dir.id, u"name")
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_subdirectory, a_file.id, u"name")
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_file, a_dir.id, u"name")
        self.assertRaises(errors.DoesNotExist,
                          sgw.make_file, a_file.id, u"name")

    def test_share_on_share_error(self):
        """Make sure you can't share nodes from a shared volume."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        d1 = sgw.make_subdirectory(sgw.get_root().id, u"hi")
        self.assertRaises(errors.NoPermission, sgw.make_share, d1.id, u"hi",
                          user_id=3)

    def test_public_from_share_error(self):
        """Users cant make public files shared."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        a_file = sgw.make_file(sgw.get_root().id, u"hi")
        self.assertRaises(errors.NoPermission,
                          sgw.change_public_access, a_file.id, True)

    def test_upload_readonly(self):
        """Test get_uploadjob should fail on readonly share."""
        share_dao = dao.SharedDirectory(self.r_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        self.assertRaises(
            errors.NoPermission, sgw.make_uploadjob,
            self.file.id, self.file.content_hash, get_fake_hash(), 1, 1, 1)

    def test_get_all_nodes_with_max_generation(self):
        """Test get_all_nodes with max_generation."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root = sgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = sgw.make_subdirectory(root_id, u"%sd" % i)
            f = sgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))
        # with a max_generation in the middle
        nodes_gen_10 = sgw.get_all_nodes(max_generation=10)
        self.assertEqual(nodes_gen_10,
                         [n for n in nodes if n.generation <= 10])
        # with the last generation
        nodes_gen_20 = sgw.get_all_nodes(
            max_generation=nodes[-1].generation)
        self.assertEqual(nodes_gen_20, nodes)

    def test_get_all_nodes_with_limit(self):
        """Test get_all_nodes with a limit."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root = sgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = sgw.make_subdirectory(root_id, u"%sd" % i)
            f = sgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        nodes = dirs + files
        nodes.append(root)
        # sort them in the right order
        nodes.sort(key=attrgetter('path', 'name'))

        # get the nodes by chunks of 10 top
        nodes_limit_10 = sgw.get_all_nodes(limit=10)
        self.assertEqual(nodes_limit_10, nodes[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            limit=10)
        self.assertEqual(nodes_limit_10, nodes[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            limit=10)
        self.assertEqual(nodes_limit_10, nodes)

    def test_get_all_nodes_with_max_generation_and_limit(self):
        """Test get_all_nodes with max_generation and limit."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root = sgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = sgw.make_subdirectory(root_id, u"%sd" % i)
            f = sgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        all_nodes = dirs + files
        all_nodes.append(root)
        # sort them in the right order
        all_nodes.sort(key=attrgetter('path', 'name'))
        # with max_generation and limit
        # first get all the nodes at gen_10
        nodes_gen_10 = sgw.get_all_nodes(max_generation=10)
        # now get them in chunks
        nodes_limit_5 = sgw.get_all_nodes(max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10[:10])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, nodes_gen_10)

        # same but with the last generation
        nodes_20 = [n for n in all_nodes if n.generation <= 20]
        nodes_limit_10 = sgw.get_all_nodes(max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes_20[:10])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes_20[:20])
        last_node = nodes_limit_10[-1]
        nodes_limit_10 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=20, limit=10)
        self.assertEqual(nodes_limit_10, nodes_20)

    def test_get_all_nodes_in_shared_root(self):
        """Test get_all_nodes in shared root."""
        s_root = self.sharer.volume().root
        # delete al nodes created by setUp
        for c in s_root.get_children():
            c.delete(cascade=True)
        afile = s_root.make_file(u"test_share_in_root")
        shared = s_root.share(self.user.id, u"Shared Root")
        share = self.user.get_share(shared.id)
        share.accept()
        sgw = self.user.volume(share.id).gateway
        sgw.get_root()
        # now get them in chunks
        nodes = sgw.get_all_nodes(max_generation=10, limit=5)
        # the seconds chunk should be empty
        last_node = nodes[-1]
        no_nodes = sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=10, limit=5)
        self.assertEqual(0, len(no_nodes))
        self.assertEqual(2, len(nodes))
        self.assertEqual([s_root.id, afile.id], [n.id for n in nodes])

    def test_get_all_nodes_chunked_with_changes(self):
        """Test chunked get_all_nodes with changes in the middle."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root = sgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = sgw.make_subdirectory(root_id, u"%sd" % i)
            f = sgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        all_nodes = dirs + files
        all_nodes.append(root)
        # sort them in the right order
        all_nodes.sort(key=attrgetter('path', 'name'))
        # filter nodes until gen 19
        nodes = [n for n in all_nodes if n.generation <= 19]
        nodes_gen_19 = sgw.get_all_nodes(max_generation=19)

        self.assertEqual(nodes_gen_19, nodes)

        # same but with the last generation
        nodes_limit_5 = sgw.get_all_nodes(max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:5])

        # now make some changes,
        # create a new dir.
        new_dir = sgw.make_subdirectory(root_id, u"01d")

        last_node = nodes_limit_5[-1]
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)

        self.assertEqual(nodes_limit_5, nodes[:10])

        # create a new file inside a existing dir.
        sgw.make_file(nodes[5].id, u"0_new_file.txt")

        last_node = nodes_limit_5[-1]
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:15])

        # and also make a move
        to_move = nodes[10:15][2]
        sgw.move_node(to_move.id, new_dir.id, to_move.name)
        last_node = nodes_limit_5[-1]
        # get the rest of the result.
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        self.assertEqual(nodes_limit_5, nodes_gen_19)

        # now move a node that should have been in the result,
        # but before getting to it.
        to_move = nodes_gen_19[-1]
        sgw.move_node(to_move.id, nodes_gen_19[0].id, to_move.name)
        last_node = nodes_gen_19[-5]
        result = sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        # check that it wasn't included in the result.
        self.assertEqual(result, nodes_gen_19[-4:-1])

    def test_get_all_nodes_chunked_with_move_middle(self):
        """Test chunked get_all_nodes with a move in the middle."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        root = sgw.get_root()
        root_id = root.id
        # make dirs and put files in them
        dirs = []
        files = []
        for i in range(10):
            d = sgw.make_subdirectory(root_id, u"%sd" % i)
            f = sgw.make_file(d.id, u"%sfile.txt" % i)
            dirs.append(d)
            files.append(f)
        all_nodes = dirs + files
        all_nodes.append(root)
        # sort them in the right order
        all_nodes.sort(key=attrgetter('path', 'name'))
        # filter nodes until gen 19
        nodes = [n for n in all_nodes if n.generation <= 19]
        nodes_gen_19 = sgw.get_all_nodes(max_generation=19)

        self.assertEqual(nodes_gen_19, nodes[:20])

        # same but with the last generation
        nodes_limit_5 = sgw.get_all_nodes(max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:5])
        last_node = nodes_limit_5[-1]
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19, limit=5)
        self.assertEqual(nodes_limit_5, nodes[:10])

        # move a node that's already in the result
        to_move = nodes[10:15][2]
        # now move a node that it's in the result,
        sgw.move_node(to_move.id, nodes[0].id, to_move.name)
        last_node = nodes_limit_5[-1]
        # get the rest of the result.
        nodes_limit_5 += sgw.get_all_nodes(
            start_from_path=(last_node.path, last_node.name),
            max_generation=19)
        # remove the moved node from the nodes_gen_19 list
        del nodes_gen_19[12]
        self.assertEqual(nodes_limit_5, nodes_gen_19)

    def test_get_all_nodes_chunked_only_root(self):
        """Test chunked get_all_nodes with only one node, the root."""
        share_dao = dao.SharedDirectory(self.rw_share, by_user=self.sharer)
        sgw = ReadWriteVolumeGateway(self.user, share=share_dao)
        # sort them in the right order
        nodes = sgw.get_all_nodes()
        self.assertEqual(len(nodes), 1)

        # same but with the last generation
        nodes_limit_5 = sgw.get_all_nodes(
            start_from_path=(nodes[-1].path, nodes[-1].name),
            max_generation=10, limit=5)
        self.assertEqual(nodes_limit_5, [])


class GenerationsTestCase(StorageDALTestCase):
    """Tests to ensure generations are applied properly."""

    def setUp(self):
        super(GenerationsTestCase, self).setUp()
        self.user = self.create_user(username=u"testuser", shard_id=u"shard1")
        self.shard_store = get_shard_store("shard1")
        # make a test file
        self.ugw = StorageUserGateway(self.user)
        self.vgw = self.ugw.get_root_gateway()
        self.volume = self.shard_store.get(model.UserVolume,
                                           self.user.root_volume_id)

    def test_make_file(self):
        """Test make_file increments generation."""
        self.assertEqual(self.volume.generation, 0)
        node = self.vgw.make_file(self.vgw.get_root().id, u"The FIle")
        self.assertEqual(node.generation, 1)
        self.assertEqual(self.volume.generation, 1)

    def test_make_subdirectory(self):
        """Test make_subdirectory increments generation."""
        self.assertEqual(self.volume.generation, 0)
        node = self.vgw.make_subdirectory(self.vgw.get_root().id, u"The Dir")
        self.assertEqual(node.generation, 1)
        self.assertEqual(self.volume.generation, 1)

    def test_delete_file(self):
        """Test delete increments generation."""
        self.assertEqual(self.volume.generation, 0)
        node = self.vgw.make_file(self.vgw.get_root().id, u"The FIle")
        node = self.vgw.delete_node(node.id)
        self.assertEqual(node.generation, 2)
        self.assertEqual(node.generation_created, 1)
        self.assertEqual(self.volume.generation, 2)

    def test_restore_file(self):
        """Test delete increments generation."""
        self.assertEqual(self.volume.generation, 0)
        node = self.vgw.make_file(self.vgw.get_root().id, u"The FIle")
        node = self.vgw.delete_node(node.id)
        node = self.vgw.restore_node(node.id)
        self.assertEqual(node.generation, 3)
        self.assertEqual(node.generation_created, 1)
        self.assertEqual(self.volume.generation, 3)

    def test_moves(self):
        """Test make_subdirectory increments generation."""
        self.assertEqual(self.volume.generation, 0)
        subdir = self.vgw.make_subdirectory(self.vgw.get_root().id, u"The Dir")
        self.assertEqual(self.volume.generation, 1)
        file1 = self.vgw.make_file(subdir.id, u"The FIle")
        self.assertEqual(self.volume.generation, 2)
        file2 = self.vgw.make_file(self.vgw.get_root().id, u"The FIle")
        self.assertEqual(self.volume.generation, 3)
        self.vgw.move_node(file2.id, subdir.id, file2.name)
        self.assertEqual(self.volume.generation, 5)
        d1 = self.shard_store.get(model.StorageObject, subdir.id)
        f1 = self.shard_store.get(model.StorageObject, file1.id)
        f2 = self.shard_store.get(model.StorageObject, file2.id)
        # the generation of original directory stays the same
        self.assertEqual(d1.generation, 1)
        # both the moved and overwritten files changed
        self.assertEqual(f1.generation, 4)
        self.assertEqual(f1.generation_created, 2)
        self.assertEqual(f1.status, model.STATUS_DEAD)
        self.assertEqual(f2.generation, 5)

    def test_make_file_with_content(self):
        """Test make_file_with_content increments generation."""
        name = u"filename"
        a_hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        a_file = self.vgw.make_file_with_content(
            self.vgw.get_root().id, name, a_hash, crc, size, deflated_size,
            storage_key)
        self.assertEqual(self.volume.generation, 2)
        self.assertEqual(a_file.generation, 2)

    def test_make_file_from_uploadjob(self):
        """Test make_file_from_uploadjob increments generation."""
        filenode = self.vgw.make_file(self.vgw.get_root().id, u"the file name")
        new_hash = get_fake_hash()
        crc = 12345
        size = 11111
        def_size = 10000
        self.vgw.make_content(filenode.id, filenode.content_hash, new_hash,
                              crc, size, def_size, uuid.uuid4())
        f1 = self.shard_store.get(model.StorageObject, filenode.id)
        self.assertEqual(self.volume.generation, 2)
        self.assertEqual(f1.generation, 2)
        self.assertEqual(f1.generation_created, 1)

    def test_change_public_access_without_public_uuid(self):
        """Test change public access."""
        saved_flag = utils.set_public_uuid
        utils.set_public_uuid = False
        a_file = self.vgw.make_file(self.vgw.get_root().id, u"the file name")
        a_file = self.vgw.change_public_access(a_file.id, True)
        f1 = self.shard_store.get(model.StorageObject, a_file.id)
        self.assertEqual(f1.generation, 2)
        self.assertEqual(f1.generation_created, 1)
        self.assertEqual(f1.public_uuid, None)
        utils.set_public_uuid = saved_flag

    def test_change_public_access_with_public_uuid(self):
        """Test change public access."""
        saved_flag = utils.set_public_uuid
        utils.set_public_uuid = True
        a_file = self.vgw.make_file(self.vgw.get_root().id, u"the file name")
        a_file = self.vgw.change_public_access(a_file.id, True)
        f1 = self.shard_store.get(model.StorageObject, a_file.id)
        self.assertEqual(f1.generation, 2)
        self.assertEqual(f1.generation_created, 1)
        self.assertNotEqual(f1.public_uuid, None)
        public_uuid = a_file.public_uuid
        pf = self.user_store.get(model.PublicNode, a_file.public_id)
        self.assertEqual(public_uuid, pf.public_uuid)
        # make sure the uuid is still saved despite other changes
        a_file = self.vgw.change_public_access(a_file.id, False)
        f1 = self.shard_store.get(model.StorageObject, a_file.id)
        self.assertEqual(f1.public_uuid, public_uuid)
        a_file = self.vgw.change_public_access(a_file.id, True)
        f1 = self.shard_store.get(model.StorageObject, a_file.id)
        self.assertEqual(f1.public_uuid, public_uuid)
        utils.set_public_uuid = saved_flag

    def test_deltas_across_volumes(self):
        """Test deltas across volumes to make sure they don't intermingle."""
        # root nodes
        r_file1 = self.vgw.make_file(self.vgw.get_root().id, u"file1.txt")
        r_dir = self.vgw.make_subdirectory(
            self.vgw.get_root().id, u"directory")
        # make some nodes on a user gateway
        udf = self.ugw.make_udf(u"~/This")
        udf_gw = self.ugw.get_volume_gateway(udf=udf)
        udf_file1 = udf_gw.make_file(udf_gw.get_root().id, u"file.txt")
        udf_dir = udf_gw.make_subdirectory(udf_gw.get_root().id, u"directory")
        # make some nodes shared
        user2 = self.create_user(username=u"shareuser", shard_id=u"shard0")
        gw = StorageUserGateway(user2).get_root_gateway()
        share = gw.make_share(gw.get_root().id, u"Shared", self.user.id,
                              readonly=False)
        share = self.ugw.accept_share(share.id)
        sh_gw = self.ugw.get_volume_gateway(share=share)
        sh_file1 = sh_gw.make_file_with_content(
            sh_gw.get_root().id, u"file.txt", get_fake_hash(), 1234, 100, 10,
            uuid.uuid4(), u'text/lame')
        sh_dir = sh_gw.make_subdirectory(sh_gw.get_root().id, u"directory")
        # get the deltas
        r_delta = list(self.vgw.get_generation_delta(0))
        udf_delta = list(udf_gw.get_generation_delta(0))
        sh_delta = list(sh_gw.get_generation_delta(0))
        self.assertEqual(len(r_delta), 3)
        self.assertEqual(len(udf_delta), 2)
        self.assertEqual(len(sh_delta), 3)
        self.assertEqual(r_file1, r_delta[0])
        # the file with content created in the share
        self.assertEqual(r_delta[2].content.size, 100)
        self.assertTrue(r_dir in r_delta)
        self.assertTrue(udf_file1 in udf_delta)
        self.assertTrue(udf_dir in udf_delta)
        self.assertTrue(sh_file1 in sh_delta)
        self.assertTrue(sh_dir in sh_delta)
        # check that the content is there for share deltas
        self.assertEqual(sh_delta[2].content.size, 100)

    def test_deltas_for_shares_with_moves(self):
        """Test deltas for shares where nodes have been moved out of them."""
        # root nodes
        root = self.vgw.get_root()
        r_dir = self.vgw.make_subdirectory(root.id, u"dir")
        r_subdir = self.vgw.make_subdirectory(r_dir.id, u"subdir")
        r_file1 = self.vgw.make_file(r_dir.id, u"file.txt")
        # user
        user2 = self.create_user(username=u"shareuser", shard_id=u"shard0")
        share = self.vgw.make_share(r_dir.id, u"Shared", user2.id)
        ugw2 = StorageUserGateway(user2)
        share = ugw2.accept_share(share.id)
        sh_gw = ugw2.get_volume_gateway(share=share)
        sh_delta = list(sh_gw.get_generation_delta(0))
        sh_subdir = sh_gw.get_node(r_subdir.id)
        sh_file1 = sh_gw.get_node(r_file1.id)
        # just the file is in there now
        self.assertEqual(len(sh_delta), 2)
        self.assertTrue(sh_subdir in sh_delta)
        self.assertTrue(sh_file1 in sh_delta)
        # after a move out of the share, the file will still be in the delta
        # but it will be Dead
        self.vgw.move_node(r_file1.id, root.id, r_file1.name)
        sh_delta = list(sh_gw.get_generation_delta(0))
        self.assertEqual(len(sh_delta), 2)
        self.assertTrue(sh_subdir in sh_delta)
        self.assertTrue(sh_file1 in sh_delta)
        self.assertTrue(sh_delta[1].id, sh_file1.id)
        self.assertTrue(sh_delta[1].status, model.STATUS_DEAD)
        # moving it back into the share in a different folder.
        # the file will be in the delta Live
        r_subdir2 = self.vgw.make_subdirectory(r_dir.id, u"subdir2")
        sh_subdir2 = sh_gw.get_node(r_subdir2.id)
        self.vgw.move_node(r_file1.id, r_subdir2.id, r_file1.name)
        sh_delta = list(sh_gw.get_generation_delta(0))
        self.assertEqual(len(sh_delta), 3)
        self.assertTrue(sh_subdir in sh_delta)
        self.assertTrue(sh_subdir2 in sh_delta)
        self.assertTrue(sh_file1 in sh_delta)
        self.assertTrue(sh_delta[2].id, sh_file1.id)
        self.assertTrue(sh_delta[2].status, model.STATUS_LIVE)
        # moving it within the share to the root.
        # the file will be in the delta Live
        self.vgw.move_node(r_file1.id, r_subdir.id, r_file1.name)
        sh_delta = list(sh_gw.get_generation_delta(0))
        self.assertEqual(len(sh_delta), 3)
        self.assertTrue(sh_subdir in sh_delta)
        self.assertTrue(sh_subdir2 in sh_delta)
        self.assertTrue(sh_file1 in sh_delta)
        self.assertTrue(sh_delta[2].id, sh_file1.id)
        self.assertTrue(sh_delta[2].status, model.STATUS_LIVE)

    def test_delta_order(self):
        """Test delta is always in order."""
        mk = self.vgw.make_file
        root_id = self.vgw.get_root().id
        nodes = [mk(root_id, u"File%s.txt" % i) for i in range(10)]
        nodes.reverse()
        for n in nodes:
            self.vgw.delete_node(n.id)
        delta = self.vgw.get_generation_delta(0)
        gens = [d.generation for d in delta]
        self.assertEqual(gens, sorted(gens))


class MetricsTestCase(StorageDALTestCase):
    """Test that methods send metric ok."""

    def setUp(self):
        """Set up."""
        super(MetricsTestCase, self).setUp()
        self.user = self.create_user(id=1, username=u"user1")
        self.gw = ReadWriteVolumeGateway(self.user)

        # put a recorder in the middle to see what was informed
        self.informed = []
        timing_metric.reporter.timing = lambda *a: self.informed.append(a)

    def _is_decorated(self, method, decorator):
        """Return if the method is decorated by a particular decorator."""

        def _check(func):
            """Check func for decorator."""
            closures = func.__closure__
            # if it doesn't have any closures, it's not decorated at all
            if closures is None:
                return False

            for closure in closures:
                # if the cell content is the searched decorator, we're done
                if closure.cell_contents is decorator:
                    return True

                # if the cell content is other function, need to go deeper
                if isinstance(closure.cell_contents, types.FunctionType):
                    if _check(closure.cell_contents):
                        # found the searched decorator in a deeper layer!
                        return True
            else:
                return False

        return _check(method.__func__)

    def test_supervised_methods_volume_gateways(self):
        """Assure all these methods are supervised by timing decorator."""
        superv = [
            'get_root', 'get_user_volume', 'get_generation_delta', 'get_node',
            'get_node_by_path', 'get_all_nodes',
            'get_deleted_files', 'get_children',
            'get_child_by_name', 'get_content', 'get_uploadjob',
            'get_user_uploadjobs', 'get_user_multipart_uploadjob',
            'get_directories_with_mimetypes', 'check_has_children',
            'make_file', 'make_subdirectory', 'make_tree', 'make_share',
            'delete_node', 'restore_node', 'move_node', 'make_uploadjob',
            'make_file_with_content', 'make_content', 'delete_uploadjob',
            'add_uploadjob_part', 'set_uploadjob_multpart_id',
            'set_uploadjob_when_last_active', 'change_public_access',
            'get_node_parent_ids', 'undelete_volume',
        ]
        for methname in superv:
            meth = getattr(self.gw, methname)
            self.assertTrue(self._is_decorated(meth, timing_metric),
                            "%r is not decorated" % (methname,))

    def test_supervised_methods_storage_user(self):
        """Assure all these methods are supervised by timing decorator."""
        superv = [
            'update', 'get_quota', 'recalculate_quota', 'get_udf_gateway',
            'get_share_gateway', 'get_volume_gateway', 'get_share',
            'get_shared_by', 'get_shares_of_nodes', 'get_shared_to',
            'accept_share', 'decline_share', 'delete_share',
            'set_share_access', 'delete_related_shares', 'make_udf',
            'get_udf_by_path', 'delete_udf', 'get_udf', 'get_udfs',
            'get_downloads', 'get_public_files', 'get_public_folders',
            'get_share_generation', 'get_photo_directories',
            'is_reusable_content',
        ]
        gw = StorageUserGateway(self.user)
        for methname in superv:
            meth = getattr(gw, methname)
            self.assertTrue(self._is_decorated(meth, timing_metric),
                            "%r is not decorated" % (methname,))

    def test_call_simple_ok(self):
        """Supervise a successful operation and see all is reported ok."""
        self.gw.get_root()
        informed = self.informed[0]
        self.assertEqual(informed[0], "shard0.get_root")
        self.assertTrue(isinstance(informed[1], float))

    def test_call_simple_error(self):
        """Supervise an erroring operation and see all is reported ok."""
        try:
            self.gw.get_root("error because get_root doesn't receive args")
        except TypeError:
            pass
        informed = self.informed[0]
        self.assertEqual(informed[0], "shard0.get_root")
        self.assertTrue(isinstance(informed[1], float))

    def test_call_complex(self):
        """Supervise an op with args and see all is reported ok."""
        root_id = self.gw.get_root().id
        self.gw.make_file(root_id, u"filename")
        self.gw.get_child_by_name(root_id, u"filename", with_content=False)
        for informed in self.informed:
            if informed[0] == "shard0.get_child_by_name":
                self.assertTrue(isinstance(informed[1], float))
                break
        else:
            self.fail("Timing was not informed for this method.")

    def test_call_writing(self):
        """Supervise a write op and see all is reported ok.

        This is tricky because write operations are also decorated for
        notifications, etc.
        """
        root_id = self.gw.get_root().id
        self.gw.make_file(root_id, u"filename")
        for informed in self.informed:
            if informed[0] == "shard0.make_file":
                self.assertTrue(isinstance(informed[1], float))
                break
        else:
            self.fail("Timing was not informed for this method.")

    def test_call_on_shared(self):
        """Supervise an operation that is being made on a share."""
        # create other user
        other_user = self.create_user(id=2, username=u"user2",
                                      shard_id=u'shard2')

        # make a share from default user to other user and accept it
        root_id = self.gw.get_root().id
        share = self.gw.make_share(root_id, u"hi", user_id=2, readonly=False)
        share = StorageUserGateway(other_user).accept_share(share.id)

        # do the operation in the gw from the share
        shared_gw = ReadWriteVolumeGateway(other_user, share=share)
        shared_gw.make_file(root_id, u"filename")

        # check that the metric informed is on shard 0, from default user,
        # as she is the owner of the volume where the node was created
        for informed in self.informed:
            if informed[0] == "shard0.make_file":
                self.assertTrue(isinstance(informed[1], float))
                break
        else:
            self.fail("Timing was not informed for this method.")


class TestVolumeGenerationUniqueKeyViolation(ORMTestCase):

    def test_fix_udfs_with_gen_out_of_sync(self):
        obj = self.obj_factory.make_file()
        obj.generation = obj.volume.generation + 1
        user2 = self.obj_factory.make_user(user_id=2)
        obj2 = self.obj_factory.make_file(user=user2)
        obj3 = self.obj_factory.make_file(user=user2)
        obj2.generation = obj2.volume.generation + 2
        obj3.generation = obj3.volume.generation + 1
        store = Store.of(obj)
        user_ids = [obj.owner_id, user2.id]
        fix_udfs_with_generation_out_of_sync(store, user_ids, logging)
        self.assertEqual(obj.generation, obj.volume.generation)
        self.assertEqual(obj2.generation, obj2.volume.generation)

    def test_limits_to_given_user_ids(self):
        obj = self.obj_factory.make_file()
        obj.generation = obj.volume.generation + 1
        user2 = self.obj_factory.make_user(user_id=2)
        obj2 = self.obj_factory.make_file(user=user2)
        obj2.generation = obj.volume.generation + 1
        store = Store.of(obj)
        user_ids = [obj.owner_id]
        # We have two UDFs with their generation out of sync, but
        # find_udfs_with_generation_out_of_sync() will return just the first
        # one because the list of user IDs we pass into it does not include
        # the owner of the second UDF.
        fix_udfs_with_generation_out_of_sync(store, user_ids, logging)
        self.assertEqual(obj.generation, obj.volume.generation)
        self.assertEqual(obj2.generation - 1, obj2.volume.generation)

    def test_fix_all_udfs_with_gen_out_of_sync(self):
        user = self.obj_factory.make_user()
        obj = self.obj_factory.make_file(user=user)
        obj.generation = obj.volume.generation + 1
        expected_generation = obj.generation
        vol_id = obj.volume.id
        shard_id = user.shard_id
        fix_all_udfs_with_generation_out_of_sync(shard_id, logging)
        store = Store.of(obj)
        self.assertEqual(expected_generation,
                         store.get(model.UserVolume, vol_id).generation)

    def test_fix_all_udfs_with_gen_out_of_sync_dry_run(self):
        user = self.obj_factory.make_user()
        obj = self.obj_factory.make_file(user=user)
        obj.generation = obj.volume.generation + 1

        with patch.object(Store, 'commit') as mock_commit:
            fix_all_udfs_with_generation_out_of_sync(
                user.shard_id, logging, dry_run=True)
            self.assertFalse(mock_commit.called)
