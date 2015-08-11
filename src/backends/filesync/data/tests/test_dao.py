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

"""Test the Data Access Objects."""

import re
import uuid

from datetime import datetime
from operator import attrgetter
from unittest import TestCase

from storm.database import Connection
from storm.tracer import install_tracer, remove_tracer_type

from mocker import Mocker, expect

from backends.filesync.data.testing.testcase import StorageDALTestCase
from backends.filesync.data.testing.testdata import (
    get_test_contentblob, get_fake_hash)
from backends.filesync.data import model, dao, errors, services, utils
from backends.filesync.data.dbmanager import get_shard_store


class DAOInitTestCase(TestCase):
    """Test to make sure DAOs are properly initialized from the storm models.

    These tests have no database access.
    """

    def _compare_props(self, storm_object, dao_object, properties):
        """Compare the common properties between a storm model and dao"""
        for prop in properties:
            self.assertEqual(
                getattr(storm_object, prop), getattr(dao_object, prop),
                "Property %s does not match" % prop)

    def test_StorageUser(self):
        """Test StorageUser init"""
        u = model.StorageUser(1, u"theusername", u"visible name", u"shard")
        u_dao = dao.StorageUser(u)
        self._compare_props(u, u_dao, ["id", "username", "root_volume_id",
                                       "visible_name", "shard_id"])
        self.assertEqual(u_dao.is_active, True)
        u.subscription_status = 'Dead'
        u_dao = dao.StorageUser(u)
        self.assertEqual(u_dao.is_active, False)

    def test_UserInfo(self):
        """Test UserInfo init."""
        info = model.StorageUserInfo(1, 100)
        i = dao.UserInfo(info)
        self.assertEqual(i.max_storage_bytes, info.max_storage_bytes)
        self.assertEqual(i.used_storage_bytes, info.used_storage_bytes)
        self.assertEqual(i.free_bytes, info.free_bytes)

    def test_StorageNode(self):
        """Test StorageNode init."""
        # the owner of the node
        u = model.StorageUser(1, u"theusername", u"visible name", u"shard")
        owner = dao.StorageUser(u)
        # the FileNodeContent of the node
        cb = get_test_contentblob()
        content = dao.FileNodeContent(cb)
        # the node
        node = model.StorageObject(1, u"Name", u"File")
        node.parent_id = uuid.uuid4()
        node.path = u"this is a path"
        node._content_hash = "this is a hash"
        node.volume_id = uuid.uuid4()
        node.mimetype = u'image/tiff'
        node.public_uuid = uuid.uuid4()
        perms = dict(can_read=True, can_write=True, can_delete=True)

        # In this case, StorageNode is different that all other DAOs,
        # it generates either a FileNode or DirectoryNode object, and normally
        # requires a gateway as the first parameter. In addition, it can be
        # created along with a mimetype, content, and owner DAO
        node_dao = dao.StorageNode.factory(None, node, owner=owner,
                                           permissions=perms,
                                           content=content)
        self._compare_props(
            node, node_dao,
            ["id", "kind", "parent_id", "owner_id", "status", "when_created",
             "when_last_modified", "generation", "generation_created",
             "mimetype", "public_uuid"])
        self.assertTrue(isinstance(node_dao, dao.FileNode))
        # mimetype object will not be directly accessible
        self.assertEqual(node_dao.nodekey, utils.make_nodekey(None, node.id))
        self.assertEqual(node_dao.content, content)
        self.assertEqual(node_dao.owner, owner)
        self.assertEqual(node_dao.can_read, True)
        self.assertEqual(node_dao.can_write, True)
        self.assertEqual(node_dao.can_delete, True)
        node_dao.public_id = 1
        # test public_key property
        self.assertEqual(node_dao.public_key,
                         utils.get_node_public_key(node_dao, True))
        node_dao.public_uuid = None
        self.assertEqual(node_dao.public_key,
                         utils.get_node_public_key(node_dao, False))
        node_dao.public_id = None
        self.assertEqual(node_dao.public_key, None)
        node.generation = None
        node.generation_created = None
        node_dao = dao.StorageNode.factory(None, node, owner=owner,
                                           permissions=perms,
                                           content=content)
        self.assertEqual(node_dao.generation, 0)
        self.assertEqual(node_dao.generation_created, 0)
        # basic check for a directory
        node.kind = 'Directory'
        dir_dao = dao.StorageNode.factory(None, node, owner=owner,
                                          content=content, permissions={})
        self.assertTrue(isinstance(dir_dao, dao.DirectoryNode))
        # content for Directories is ignored
        self.assertEqual(dir_dao.content, None)
        self.assertEqual(dir_dao.can_read, False)
        self.assertEqual(dir_dao.can_write, False)
        self.assertEqual(dir_dao.can_delete, False)

    def test_FileNodeContent(self):
        """Test ContentBlob init."""
        cb = get_test_contentblob()
        cb_dao = dao.FileNodeContent(cb)
        self._compare_props(cb, cb_dao, ["hash", "size", "deflated_size",
                                         "storage_key", "crc32", "status",
                                         "magic_hash", "when_created"])
        cb.size = 0
        cb_dao = dao.FileNodeContent(cb)
        self.assertEqual(cb_dao.deflated_size, 0)
        self.assertEqual(cb_dao.storage_key, None)
        cb.size = 10
        cb.deflated_size = None
        cb_dao = dao.FileNodeContent(cb)
        self.assertEqual(cb_dao.deflated_size, 0)

    def test_SharedFolder(self):
        """Test SharedFolder init."""
        #to test the shared_to and shared_by properties
        u = model.StorageUser(1, u"theusername", u"visible name", u"shard")
        user1 = dao.StorageUser(u)
        user2 = dao.StorageUser(u)

        share = model.Share(
            1, uuid.uuid4(), 2, u"share name", "View", u"email")
        share_dao = dao.SharedDirectory(share, by_user=user1, to_user=user2)
        self._compare_props(share, share_dao, ["name", "accepted",
                                               "when_shared", "status"])
        self.assertEqual(share_dao.root_id, share.subtree)
        self.assertEqual(share_dao.read_only, True)
        self.assertEqual(share_dao.offered_to_email, share.email)
        self.assertEqual(share_dao.shared_by, user1)
        self.assertEqual(share_dao.shared_to, user2)

    def test_UserVolume(self):
        """Test UserVolume init."""
        udf = model.UserVolume(1, uuid.uuid4(), u"~/the path")
        udf.id = uuid.uuid4()
        udf_dao = dao.UserVolume(udf, None)
        self._compare_props(udf, udf_dao, ["id", "owner_id", "root_id",
                                           "path", "generation"])
        udf.generation = None
        udf_dao = dao.UserVolume(udf, None)
        self.assertEqual(udf_dao.generation, 0)

    def test_UploadJob(self):
        """Test UploadJob init."""
        upload = model.UploadJob(uuid.uuid4())
        upload.hash_hint = "fake hash hint"
        upload.crc32_hint = 1234
        upload.inflated_size_hint = 12345
        upload.deflated_size_hint = 54321
        upload.when_started = datetime.utcnow()
        upload.when_last_active = datetime.utcnow()
        upload_dao = dao.UploadJob(upload)
        self._compare_props(upload, upload_dao,
                            ["storage_object_id", "chunk_count",
                             "hash_hint", "crc32_hint", "deflated_size_hint",
                             "inflated_size_hint", "when_started",
                             "when_last_active", "multipart_id",
                             "multipart_key", "uploaded_bytes", "hash_context",
                             "decompress_context"])

    def test_Download(self):
        """Test Download init."""
        volume_id = uuid.uuid4()
        path = u"The Path"
        url = u"The Url"
        key = u"Key"
        download = model.Download(1, volume_id, path, url, key)
        self.assertEqual(download.owner_id, 1)
        self.assertEqual(download.file_path, path)
        self.assertEqual(download.download_url, url)
        self.assertEqual(download.download_key, repr(key))


class VolumeProxyTestCase(StorageDALTestCase):
    """Test the VolumeProxy class."""

    def _make_content_on_volume(self, vol_root):
        """Make content on this volume to get it with the proxy."""
        name = u"filename"
        mime = u'image/tif'
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        vol_root.make_file_with_content(name, hash, crc, size,
                                        deflated_size, storage_key,
                                        mimetype=mime)
        return hash

    def test_VolumeProxy_root(self):
        """Test the VolumeProxy.

        This will also test the laziness of the root property
        """
        user = self.create_user()
        volume = user.volume()
        root = user.volume().root
        #the root on the volume proxy is an uninitialized DirectoryNode
        #the id is set to 'root' until it get's resolve.
        self.assertTrue(isinstance(root, dao.DirectoryNode))
        #the root won't get resolved until a db operation
        #is needed
        self.assertEqual(volume.root.id, 'root')
        self.assertEqual(root.id, 'root')
        #we'll force it to load, by doing something
        dir = root.make_subdirectory(u"A Subdirectory")
        self.assertNotEqual(volume.root.id, 'root')
        self.assertNotEqual(root.id, 'root')
        self.assertEqual(root.id, dir.parent_id)
        self.assertEqual(dir.vol_type, 'root')
        self.assertEqual(dir.vol_share, None)
        self.assertEqual(dir.vol_udf, None)
        self.assertEqual(root.volume_id, dir.volume_id)
        hash = self._make_content_on_volume(root)
        content = user.volume().get_content(hash)
        self.assertEqual(content.hash, hash)

    def test_VolumeProxy_udf(self):
        """Test the VolumeProxy."""
        user = self.create_user()
        #Test a udf volume:
        udf = user.make_udf(u"~/path/name")
        udf_volume = user.volume(udf.id)
        root = udf_volume.root
        dir = root.make_subdirectory(u"A Subdirectory")
        self.assertEqual(dir.parent_id, udf.root_id)
        self.assertEqual(dir.volume_id, udf.id)
        self.assertEqual(dir.vol_id, udf.id)
        self.assertEqual(dir.vol_type, 'udf')
        self.assertEqual(dir.vol_share, None)
        self.assertEqual(dir.vol_udf.id, udf.id)
        self.assertEqual(root.volume_id, udf.id)
        hash = self._make_content_on_volume(root)
        content = user.volume(udf.id).get_content(hash)
        self.assertEqual(content.hash, hash)

    def test_VolumeProxy_share(self):
        """Test the VolumeProxy."""
        user = self.create_user()
        user2 = self.create_user(id=2, username=u"user2")
        share = user.root.share(user2.id, u"ShareName")
        user2.get_share(share.id).accept()
        root = user2.volume(share.id).root
        dir = root.make_subdirectory(u"A Subdirectory")
        self.assertEqual(dir.parent_id, share.root_id)
        self.assertEqual(dir.vol_id, share.id)
        self.assertEqual(dir.vol_type, 'share')
        self.assertEqual(dir.vol_udf, None)
        self.assertEqual(dir.vol_share.id, share.id)
        hash = self._make_content_on_volume(root)
        content = user2.volume(share.id).get_content(hash)
        self.assertEqual(content.hash, hash)

    def test_VolumeProxy_get_root_and_volume(self):
        """Test the get_root method."""
        user = self.create_user()
        volume = user.volume().get_volume()
        root = user.volume().get_root()
        self.assertTrue(isinstance(root, dao.DirectoryNode))
        self.assertTrue(isinstance(volume, dao.UserVolume))
        self.assertEqual(volume.id, user.root_volume_id)
        self.assertEqual(root.id, volume.root_id)

    def test_VolumeProxy_udf_get_root_and_volume(self):
        """Test the get_root method."""
        user = self.create_user()
        udf = user.make_udf(u"~/Documents")
        volume = user.volume(udf.id).get_volume()
        root = user.volume(udf.id).get_root()
        self.assertTrue(isinstance(root, dao.DirectoryNode))
        self.assertTrue(isinstance(volume, dao.UserVolume))
        self.assertEqual(volume.id, udf.id)
        self.assertEqual(root.id, volume.root_id)

    def test_VolumeProxy_share_get_root_and_volume(self):
        """Test the get_root method."""
        user = self.create_user()
        user2 = self.create_user(id=2, username=u"user2")
        share = user.root.share(user2.id, u"ShareName")
        user2.get_share(share.id).accept()
        volume = user2.volume(share.id).get_volume()
        root = user2.volume(share.id).get_root()
        self.assertTrue(isinstance(root, dao.DirectoryNode))
        self.assertTrue(isinstance(volume, dao.UserVolume))
        self.assertEqual(volume.id, user.root_volume_id)
        self.assertEqual(root.id, volume.root_id)

    def test_get_all_nodes(self):
        """Test get_all_nodes."""
        user = self.create_user(max_storage_bytes=2 * (2 ** 30))
        mime = u'image/tif'
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        mkfile = lambda i: user.root.make_file_with_content(
            u"f%s" % i, hash, crc, size, deflated_size, storage_key,
            mimetype=mime)
        files = [mkfile(i) for i in range(10)]
        nodes = user.volume().get_all_nodes(kind='File')
        self.assertEqual(nodes, files)
        nodes = user.volume().get_all_nodes(mimetypes=[u"xxx"])
        self.assertEqual(nodes, [])
        nodes = user.volume().get_all_nodes(mimetypes=[mime],
                                            with_content='True')
        self.assertEqual(nodes, files)
        self.assertEqual(nodes[0].content.hash, hash)

    def test_get_deleted_files(self):
        """Test get_deleted_files."""
        user = self.create_user(max_storage_bytes=2 * (2 ** 30))
        files = [user.root.make_file(u"file%s.txt" % i) for i in range(20)]
        dead_files = user.volume().get_deleted_files()
        self.assertEqual(dead_files, [])
        for f in files:
            f.delete()
        deleted_files = user.volume().get_deleted_files()
        files.sort(key=attrgetter('when_last_modified'), reverse=True)
        self.assertEqual(len(deleted_files), 20)
        self.assertEqual(files, deleted_files)
        ## basic tests of start and limit
        deleted_files = user.volume().get_deleted_files(limit=2)
        self.assertEqual(len(deleted_files), 2)
        self.assertEqual(files[:2], deleted_files)
        deleted_files = user.volume().get_deleted_files(start=3, limit=5)
        self.assertEqual(len(deleted_files), 5)
        self.assertEqual(files[3:8], deleted_files)


class DAOTestCase(StorageDALTestCase):
    """Test the DAO with database access."""

    def create_file(
            self, user, name=None, size=100, deflated_size=None,
            mime=u'image/tif', crc=12345, storage_key=None,
            hash=None, udf=None):
        """Create a file."""
        if name is None:
            name = u'my-file-{}'.format(uuid.uuid4())
        if storage_key is None:
            storage_key = uuid.uuid4()
        if hash is None:
            hash = get_fake_hash()
        if deflated_size is None:
            deflated_size = size
        if udf is None:
            base_volume = user.root
        else:
            base_volume = user.volume(udf.id).root
        file = base_volume.make_file_with_content(
            name, hash, crc, size, deflated_size, storage_key)
        return file

    def test_StorageUser(self):
        """Basic test for StorageUser."""
        user = self.create_user()
        user.update(max_storage_bytes=2)
        quota = user.get_quota()
        self.assertEqual(quota.max_storage_bytes, 2)
        self.assertEqual(quota.free_bytes, 2)
        self.assertEqual(user.is_active, True)
        user.update(subscription=False)
        self.assertEqual(user.is_active, False)
        quota = user.recalculate_quota()
        self.assertEqual(quota.max_storage_bytes, 2)
        self.assertEqual(quota.free_bytes, 2)

    def test_UserVolume(self):
        """Test the various StorageUser data access functions."""
        user = self.create_user()
        self.assertEqual(len(user.get_udfs()), 0)
        udf = user.make_udf(u"~/path/name")
        self.assertEqual(udf.path, u"~/path/name")
        self.assertEqual(udf.owner_id, user.id)
        self.assertEqual(udf.status, model.STATUS_LIVE)
        self.assertEqual(udf.owner, user)
        self.assertEqual(udf.is_root, False)
        udf2 = user.get_udf_by_path(u"~/path/name")
        self.assertEqual(udf.id, udf2.id)
        self.assertEqual(len(user.get_udfs()), 1)
        udf = user.delete_udf(udf.id)
        self.assertEqual(len(user.get_udfs()), 0)
        self.assertEqual(udf.status, model.STATUS_DEAD)
        self.assertRaises(errors.DoesNotExist, user.get_udf, udf.id)

    def test_get_udf_by_path(self):
        """Test the various methods of getting a UDF by path."""
        user = self.create_user()
        udf = user.make_udf(u"~/a/b/c")
        udf1 = user.get_udf_by_path(u"~/a/b/c")
        self.assertEqual(udf.id, udf1.id)
        udf1 = user.get_udf_by_path(u"~/a/b/c/", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        udf1 = user.get_udf_by_path(u"~/a/b/c/d/e/f", from_full_path=True)
        self.assertEqual(udf.id, udf1.id)
        self.assertRaises(errors.DoesNotExist,
                          user.get_udf_by_path, u"~/a/b/c/d/e/f")

    def test_user_get_node_by_path_root(self):
        """Test get_node_by_path from root."""
        user = self.create_user()
        udf = user.volume().get_volume()
        d1 = user.get_node_by_path(model.ROOT_USERVOLUME_PATH)
        self.assertEqual(d1.id, udf.root_id)
        trick_path = udf.path + " Tricky"
        udf2 = user.make_udf(trick_path)
        d1 = user.get_node_by_path(trick_path)
        self.assertEqual(d1.id, udf2.root_id)

    def test_user_get_node_by_path(self):
        """Test get_node_by_path."""
        user = self.create_user()
        udf = user.make_udf(u"~/a/b/c")
        user.make_udf(u"~/a/b/c_continued")
        d = user.volume(udf.id).root.make_tree(u"/a/b/c/d")
        f = d.make_file(u"file.txt")
        # make sure we return the root node
        d1 = user.get_node_by_path(udf.path)
        self.assertEqual(d1.id, udf.root_id)
        d1 = user.get_node_by_path(udf.path + '/')
        self.assertEqual(d1.id, udf.root_id)
        d1 = user.get_node_by_path(udf.path + d.full_path)
        self.assertEqual(d.id, d1.id)
        f1 = user.get_node_by_path(udf.path + f.full_path)
        self.assertEqual(f.id, f1.id)
        self.assertRaises(errors.DoesNotExist,
                          user.get_node_by_path, udf.path + u"/x/y/x")
        self.assertRaises(errors.DoesNotExist,
                          user.get_node_by_path, udf.path + u"/a/b/c/d/file")
        self.assertRaises(errors.DoesNotExist,
                          user.get_node_by_path, udf.path + u"/a/b/c/d/e/f/g")

    def test_user_make_tree_by_path(self):
        """Test user.make_tree_by_path."""
        user = self.create_user()
        d = user.make_tree_by_path(u"~/Ubuntu One/a/b")
        self.assertEqual(d.full_path, u"/a/b")
        d1 = user.get_node_by_path(u"~/Ubuntu One/a/b")
        self.assertEqual(d.id, d1.id)

    def test_user_make_file_by_path1(self):
        """Test user.make_tree_by_path."""
        user = self.create_user()
        f = user.make_file_by_path(u"~/Ubuntu One/file.txt")
        self.assertEqual(f.full_path, u"/file.txt")

    def test_user_make_file_by_path2(self):
        """Test user.make_tree_by_path."""
        user = self.create_user()
        f = user.make_file_by_path(u"~/Ubuntu One/a/b/file.txt")
        self.assertEqual(f.full_path, u"/a/b/file.txt")

    def test_SharedDirectories(self):
        """Test SharedDirectory features of the api."""
        user = self.create_user()
        # first do a share offer...
        root = user.volume().root
        share = root.make_shareoffer(u"email@example.com", u"share")
        self.assertEqual(share.offered_to_email, u"email@example.com")
        self.assertEqual(share.root_id, root.id)
        #make sure get_shared_by picks these up
        self.assertEqual(len(user.get_shared_by(accepted=True)), 0)
        self.assertEqual(len(user.get_shared_by()), 1)
        self.assertEqual(len(user.get_shared_by(node_id=root.id)), 1)
        so2 = services.claim_shareoffer(2, u"usern2", u"visible2", share.id)
        self.assertEqual(so2.shared_by.id, user.id)
        self.assertEqual(so2.shared_to.id, 2)
        self.assertEqual(so2.root_id, root.id)
        #make sure get_shared_by picks these up
        self.assertEqual(len(user.get_shared_by(accepted=False)), 0)
        self.assertEqual(len(user.get_shared_by(accepted=True)), 1)
        self.assertEqual(len(user.get_shared_by()), 1)
        self.assertEqual(len(user.get_shared_by(node_id=root.id)), 1)
        share = user.get_share(so2.id)
        share.delete()
        self.assertEqual(share.status, model.STATUS_DEAD)
        self.assertRaises(errors.DoesNotExist, user.get_share, so2.id)
        # test a direct share to user3
        user3 = self.create_user(id=3, username=u"user3")
        share = root.share(user3.id, u"Share Name")
        self.assertEqual(share.shared_to_id, user3.id)
        self.assertEqual(share.accepted, False)
        share = user3.get_share(share.id)
        self.assertEqual(len(user3.get_shared_to()), 1)
        self.assertEqual(len(user3.get_shared_to(accepted=True)), 0)
        share.accept()
        self.assertEqual(len(user3.get_shared_to()), 1)
        self.assertEqual(len(user3.get_shared_to(accepted=True)), 1)
        self.assertEqual(len(user3.get_shared_to(accepted=False)), 0)
        self.assertEqual(share.accepted, True)
        shared_volume = user3.volume(share.id)
        dir = shared_volume.root.make_subdirectory(u"Hi I have a share")
        #the dir is still owned by user
        self.assertEqual(dir.owner_id, user.id)
        #make sure user3 can get the dirctory from the share_volume
        dir = shared_volume.get_node(dir.id)
        #but not from his own volume root
        self.assertRaises(errors.DoesNotExist,
                          user3.volume().get_node, dir.id)
        #user3 got this share, so he can delete it
        share.delete()
        self.assertEqual(share.status, model.STATUS_DEAD)
        #the volume can no longer work
        self.assertRaises(
            errors.DoesNotExist, shared_volume.get_node, dir.id)
        self.assertRaises(
            errors.DoesNotExist,
            shared_volume.root.make_subdirectory, u"Hi I have a share2")
        #user can still get the new directory from user3
        node = user.get_node(dir.id)
        self.assertEqual(node.id, dir.id)
        #user has no shares any more as they are all Dead
        self.assertEqual(len(user.get_shared_by(accepted=False)), 0)
        self.assertEqual(len(user.get_shared_by(accepted=True)), 0)
        self.assertEqual(len(user.get_shared_by()), 0)
        self.assertEqual(len(user.get_shared_by(node_id=root.id)), 0)
        #make another share to user3 so he can decline it.
        share = root.share(user3.id, u"Share Name2")
        user3.get_share(share.id).decline()
        #once it's declined it's gone forever
        self.assertRaises(errors.DoesNotExist,
                          user.get_share, share.id)
        self.assertRaises(errors.DoesNotExist,
                          user3.get_share, share.id)

    def test_get_node_shares(self):
        """Test get_node_shares."""
        usera = self.create_user()
        userb = self.create_user(2, u"usera", shard_id=u"shard0")
        userc = self.create_user(3, u"userb", shard_id=u"shard1")
        dir1 = usera.root.make_subdirectory(u"root1")
        dir1_tree = []
        p = dir1
        for i in range(10):
            p = p.make_subdirectory(u"dir")
            f = p.make_file(u"file")
            dir1_tree.append(p)
            dir1_tree.append(f)
        self.assertEqual(len(dir1_tree), 20)
        #get the last node and see if it's shared
        shares = usera.get_node_shares(dir1_tree[len(dir1_tree) - 1].id)
        self.assertEqual(shares, [])
        sharea = dir1.share(usera.id, u"sharea")
        usera.get_share(sharea.id).accept()
        shareb = dir1.share(userb.id, u"shareb")
        userb.get_share(shareb.id).accept()
        dir1.share(userc.id, u"sharec")
        #get the last node and see if it's shared
        shares = usera.get_node_shares(dir1_tree[len(dir1_tree) - 1].id)
        self.assertEqual(len(shares), 2)
        #all nodes in the tree will result in 2 shares
        for n in dir1_tree:
            shares = usera.get_node_shares(n.id)
            self.assertEqual(len(shares), 2)

    def test_paths_on_shares(self):
        """Test paths on shares."""
        usera = self.create_user()
        userb = self.create_user(2, u"usera", shard_id=u"shard0")
        a = usera.root.make_subdirectory(u"a")
        b = a.make_subdirectory(u"b")
        c = b.make_subdirectory(u"c")
        share = c.share(userb.id, u"ShareName")
        userb.get_share(share.id).accept()
        d = c.make_subdirectory(u"d")
        e = d.make_subdirectory(u"e")
        userb_c = userb.volume(share.id).get_root()
        #path from shares
        self.assertEqual(userb_c.parent_id, None)
        self.assertEqual(userb_c.path, u"/")
        self.assertEqual(userb_c.name, u"")
        self.assertEqual(userb_c.full_path, u"/")
        userb_e = userb.volume(share.id).get_node(e.id)
        self.assertEqual(userb_e.path, u"/d")
        self.assertEqual(userb_e.full_path, u"/d/e")
        #make sure paths in deltas are correct
        vol, free, delta = userb.volume(share.id).get_delta(0)
        self.assertEqual(delta[0].full_path, u"/d")
        self.assertEqual(delta[1].full_path, u"/d/e")
        vol, free, delta = userb.volume(share.id).get_from_scratch()
        self.assertEqual(delta[0].full_path, u"/")
        self.assertEqual(delta[1].full_path, u"/d")
        self.assertEqual(delta[2].full_path, u"/d/e")

    def test_StorageNode__eq__(self):
        """Test the StorageNode __eq___"""
        user = self.create_user(123)
        dir = user.root.make_file(u"file.txt")
        dir2 = user.volume().get_node(dir.id)
        self.assertEqual(dir, dir2)
        #it is not the same object
        self.assertFalse(dir is dir2)

    def test_StorageNode_return_self(self):
        """Test return self of StorageNode methods."""
        user = self.create_user()
        root = user.root
        node = root.make_file(u"file")
        n = node.delete()
        self.assertTrue(n is node)
        n = node.restore()
        self.assertTrue(n is node)
        n = node.move(root.id, u"new name")
        self.assertTrue(n is node)

    def test_FileNode_return_self(self):
        """Test return self of FileNode methods."""
        user = self.create_user()
        node = user.root.make_file(u"file")
        n = node.change_public_access(True)
        self.assertTrue(n is node)

    def test_Share_return_self(self):
        """Test return self of Share methods."""
        user = self.create_user()
        user2 = self.create_user(id=2, username=u"user2")
        share = user.root.share(user2.id, u"TheShare")
        share2 = user2.get_share(share.id)
        s = share2.accept()
        self.assertTrue(s is share2)
        s = share.set_access(True)
        self.assertTrue(s is share)
        s = share.delete()
        self.assertTrue(s is share)
        share = user.root.share(user2.id, u"TheShare")
        share2 = user2.get_share(share.id)
        s = share2.decline()
        self.assertTrue(s is share2)

    def test_DirectoryNode(self):
        """Test DirectoryNode features in api."""
        user = self.create_user()
        root = user.volume().root
        dir = root.make_subdirectory(u"A New Subdirectory")
        self.assertTrue(isinstance(dir, dao.DirectoryNode))
        self.assertEqual(dir.parent_id, root.id)
        children = root.get_children()
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].id, dir.id)
        self.assertFalse(dir.has_children())
        subdir = dir.make_subdirectory(u"Another Subdirectory")
        dir.load()
        self.assertTrue(dir.has_children())
        self.assertTrue(dir.has_children(kind='Directory'))
        self.assertFalse(dir.has_children(kind='File'))
        file = dir.make_file(u"A File")
        dir.load()
        self.assertTrue(dir.has_children(kind='Directory'))
        self.assertTrue(dir.has_children(kind='File'))
        self.assertEqual(subdir.parent_id, dir.id)
        self.assertEqual(file.parent_id, dir.id)
        children = dir.get_children()
        self.assertEqual(len(children), 2)
        self.assertRaises(errors.NotEmpty, dir.delete)
        dir.delete(cascade=True)
        root.load()
        self.assertFalse(root.has_children())
        self.assertFalse(root.has_children(kind='File'))
        self.assertFalse(root.has_children(kind='Directory'))
        #
        ##
        ##do it the lazy way:
        ##
        dir = user.volume().root.make_subdirectory(u"LazyDir")
        dir.make_file(u"file")
        root.load()
        self.assertEqual(root.has_children(), True)
        self.assertRaises(errors.NotEmpty, user.volume().node(dir.id).delete)
        user.volume().node(dir.id).delete(cascade=True)
        root.load()
        self.assertEqual(root.has_children(), False)

    def test_DirectoryNode_make_tree(self):
        """Test make_tree in directory node."""
        user = self.create_user()
        d = user.volume().root.make_tree(u"/a/b/c/d/")
        self.assertEqual(d.full_path, u"/a/b/c/d")
        c = user.volume().get_node_by_path(u"/a/b/c")
        self.assertEqual(d.parent_id, c.id)

    def test_FileNode(self):
        """Test FileNode features in api."""
        user = self.create_user()
        root = user.root
        file = root.make_file(u"A new file")
        self.assertTrue(isinstance(file, dao.FileNode))
        self.assertEqual(file.parent_id, root.id)
        children = root.get_children()
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].id, file.id)
        self.assertFalse(file.is_public)
        self.assertEqual(file.public_url, None)
        file.change_public_access(True)
        self.assertTrue(file.is_public)
        self.assertEqual(file.public_url, utils.get_public_file_url(file))
        file.change_public_access(False)
        self.assertFalse(file.is_public)
        self.assertEqual(file.public_url, None)
        file.delete()
        self.assertEqual(file.status, model.STATUS_DEAD)
        #
        # do it the lazy way:
        #
        file = user.volume().root.make_file(u"A new file")
        user.volume().node(file.id).delete()
        self.assertRaises(errors.DoesNotExist,
                          user.volume().get_node, file.id)

    def test_make_filepath_with_content(self):
        """Make file with content using paths."""
        user = self.create_user(max_storage_bytes=200)
        path = u"~/Ubuntu One/a/b/c/filename.txt"
        mime = u'image/tif'
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        node = user.make_filepath_with_content(
            path, hash, crc, size, deflated_size, storage_key, mimetype=mime)
        file = user.get_node(node.id, with_content=True)
        self.assertEqual(file.name, "filename.txt")
        self.assertEqual(file.full_path, "/a/b/c/filename.txt")
        self.assertEqual(file.mimetype, mime)
        self.assertEqual(file.status, model.STATUS_LIVE)
        self.assertEqual(file.content.hash, hash)
        self.assertEqual(file.content.crc32, crc)
        self.assertEqual(file.content.size, size)
        self.assertEqual(file.content.deflated_size, deflated_size)
        self.assertEqual(file.content.storage_key, storage_key)

    def test_make_file_with_content(self):
        """Make file with contentblob.

        This is similar to the way the updown server creates a file. But it's
        all handled in one function after the upload.

        This also tests StorageUser.get_content
        """
        user = self.create_user(max_storage_bytes=200)
        name = u"filename"
        mime = u'image/tif'
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        self.assertRaises(errors.DoesNotExist, user.volume().get_content, hash)
        f = user.root.make_file_with_content
        #can't create a file with the same name as a directory
        user.root.make_subdirectory(u"dupe")
        self.assertRaises(errors.AlreadyExists, f, u"dupe", hash, crc,
                          size, deflated_size, storage_key)
        #can't exceed your quota fool!
        self.assertRaises(errors.QuotaExceeded, f, name, hash, crc,
                          3000, 3000, storage_key)
        node = f(name, hash, crc, size, deflated_size, storage_key,
                 mimetype=mime)
        file = user.get_node(node.id, with_content=True)
        self.assertEqual(file.name, name)
        self.assertEqual(file.mimetype, mime)
        self.assertEqual(file.status, model.STATUS_LIVE)
        self.assertEqual(file.content.hash, hash)
        self.assertEqual(file.content.crc32, crc)
        self.assertEqual(file.content.size, size)
        self.assertEqual(file.content.deflated_size, deflated_size)
        self.assertEqual(file.content.storage_key, storage_key)
        #make sure the user can get the content
        content = user.volume().get_content(hash)
        self.assertEqual(content.hash, hash)
        self.assertEqual(content.crc32, crc)
        self.assertEqual(content.size, size)
        self.assertEqual(content.deflated_size, deflated_size)
        self.assertEqual(content.storage_key, storage_key)
        quota = user.get_quota()
        self.assertEqual(quota.used_storage_bytes, 100)
        #a call later to the same function will create a new content blob and
        #update the file
        new_hash = get_fake_hash()
        new_storage_key = uuid.uuid4()
        new_crc = 54321
        new_size = 99
        new_deflated_size = 2000
        node = f(name, new_hash, new_crc, new_size, new_deflated_size,
                 new_storage_key)
        file = user.get_node(node.id, with_content=True)
        self.assertEqual(file.name, name)
        self.assertEqual(file.mimetype, mime)
        self.assertEqual(file.status, model.STATUS_LIVE)
        self.assertEqual(file.content.hash, new_hash)
        self.assertEqual(file.content.crc32, new_crc)
        self.assertEqual(file.content.size, new_size)
        self.assertEqual(file.content.deflated_size, new_deflated_size)
        self.assertEqual(file.content.storage_key, new_storage_key)
        #the user's quota decreased
        quota.load()
        self.assertEqual(quota.used_storage_bytes, 99)
        #uhoh this file grew to big!!
        new_hash = get_fake_hash()
        new_size = 10000
        self.assertRaises(errors.QuotaExceeded, f, name, new_hash, new_crc,
                          new_size, new_deflated_size, storage_key)

    def test_make_file_with_content_public(self):
        """Make file with contentblob."""
        user = self.create_user(max_storage_bytes=200)
        name = u"filename"
        a_hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = 100
        deflated_size = 10000
        f = user.root.make_file_with_content(
            name, a_hash, crc, size, deflated_size, storage_key,
            is_public=True)
        self.assertNotEqual(f.public_url, None)

    def test_make_file_with_content_enforces_quota(self):
        """Make file with contentblob enforces quota check (or not)."""
        user = self.create_user(max_storage_bytes=200)
        name = u"filename"
        mime = u'image/tif'
        hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = deflated_size = 300
        f = user.root.make_file_with_content
        self.assertRaises(errors.QuotaExceeded, f, name, hash, crc,
                          size, deflated_size, storage_key, enforce_quota=True)
        node = f(name, hash, crc, size, deflated_size, storage_key,
                 mimetype=mime, enforce_quota=False)
        self.assertTrue(node is not None)

    def test_make_file_with_content_overwrite_hashmismatch(self):
        """Make file with contentblob enforces quota check (or not)."""
        user = self.create_user(max_storage_bytes=200)
        name = u"filename"
        mime = u'image/tif'
        a_hash = get_fake_hash()
        storage_key = uuid.uuid4()
        crc = 12345
        size = deflated_size = 300
        f = user.root.make_file_with_content
        self.assertRaises(errors.QuotaExceeded, f, name, a_hash, crc,
                          size, deflated_size, storage_key, enforce_quota=True)
        f(name, a_hash, crc, size, deflated_size, storage_key,
          mimetype=mime, enforce_quota=False)
        self.assertRaises(
            errors.HashMismatch,
            f, name, a_hash, crc, size, deflated_size, storage_key,
            previous_hash=get_fake_hash('ABC'))

    def test_UploadJob(self):
        """Test the UploadJob."""
        user = self.create_user(max_storage_bytes=200)
        file = user.root.make_file(u"A new file")
        new_hash = get_fake_hash()
        crc = 12345
        size = 100
        deflated_size = 10000
        #
        # Some steps to simulate what happens during put content.
        #
        #first the expected failures
        self.assertRaises(errors.QuotaExceeded, file.make_uploadjob,
                          file.content_hash, new_hash, crc, 300, deflated_size)
        self.assertRaises(errors.HashMismatch, file.make_uploadjob,
                          "WRONG OLD HASH", new_hash, crc, 300, deflated_size)
        upload_job = file.make_uploadjob(file.content_hash, new_hash, crc,
                                         size, deflated_size)
        job = user.get_uploadjob(upload_job.id)
        self.assertEqual(job.id, upload_job.id)
        jobs = user.get_uploadjobs()
        self.assertEqual(jobs[0].id, upload_job.id)

        # get the file, there should be no Content for the file yet
        new_file = user.get_node(file.id, with_content=True)
        self.assertEqual(new_file.kind, 'File')
        self.assertEqual(new_file.content, None)

        # now play a bit with multipart support
        job = file.make_uploadjob(file.content_hash, new_hash, crc, size,
                                  deflated_size, multipart_id="foo",
                                  multipart_key=uuid.uuid4())
        old = job.when_last_active
        job.add_part(10, 15, 1, "hash context", "magic hash context",
                     "zlib context")
        self.assertTrue(job.when_last_active > old)
        self.assertEqual(job.uploaded_bytes, 10)
        self.assertEqual(job.inflated_size, 15)
        self.assertEqual(job.crc32, 1)
        self.assertEqual(job.chunk_count, 1)
        self.assertEqual(job.hash_context, "hash context")
        self.assertEqual(job.magic_hash_context, "magic hash context")
        self.assertEqual(job.decompress_context, "zlib context")
        old = job.when_last_active
        job.add_part(10, 30, 2, "more hash context", "more magic hash context",
                     "more zlib context")
        self.assertTrue(job.when_last_active > old)
        self.assertEqual(job.uploaded_bytes, 20)
        self.assertEqual(job.inflated_size, 30)
        self.assertEqual(job.crc32, 2)
        self.assertEqual(job.chunk_count, 2)
        self.assertEqual(job.hash_context, "more hash context")
        self.assertEqual(job.magic_hash_context, "more magic hash context")
        self.assertEqual(job.decompress_context, "more zlib context")
        self.assertEqual(job.file, file)
        job.delete()

        # set the multipart id
        job = file.make_uploadjob(file.content_hash, new_hash, crc, size,
                                  deflated_size, multipart_key=uuid.uuid4())
        self.assertEqual(job.multipart_id, None)
        job.set_multipart_id("foo")
        self.assertEqual(job.multipart_id, "foo")

        # update the last active time
        job = file.make_uploadjob(file.content_hash, new_hash, crc, size,
                                  deflated_size, multipart_key=uuid.uuid4())
        self.assertEqual(job.multipart_id, None)
        old = job.when_last_active
        job.touch()
        self.assertTrue(job.when_last_active > old)

    def test_encode_decode(self):
        """Make sure encode/decode can handle UUIDs properly."""
        for i in range(100):
            id1 = uuid.uuid4()
            id2 = uuid.uuid4()
            key = utils.make_nodekey(id1, id2)
            self.assertEqual(utils.split_nodekey(key), (id1, id2))

    def test_nodekey(self):
        """Test the vairous uses of nodekeys."""
        user = self.create_user()
        root = user.volume().root
        #this is uninitialized so...
        self.assertEqual(root.nodekey, utils.make_nodekey(None, 'root'))
        root._load()
        self.assertEqual(root.nodekey, utils.make_nodekey(None, root.id))
        file = root.make_file(u"The file")
        self.assertEqual(file.nodekey, utils.make_nodekey(None, file.id))
        file_from_key = user.get_node_with_key(file.nodekey)
        self.assertEqual(file_from_key.id, file.id)
        self.assertEqual(file_from_key.vol_id, file.vol_id)
        user3 = self.create_user(id=3, username=u"user3")
        self.assertRaises(errors.DoesNotExist,
                          user3.get_node_with_key, file.nodekey)
        #do some shares with user3
        share = root.share(user3.id, u"ShareName")
        share = user3.get_share(share.id)
        share.accept()
        node = user3.volume(share.id).get_node(file.id)
        node2 = user3.get_node_with_key(utils.make_nodekey(share.id, file.id))
        self.assertEqual(node.id, node2.id)
        self.assertEqual(node.nodekey, node2.nodekey)
        #how bout a udf
        udf = user.make_udf(u"~/.fake/path")
        file = user.volume(udf.id).root.make_file(u"new file")
        node = user.get_node_with_key(utils.make_nodekey(udf.id, file.id))
        self.assertEqual(file.id, node.id)
        self.assertEqual(file.nodekey, node.nodekey)

    def test_volumes(self):
        """Test the root shares and udf volumes and nodekeys.

        This will create files on root, udf and share volumes and collect the
        nodekeys. Then it will verify that the nodes can be retrieved from
        volumes via nodekeys.
        """
        user = self.create_user()
        keys = []
        #add some files to the root and append the keys
        for i in range(5):
            file = user.root.make_file(u"file%s" % i)
            keys.append((file.nodekey, file))
        #make a few udfs and add some files to it
        for i in range(10):
            user.make_udf(u"~/path/uname%s" % i)
        for vol in user.get_udf_volumes():
            for i in range(5):
                file = vol.root.make_file(u"file%s" % i)
                keys.append((file.nodekey, file))
        #make a few shares from different users
        for i in range(10):
            userx = self.create_user(id=i + 1, username=u"user%s" % i)
            share = userx.root.share(user.id, u"Share%s" % i)
            user.get_share(share.id).accept()
        for vol in user.get_share_volumes():
            for i in range(5):
                file = vol.root.make_file(u"file%s" % i)
                keys.append((file.nodekey, file))
        #Go through all the node keys and make sure they work
        self.assertEqual(len(keys), 105)
        for key, file in keys:
            node = user.get_node_with_key(key)
            self.assertEqual(node.id, file.id)

    def test_move(self):
        """Test StorageNode.move."""
        user = self.create_user()
        dir1 = user.root.make_subdirectory(u"dir1")
        dir11 = dir1.make_subdirectory(u"dir1.1")
        dir2 = user.root.make_subdirectory(u"dir2")
        dir21 = dir2.make_subdirectory(u"dir2.1")
        self.assertTrue(dir2.has_children())
        dir1.move(dir2.id, u"new name")
        dir2.load()
        dir11.load()
        self.assertTrue(dir2.has_children())
        self.assertEqual(dir1.parent_id, dir2.id)
        self.assertEqual(dir1.name, u"new name")
        self.assertEqual(dir11.path, u"/dir2/new name")
        #rename only changes child paths
        dir1.move(dir1.parent_id, u"another name")
        dir11.load()
        self.assertEqual(dir1.name, u"another name")
        self.assertEqual(dir11.path, u"/dir2/another name")
        #moving on top of a node, deletes it
        dir1.move(dir1.parent_id, u"dir2.1")
        self.assertRaises(errors.DoesNotExist, user.get_node, dir21.id)

    def test_get_node_by_path(self):
        """Test VolumeProxy.get_node_by_path.

        This test makes sure that common paths don't step on each other, the
        detailed tests for this are in the gateway.
        """

        def make_tree_on_volume(vol):
            """Create the same directory structure on a volume."""
            d1 = vol.make_subdirectory(u"d1")
            d2 = d1.make_subdirectory(u"d2")
            d2.make_subdirectory(u"d3")

        user1 = self.create_user(id=1, username=u"user1")
        user2 = self.create_user(id=2, username=u"user2")
        udf1 = user1.make_udf(u"~/UDF/path")
        udf1a = user1.make_udf(u"~/UDF1a/path")
        udf2 = user2.make_udf(u"~/UDF/path")
        d1 = user1.root.make_subdirectory(u'shared')
        share = d1.share(user2.id, u"name")
        user2.get_share(share.id).accept()
        make_tree_on_volume(user1.root)
        make_tree_on_volume(user1.volume(udf1.id).root)
        make_tree_on_volume(user1.volume(udf1a.id).root)
        make_tree_on_volume(d1)
        make_tree_on_volume(user2.root)
        make_tree_on_volume(user2.volume(udf2.id).root)
        #now lets get some stuff...
        r = user1.volume().get_node_by_path(u'/')
        self.assertEqual(r.owner_id, user1.id)
        self.assertEqual(r.id, user1.root.load().id)
        d3 = user1.volume().get_node_by_path(u'/d1/d2/d3')
        self.assertEqual(d3.vol_id, None)
        d3 = user1.volume(udf1.id).get_node_by_path(u'/d1/d2/d3')
        self.assertEqual(d3.owner_id, user1.id)
        self.assertEqual(d3.vol_id, udf1.id)
        self.assertEqual(d3.path, u'/d1/d2')
        self.assertEqual(d3.name, u'd3')
        d3 = user2.volume(udf2.id).get_node_by_path(u'/d1/d2/d3')
        self.assertEqual(d3.owner_id, user2.id)
        self.assertEqual(d3.vol_id, udf2.id)
        self.assertEqual(d3.path, u'/d1/d2')
        self.assertEqual(d3.name, u'd3')
        d3 = user2.volume(share.id).get_node_by_path(u'/d1/d2/d3')
        self.assertEqual(d3.owner_id, user1.id)
        self.assertEqual(d3.vol_id, share.id)
        self.assertEqual(d3.path, u'/d1/d2')
        self.assertEqual(d3.name, u'd3')

    def test_get_public_files(self):
        """Test StorageUser.get_public_files method."""
        user = self.create_user()
        udf = user.make_udf(u"~/myfiles/are here")
        # create some files and make them public
        for i in range(5):
            root_file = user.root.make_file(u"file_%s" % i)
            root_file.change_public_access(True)
            udf_file = user.volume(udf.id).root.make_file(u"udf file%s" % i)
            udf_file.change_public_access(True)
            #user has no access to noread files
        nodes = user.get_public_files()
        self.assertTrue(isinstance(nodes, list),
                        'get_public_files should return a list')
        self.assertEqual(10, len(nodes))

    def test_get_public_folders(self):
        """Test StorageUser.get_public_folders method."""
        user = self.create_user()
        # create some folders and make them public
        for i in range(5):
            d = user.root.make_subdirectory(u"folder_%s" % i)
            d.change_public_access(True, allow_directory=True)
        nodes = user.get_public_folders()
        self.assertTrue(isinstance(nodes, list),
                        'get_public_folders should return a list')
        self.assertEqual(5, len(nodes))

    def test_change_public_access_file(self):
        """Test the basics of changing public access to a file."""
        utils.set_public_uuid = False
        user = self.create_user()
        f1 = user.root.make_file(u"a-file.txt")
        # It has no public ID
        self.assertEqual(f1.public_uuid, None)
        self.assertEqual(f1.public_url, None)
        # It now has a public ID
        f1.change_public_access(True)
        self.assertEqual(f1.public_uuid, None)
        self.assertNotEqual(f1.public_url, None)
        f1.change_public_access(False)
        self.assertEqual(f1.public_uuid, None)
        self.assertEqual(f1.public_url, None)

    def test_change_public_access_file_uuid(self):
        """Test the basics of changing public access to a file using uuid."""
        utils.set_public_uuid = True
        user = self.create_user()
        f1 = user.root.make_file(u"a-file.txt")
        # It has no public ID
        self.assertEqual(f1.public_id, None)
        self.assertEqual(f1.public_url, None)
        # It now has a public ID
        f1.change_public_access(True)
        self.assertNotEqual(f1.public_id, None)
        self.assertNotEqual(f1.public_url, None)
        f1.change_public_access(False)
        self.assertEqual(f1.public_id, None)
        self.assertEqual(f1.public_url, None)

    def test_change_public_access_directory_nopermission(self):
        """Test that by default you can't make a directory public."""
        user = self.create_user()
        dir = user.root.make_subdirectory(u'xyz')
        self.assertRaises(errors.NoPermission,
                          dir.change_public_access, True)

    def test_change_public_access_directory(self):
        """Test that directories can be made public if explicitly requested."""
        user = self.create_user()
        a_dir = user.root.make_subdirectory(u'xyz')
        # It has no public ID
        self.assertEqual(a_dir.public_id, None)
        # It now has a public ID
        self.assertEqual(
            a_dir.change_public_access(True, True).public_id, 1)

    def test_undelete(self):
        """Test various ways of restoring data."""
        user = self.create_user(max_storage_bytes=1000)
        size = 300
        file = self.create_file(user, size=size)
        self.assertEqual(user.get_quota().free_bytes, 700)
        file.delete()
        self.assertEqual(user.get_quota().free_bytes, 1000)
        file.restore()
        self.assertEqual(user.get_quota().free_bytes, 700)
        file.delete()
        #use the restore all which will restore the file in a special directory
        rstore_dir = user.volume().undelete_all(u"RestoreHere")
        self.assertEqual(user.get_quota().free_bytes, 700)
        node = user.volume().get_node(file.id)
        self.assertTrue(node.full_path.startswith(rstore_dir.full_path))
        #udfs and restores
        udf = user.make_udf(u"~/Something")
        file = self.create_file(user, size=size, udf=udf)
        self.assertEqual(user.get_quota().free_bytes, 400)
        file.delete()
        self.assertEqual(user.get_quota().free_bytes, 700)
        #this directory will be in the UDF volume, not root
        rstore_dir = user.volume(udf.id).undelete_all(u"RestoreHere")
        self.assertEqual(user.get_quota().free_bytes, 400)
        node = user.volume().get_node(file.id)
        self.assertEqual(node.volume_id, udf.id)
        self.assertTrue(node.full_path.startswith(rstore_dir.full_path))

    def test_undelete_above_quota(self):
        """Fail to restore if there's not enough quota."""
        max_quota = 200
        file_size = 100
        user = self.create_user(max_storage_bytes=max_quota)

        self.create_file(user, size=file_size)
        file2 = self.create_file(user, size=file_size)

        file2.delete()

        self.create_file(user, size=file_size)
        self.assertEqual(user.get_quota().free_bytes, 0)

        try:
            user.volume().undelete_all(u"RestoreHere")
        except errors.QuotaExceeded:
            self.fail("Shouldn't have failed to undelete when exceeding "
                      "quota.")
        else:
            self.assertEqual(user.get_quota().free_bytes, 0)

    def test_reusable_content(self):
        """Test StorageUser.is_reusable_content."""
        user = self.create_user()
        mocker = Mocker()
        gw = mocker.mock()
        expect(gw.is_reusable_content('hash_value', 'magic_hash'))
        user._gateway = gw
        with mocker:
            user.is_reusable_content('hash_value', 'magic_hash')

    def test_node_make_content(self):
        """Test the make_content call in the node."""
        user = self.create_user()
        filenode = user.root.make_file(u"A new file")
        ohash, nhash, crc32, size = "old_hash new_hash crc32 size".split()
        deflated, skey, magic = "deflated_size storage_key magic_hash".split()
        mocker = Mocker()

        # it needs to be reloaded
        load = mocker.mock()
        expect(load())
        filenode._load = load

        # the make content call to the gateway
        gw = mocker.mock()
        new_node = object()
        expect(gw.make_content(filenode.id, ohash, nhash, crc32, size,
                               deflated, skey, magic)).result(new_node)
        filenode._gateway = gw

        # it needs to copy the stuff to self
        copy = mocker.mock()
        expect(copy(new_node))
        filenode._copy = copy

        with mocker:
            r = filenode.make_content(ohash, nhash, crc32, size,
                                      deflated, skey, magic)
        self.assertTrue(r is filenode)

    def test_get_photo_directories(self):
        """Make file with contentblob."""
        user = self.create_user(max_storage_bytes=200000)
        hash = get_fake_hash()
        key = uuid.uuid4()
        crc = 12345
        size = 100
        dsize = 10000
        a = user.root.make_subdirectory(u"a")
        ab = a.make_subdirectory(u"b")
        ab.change_public_access(True, allow_directory=True)
        b = user.root.make_subdirectory(u"b")
        a.make_file_with_content(u"file1.jpg", hash, crc, size, dsize, key)
        a.make_file_with_content(u"file2.jpg", hash, crc, size, dsize, key)
        a.make_file(u"file3.jpg")
        a.make_file(u"file3.txt")
        b.make_file_with_content(u"file1.txt", hash, crc, size, dsize, key)
        b.make_file_with_content(u"file2.txt", hash, crc, size, dsize, key)
        # these should not show up
        b.make_file(u"file3.jpg")
        b.make_file(u"file3.jpg")
        ab.make_file_with_content(u"file1.txt", hash, crc, size, dsize, key)
        ab.make_file_with_content(u"file2.jpg", hash, crc, size, dsize, key)
        ab.make_file(u"file3.jpg")
        dirs = user.get_photo_directories()
        self.assertEqual(len(dirs), 2)
        self.assertTrue('/a' in [d.full_path for d in dirs])
        self.assertTrue('/a/b' in [d.full_path for d in dirs])
        self.assertTrue('/b' not in [d.full_path for d in dirs])
        # make sure public_key and public_uuid are set correctly
        public_a, = [d for d in dirs if d.full_path == '/a']
        public_ab, = [d for d in dirs if d.full_path == '/a/b']
        self.assertEqual(public_a.public_key, None)
        self.assertEqual(public_ab.public_key, ab.public_key)

    def test_get_directories_with_mimetypes(self):
        """Make file with contentblob."""
        user = self.create_user(max_storage_bytes=200000)
        dirs = user.volume().get_directories_with_mimetypes([u'image/jpeg'])
        self.assertEqual(len(dirs), 0)
        hash = get_fake_hash()
        key = uuid.uuid4()
        crc = 12345
        size = 100
        dsize = 10000
        a = user.root.make_subdirectory(u"a")
        ab = a.make_subdirectory(u"b")
        b = user.root.make_subdirectory(u"b")
        a.make_file_with_content(u"file1.jpg", hash, crc, size, dsize, key)
        a.make_file_with_content(u"file2.jpg", hash, crc, size, dsize, key)
        a.make_file(u"file3.jpg")
        a.make_file(u"file3.txt")
        b.make_file_with_content(u"file1.txt", hash, crc, size, dsize, key)
        b.make_file_with_content(u"file2.jpg", hash, crc, size, dsize, key)
        b.make_file(u"file3.jpg")
        b.make_file(u"file3.txt")
        ab.make_file_with_content(u"file1.txt", hash, crc, size, dsize, key)
        ab.make_file_with_content(u"file2.txt", hash, crc, size, dsize, key)
        ab.make_file(u"file3.jpg")
        dirs = user.volume().get_directories_with_mimetypes([u'image/jpeg'])
        self.assertEqual(len(dirs), 2)
        self.assertTrue('/a' in [d.full_path for d in dirs])
        self.assertTrue('/b' in [d.full_path for d in dirs])
        dirs = user.volume().get_directories_with_mimetypes([u'text/plain'])
        self.assertEqual(len(dirs), 2)
        self.assertTrue('/a/b' in [d.full_path for d in dirs])
        self.assertTrue('/b' in [d.full_path for d in dirs])
        dirs = user.volume().get_directories_with_mimetypes(
            [u'image/jpeg', u'text/plain'])
        self.assertEqual(len(dirs), 3)
        self.assertTrue('/a' in [d.full_path for d in dirs])
        self.assertTrue('/a/b' in [d.full_path for d in dirs])
        self.assertTrue('/b' in [d.full_path for d in dirs])


class GenerationsDAOTestCase(StorageDALTestCase):
    """Test generation specifics from DAO."""

    def test_get_delta(self):
        """Test basic generations delta.

        Most of this is all tested in the gateway, no details here.
        """
        user = self.create_user(max_storage_bytes=1000)
        nodes = [user.root.make_file(u"name%s" % i) for i in range(10)]
        generation, free_bytes, delta = user.volume().get_delta(0)
        self.assertEqual(generation, delta[-1].generation)
        #we're not taking up any space with these empty files
        self.assertEqual(free_bytes, 1000)
        #10 changes
        self.assertEqual(len(delta), 10)
        for n in nodes:
            self.assertTrue(n in delta)

    def test_delta_info(self):
        """A basic test of free_bytes and generation from deltas."""
        user = self.create_user(max_storage_bytes=1000)
        test_file = self.obj_factory.make_file(user, user.root, u"file.txt")
        file_size = test_file.content.size
        generation, free_bytes, delta = user.volume().get_delta(1)
        self.assertEqual(len(delta), 1)
        self.assertEqual(free_bytes, 1000 - file_size)
        self.assertEqual(generation, test_file.generation)
        self.assertEqual(delta[0], test_file)

    def test_delta_info_multi(self):
        """Test of free_bytes and generation from deltas with many changes."""
        user = self.create_user(max_storage_bytes=1000)
        mfc = lambda u, i: self.obj_factory.make_file(u, u.root, u"f%s" % i)
        files = [mfc(user, i) for i in range(10)]
        file_size = files[0].content.size
        new_gen = user.volume().get_volume().generation
        generation, free_bytes, delta = user.volume().get_delta(1, limit=5)
        self.assertEqual(len(delta), 5)
        self.assertEqual(free_bytes, 1000 - file_size * 10)
        self.assertEqual(generation, new_gen)
        self.assertEqual(delta, files[:5])
        start_gen = delta[-1].generation
        generation, free_bytes, delta = user.volume().get_delta(start_gen)
        self.assertEqual(len(delta), 5)
        self.assertEqual(free_bytes, 1000 - file_size * 10)
        self.assertEqual(generation, new_gen)
        self.assertEqual(delta, files[-5:])

    def test_get_from_scratch(self):
        """Test get_from_scratch."""
        user = self.create_user(max_storage_bytes=1000)
        root = user.root.load()
        mfc = lambda u, i: self.obj_factory.make_file(u, root, u"f%s" % i)
        files = [mfc(user, i) for i in range(10)]
        file_size = files[0].content.size
        new_gen = user.volume().get_volume().generation
        files_with_root = [root] + files
        generation, free_bytes, delta = user.volume().get_from_scratch()
        self.assertEqual(len(delta), 11)
        self.assertEqual(free_bytes, 1000 - file_size * 10)
        self.assertEqual(generation, new_gen)
        self.assertEqual(delta[0], root)
        self.assertEqual(delta, files_with_root)
        #delete the first 5 files and check again
        for f in files[:5]:
            f.delete()
        new_gen = user.volume().get_volume().generation
        generation, free_bytes, delta = user.volume().get_from_scratch()
        self.assertEqual(len(delta), 6)
        self.assertEqual(free_bytes, 1000 - file_size * 5)
        self.assertEqual(generation, new_gen)
        self.assertEqual(delta[0], root)
        files_with_root = [root] + files[-5:]
        self.assertEqual(delta, files_with_root)

    def test_SharedDirectory_get_generation(self):
        """Test for SharedDirectory.get_generation method."""
        user = self.create_user()
        user2 = self.create_user(id=2, username=u"user2")
        share = user.root.share(user2.id, u"ShareName")
        share = user2.get_share(share.id)
        share.accept()
        self.assertEqual(0, share.get_generation())
        user.root.make_file(u"a file in a share")
        self.assertEqual(1, share.get_generation())


class TestSQLStatementCount(StorageDALTestCase):
    """Test the number of SQL statements issued by some critical operations.

    The tests here should just assert that the number of SQL statements issued
    by some performance-sensitive operations are what we expect. This is
    necessary because when using an ORM it's way too easy to make changes that
    seem innocuous but in fact affect the performance in a significant (and
    bad) way. When that happens, one or more tests here may break and
    developers will then be forced to assess the consequences of their
    changes on those operations, and either provide a good reason for them or
    tweak their changes to avoid the extra SQL statement(s).

    When any of those tests fail they'll print all the SQL statements issued
    within the StormStatementRecorder() with block, to make it easy to detect
    what are the new queries being issued and where they're coming from.
    """

    mimetype = u'image/jpeg'

    def _flush_shard_store(self):
        """Flushes the default shard store used in tests."""
        get_shard_store('shard0').flush()

    def _create_directory_with_five_files(self):
        """Creates a DirectoryNode with 5 files inside it."""
        user = self.obj_factory.make_user()
        directory = user.root.make_subdirectory(u'test')
        for i in range(0, 5):
            self.obj_factory.make_file(
                parent=directory, mimetype=self.mimetype)
        self._flush_shard_store()
        return directory

    def test_move_directory_with_files(self):
        """Move a directory with files inside it."""
        directory = self._create_directory_with_five_files()
        new_parent = directory.owner.root.make_subdirectory(u'test2')
        with StormStatementRecorder() as recorder:
            directory.move(new_parent.id, directory.name)
            self._flush_shard_store()
        self.assertEqual(recorder.count, 19)

    def test_delete_directory_with_files(self):
        """Delete a directory with files inside it."""
        directory = self._create_directory_with_five_files()
        with StormStatementRecorder() as recorder:
            directory.delete(cascade=True)
            self._flush_shard_store()
        self.assertEqual(recorder.count, 17)

    def test_delete_file(self):
        """Delete a file."""
        f = self.obj_factory.make_file(mimetype=self.mimetype)
        self._flush_shard_store()
        with StormStatementRecorder() as recorder:
            f.delete()
            self._flush_shard_store()
        self.assertEqual(recorder.count, 10)

    # TODO: Optimize dao.DirectoryNode.make_file_with_content(); there should
    # be lots of low-hanging fruit there that would allow us to reduce the
    # number of queries it issues.
    def test_make_file_with_content(self):
        """Create a file with content."""
        user = self.obj_factory.make_user()
        directory = user.root.make_subdirectory(u'test')
        self._flush_shard_store()
        hash_ = get_fake_hash()
        name = self.obj_factory.get_unique_unicode()
        size = self.obj_factory.get_unique_integer()
        crc32 = self.obj_factory.get_unique_integer()
        storage_key = uuid.uuid4()
        with StormStatementRecorder() as recorder:
            directory.make_file_with_content(
                name, hash_, crc32, size, size, storage_key,
                mimetype=self.mimetype)
            self._flush_shard_store()
        self.assertEqual(recorder.count, 21)


class StormStatementRecorder(object):
    """Storm tracer class to log executed statements."""

    def __init__(self):
        self._statements = []

    def connection_raw_execute(self, connection, raw_cursor,
                               statement, params):
        """Executes raw queries."""
        statement_to_log = statement
        if params:
            # There are some bind parameters so we want to insert them into
            # the sql statement so we can log the statement.
            query_params = list(Connection.to_database(params))
            # We need to ensure % symbols used for LIKE statements etc are
            # properly quoted or else the string format operation will fail.
            quoted_statement = re.sub(
                "%%%", "%%%%", re.sub("%([^s])", r"%%\1", statement))
            # We need to massage the query parameters a little to deal with
            # string parameters which represent encoded binary data.
            param_strings = [repr(p) if isinstance(p, basestring) else p
                             for p in query_params]
            statement_to_log = quoted_statement % tuple(param_strings)

        # Stash some information for logging at the end of the
        # SQL execution.
        connection._statement_info = (
            'DB:%s' % connection._database._dsn,
            statement_to_log)

    def connection_raw_execute_success(self, connection, raw_cursor,
                                       statement, params):
        """Aggregates success info."""
        info = getattr(connection, '_statement_info', None)
        if info is not None:
            connection._statement_info = None
            self._statements.append(info)

    def connection_raw_execute_error(self, connection, raw_cursor,
                                     statement, params, error):
        """Aggregates error info."""
        # Since we are just logging durations, we execute the same
        # hook code for errors as successes.
        self.connection_raw_execute_success(
            connection, raw_cursor, statement, params)

    @property
    def count(self):
        """Returns the quantity of statements."""
        return len(self._statements)

    @property
    def statements(self):
        """Returns the list of statements."""
        return [record[1] for record in self._statements]

    def __enter__(self):
        install_tracer(self)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        remove_tracer_type(StormStatementRecorder)

    def __str__(self):
        return str(self.statements)


class UserDAOTest(StorageDALTestCase):
    """Tests for UserDAO."""

    def test_gets_a_random_user_id(self):
        """Tests if the DAO retrieves a random ID."""
        user_dao = dao.UserDAO()

        user = self.obj_factory.make_user()

        expected_id = user.id
        actual_id = user_dao.get_random_user_id()

        self.assertEqual(actual_id, expected_id)

    def test_makes_sure_always_to_return_a_random_id(self):
        """All user IDs should be retrieved.

        (Given their probability to raise up in the random search).

        """
        user_dao = dao.UserDAO()

        user1 = self.obj_factory.make_user()
        user2 = self.obj_factory.make_user()

        searches = 100
        expected_user_ids = sorted(set((user1.id, user2.id)))
        retrieved_ids = [user_dao.get_random_user_id()
                         for i in xrange(searches)]
        actual_user_ids = sorted(set(retrieved_ids))

        self.assertEqual(len(retrieved_ids), 100)
        self.assertEqual(actual_user_ids, expected_user_ids)
