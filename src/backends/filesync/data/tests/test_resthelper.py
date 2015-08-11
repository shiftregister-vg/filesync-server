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

"""Test the resthelper."""

from datetime import datetime
import logging
import operator
import os.path
import unittest

import uuid
from metrics.tests import FakeMetrics
from backends.filesync.data import errors
from backends.filesync.data.testing.testcase import StorageDALTestCase
from backends.filesync.data.testing.testdata import get_test_contentblob
from backends.filesync.data.resthelper import (
    CannotPublishDirectory,
    FileNodeHasNoChildren,
    InvalidKind,
    ResourceMapper,
    RestHelper,
    date_formatter,
)
from ubuntuone.devtools.handlers import MementoHandler


class MockUser(object):
    """Fake user for testing."""
    id = 0
    visible_name = "Bob Smith"


class MockQuota(object):
    """Fake quota for testing."""
    used_storage_bytes = 10
    free_bytes = 2 ** 8
    max_storage_bytes = 2 ** 10


class MockVolume(object):
    """Fake Volume for testing."""
    generation = 1
    id = uuid.uuid4()
    is_root = False
    path = "~/Documents"
    when_created = datetime.utcnow()


class MockNode(object):
    """Fake Node for testing."""
    id = uuid.uuid4()
    nodekey = 'nodekey'
    kind = 'File'
    path = '/a/b/c/d'
    full_path = '/a/b/c/d/file.txt'
    name = 'file.txt'
    content_hash = 'abcdefg'
    when_created = datetime.utcnow()
    when_last_modified = datetime.utcnow()
    generation = 1
    generation_created = 1
    mimetype = 'text'
    has_children = lambda _: False
    public_url = "public url"
    is_public = False
    vol_type = 'root'
    status = 'Live'
    vol_udf = MockVolume()
    vol_udf.is_root = True
    vol_udf.path = "~/Ubuntu One"

    class _content(object):
        """Fake content within a Node."""
        size = 12000
    content = _content()


class ResourceMapperTestCase(unittest.TestCase):
    """Test the resource mapper."""
    def setUp(self):
        super(ResourceMapperTestCase, self).setUp()
        self.mapper = ResourceMapper()

    def test_mapping(self):
        """Test mapping."""
        self.assertEquals(self.mapper.user(), '')
        self.assertEquals(self.mapper.volume('~/1'), '/volumes/~/1')
        self.assertEquals(self.mapper.node('~/1', '/x'), '/~/1/x')

    def test_mapping_override(self):
        """Test mapping."""
        self.mapper.root = ''
        self.mapper.mapping['NODE_INFO'] = '/n/%(node_path)s'
        self.assertEquals(self.mapper.user(), '')
        self.assertEquals(self.mapper.volume(1), '/volumes/1')
        self.assertEquals(self.mapper.node('a', 'b'), '/n/b')

    def test_user_repr(self):
        """Test Rest conversion of a user."""
        user = MockUser()
        quota = MockQuota()
        udf = MockVolume()
        info = self.mapper.user_repr(user, quota, [udf])
        self.assertEquals(info['user_id'], user.id)
        self.assertEquals(info['visible_name'], user.visible_name)
        self.assertEquals(info['used_bytes'], quota.used_storage_bytes)
        self.assertEquals(info['max_bytes'], quota.max_storage_bytes)
        self.assertEquals(info['root_node_path'],
                          self.mapper.node('~/Ubuntu One'))
        self.assertEquals(info['user_node_paths'],
                          [self.mapper.node(udf.path)])

    def test_volume_repr(self):
        """Test Rest conversion of a volume."""
        udf = MockVolume()
        info = self.mapper.volume_repr(udf)
        self.assertEquals(info['resource_path'], u'/volumes/~/Documents')
        self.assertEquals(info['type'], u'root' if udf.is_root else u'udf')
        self.assertEquals(info['path'], udf.path)
        self.assertEquals(info['generation'], udf.generation)
        self.assertEquals(info['node_path'], self.mapper.node(udf.path))
        self.assertEquals(info['when_created'],
                          date_formatter(udf.when_created))

    def test_volume_with_delta_repr0(self):
        """Test Rest conversion of a vol with delta information, no nodes."""
        udf = MockVolume()
        nodes = []
        info = self.mapper.volume_repr(
            volume=udf, from_generation=0, nodes=nodes)
        self.assertEquals(info['resource_path'], u'/volumes/~/Documents')
        self.assertEquals(info['type'], u'root' if udf.is_root else u'udf')
        self.assertEquals(info['path'], udf.path)
        self.assertEquals(info['generation'], udf.generation)
        self.assertEquals(info['node_path'], self.mapper.node(udf.path))
        self.assertEquals(info['when_created'],
                          date_formatter(udf.when_created))
        self.assertEquals(info['delta']['from_generation'], 0)
        self.assertEquals(info['delta']['nodes'], nodes)

    def test_volume_with_delta_repr1(self):
        """Test Rest conversion of a volume with delta innformation,
        with nodes."""
        udf = MockVolume()
        nodes = [MockNode(), MockNode()]
        info = self.mapper.volume_repr(
            volume=udf, from_generation=0, nodes=nodes)
        self.assertEquals(
            info['delta']['from_generation'], 0)
        self.assertEquals(
            info['delta']['nodes'],
            [self.mapper.node_repr(node) for node in nodes])

    def test_file_node_repr(self):
        """Test Rest conversion of a file node."""
        f1 = MockNode()
        info = self.mapper.node_repr(f1)
        self.assertEquals(info['key'], f1.nodekey)
        self.assertEquals(info['kind'], f1.kind.lower())
        self.assertEquals(info['path'], f1.full_path)
        self.assertEquals(info['hash'], f1.content_hash)
        self.assertEquals(info['when_created'],
                          date_formatter(f1.when_created))
        self.assertEquals(info['when_changed'],
                          date_formatter(f1.when_last_modified))
        self.assertEquals(info['generation'], f1.generation)
        self.assertEquals(info['generation_created'], f1.generation_created)
        self.assertEquals(info['public_url'], f1.public_url)
        self.assertEquals(info['is_public'], f1.is_public)
        self.assertEquals(info['parent_path'],
                          "/~/Ubuntu One/a/b/c/d")
        self.assertEquals(info['volume_path'],
                          "/volumes/~/Ubuntu One")
        self.assertEquals(info['content_path'],
                          u'/content/~/Ubuntu One/a/b/c/d/file.txt')
        # make sure file specific rules apply
        self.assertTrue('has_children' not in info)
        self.assertEquals(info['is_live'], True)

    def test_dir_node_repr(self):
        """Utility method to test Rest conversion of a directory node."""
        f1 = MockNode()
        f1.kind = 'Directory'
        info = self.mapper.node_repr(f1)
        self.assertEquals(info['key'], f1.nodekey)
        self.assertEquals(info['kind'], f1.kind.lower())
        self.assertEquals(info['path'], f1.full_path)
        self.assertEquals(info['when_created'],
                          date_formatter(f1.when_created))
        self.assertEquals(info['when_changed'],
                          date_formatter(f1.when_last_modified))
        self.assertEquals(info['generation'], f1.generation)
        self.assertEquals(info['generation_created'], f1.generation_created)
        self.assertEquals(info['parent_path'],
                          "/~/Ubuntu One/a/b/c/d")
        self.assertEquals(info['volume_path'],
                          "/volumes/~/Ubuntu One")
        self.assertEquals(info['content_path'],
                          u'/content/~/Ubuntu One/a/b/c/d/file.txt')
        # make sure directory specific rules apply
        self.assertTrue('hash' not in info)
        self.assertTrue('is_public' not in info)
        self.assertTrue('public_url' not in info)
        self.assertTrue('has_children' in info)
        self.assertEquals(info['is_live'], True)

    def test_root_dir_node_repr(self):
        """Utility method to test Rest conversion of a root directory node."""
        f1 = MockNode()
        f1.kind = 'Directory'
        f1.name = ""
        f1.path = '/'
        f1.full_path = "/"
        info = self.mapper.node_repr(f1)
        self.assertEquals(info['key'], f1.nodekey)
        self.assertEquals(info['kind'], f1.kind.lower())
        self.assertEquals(info['path'], f1.full_path)
        self.assertEquals(info['when_created'],
                          date_formatter(f1.when_created))
        self.assertEquals(info['when_changed'],
                          date_formatter(f1.when_last_modified))
        self.assertEquals(info['generation'], f1.generation)
        self.assertEquals(info['generation_created'], f1.generation_created)
        self.assertEquals(info['parent_path'], None)
        self.assertEquals(info['volume_path'],
                          "/volumes/~/Ubuntu One")
        self.assertEquals(info['content_path'],
                          u'/content/~/Ubuntu One')
        # make sure directory specific rules apply
        self.assertTrue('hash' not in info)
        self.assertTrue('is_public' not in info)
        self.assertTrue('public_url' not in info)
        self.assertTrue('has_children' in info)
        self.assertEquals(info['is_live'], True)


class RestHelperTestCase(StorageDALTestCase):
    """Test the resthelper."""

    def setUp(self):
        super(RestHelperTestCase, self).setUp()
        self.handler = MementoHandler()
        self.user = self.obj_factory.make_user(
            1, u"bob", u"bobby boo", 2 * (2 ** 30))
        self.mapper = ResourceMapper()
        logger = logging.getLogger("test")
        logger.addHandler(self.handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        self.helper = RestHelper(self.mapper, logger=logger)
        self.store = self.get_shard_store(self.user.shard_id)

    def test_GET_user(self):
        """Test for dao to REST conversion of user"""
        info = self.helper.get_user(self.user)
        self.assertEqual(info,
                         self.mapper.user_repr(self.user,
                                               self.user.get_quota()))
        user_id = repr(self.user.id)
        self.assertTrue(self.handler.check_info("get_quota", user_id))
        self.assertTrue(self.handler.check_info("get_udfs", user_id))

    def test_GET_user_with_udf(self):
        """Test get_user with udf."""
        udf = self.user.make_udf(u"~/Documents")
        info = self.helper.get_user(self.user)
        self.assertEqual(info, self.mapper.user_repr(self.user,
                                                     self.user.get_quota(),
                                                     [udf]))

    def test_GET_volume(self):
        """Test get_volume."""
        volume_path = u"~/Documents"
        udf = self.user.make_udf(volume_path)
        info = self.helper.get_volume(user=self.user,
                                      volume_path=volume_path)
        self.assertEqual(info, self.mapper.volume_repr(udf))
        ids = [repr(x) for x in [self.user.id, unicode(volume_path)]]
        self.assertTrue(self.handler.check_info("get_udf_by_path", *ids))

    def test_GET_volume_with_delta0(self):
        """Test get_volume with delta, no nodes"""
        volume_path = u"~/Documents"
        udf = self.user.make_udf(volume_path)
        info = self.helper.get_volume(
            user=self.user,
            volume_path=volume_path,
            from_generation=0)
        self.assertEqual(
            info,
            self.mapper.volume_repr(
                volume=udf,
                from_generation=0,
                nodes=[]))
        ids = [repr(x) for x in [self.user.id, unicode(volume_path)]]
        self.assertTrue(self.handler.check_info("get_udf_by_path", *ids))
        ids = [repr(x) for x in [self.user.id, udf.id, 0]]
        self.assertTrue(self.handler.check_info("get_delta", *ids))

    def test_GET_volume_with_delta1(self):
        """Test get_volume with delta, with nodes"""
        volume_path = u"~/Documents"
        self.user.make_udf(volume_path)
        node0 = self.user.make_file_by_path(u"~/Documents/file0.txt")
        node1 = self.user.make_file_by_path(u"~/Documents/file1.txt")
        info = self.helper.get_volume(
            user=self.user,
            volume_path=volume_path,
            from_generation=0)
        udf = self.user.get_udf_by_path(u'~/Documents')
        self.assertEqual(
            info,
            self.mapper.volume_repr(
                volume=udf,
                from_generation=0,
                nodes=[node0, node1]))
        node0.delete()
        info = self.helper.get_volume(
            user=self.user,
            volume_path=volume_path,
            from_generation=0)
        self.assertEqual(
            info['delta']['nodes'][1]['is_live'], False)

    def test_PUT_volume(self):
        """Test put volume."""
        path = u"~/Documents"
        info = self.helper.put_volume(user=self.user, path=path)
        udf = self.user.get_udf_by_path(path)
        self.assertEquals(self.mapper.volume_repr(udf), info)
        ids = [repr(x) for x in [self.user.id, unicode(path)]]
        self.assertTrue(self.handler.check_info("make_udf", *ids))

    def test_GET_node_directory(self):
        """Test for get_node a directory node."""
        root = self.user.volume().get_root()
        d1 = root.make_subdirectory(u"dir1")
        full_path = u"~/Ubuntu One" + d1.full_path
        info = self.helper.get_node(user=self.user, node_path=full_path)
        self.assertEquals(info, self.mapper.node_repr(d1))

    def test_GET_node_file(self):
        """Test for  get_node conversion of a file node."""
        root = self.user.volume().get_root()
        f1 = root.make_file(u"file.txt")
        volume_path = u"~/Ubuntu One"
        full_path = volume_path + f1.full_path
        info = self.helper.get_node(user=self.user, node_path=full_path)
        self.assertEquals(info, self.mapper.node_repr(f1))
        ids = [repr(x) for x in [self.user.id, full_path, True]]
        self.assertTrue(self.handler.check_info("get_node_by_path", *ids))

    def test_GET_volumes(self):
        """Test get_volume."""
        udfs = [self.user.make_udf(u"~/Udf%s" % i) for i in range(10)]
        info = self.helper.get_volumes(self.user)
        root = self.user.volume().get_volume()
        expected_repr = [self.mapper.volume_repr(root)]
        expected_repr.extend([self.mapper.volume_repr(u) for u in udfs])
        info = info.sort(key=operator.itemgetter('path'))
        expected_repr = expected_repr.sort(key=operator.itemgetter('path'))
        self.assertEquals(info, expected_repr)
        self.assertTrue(self.handler.check_info("get_volume",
                                                repr(self.user.id)))
        self.assertTrue(self.handler.check_info(
            "get_udfs", repr(self.user.id)))

    def test_DELETE_volume(self):
        """Test delete_volume."""
        udf = self.user.make_udf(u"~/Documents")
        self.helper.delete_volume(self.user, udf.path)
        self.assertRaises(errors.DoesNotExist,
                          self.user.get_udf, udf.id)
        ids = [repr(x) for x in [self.user.id, udf.path]]
        self.assertTrue(self.handler.check_info("get_udf_by_path", *ids))
        ids = [repr(x) for x in [self.user.id, udf.id]]
        self.assertTrue(self.handler.check_info("delete_udf", *ids))

    def test_GET_node0(self):
        """Test simple node info."""
        root = self.user.volume().get_root()
        f1 = root.make_file(u"file.txt")
        full_path = u"~/Ubuntu One" + f1.full_path
        info = self.helper.get_node(self.user, full_path)
        self.assertEqual(info, self.mapper.node_repr(f1))

    def test_GET_node1(self):
        """Test child node info."""
        root = self.user.volume().get_root()
        d1 = root.make_subdirectory(u"Documents")
        f1 = d1.make_file(u"file.txt")
        full_path = u"~/Ubuntu One" + os.path.join(d1.full_path, f1.name)
        info = self.helper.get_node(self.user, full_path)
        self.assertEquals(info['key'], f1.nodekey)
        self.assertEquals(info['path'], f1.full_path)

    def test_GET_node2(self):
        """Test simple udf node info."""
        self.user.make_udf(u"~/Documents")
        udf = self.user.get_node_by_path(u"~/Documents")
        f1 = udf.make_file(u"file.txt")
        full_path = u"~/Documents" + f1.full_path
        info = self.helper.get_node(self.user, full_path)
        self.assertEquals(info['key'], f1.nodekey)
        self.assertEquals(info['path'], f1.full_path)

    def test_GET_node3(self):
        """Test child udf node info."""
        self.user.make_udf(u"~/Documents")
        udf = self.user.get_node_by_path(u"~/Documents")
        d1 = udf.make_subdirectory(u"slides")
        f1 = d1.make_file(u"file.txt")
        full_path = u"~/Documents" + f1.full_path
        info = self.helper.get_node(self.user, full_path)
        self.assertEqual(info, self.mapper.node_repr(f1))

    def test_DELETE_node(self):
        """Test delete_volume."""
        root = self.user.volume().get_root()
        f1 = root.make_file(u"file.txt")
        full_path = u"~/Ubuntu One" + f1.full_path
        self.helper.delete_node(self.user, full_path)
        self.assertRaises(errors.DoesNotExist,
                          self.user.volume().get_node, f1.id)
        ids = [repr(x) for x in [self.user.id, full_path]]
        self.assertTrue(self.handler.check_info("get_node_by_path", *ids))
        ids = [repr(x) for x in [self.user.id, f1.id, True]]
        self.assertTrue(self.handler.check_info("delete", *ids))

    def test_GET_node_children(self):
        """Test get_node_children."""
        root = self.user.volume().get_root()
        files = [root.make_file(u"file%s.txt" % i) for i in range(10)]
        full_path = u"~/Ubuntu One"
        root.load()
        expected = self.mapper.node_repr(root)
        expected['children'] = [self.mapper.node_repr(n) for n in files]
        info = self.helper.get_node(
            self.user, full_path, include_children=True)
        self.assertEquals(info, expected)
        ids = [repr(x) for x in [self.user.id, full_path, True]]
        self.assertTrue(self.handler.check_info("get_node", *ids))
        ids = [repr(x) for x in [self.user.id, root.id, True]]
        self.assertTrue(self.handler.check_info("get_children", *ids))

    def test_GET_file_node_children(self):
        """Test get_node_children."""
        self.user.volume().root.make_file(u"file.txt")
        self.assertRaises(FileNodeHasNoChildren,
                          self.helper.get_node, self.user,
                          "~/Ubuntu One/file.txt", include_children=True)

    def test_PUT_node_is_public(self):
        """Test put node to make existing file public."""
        original_metrics = self.helper.metrics
        self.helper.metrics = FakeMetrics()
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        self.assertEqual(node.is_public, False)
        node_rep = self.mapper.node_repr(node)
        node_rep['is_public'] = True
        info = self.helper.put_node(self.user, new_file_path, node_rep)

        ids = [repr(x) for x in [self.user.id, new_file_path]]
        self.assertTrue(self.handler.check_info("get_node_by_path", *ids))
        ids = [repr(x) for x in [self.user.id, node.id, True]]
        self.assertTrue(self.handler.check_info("change_public_access", *ids))

        node.load()
        self.assertEqual(node.is_public, True)
        self.assertEqual(info, self.mapper.node_repr(node))
        info['is_public'] = False
        info = self.helper.put_node(self.user, new_file_path, info)
        node.load()
        self.assertEqual(node.is_public, False)
        self.assertEqual(info, self.mapper.node_repr(node))
        self.helper.metrics.make_all_assertions(
            self, 'resthelper.put_node.change_public')
        self.helper.metrics = original_metrics

    def test_GET_public_files(self):
        """Test public_files returns the list of public files."""
        self.assertEqual(self.helper.get_public_files(self.user), [])
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        self.assertEqual(node.is_public, False)
        node_rep = self.mapper.node_repr(node)
        node_rep['is_public'] = True
        info = self.helper.put_node(self.user, new_file_path, node_rep)
        self.assertEqual(self.helper.get_public_files(self.user), [info])
        self.assertTrue(self.handler.check_info("get_public_files",
                                                repr(self.user.id)))

    def test_PUT_node_is_public_directory(self):
        """Test put node to make existing file public."""
        dir_path = u"~/Ubuntu One/a/b/c"
        node = self.user.make_tree_by_path(dir_path)
        self.assertEqual(node.is_public, False)
        node_rep = self.mapper.node_repr(node)
        node_rep['is_public'] = True
        self.assertRaises(CannotPublishDirectory,
                          self.helper.put_node, self.user, dir_path, node_rep)

    def test_PUT_node_path(self):
        """Test put node with a new path."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        self.assertEqual(node.full_path, "/a/b/c/file.txt")
        node_rep = self.mapper.node_repr(node)
        new_path = "/a/newfile.txt"
        node_rep['path'] = new_path
        info = self.helper.put_node(self.user, new_file_path, node_rep)
        node.load()
        self.assertEqual(node.full_path, new_path)
        self.assertEqual(info, self.mapper.node_repr(node))
        ids = [repr(x) for x in [self.user.id, new_file_path]]
        self.assertTrue(self.handler.check_info("get_node_by_path", *ids))
        new_dir, new_name = os.path.split(new_path)
        ids = [repr(x) for x in [self.user.id, node.vol_id, unicode(new_dir)]]
        self.assertTrue(self.handler.check_info("get_node_by_path", *ids))
        ids = [repr(x) for x in [self.user.id, node.id, unicode(new_name)]]
        self.assertTrue(self.handler.check_info("move", *ids))

    def test_PUT_node_path_is_public(self):
        """Test put node with a new path and make it public."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        self.assertEqual(node.full_path, "/a/b/c/file.txt")
        node_rep = self.mapper.node_repr(node)
        node_rep['path'] = "/a/newfile.txt"
        node_rep['is_public'] = True
        info = self.helper.put_node(self.user, new_file_path, node_rep)
        node.load()
        self.assertEqual(node.is_public, True)
        self.assertEqual(node.full_path, "/a/newfile.txt")
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_is_public_partial(self):
        """Test put node."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        self.assertEqual(node.is_public, False)
        info = self.helper.put_node(self.user, new_file_path,
                                    {'is_public': True})
        node.load()
        self.assertEqual(node.is_public, True)
        self.assertEqual(info, self.mapper.node_repr(node))
        info = self.helper.put_node(self.user, new_file_path,
                                    {'is_public': False})
        node.load()
        self.assertEqual(node.is_public, False)
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_path_partial(self):
        """Test put node with a new path with partial info."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        info = self.helper.put_node(self.user, new_file_path,
                                    {'path': "/a/newfile.txt"})
        node.load()
        self.assertEqual(node.full_path, "/a/newfile.txt")
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_path_is_pulic_partial(self):
        """Test put node with a new path and make it public."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        info = self.helper.put_node(
            self.user, new_file_path,
            {'path': "/a/newfile.txt", 'is_public': True})
        node.load()
        self.assertEqual(node.full_path, "/a/newfile.txt")
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_do_nothing(self):
        """Test put_node with nothing to do."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        node = self.user.make_file_by_path(new_file_path)
        node_repr = self.mapper.node_repr(node)
        info = self.helper.put_node(self.user, new_file_path,
                                    dict(a=2, b='hi', c='ignored'))
        node.load()
        # here nothing is changed and the info returned
        # matches the existing node_repr
        self.assertEqual(info, node_repr)
        self.assertEqual(node_repr, self.mapper.node_repr(node))

    def test_PUT_node_new_file_magic(self):
        """Test put_node to make a new file with content."""
        cb = get_test_contentblob("FakeContent")
        cb.magic_hash = 'magic'
        self.store.add(cb)
        self.store.commit()
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        info = self.helper.put_node(
            self.user, new_file_path,
            {'kind': 'file', 'hash': cb.hash, 'magic_hash': 'magic'})
        node = self.user.get_node_by_path(new_file_path)
        self.assertEqual(node.kind, 'File')
        self.assertEqual(node.full_path, '/a/b/c/file.txt')
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_update_file_magic(self):
        """Test put_node to make a new file with content."""
        cb = get_test_contentblob("FakeContent")
        cb.magic_hash = 'magic'
        self.store.add(cb)
        self.store.commit()
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        info = self.helper.put_node(
            self.user, new_file_path,
            {'kind': 'file', 'hash': cb.hash, 'magic_hash': 'magic'})
        cb = get_test_contentblob("NewFakeContent")
        cb.magic_hash = 'magic2'
        self.store.add(cb)
        self.store.commit()
        info = self.helper.put_node(
            self.user, new_file_path,
            {'kind': 'file', 'hash': cb.hash, 'magic_hash': 'magic2'})
        node = self.user.get_node_by_path(new_file_path, with_content=True)
        self.assertEqual(node.kind, 'File')
        self.assertEqual(node.full_path, '/a/b/c/file.txt')
        self.assertEqual(info, self.mapper.node_repr(node))
        self.assertEqual(node.content.magic_hash, 'magic2')

    def test_PUT_node_new_file(self):
        """Test put_node to make a new file."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        info = self.helper.put_node(self.user, new_file_path,
                                    {'kind': 'file'})
        node = self.user.get_node_by_path(new_file_path)
        self.assertEqual(node.kind, 'File')
        self.assertEqual(node.full_path, '/a/b/c/file.txt')
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_new_directory(self):
        """Test put_node to make a new directory."""
        new_file_path = u"~/Ubuntu One/a/b/c/file.txt"
        info = self.helper.put_node(self.user, new_file_path,
                                    {'kind': 'directory'})
        node = self.user.get_node_by_path(new_file_path)
        self.assertEqual(node.kind, 'Directory')
        self.assertEqual(node.full_path, '/a/b/c/file.txt')
        self.assertEqual(info, self.mapper.node_repr(node))

    def test_PUT_node_exceptions(self):
        """Test put_node exceptions."""
        self.assertRaises(InvalidKind,
                          self.helper.put_node,
                          self.user, "~/Ubuntu one/x", {"kind": "ABC"})
        # PUT to a non existent node.
        self.assertRaises(errors.DoesNotExist,
                          self.helper.put_node,
                          self.user, "~/Ubuntu/x", {})
        # PUT to a non existent node.
        self.assertRaises(errors.DoesNotExist,
                          self.helper.put_node,
                          self.user, "~/Ubuntu One/x", {})
