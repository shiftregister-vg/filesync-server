# -*- coding: utf-8 -*-

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

"""Tests for generations-related operations."""

import calendar
import unittest

from twisted.internet import defer

from ubuntuone.storageprotocol import request, delta as protodelta
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storage.server import server


class GenerationsTestCase(TestWithDatabase):
    """Base Generations test case."""

    def assertEqualNode(self, delta_info, node):
        """Assert if a delta_info match with a node."""
        kind = protodelta.DIRECTORY
        if node.kind == 'File':
            kind = protodelta.FILE
        if node.parent_id is None:
            parent_id = None
        else:
            parent_id = str(node.parent_id)
        self.assertEqual(node.generation, delta_info.generation)
        self.assertTrue(delta_info.is_live)
        self.assertEqual(kind, delta_info.file_type)
        self.assertEqual(node.name, delta_info.name)
        self.assertEqual(request.ROOT, delta_info.share_id)
        self.assertEqual(str(node.id), delta_info.node_id)
        self.assertEqual(parent_id, delta_info.parent_id)
        self.assertFalse(delta_info.is_public)
        self.assertEqual(node.content_hash or '', delta_info.content_hash)
        content = node.content
        if content:
            self.assertEqual(content.crc32, delta_info.crc32)
            self.assertEqual(content.size, delta_info.size)
        else:
            self.assertEqual(0, delta_info.crc32)
            self.assertEqual(0, delta_info.size)
        timestamp = calendar.timegm(node.when_last_modified.timetuple())
        self.assertEqual(timestamp, delta_info.last_modified)


class TestGetDelta(GenerationsTestCase):
    """Tests for get delta responses."""

    def test_get_delta_empty(self):
        """Test for an empty delta response."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            req = yield client.get_delta(request.ROOT, 0)
            self.assertEqual(req.response, [])
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_get_delta_from_0(self):
        """Test for a simple (and full) delta response."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            root = self.usr0.root
            nodes = [root.make_file(u"name%s" % i) for i in range(5)]
            nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
            req = yield client.get_delta(request.ROOT, 0)
            self.assertEqual(len(req.response), len(nodes))
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 10)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_get_delta_from_middle(self):
        """Test for delta response from a specific generation."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            root = self.usr0.root
            nodes = [root.make_file(u"name%s" % i) for i in range(5)]
            nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
            from_generation = nodes[5].generation
            req = yield client.get_delta(request.ROOT, from_generation)
            self.assertEqual(len(req.response), len(nodes[6:]))
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 10)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_get_delta_from_last(self):
        """Test for delta response from last generation (empty)."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            nodes = [self.usr0.root.make_file(u"name%s" % i) for i in range(5)]
            from_generation = nodes[-1].generation
            req = yield client.get_delta(request.ROOT, from_generation)
            self.assertEqual(len(req.response), 0)
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 5)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_get_delta_partial(self):
        """Test for partial delta response."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            limit = 10
            self.patch(server.config.api_server, 'delta_max_size', limit)
            for i in range(20):
                self.usr0.root.make_file(u"name%s" % i)
            req = yield client.get_delta(request.ROOT, 5)
            self.assertEqual(len(req.response), limit)
            self.assertFalse(req.full)
            self.assertEqual(req.end_generation, 15)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    @unittest.skip('Delta not possible not implemented.')
    def test_get_delta_not_possible(self):
        """Test for delta not possible response."""
        self.fail(NotImplementedError('Not implemented yet'))

    def test_get_delta_info(self):
        """Test that the delta info is ok."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            nodes = [self.usr0.root.make_file(u"name_1")]
            nodes += [self.usr0.root.make_subdirectory(u"dir_1")]
            req = yield client.get_delta(request.ROOT, 0)
            self.assertEqual(len(req.response), 2)
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 2)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)
            self.assertEqualNode(req.response[0], nodes[0])
            self.assertEqualNode(req.response[1], nodes[1])

        return self.callback_test(auth, add_default_callbacks=True)

    def test_max_delta_info(self):
        """Test for multiple iterations in send_delta_info."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            self.patch(server.config.api_server, 'max_delta_info', 5)
            for i in range(20):
                self.usr0.root.make_file(u"name%s" % i)
            req = yield client.get_delta(request.ROOT, 5)
            self.assertEqual(len(req.response), 15)
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 20)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)


class TestRescanFromScratch(GenerationsTestCase):
    """Test RescanFromScratch Response."""

    def test_from_scratch_empty(self):
        """Test rescan_from_scratch for an empty volume."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            root = self.usr0.volume().get_root()
            req = yield client.get_delta(request.ROOT, from_scratch=True)
            self.assertEqual(len(req.response), 1)  # the root
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 0)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)
            self.assertEqualNode(req.response[0], root)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_from_scratch_ok(self):
        """Test rescan_from_scratch."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            root = self.usr0.root
            nodes = [root.make_file(u"name%s" % i) for i in range(5)]
            nodes += [root.make_subdirectory(u"dir%s" % i) for i in range(5)]
            for f in [root.make_file(u"name%s" % i) for i in range(5, 10)]:
                f.delete()
            dirs = [root.make_subdirectory(u"dir%s" % i) for i in range(5, 10)]
            for d in dirs:
                d.delete()
            from operator import attrgetter
            nodes.sort(key=attrgetter('path', 'name'))
            nodes.insert(0, self.usr0.volume().get_root())
            req = yield client.get_delta(request.ROOT, from_scratch=True)
            self.assertEqual(len(req.response), len(nodes))
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 30)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)
            for delta_info, node in zip(req.response, nodes):
                self.assertEqualNode(delta_info, node)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_max_delta_info(self):
        """Test for multiple iterations in send_delta_info."""

        @defer.inlineCallbacks
        def auth(client):
            yield client.dummy_authenticate("open sesame")
            # create some nodes to get a delta
            self.patch(server.config.api_server, 'max_delta_info', 5)
            for i in range(20):
                self.usr0.root.make_file(u"name%s" % i)
            req = yield client.get_delta(request.ROOT, 5)
            self.assertEqual(len(req.response), 15)
            self.assertTrue(req.full)
            self.assertEqual(req.end_generation, 20)
            self.assertEqual(req.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)
