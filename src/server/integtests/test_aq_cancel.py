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

"""Test cancellation of the action queue's commands."""

import os

from cStringIO import StringIO

from twisted.internet import defer, error

from ubuntuone.storageprotocol import request
from ubuntuone.storage.server.testing.aq_helpers import (
    FakeGetContent,
    NO_CONTENT_HASH,
    TestContentBase,
    aShareUUID,
    anEmptyShareList,
    anUUID,
)
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32
from ubuntuone.syncdaemon.marker import MDMarker as Marker


class AQCancelTestBase(TestContentBase):
    """Things common to TestCancel and TestCancelMarker."""

    def setUp(self):
        """Set up."""
        self.connlost_deferred = defer.Deferred()
        return super(AQCancelTestBase, self).setUp()

    def tearDown(self):
        """Tear thins down."""
        self.eq.push('SYS_NET_DISCONNECTED')
        self.eq.push('SYS_USER_DISCONNECT')
        return super(AQCancelTestBase, self).tearDown()

    def hiccup(self):
        """Hiccup the network."""
        self.eq.push('SYS_NET_DISCONNECTED')
        self.eq.push('SYS_USER_DISCONNECT')
        self.connlost_deferred.errback(error.ConnectionLost())
        self.eq.push('SYS_CONNECTION_LOST')
        self.eq.push('SYS_USER_CONNECT',
                     access_token=self.access_tokens['jack'])
        self.eq.push('SYS_NET_CONNECTED')
        return self.wait_for_nirvana(.1)


class TestCancel(AQCancelTestBase):
    """Cancellation of non-marker-related things."""

    def test_list_shares(self):
        """Hiccup the network in the middle of a list_shares."""
        def worker():
            """Async worker."""
            self.aq.list_shares()
            return self.hiccup()
        d = self.nuke_client_method('query', worker,
                                    lambda: self.connlost_deferred)
        self.assertInQ(d, ('AQ_SHARES_LIST', {'shares_list':
                                              anEmptyShareList}))
        return d

    def test_create_share(self):
        """Hiccup the network in the middle of a create_share."""
        def worker():
            """Async worker."""
            self.aq.create_share(self.root, 'jack', '', 'View', 'marker:x', '')
            return self.hiccup()
        d = self.nuke_client_method('create_share', worker,
                                    lambda: self.connlost_deferred)
        self.assertInQ(d, ('AQ_CREATE_SHARE_OK',
                           {'marker': 'marker:x',
                            'share_id': aShareUUID}))
        return d

    def test_answer_share(self):
        """Hiccup the network in the middle of an answer_share."""
        def worker():
            """Asynch worker."""
            share_id = self.listener.get_id_for_marker('marker:x')
            self.aq.answer_share(share_id, 'Yes')
            return self.hiccup()
        d = self.wait_for_nirvana()
        d.addCallback(lambda _: self.nuke_client_method(
                      'accept_share', worker, lambda: self.connlost_deferred))

        self.aq.create_share(self.root, 'jack', '', 'View', 'marker:x', '')
        self.assertInQ(d, ('AQ_ANSWER_SHARE_OK',
                           {'answer': 'Yes', 'share_id': anUUID}))
        return d

    def test_unlink(self):
        """Hiccup the network in the middle of an unlink."""
        def worker():
            """Asynch worker."""
            new_id = self.listener.get_id_for_marker('marker:x')
            self.aq.unlink('', self.root, new_id, '', False)
            return self.hiccup()
        d = self.wait_for_nirvana()
        d.addCallback(lambda _: self.nuke_client_method(
                      'unlink', worker, lambda: self.connlost_deferred))

        parent_path = self.main.fs.get_by_node_id('', self.root).path
        mdid = self.main.fs.create(os.path.join(parent_path, u"test"), '')
        self.aq.make_file('', self.root, 'hola', 'marker:x', mdid)
        self.assertInQ(d, ('AQ_UNLINK_OK', {'share_id': '',
                                            'node_id': anUUID,
                                            'parent_id': self.root,
                                            'was_dir': False,
                                            'old_path': '',
                                            'new_generation': 2L}))
        return d

    def test_move(self):
        """Hiccup the network in the middle of a move."""
        def worker():
            """Async worker."""
            new_id = self.listener.get_id_for_marker('marker:x')
            self.aq.move('', new_id, self.root, self.root,
                         'chau', 'from', 'to')
            return self.hiccup()
        d = self.wait_for_nirvana()
        d.addCallback(lambda _: self.nuke_client_method(
                      'move', worker, lambda: self.connlost_deferred))
        parent_path = self.main.fs.get_by_node_id('', self.root).path
        mdid = self.main.fs.create(os.path.join(parent_path, u"test"), '')
        self.aq.make_file('', self.root, 'hola', 'marker:x', mdid)

        self.assertInQ(d, ('AQ_MOVE_OK', {'share_id': '',
                                          'node_id': anUUID,
                                          'new_generation': 2L}))
        return d

    def test_make_file(self):
        """Hiccup the network in the middle of a make_file."""
        def worker():
            """Async worker."""
            parent_path = self.main.fs.get_by_node_id('', self.root).path
            mdid = self.main.fs.create(os.path.join(parent_path, u"test"), '')
            self.aq.make_file('', self.root, 'hola', 'marker:x', mdid)
            return self.hiccup()
        d = self.nuke_client_method('make_file', worker,
                                    lambda: self.connlost_deferred)
        self.assertInQ(d, ('AQ_FILE_NEW_OK', {'new_id': anUUID,
                                              'marker': 'marker:x',
                                              'new_generation': 1L,
                                              'volume_id': request.ROOT}))
        return d

    @defer.inlineCallbacks
    def test_download(self):
        """Hiccup the network in the middle of a download."""
        self.patch(self.main.fs, 'get_partial_for_writing',
                   lambda s, n: StringIO())
        hash_value, _, _, d = self._mk_file_w_content()
        mdid, node_id = yield d

        def worker():
            """Async worker."""
            self.aq.download('', node_id, hash_value, mdid)
            return self.hiccup()
        fake_gc = FakeGetContent(self.connlost_deferred, '',
                                 self.root, hash_value)
        d = self.nuke_client_method('get_content_request', worker,
                                    lambda: fake_gc)
        self.assertInQ(d, ('AQ_DOWNLOAD_COMMIT',
                           {'share_id': '',
                            'node_id': node_id,
                            'server_hash': hash_value}))
        yield d

    @defer.inlineCallbacks
    def test_upload(self):
        """Hiccup the network in the middle of an upload."""
        data = os.urandom(1000)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        self.patch(self.main.fs, 'open_file', lambda mdid: StringIO(data))
        mdid, node_id = yield self._mkfile('hola')

        def worker():
            """Async worker."""
            self.aq.upload('', node_id, NO_CONTENT_HASH, hash_value,
                           crc32_value, size, mdid)
            return self.hiccup()
        d = self.wait_for_nirvana()
        d.addCallback(lambda _: self.nuke_client_method(
            'put_content_request', worker, lambda: self.connlost_deferred))

        self.assertInQ(d, lambda: ('AQ_UPLOAD_FINISHED',
                                   {'share_id': '',
                                    'hash': hash_value,
                                    'node_id': anUUID,
                                    'new_generation': 2L}))
        yield d


class TestCancelMarker(AQCancelTestBase):
    """Cancellation of marker-related things."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set things up."""
        yield super(TestCancelMarker, self).setUp()
        self.marker = Marker('marker')

    def test_create_share(self):
        """Hiccup the network in the middle of a create_share."""
        def worker():
            """Async worker."""
            dir_path = os.path.join(self.main.root_dir, 'testdir')
            mdid = self.main.fs.create(dir_path, request.ROOT)
            marker = Marker(mdid)
            self.aq.make_dir('', self.root, 'hola', marker, mdid)
            self.aq.create_share(marker, 'jack', '', 'View', 'marker:x', '')
            return self.hiccup()
        d = self.nuke_client_method('make_dir', worker)
        self.assertInQ(d, ('AQ_CREATE_SHARE_OK',
                           {'marker': 'marker:x',
                            'share_id': aShareUUID}))
        return d

    def test_unlink(self):
        """Hiccup the network in the middle of an unlink."""
        def worker():
            """Async worker."""
            dir_path = os.path.join(self.main.root_dir, 'testdir')
            mdid = self.main.fs.create(dir_path, request.ROOT)
            marker = Marker(mdid)
            self.aq.make_dir('', self.root, 'hola', marker, mdid)
            self.aq.unlink('', self.root, marker, 'dir', False)
            return self.hiccup()
        d = self.nuke_client_method('make_dir', worker)

        self.assertInQ(d, ('AQ_UNLINK_OK', {'share_id': '',
                                            'node_id': anUUID,
                                            'parent_id': self.root,
                                            'was_dir': False,
                                            'old_path': 'dir',
                                            'new_generation': 2L}))
        return d

    def test_move(self):
        """Hiccup the network in the middle of a move."""
        def worker():
            """Async worker."""
            dir_path = os.path.join(self.main.root_dir, 'testdir')
            mdid = self.main.fs.create(dir_path, request.ROOT)
            marker = Marker(mdid)
            self.aq.make_dir('', self.root, 'hola', marker, mdid)
            self.aq.move('', marker, self.root, self.root, 'chau', 'dir', 'to')
            return self.hiccup()
        d = self.nuke_client_method('make_dir', worker)
        self.assertInQ(d, ('AQ_MOVE_OK', {'share_id': '',
                                          'node_id': anUUID,
                                          'new_generation': 2L}))
        return d

    def test_make_file(self):
        """Hiccup the network in the middle of a make_file."""
        def worker():
            """Async worker."""
            dir_path = os.path.join(self.main.root_dir, 'testdir')
            mdid = self.main.fs.create(dir_path, request.ROOT)
            marker = Marker(mdid)
            self.aq.make_dir('', self.root, 'chau', marker, mdid)
            file_path = os.path.join(dir_path, 'testfile')
            mdid = self.main.fs.create(file_path, request.ROOT)
            self.aq.make_file('', marker, 'hola', 'marker:x', mdid)
            return self.hiccup()

        d = self.nuke_client_method('make_dir', worker)
        self.assertInQ(d, ('AQ_FILE_NEW_OK', {'new_id': anUUID,
                                              'marker': 'marker:x',
                                              'new_generation': 2L,
                                              'volume_id': request.ROOT}))
        return d
