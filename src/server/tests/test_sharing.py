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

"""Test sharing operations."""

import uuid
import zlib

from StringIO import StringIO

from twisted.internet import reactor, defer

from backends.filesync.data import errors, dbmanager, model
from ubuntuone.storage.server.testing.testcase import (
    TestWithDatabase, FactoryHelper)
from ubuntuone.storageprotocol import request
from ubuntuone.storageprotocol import errors as protocol_errors
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32

EMPTY_HASH = content_hash_factory().content_hash()
NO_CONTENT_HASH = ""


class TestCreateShare(TestWithDatabase):
    """Test crate_share command."""

    def test_delete_share(self):
        """Test deletion of an offered by me share."""

        @defer.inlineCallbacks
        def _do_delete(client):
            """Callback to do delete."""
            # authenticate and create a root to share
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            res = yield client.create_share(root, self.usr1.username,
                                            u"name", "View")
            share_id = uuid.UUID(res.share_id)
            yield client.delete_share(share_id)
            self.assertRaises(errors.DoesNotExist,
                              self.usr1.get_share, share_id)
            client.test_done()

        def do_delete(client):
            """Test body callback."""
            d = _do_delete(client)
            d.addErrback(client.test_fail)

        return self.callback_test(do_delete)

    def test_delete_volume(self):
        """Test deletion of an offered to me and accepted share."""

        def create_share(_):
            share = self.usr1.root.share(self.usr0.id, u"sharename",
                                         readonly=True)
            self.usr0.get_share(share.id).accept()
            return share

        def auth(client):
            # authenticate and create the share
            d = client.dummy_authenticate("open sesame")
            d.addCallback(create_share)

            # delete the share and check
            d.addCallback(lambda share: client.delete_volume(share.id))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_create_view(self):
        """Create a share using View."""

        def auth(client):
            def verifyDB():
                reg = self.usr1.get_shared_to()[0]
                self.assertEqual(reg.name, "name")
                self.assertEqual(reg.access, "View")

            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"name", "View"),
                client.test_fail)

            d.addCallback(lambda _: verifyDB())

            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_create_basic_modify(self):
        """Create a share using Modify."""

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"name", "Modify"),
                client.test_fail)

            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_create_usr_twice(self):
        """Create a share that tries to share twice same node to same user."""

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"name1", "Modify"),
                client.test_fail)

            # create a share of the same node to the same user
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"name2", "Modify"),
                client.test_fail)

            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))

        return self.callback_test(auth)

    def test_create_same_name_different_user(self):
        """Create two shares repeating the name but changing the user."""

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create the share to one user
            d.addCallbacks(
                lambda r: client.create_share(self._state.root_id,
                                              self.usr1.username,
                                              u"same name", "View"),
                client.test_fail)

            # create the share to other user
            d.addCallbacks(
                lambda r: client.create_share(self._state.root_id,
                                              self.usr2.username,
                                              u"same name", "View"),
                client.test_fail)

            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_create_different_name_same_user(self):
        """Create two shares repeating the name but changing the user."""

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create other directory to share
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola"),
                client.test_fail)

            # create the share using the just created directory
            d.addCallbacks(
                lambda r: client.create_share(r.new_id,
                                              self.usr1.username,
                                              u"new dir", "View"),
                client.test_fail)

            # create the share using root
            d.addCallbacks(
                lambda r: client.create_share(self._state.root_id,
                                              self.usr1.username,
                                              u"root dir", "View"),
                client.test_fail)

            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_create_same_name_same_user(self):
        """Create two shares repeating the name and the user."""
        @defer.inlineCallbacks
        def test(client):
            """Test."""
            # authenticate and create a root to share
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()

            # create other directory to share
            makedir_req = yield client.make_dir(request.ROOT, root_id, u"hola")

            # create the share using the just created directory
            yield client.create_share(makedir_req.new_id, self.usr1.username,
                                      u"same_name", "View")

            # create the share using root
            d = client.create_share(root_id, self.usr1.username,
                                    u"same_name", "View")

            # the last call must fail with AlreadyExist
            yield self.assertFailure(d, protocol_errors.AlreadyExistsError)

        return self.callback_test(test, add_default_callbacks=True)

    def test_create_share_file(self):
        """Create a share on a file."""

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)

            # create the file to share
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(r.new_id, self.usr1.username,
                                              u"foo", "Modify"),
                client.test_fail)

            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))

        return self.callback_test(auth)

    def test_notify_creation_to_myself(self):
        """Create a share to myself and listen to the notification."""

        def auth(client):
            def notif(share_notif):
                self.assertEqual(
                    share_notif.subtree, str(self._state.root_id))
                self.assertEqual(share_notif.share_name, "name")
                self.assertEqual(share_notif.from_username,
                                 self.usr0.username)
                self.assertEqual(share_notif.from_visible_name,
                                 self.usr0.visible_name)
                self.assertEqual(share_notif.access_level, "View")
                client.test_done()

            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # hook ourselves and create the share
            d.addCallbacks(lambda r: client.set_share_change_callback(notif),
                           client.test_fail)
            d.addCallbacks(
                lambda _: client.create_share(
                    self._state.root_id, "usr0", u"name", "View"),
                client.test_fail)

        return self.callback_test(auth)

    def test_notify_creation_to_myself_accepted_yes(self):
        """Create a share to myself and accept it."""

        def auth(client):
            def notif_share(share_notif):
                #check the DB
                shares = self.usr0.get_shared_to(accepted=False)
                share = shares[0]
                self.assertEquals(share.name, u"acc Y")
                self.assertEqual(str(share.root_id), self._state.root_id)
                self.assertEqual(share.access, "View")
                self.assertEqual(share.accepted, False)

                # send the answer
                client.accept_share(share_notif.share_id, "Yes")

            def notif_answer(share_id, answer):
                # check the notification
                self.assertEqual(answer, "Yes")

                # check the DB
                share = self.usr0.get_share(share_id)
                self.assertEqual(str(share.root_id), self._state.root_id)
                self.assertEqual(share.accepted, True)
                # so, everything ok!
                client.test_done()

            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # hook ourselves to both notifications
            d.addCallbacks(
                lambda r: client.set_share_change_callback(notif_share),
                client.test_fail)

            d.addCallbacks(
                lambda r: client.set_share_answer_callback(notif_answer),
                client.test_fail)

            # create the share
            d.addCallbacks(
                lambda _: client.create_share(
                    self._state.root_id, self.usr0.username, u"acc Y", "View"),
                client.test_fail)

        return self.callback_test(auth)

    def test_notify_creation_to_myself_accepted_no(self):
        """Create a share to myself and reject it."""

        def auth(client):
            def notif_share(share_notif):

                #check the DB
                reg = self.usr0.get_shared_to()[0]
                self.assertEqual(str(reg.root_id), self._state.root_id)
                self.assertEqual(reg.access, "View")
                self.assertEqual(reg.accepted, False)
                self.assertEqual(reg.status, "Live")

                # send the answer
                client.accept_share(share_notif.share_id, "No")

            def notif_answer(share_id, answer):
                # check the notification
                self.assertEqual(answer, "No")

                # check the DB
                self.assertRaises(errors.DoesNotExist,
                                  self.usr0.get_share, share_id)

                # so, everything ok!
                client.test_done()

            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # hook ourselves to both notifications
            d.addCallbacks(
                lambda r: client.set_share_change_callback(notif_share),
                client.test_fail)

            d.addCallbacks(
                lambda r: client.set_share_answer_callback(notif_answer),
                client.test_fail)

            # create the share
            d.addCallbacks(
                lambda _: client.create_share(
                    self._state.root_id, self.usr0.username, u"acc N", "View"),
                client.test_fail)

        return self.callback_test(auth)

    def test_notify_answer_two_users(self):
        """Create check answer notifications between two clients."""

        def login1(client):
            self._state.client1 = client
            client.set_share_answer_callback(client1_notif_answer)
            d = client.dummy_authenticate("open sesame")  # for user #0
            d.addCallback(new_client)
            d.addCallback(make_share)

        def login2(client):
            self._state.client2 = client
            client.set_share_change_callback(client2_notif_change)
            d = client.dummy_authenticate("friend")  # for user #1
            d.addCallback(done_auth)

        # setup
        factory = FactoryHelper(login1)
        factory2 = FactoryHelper(login2)
        d1 = defer.Deferred()
        d2 = defer.Deferred()
        timeout = reactor.callLater(3, d1.errback, Exception("timeout"))

        def new_client(_):
            reactor.connectTCP('localhost', self.port, factory2)
            return d2

        def done_auth(result):
            d2.callback(result)

        def make_share(_):
            # create the share
            client1 = self._state.client1
            d = client1.get_root()
            d.addCallback(self.save_req, 'root_id')
            d.addCallbacks(
                lambda _: client1.create_share(
                    self._state.root_id, self.usr1.username, u"acc Y", "View"),
                client1.test_fail)

        def client2_notif_change(share_notif):
            #received change notification for the new share
            #check the share
            shares = self.usr1.get_shared_to(accepted=False)
            share = shares[0]
            self.assertEquals(share.name, u"acc Y")
            self.assertEqual(str(share.root_id), self._state.client1.root_id)
            self.assertEqual(share.access, "View")
            self.assertEqual(share.accepted, False)
            # send the answer
            self._state.client2.accept_share(share_notif.share_id, "Yes")

        def client1_notif_answer(share_id, answer):
            # check the notification
            self.assertEqual(answer, "Yes")
            #check the share
            share = self.usr0.get_share(share_id)
            self.assertEqual(share.accepted, True)

            # cleanup
            factory.timeout.cancel()
            factory2.timeout.cancel()
            timeout.cancel()
            d1.callback((share_id, answer))
            self._state.client1.transport.loseConnection()
            self._state.client2.transport.loseConnection()

        reactor.connectTCP('localhost', self.port, factory)
        return d1

    def test_notify_creation_bad(self):
        """Create a share and see that the own user is not notified."""

        def auth(client):
            def notif(*args):
                raise ValueError("This notification shouldn't exist!")

            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # hook ourselves and create the share
            d.addCallbacks(lambda r: client.set_share_change_callback(notif),
                           client.test_fail)
            d.addCallbacks(
                lambda _: client.create_share(
                    self._state.root_id, self.usr1.username, u"name", "View"),
                client.test_fail)

            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_create_got_shareid(self):
        """Create a share using and check if the share_id is returned. """

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)

            # create the share
            def check_share_id(share_id):
                self.assertFalse(share_id is None, 'Oops!, share_id is None!')
                return share_id

            def create_share(r):
                d = client.create_share(r, self.usr1.username, u"name", "View")
                d.addCallbacks(check_share_id, client.test_fail)
                return d
            d.addCallbacks(lambda r: create_share(r), client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_accept_dead_share(self):
        """Try to accept a dead share."""
        share = self.usr0.root.share(self.usr1.id, u"a dead share")
        share.delete()

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            # accept the Dead share
            d.addCallbacks(lambda _: client.accept_share(share.id, "Yes"),
                           client.test_fail)
            self.assertFails(d, "DOES_NOT_EXIST")
            d.addCallbacks(client.test_done)

        return self.callback_test(auth)


class TestSharesWithData(TestWithDatabase):
    """Tests that require simple sharing data setup."""

    @defer.inlineCallbacks
    def setUp(self):
        """Create users, files and shares."""
        yield super(TestSharesWithData, self).setUp()

        root1 = self.usr1.root
        root2 = self.usr2.root
        filero = root2.make_file(u"file")
        rwdir = root2.make_subdirectory(u"rwdir")
        filerw = rwdir.make_file(u"file")
        subdir = root2.make_subdirectory(u"subdir")
        subfile = subdir.make_file(u"subfile")
        subsubdir = subdir.make_subdirectory(u"subsubdir")
        subsubfile = subsubdir.make_file(u"subsubfile")

        share = root2.share(0, u"foo", readonly=True)
        share_other = root2.share(self.usr1.id, u"foo", readonly=True)
        subshare = subdir.share(0, u"foo2", readonly=True)
        share_owner = root1.share(0, u"foo3", readonly=True)
        share_modify = rwdir.share(0, u"foo4")
        for s in self.usr0.get_shared_to(accepted=False):
            s.accept()
        for s in self.usr1.get_shared_to(accepted=False):
            s.accept()

        self.share = str(share.id)
        self.share_modify = str(share_modify.id)
        self.subshare = str(subshare.id)
        self.share_other = str(share_other.id)
        self.share_owner = str(share_owner.id)
        self.root = root2.id
        self.filero = str(filero.id)
        self.subdir = subdir.id
        self.subfile = subfile.id
        self.subsubdir = subsubdir.id
        self.subsubfile = subsubfile.id
        self.rwdir = rwdir.id
        self.filerw = str(filerw.id)

    def test_unlink_mount_point(self):
        """Test unlinking a dir."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.unlink(self.subshare, str(self.subdir)))
            self.assertFails(d, "NO_PERMISSION")
            d.addCallback(client.test_done)

        return self.callback_test(auth)

    def test_mkfile_on_share(self):
        """Create a file on a share."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.make_file(
                    self.share_modify, self.rwdir, "test_file"))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_mkfile_on_share_ro(self):
        """Create a file on a share thats read only."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.make_file(self.share, self.root, "test_file"))
            d.addCallbacks(client.test_fail, lambda x: client.test_done())

        return self.callback_test(auth)

    def test_mkdir_on_share(self):
        """Create a dir on a share."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.make_dir(
                    self.share_modify, self.rwdir, "test_dir"))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_mkdir_on_share_ro(self):
        """Create a dir on a share thats read only."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.make_dir(self.share, self.root, "test_dir"))
            d.addCallbacks(client.test_fail, lambda x: client.test_done())

        return self.callback_test(auth)

    def test_unlink_on_share(self):
        """unlink a file on a share."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.unlink(self.share_modify, self.filerw))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_unlink_on_share_ro(self):
        """unlink a file on a share thats read only."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.unlink(self.share, self.filero))
            d.addCallbacks(client.test_fail, lambda x: client.test_done())

        return self.callback_test(auth)

    def test_move_on_share(self):
        """move a file on a share."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.move(
                    self.share_modify, self.filerw, self.rwdir, "newname"))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_move_on_share_ro(self):
        """move a file on a share thats read only."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.move(
                    self.share, self.filero, self.root, "newname"))
            d.addCallbacks(client.test_fail, lambda x: client.test_done())

        return self.callback_test(auth)

    def test_move_on_share_from_mine(self):
        """make sure we cant move across roots"""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(
                lambda r: client.make_file("", r, "file"))
            d.addCallback(
                lambda r: client.move(
                    self.share_modify, r.new_id, self.root, "newname"))
            self.assertFails(d, "DOES_NOT_EXIST")
            d.addCallback(client.test_done)

        return self.callback_test(auth)

    def test_get_content_on_share(self):
        """read a file on a share."""
        data = ""
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            # need to put data to be able to retrieve it!
            d.addCallback(
                lambda r: client.put_content(
                    self.share_modify, self.filerw, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)))
            d.addCallback(
                lambda r: client.get_content(
                    self.share_modify, self.filerw, EMPTY_HASH))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_put_content_on_share(self):
        """write a file on a share."""
        data = "*" * 100000
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.put_content(
                    self.share_modify, self.filerw, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_put_content_on_share_ro(self):
        """Write a file on a share thats read only."""
        data = "*" * 100000
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.put_content(
                    self.share, self.filero, NO_CONTENT_HASH, hash_value,
                    crc32_value, size, StringIO(data)))
            d.addCallbacks(client.test_fail, lambda x: client.test_done())

        return self.callback_test(auth)


class TestSharesWithDataLegacy(TestWithDatabase):
    """Tests that require simple sharing data setup, but legacy."""

    @defer.inlineCallbacks
    def setUp(self):
        """Create users, files and shares."""
        yield super(TestSharesWithDataLegacy, self).setUp()

        root1 = self.usr1.root
        root2 = self.usr2.root
        file = root2.make_file(u"file")
        filero = root2.make_file(u"file")
        rwdir = root2.make_subdirectory(u"rwdir")
        filerw = rwdir.make_file(u"file")
        subdir = root2.make_subdirectory(u"subdir")
        subfile = subdir.make_file(u"subfile")
        subsubdir = subdir.make_subdirectory(u"subsubdir")
        subsubfile = subsubdir.make_file(u"subsubfile")
        store = dbmanager.get_shard_store(self.usr2.shard_id)
        #set all files with an empty hash
        store.find(model.StorageObject, model.StorageObject.kind == 'File'
                   ).set(_content_hash=EMPTY_HASH)
        store.commit()

        share = root2.share(0, u"foo", readonly=True)
        share_other = root2.share(self.usr1.id, u"foo", readonly=True)
        subshare = subdir.share(0, u"foo2", readonly=True)
        share_owner = root1.share(0, u"foo3", readonly=True)
        share_modify = rwdir.share(0, u"foo4")
        for s in self.usr0.get_shared_to(accepted=False):
            s.accept()
        for s in self.usr1.get_shared_to(accepted=False):
            s.accept()

        self.share = str(share.id)
        self.share_modify = str(share_modify.id)
        self.subshare = str(subshare.id)
        self.share_other = str(share_other.id)
        self.share_owner = str(share_owner.id)
        self.root = root2.id
        self.file = str(file.id)
        self.filero = str(filero.id)
        self.subdir = subdir.id
        self.subfile = subfile.id
        self.subsubdir = subsubdir.id
        self.subsubfile = subsubfile.id
        self.rwdir = rwdir.id
        self.filerw = str(filerw.id)

    def test_get_content_on_share(self):
        """read a file on a share."""

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            # need to put data to be able to retrieve it!
            d.addCallback(
                lambda r: client.get_content(
                    self.share, self.file, EMPTY_HASH))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_put_content_on_share(self):
        """write a file on a share."""
        data = "*" * 100000
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            """auth"""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(
                lambda r: client.put_content(
                    self.share_modify, self.filerw, EMPTY_HASH, hash_value,
                    crc32_value, size, deflated_size, StringIO(deflated_data)))
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)


class TestListShares(TestWithDatabase):
    """Test list_shares command."""

    def test_one_share_view(self):
        """List a created share from_me with View."""

        def check(resp):
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, u"usr1")
            self.assertEqual(share.name, "n1")
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.subtree, uuid.UUID(self._state.root_id))

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"n1", "View"),
                client.test_fail)

            # list the shares and check
            d.addCallbacks(lambda _: client.list_shares(), client.test_fail)
            d.addCallbacks(check, client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_one_share_modify(self):
        """List a created share from_me with Modify."""

        def check(resp):
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.name, "n1")
            self.assertEqual(share.access_level, "Modify")
            self.assertEqual(share.subtree, uuid.UUID(self._state.root_id))

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create the share
            d.addCallbacks(
                lambda r: client.create_share(
                    r, self.usr1.username, u"n1", "Modify"),
                client.test_fail)

            # list the shares and check
            d.addCallbacks(lambda _: client.list_shares(), client.test_fail)
            d.addCallbacks(check, client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def _create_share(self, _, accepted=False):
        """Creates a share, optionally accepted."""
        root = self.usr1.root.load()
        share = root.share(0, u"sharename", readonly=True)
        if accepted:
            self.usr0.get_share(share.id).accept()
        # save the node id to be able to compare it later

        self.save_req(root.id, 'root_id')

    def test_shares_to_me_noaccept(self):
        """List shares where user is recipient, no accepted."""

        def check(resp):
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "to_me")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.name, "sharename")
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.accepted, False)
            self.assertEqual(share.subtree, self._state.root_id)

        def auth(client):
            # authenticate and create the share
            d = client.dummy_authenticate("open sesame")
            d.addCallback(self._create_share)

            # list the shares and check
            d.addCallback(lambda _: client.list_shares())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_shares_to_me_accepted(self):
        """List shares where user is recipient, accepted."""

        def check(resp):
            self.assertEqual(len(resp.shares), 0)

        def auth(client):
            # authenticate and create the share
            d = client.dummy_authenticate("open sesame")
            d.addCallback(self._create_share, accepted=True)

            # list the shares and check
            d.addCallback(lambda _: client.list_shares())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_several_shares(self):
        """List several mixed shares."""

        def createShare():
            self.usr1.root.share(self.usr0.id, u"share 3", readonly=True)

        def check(resp):
            self.assertEqual(len(resp.shares), 3)

            share = [s for s in resp.shares if s.name == "share 1"][0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.access_level, "View")

            share = [s for s in resp.shares if s.name == "share 2"][0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, self.usr2.username)
            self.assertEqual(share.access_level, "Modify")

            share = [s for s in resp.shares if s.name == "share 3"][0]
            self.assertEqual(share.direction, "to_me")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.access_level, "View")

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create one share from me with View
            d.addCallbacks(
                lambda r: client.create_share(
                    self._state.root_id, self.usr1.username,
                    u"share 1", "View"),
                client.test_fail)

            # create one share from me with Modify
            d.addCallbacks(
                lambda r: client.create_share(
                    self._state.root_id, self.usr2.username,
                    u"share 2", "Modify"),
                client.test_fail)

            # create the share to me
            d.addCallbacks(lambda _: createShare(), client.test_fail)

            # list the shares and check
            d.addCallbacks(lambda _: client.list_shares(), client.test_fail)
            d.addCallbacks(check, client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_share_offer(self):
        """Test a share offer which has no to_user."""

        def createShareOffer():
            #test a share offer
            self.usr0.root.make_shareoffer(u"me@example.com", u"share Offer",
                                           readonly=True)

        def check(resp):
            self.assertEqual(len(resp.shares), 1)

            share = [s for s in resp.shares if s.name == "share Offer"][0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, u"")
            self.assertEqual(share.access_level, "View")

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create the share offer to me
            d.addCallbacks(lambda _: createShareOffer(), client.test_fail)

            # list the shares and check
            d.addCallbacks(lambda _: client.list_shares(), client.test_fail)
            d.addCallbacks(check, client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_dead_share(self):
        """Test that Dead shares are not included in the list. """

        def createShare():
            share = self.usr1.root.share(self.usr0.id,
                                         u"another dead share", readonly=True)
            share.delete()

        def check(resp):
            self.assertEqual(len(resp.shares), 0, resp.shares)

        def auth(client):
            # authenticate and create a root to share
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallback(self.save_req, 'root_id')

            # create the share
            d.addCallbacks(lambda _: createShare(), client.test_fail)
            # list the shares and check
            d.addCallbacks(lambda _: client.list_shares(), client.test_fail)
            d.addCallbacks(check, client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)

        return self.callback_test(auth)

    def test_share_in_root_from_me_with_volume_id(self):
        """List a share from_me and check that we have the node volume_id."""

        @defer.inlineCallbacks
        def auth(client):
            # authenticate and create a root to share
            yield client.dummy_authenticate("open sesame")
            root_id = yield client.get_root()
            yield client.create_share(root_id, self.usr1.username,
                                      u"n1", "View")
            # list the shares and check
            resp = yield client.list_shares()
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, u"usr1")
            self.assertEqual(share.name, "n1")
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.subtree, uuid.UUID(root_id))
            self.assertEqual(share.subtree_volume_id, None)  # the root

        return self.callback_test(auth, add_default_callbacks=True)

    def test_share_in_udf_from_me_with_volume_id(self):
        """List a share from_me and check that we have the node volume_id."""

        @defer.inlineCallbacks
        def auth(client):
            # authenticate and create a root to share
            yield client.dummy_authenticate("open sesame")
            yield client.get_root()
            udf = yield client.create_udf(u"~/myudf", u"foo")
            yield client.create_share(udf.node_id, self.usr1.username,
                                      u"n1", "View")
            # list the shares and check
            resp = yield client.list_shares()
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "from_me")
            self.assertEqual(share.other_username, u"usr1")
            self.assertEqual(share.name, "n1")
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.subtree, uuid.UUID(udf.node_id))
            self.assertEqual(share.subtree_volume_id, uuid.UUID(udf.volume_id))

        return self.callback_test(auth, add_default_callbacks=True)

    def test_share_to_me_without_volume_id(self):
        """List a share to_me, check that we don't have the node volume_id."""

        @defer.inlineCallbacks
        def auth(client):
            # authenticate and create a root to share
            yield client.dummy_authenticate("open sesame")
            self._create_share(None, accepted=False)
            # list the shares and check
            resp = yield client.list_shares()
            self.assertEqual(len(resp.shares), 1)
            share = resp.shares[0]
            self.assertEqual(share.direction, "to_me")
            self.assertFalse(hasattr(share, 'subtree_volume_id'))

        return self.callback_test(auth, add_default_callbacks=True)


class TestSharesNotified(TestWithDatabase):
    """Test that notifications are sent to share receivers."""

    def test_notify_shared_node(self):
        """The notif should be sent to other users if the node is shared."""
        def login1(client):
            """client1 login"""
            self._state.client1 = client
            d = client.dummy_authenticate("open sesame")  # for user #0
            d.addCallback(lambda _: new_client())
            d.addCallback(make_notification)

        def login2(client):
            """client2 login"""
            self._state.client2 = client
            client.set_volume_new_generation_callback(on_notification)
            d = client.dummy_authenticate("friend")  # for user #1
            d.addCallback(done_auth)

        # setup
        factory = FactoryHelper(login1)
        factory2 = FactoryHelper(login2)
        d1 = defer.Deferred()
        d2 = defer.Deferred()
        timeout = reactor.callLater(3, d1.errback, Exception("timeout"))

        def new_client():
            """add the second client"""
            reactor.connectTCP('localhost', self.port, factory2)
            return d2

        def on_notification(volume, generation):
            """notification arrived, check and cleanup"""
            # check
            self.assertEqual(volume, self._state.share_id)

            # cleanup
            factory.timeout.cancel()
            factory2.timeout.cancel()
            timeout.cancel()
            d1.callback(True)
            self._state.client1.transport.loseConnection()
            self._state.client2.transport.loseConnection()

        def done_auth(result):
            """authentication done for client2, we can start making changes"""
            d2.callback(result)

        def mark_share(_):
            # mark the share as accepted by hand, so we don't
            # have to emulate the whole process just for this test
            shares = [s for s in self.usr1.get_shared_to()
                      if s.shared_by_id == 0]
            share = shares[0]
            share.accept()
            # also store it in the state for further control
            self._state.share_id = share.id

        def make_notification(_):
            """create a change that should create a notification"""
            # create the share
            d = self._state.client1.get_root()
            d.addCallback(self.save_req, 'root_id')

            # create the share
            d.addCallback(
                lambda r: self._state.client1.create_share(
                    r, self.usr1.username, u"name", "View"))
            d.addCallback(mark_share)

            # create a file in the root
            d.addCallback(
                lambda _: self._state.client1.make_file(
                    request.ROOT, self._state.root_id, "hola"))

        reactor.connectTCP('localhost', self.port, factory)
        return d1

    def test_share_node_deleted(self):
        '''Remove the node that was shared.'''
        def login1(client):
            """client1 login"""
            self._state.client1 = client
            d = client.dummy_authenticate("open sesame")  # for user #0
            d.addCallback(new_client)
            d.addCallback(make_notification)

        def login2(client):
            """client2 login"""
            self._state.client2 = client
            client.set_share_change_callback(notif1)
            d = client.dummy_authenticate("friend")  # for user #1
            d.addCallback(done_auth)

        # setup
        factory = FactoryHelper(login1)
        factory2 = FactoryHelper(login2)
        d1 = defer.Deferred()
        d2 = defer.Deferred()
        timeout = reactor.callLater(3, d1.errback, Exception("timeout"))

        def new_client(_):
            """add the second client"""
            reactor.connectTCP('localhost', self.port, factory2)
            return d2

        def notif1(share_info):
            '''First notification, for the created share.'''
            self._state.client2.set_share_delete_callback(notif2)

        def notif2(share_id):
            '''Second notification, the tested one.'''
            # find the real share
            self.assertRaises(errors.DoesNotExist,
                              self.usr1.get_share, self._state.share_id)

            # cleanup
            factory.timeout.cancel()
            factory2.timeout.cancel()
            timeout.cancel()
            d1.callback((share_id))
            self._state.client1.transport.loseConnection()
            self._state.client2.transport.loseConnection()

        def done_auth(result):
            """authentication done for client2, we can start making changes"""
            d2.callback(result)

        def mark_share(_):
            # mark the share as accepted by hand, so we don't
            # have to emulate the whole process just for this test
            share = self.usr1.get_shared_to(accepted=False)[0]
            share.accept()
            # also store it in the state for further control
            self._state.share_id = share.id

        def make_notification(_):
            """create a change that should create a notification"""
            # create the share
            client1 = self._state.client1
            d = client1.get_root()
            d.addCallback(self.save_req, 'root_id')

            # create the dir to share
            d.addCallback(lambda r: client1.make_dir(request.ROOT, r, "hi"))
            d.addCallback(self.save_req, "req")

            # create the share
            d.addCallback(
                lambda _: client1.create_share(
                    self._state.req.new_id, self.usr1.username,
                    u"name", "View"))
            d.addCallback(mark_share)

            # remove the shared node
            d.addCallback(
                lambda _: client1.unlink(request.ROOT, self._state.req.new_id))

        reactor.connectTCP('localhost', self.port, factory)
        return d1
    test_share_node_deleted.skip = 'LP: #766088'

    def test_share_node_overwritten_with_move(self):
        '''Move something else over the node that was shared.'''
        def login1(client):
            """client1 login"""
            self._state.client1 = client
            d = client.dummy_authenticate("open sesame")  # for user #0
            d.addCallback(new_client)
            d.addCallback(make_notification)

        def login2(client):
            """client2 login"""
            self._state.client2 = client
            client.set_share_change_callback(notif1)
            d = client.dummy_authenticate("friend")  # for user #1
            d.addCallback(done_auth)

        # setup
        factory = FactoryHelper(login1)
        factory2 = FactoryHelper(login2)
        d1 = defer.Deferred()
        d2 = defer.Deferred()
        timeout = reactor.callLater(3, d1.errback, Exception("timeout"))

        def new_client(_):
            """add the second client"""
            reactor.connectTCP('localhost', self.port, factory2)
            return d2

        def notif1(share_id):
            '''First notification, for the created share.'''
            self._state.client2.set_share_delete_callback(notif2)

        def notif2(share_id):
            '''Second notification, the tested one.'''
            # find the real share
            self.assertRaises(errors.DoesNotExist,
                              self.usr1.get_share, self._state.share_id)

            # cleanup
            factory.timeout.cancel()
            factory2.timeout.cancel()
            timeout.cancel()
            d1.callback((share_id))
            self._state.client1.transport.loseConnection()
            self._state.client2.transport.loseConnection()

        def done_auth(result):
            """authentication done for client2, we can start making changes"""
            d2.callback(result)

        def mark_share(_):
            # mark the share as accepted by hand, so we don't
            # have to emulate the whole process just for this test
            share = self.usr1.get_shared_to(accepted=False)[0]
            share.accept()
            # also store it in the state for further control
            self._state.share_id = share.id

        def make_notification(_):
            """create a change that should create a notification"""
            # create the share
            client1 = self._state.client1
            d = client1.get_root()
            d.addCallback(self.save_req, 'root_id')

            # create the dir to share
            d.addCallback(lambda r: client1.make_dir(request.ROOT, r, "prnt"))
            d.addCallback(self.save_req, 'parent')
            d.addCallback(
                lambda r: client1.make_dir(request.ROOT, r.new_id, "hi"))
            d.addCallback(self.save_req, 'node')

            # create another dir, to be used as source
            d.addCallback(
                lambda _: client1.make_dir(
                    request.ROOT, self._state.root_id, "hi"))
            d.addCallback(self.save_req, 'srcdir')

            # create the share
            d.addCallback(
                lambda _: client1.create_share(
                    self._state.node.new_id, self.usr1.username,
                    u"name", "View"))
            d.addCallback(mark_share)

            # move the source over the shared node
            d.addCallback(
                lambda _: client1.move(request.ROOT, self._state.srcdir.new_id,
                                       self._state.parent.new_id, "hi"))

        reactor.connectTCP('localhost', self.port, factory)
        return d1
