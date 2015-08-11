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

"""Test volume operations."""

from twisted.internet import defer

from backends.filesync.data.services import get_storage_user

from ubuntuone.storageprotocol import request
from ubuntuone.storage.server.testing.testcase import TestWithDatabase

from ubuntuone.storageprotocol.volumes import (
    RootVolume, UDFVolume, ShareVolume)


class TestListVolumes(TestWithDatabase):
    """Test list_volumes command."""

    def test_root_only(self):
        """Users have one volume by default: root."""
        @defer.inlineCallbacks
        def auth(client):
            """Authenticate and test."""
            yield client.dummy_authenticate("open sesame")
            root_node_id = yield client.get_root()
            req = yield client.list_volumes()
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), root_node_id)
            self.assertEqual(root.generation, 0)
            self.assertEqual(root.free_bytes, self.usr0.get_quota().free_bytes)
        return self.callback_test(auth, add_default_callbacks=True)

    def test_root_only_with_generation(self):
        """Test that the Root volume gets it generation."""
        @defer.inlineCallbacks
        def auth(client):
            """Authenticate and test."""
            # create a file in order to get a generation > 0
            self.usr0.root.make_file(u"filename_1")
            yield client.dummy_authenticate("open sesame")
            root_node_id = yield client.get_root()
            req = yield client.list_volumes()
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), root_node_id)
            self.assertEqual(root.generation, 1)
            self.assertEqual(root.free_bytes, self.usr0.get_quota().free_bytes)
        return self.callback_test(auth, add_default_callbacks=True)

    def test_one_share_offered(self):
        """Offered shares are not shown in volumes."""

        def check(req):
            """Check volumes response."""
            # root should be here only
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), self._state.root)

        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")

            d.addCallback(lambda r: client.create_share(r, self.usr1.username,
                                                        u"n1", "View"))

            d.addCallback(lambda _: client.list_volumes())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def _create_share(self, _, accept=False, dead=False, from_id=1):
        """Create the share to me."""
        fromusr = get_storage_user(from_id)
        node = fromusr.root.load()
        share = node.share(self.usr0.id, u"name", readonly=True)
        self._state.subtree_id = node.id

        if accept:
            self.usr0.get_share(share.id).accept()
            self._state.share_id = share.id

        if dead:
            share.delete()
        return share

    def test_share_to_me_no_accept(self):
        """A share offered to me should not be in the list if not accepted."""

        def check(req):
            """Check volumes response."""
            # root should be here only
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), self._state.root)

        def auth(client):
            """Authenticate and run the test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")

            # create the share
            d.addCallback(self._create_share)

            # list the volumes and check
            d.addCallback(lambda _: client.list_volumes())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_share_to_me_accepted(self):
        """A share offered to me should be in the volumes list if accepted."""
        @defer.inlineCallbacks
        def auth(client):
            """Authenticate and run the test."""
            yield client.dummy_authenticate("open sesame")
            client_root = yield client.get_root()
            # create the share
            _share = self._create_share(client_root, accept=True)
            # create a file in order to get a generation > 0
            self.usr0.root.make_file(u"filename_1")
            # list the volumes and check
            req = yield client.list_volumes()
            # check volumes response.
            self.assertEqual(len(req.volumes), 2)
            # test root
            root = [v for v in req.volumes if isinstance(v, RootVolume)][0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), client_root)
            self.assertEqual(root.generation, 1)
            self.assertEqual(root.free_bytes, self.usr0.get_quota().free_bytes)
            # test share
            share = [v for v in req.volumes if isinstance(v, ShareVolume)][0]
            self.assertEqual(share.volume_id, _share.id)
            self.assertEqual(share.node_id, _share.root_id)
            self.assertEqual(share.direction, "to_me")
            self.assertEqual(share.share_name, "name")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.accepted, True)
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.free_bytes,
                             self.usr1.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_share_to_me_accepted_with_generation(self):
        """A share offered to me should be in the volumes list if accepted."""
        @defer.inlineCallbacks
        def auth(client):
            """Authenticate and run the test."""
            yield client.dummy_authenticate("open sesame")
            client_root = yield client.get_root()
            # create the share
            _share = self._create_share(client_root, accept=True)
            # increae the generation of the share
            self.usr1.root.make_file(u"filename_1")
            # create a file in order to get a generation > 0
            self.usr0.root.make_file(u"filename_1")
            # list the volumes and check
            req = yield client.list_volumes()
            # check volumes response.
            self.assertEqual(len(req.volumes), 2)
            # test root
            root = [v for v in req.volumes if isinstance(v, RootVolume)][0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), client_root)
            self.assertEqual(root.generation, 1)
            self.assertEqual(root.free_bytes, self.usr0.get_quota().free_bytes)
            # test share
            share = [v for v in req.volumes if isinstance(v, ShareVolume)][0]
            self.assertEqual(share.volume_id, _share.id)
            self.assertEqual(share.node_id, _share.root_id)
            self.assertEqual(share.direction, "to_me")
            self.assertEqual(share.share_name, "name")
            self.assertEqual(share.other_username, self.usr1.username)
            self.assertEqual(share.accepted, True)
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.free_bytes,
                             self.usr1.get_quota().free_bytes)
            self.assertEqual(share.generation, 1)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_udf(self):
        """An UDF should be in the volume list."""
        @defer.inlineCallbacks
        def auth(client):
            """Authenticate and test."""
            # increase the generation in the root
            self.usr0.root.make_file(u"filename_1")
            yield client.dummy_authenticate("open sesame")
            client_root = yield client.get_root()
            # create the udf
            client_udf = yield client.create_udf(u"~/ñ", u"foo")

            # increase the generation in the udf
            self.usr0.volume(client_udf.volume_id).root.make_file(u"file_1")
            # list the volumes and check
            req = yield client.list_volumes()
            # check
            self.assertEqual(len(req.volumes), 2)
            # test root
            root = [v for v in req.volumes if isinstance(v, RootVolume)][0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), client_root)
            self.assertEqual(root.generation, 1)
            self.assertEqual(root.free_bytes, self.usr0.get_quota().free_bytes)
            # test udf
            udf = [v for v in req.volumes if isinstance(v, UDFVolume)][0]
            self.assertEqual(str(udf.volume_id), client_udf.volume_id)
            self.assertEqual(str(udf.node_id), client_udf.node_id)
            self.assertEqual(udf.suggested_path, u"~/ñ/foo")
            self.assertEqual(udf.generation, 1)
            self.assertEqual(udf.free_bytes, self.usr0.get_quota().free_bytes)

        return self.callback_test(auth, add_default_callbacks=True)

    def test_shares_to_me_accepted_dead(self):
        """A dead share offered to me should not be in the list."""

        def check(req):
            """Check volumes response."""
            # root should be here only
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), self._state.root)

        def auth(client):
            """Authenticate and run the test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")

            # create the share
            d.addCallback(self._create_share, accept=True, dead=True)

            # list the volumes and check
            d.addCallback(lambda _: client.list_volumes())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_udf_dead(self):
        """A dead UDF should not be in the volume list."""

        def check(req):
            """Check volumes response."""
            # root should be here only
            self.assertEqual(len(req.volumes), 1)
            root = req.volumes[0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), self._state.root)

        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")

            # create the udf
            d.addCallback(lambda _: client.create_udf(u"~/ñ", u"foo"))
            d.addCallback(lambda r: client.delete_volume(r.volume_id))

            # list the volumes and check
            d.addCallback(lambda _: client.list_volumes())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mixed(self):
        """Mix of UDFs and shares, dead and alive."""

        def check(req):
            """Check volumes response."""
            self.assertEqual(len(req.volumes), 3)

            # test root
            root = [v for v in req.volumes if isinstance(v, RootVolume)][0]
            self.assertEqual(root.volume_id, None)
            self.assertEqual(str(root.node_id), self._state.root)

            # test udf
            udf = [v for v in req.volumes if isinstance(v, UDFVolume)][0]
            self.assertEqual(str(udf.volume_id), self._state.udf.volume_id)
            self.assertEqual(str(udf.node_id), self._state.udf.node_id)
            self.assertEqual(udf.suggested_path, u"~/ñ/foo")
            self.assertEqual(udf.free_bytes, self.usr0.get_quota().free_bytes)

            # test share
            share = [v for v in req.volumes if isinstance(v, ShareVolume)][0]
            self.assertEqual(share.volume_id, self._state.share_id)
            self.assertEqual(share.node_id, self._state.subtree_id)
            self.assertEqual(share.direction, "to_me")
            self.assertEqual(share.share_name, "name")
            self.assertEqual(share.other_username, self.usr2.username)
            self.assertEqual(share.accepted, True)
            self.assertEqual(share.access_level, "View")
            self.assertEqual(share.free_bytes,
                             self.usr1.get_quota().free_bytes)

        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")

            # create two udfs, kill one
            d.addCallback(lambda _: client.create_udf(u"~/ñ", u"foo"))
            d.addCallback(self.save_req, "udf")
            d.addCallback(lambda _: client.create_udf(u"~/moño", u"groovy"))
            d.addCallback(lambda r: client.delete_volume(r.volume_id))

            # create two shares, one dead (the second one should be the live
            # one because the helper function stores data for comparison)
            d.addCallback(self._create_share, accept=True, dead=True)
            d.addCallback(self._create_share, accept=True, from_id=2)

            # list the volumes and check
            d.addCallback(lambda _: client.list_volumes())
            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)


class TestDataWithVolumes(TestWithDatabase):
    """Tests data handling in the context of several volumes."""

    def test_same_names(self):
        """Be able to have same names in different roots."""
        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.get_root())

            # create a subdir in root
            d.addCallback(lambda root: client.make_dir(request.ROOT,
                                                       root, "subdir"))

            # create the udf, with a dir of same name
            d.addCallback(lambda _: client.create_udf(u"~", u"myudf"))
            d.addCallback(lambda r: client.make_dir(r.volume_id,
                                                    r.node_id, "subdir"))

            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_unlink_same_path(self):
        """Unlink with similar paths, should work ok."""
        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.get_root())

            # create a subdir in root
            d.addCallback(lambda root: client.make_dir(request.ROOT,
                                                       root, "tdir1"))
            d.addCallback(self.save_req, "dir_del")

            # create the udf, with two subdirs
            d.addCallback(lambda _: client.create_udf(u"~", u"myudf"))
            d.addCallback(self.save_req, "udf")
            d.addCallback(lambda r: client.make_dir(r.volume_id,
                                                    r.node_id, "tdir1"))
            d.addCallback(lambda r: client.make_dir(self._state.udf.volume_id,
                                                    r.new_id, "tdir2"))

            # delete one dir in one volume
            d.addCallback(lambda _: client.unlink(request.ROOT,
                                                  self._state.dir_del.new_id))
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)


class TestVolumesBasic(TestWithDatabase):
    """Test basic operations on volumes."""

    def test_delete_root(self):
        """Test deletion of root volume."""
        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.delete_volume(request.ROOT))

            def check(failure):
                """Checks the error returned."""
                self.assertIsInstance(failure.value,
                                      request.StorageRequestError)
                self.assertEqual(str(failure.value), 'NO_PERMISSION')
                client.test_done(True)
            d.addCallbacks(client.test_fail, check)
        return self.callback_test(auth)

    def test_delete_bad_volume_id(self):
        """Test deletion of bad volume id."""
        def auth(client):
            """Authenticate and test."""
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda _: client.delete_volume('foo bar'))

            def check(failure):
                """Checks the error returned."""
                self.assertIsInstance(failure.value,
                                      request.StorageRequestError)
                self.assertEqual(str(failure.value), 'DOES_NOT_EXIST')
                client.test_done(True)
            d.addCallbacks(client.test_fail, check)
        return self.callback_test(auth)
