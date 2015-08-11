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

"""Test making nodes."""

import uuid
import zlib

from StringIO import StringIO

from twisted.internet import threads, defer

from backends.filesync.data import errors
from ubuntuone.storageprotocol import request, volumes
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32

NO_CONTENT_HASH = ""


class TestMakeFile(TestWithDatabase):
    """Test make_file command."""

    def test_mkfile(self):
        """Create a file."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def build_mime_test(filename, file_mime):
        """create test cases for mime type checking."""
        def test_mkfile_mime_type(self):
            """Create a file."""
            def auth(client):
                def check_file(result):
                    def _check_file():
                        try:
                            file = self.usr0.get_node(result.new_id)
                        except errors.DoesNotExist:
                            raise ValueError("storage object is missing")
                        assert(file.mimetype == file_mime)
                    d = threads.deferToThread(_check_file)
                    return d
                d = client.dummy_authenticate("open sesame")
                d.addCallbacks(lambda r: client.get_root(), client.test_fail)
                d.addCallbacks(
                    lambda r: client.make_file(request.ROOT, r, filename),
                    client.test_fail)
                d.addCallback(check_file)
                d.addCallbacks(client.test_done, client.test_fail)
            return self.callback_test(auth)
        return test_mkfile_mime_type

    test_mkfile_mime1 = build_mime_test("image.png", "image/png")
    test_mkfile_mime2 = build_mime_test("noextension", None)
    test_mkfile_mime3 = build_mime_test("music.mp3", "audio/mpeg")
    test_mkfile_mime4 = build_mime_test("document.doc", "application/msword")
    test_mkfile_mime5 = build_mime_test("test.txt", "text/plain")
    test_mkfile_mime6 = build_mime_test("package.zip", "application/zip")

    del build_mime_test

    def test_mkfile_unicode(self):
        """Create a file."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, u"á"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkfile2(self):
        """Create a file in a sub directory."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r.new_id, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkfile3(self):
        """Create two files with different names."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "chau"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkfile_on_file(self):
        """Create a file on a file"""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r.new_id, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkdir_on_file(self):
        """Create a dir on a file and fail."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r.new_id, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkfile_already_exists_empty(self):
        """Create a file on a file that already exists."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda x: client.test_done("ok"), client.test_fail)
        return self.callback_test(auth)

    def test_mkfile_already_exists_content(self):
        """Create a file on a file that already exists and have content."""
        data = "*" * 100
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")
            d.addCallback(lambda r: client.make_file(request.ROOT, r, "hola"))
            d.addCallback(lambda req: client.put_content(request.ROOT,
                          req.new_id, NO_CONTENT_HASH, hash_value, crc32_value,
                          size, deflated_size, StringIO(deflated_data)))
            d.addCallback(lambda r: client.make_file(request.ROOT,
                                                     self._state.root, "hola"))
            d.addCallbacks(
                lambda x: client.test_done("ok"), client.test_fail)
        return self.callback_test(auth)

    def test_mkfile_auth_required(self):
        """Require authentication for make_file."""
        def auth(client):
            d = client.make_file(request.ROOT, uuid.uuid4(), "hola")
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkfile_doesnt_exists(self):
        """Make a file with a parent that does not exists. Fail."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(request.ROOT, uuid.uuid4(), "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkfile_doesnt_exist_not_uuid(self):
        """Create a file with a parent that is not a valid uuid."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file(
                    request.ROOT, 'idontexist_imnotanuuid', "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkfile_generations(self):
        """Create a file and receive new generation."""
        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            req = yield client.list_volumes()
            root = [v for v in req.volumes
                    if isinstance(v, volumes.RootVolume)][0]
            req = yield client.make_file(request.ROOT, root.node_id, "hola")
            self.assertEqual(req.new_generation, root.generation + 1)
        return self.callback_test(test, add_default_callbacks=True)


class TestMakeDir(TestWithDatabase):
    """Test make_dir command."""
    def test_mkdir(self):
        """Create a directory."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkdir_unicode(self):
        """Create a directory."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, u"¶á"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkdir_invalid_character(self):
        """Try to create a dir with invalid characters and fail."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola / "),
                client.test_fail)

            def check(failure):
                """Checks the error returned."""
                self.assertIsInstance(failure.value,
                                      request.StorageRequestError)
                client.test_done(True)
            d.addCallbacks(client.test_fail, check)
        return self.callback_test(auth)

    def test_mkdir_unicode_surrogates(self):
        """Try to create a dir with unicode data points that are not chars."""
        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            root = yield client.get_root()
            d = client.make_dir(request.ROOT, root, u"surrogate \udad6")
            res = yield self.assertFailure(d, request.StorageRequestError)
            self.assertEqual(str(res), "INVALID_FILENAME")
        return self.callback_test(test, add_default_callbacks=True)

    def test_mkdir2(self):
        """Create a directory on a sub directory."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r.new_id, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkdir3(self):
        """Create two directories."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "hola"),
                client.test_fail)
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, r, "chau"),
                client.test_fail)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkdir_already_exists(self):
        """Create a directory with a name that already exists."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallback(lambda r: client.get_root())
            d.addCallback(self.save_req, "root")
            d.addCallback(lambda r: client.make_dir(request.ROOT, r, "hola"))
            d.addCallback(self.save_req, "dir")
            d.addCallback(lambda r: client.make_dir(request.ROOT,
                                                    self._state.root, "hola"))

            def check(req):
                self.assertEqual(req.new_id, self._state.dir.new_id)

            d.addCallback(check)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)

    def test_mkdir_auth_required(self):
        """Make dir requires authentication."""
        def auth(client):
            d = client.make_dir(request.ROOT, uuid.uuid4(), "hola")
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkdir_doesnt_exists(self):
        """Make a directory on a node that does not exists."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(request.ROOT, uuid.uuid4(), "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkdir_doesnt_exist_not_uuid(self):
        """Make a directory on a node that is not an uuid."""
        def auth(client):
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir(
                    request.ROOT, 'idontexist_imnotanuuid', "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, lambda x: client.test_done("ok"))
        return self.callback_test(auth)

    def test_mkdir_generations(self):
        """Create a dir and receive new generation."""
        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield client.dummy_authenticate("open sesame")
            req = yield client.list_volumes()
            root = [v for v in req.volumes
                    if isinstance(v, volumes.RootVolume)][0]
            req = yield client.make_dir(request.ROOT, root.node_id, "hola")
            self.assertEqual(req.new_generation, root.generation + 1)
        return self.callback_test(test, add_default_callbacks=True)
