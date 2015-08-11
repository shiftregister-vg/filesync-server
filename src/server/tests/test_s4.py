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

"""S4 specific tests."""

import zlib

from StringIO import StringIO

from twisted.internet import threads

from s3lib.s3lib import S3
from s4 import s4
from backends.filesync.data import errors
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storageprotocol import request
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32

AWS_HOST = "localhost"
NO_CONTENT_HASH = ""
EMPTY_HASH = content_hash_factory().content_hash()


class TestS4(TestWithDatabase):
    """Test S4"""

    def test_alive(self):
        """Test that s4 has started."""
        size = 100
        s3 = S3(AWS_HOST, self.s4_port, s4.AWS_DEFAULT_ACCESS_KEY_ID,
                s4.AWS_DEFAULT_SECRET_ACCESS_KEY)
        f = s3.get("size", str(size))
        d = f.deferred

        def done(result):
            self.assertEqual(result, "0" * size)

        def err(result):
            raise self.fail(result)

        d.addCallbacks(done, err)
        return d

    def test_putcontent(self):
        """Test putting content to a file."""
        data = "*" * 100000
        deflated_data = zlib.compress(data)
        hash_object = content_hash_factory()
        hash_object.update(data)
        hash_value = hash_object.content_hash()
        crc32_value = crc32(data)
        size = len(data)
        deflated_size = len(deflated_data)

        def auth(client):
            def check_file(result):
                def _check_file():
                    try:
                        content_blob = self.usr0.volume().get_content(
                            hash_value)
                    except errors.DoesNotExist:
                        raise ValueError("content blob is not there")
                    assert self.s4_site.resource.buckets["test"] \
                        .bucket_children[str(content_blob.storage_key)] \
                        .contents == deflated_data

                d = threads.deferToThread(_check_file)
                return d

            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda _: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda root: client.make_file(request.ROOT, root, "hola"),
                client.test_fail)
            d.addCallbacks(
                lambda mkfile_req: client.put_content(
                    request.ROOT, mkfile_req.new_id, NO_CONTENT_HASH,
                    hash_value, crc32_value, size, deflated_size,
                    StringIO(deflated_data)),
                client.test_fail)
            d.addCallback(check_file)
            d.addCallbacks(client.test_done, client.test_fail)
        return self.callback_test(auth)
