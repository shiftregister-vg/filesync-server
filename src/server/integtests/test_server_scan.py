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

"""Tests for server rescan."""

import logging
import zlib

from cStringIO import StringIO

from twisted.internet import defer

from ubuntuone.storageprotocol import request
from test_sync import TestServerBase
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32
from ubuntuone.storage.server.testing.aq_helpers import NO_CONTENT_HASH
from ubuntuone.devtools.handlers import MementoHandler


class TestServerScan(TestServerBase):
    """Basic tests of the server rescan."""

    N = 10  # number of files to create

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestServerScan, self).setUp()
        yield self.get_client()
        yield self.do_create_lots_of_files('_pre')
        self.handler = handler = MementoHandler()
        handler.setLevel(logging.DEBUG)
        logging.getLogger('fsyncsrvr.SyncDaemon').addHandler(handler)

    @defer.inlineCallbacks
    def do_create_lots_of_files(self, suffix=''):
        """A helper that creates N files."""
        # data for putcontent
        ho = content_hash_factory()
        hash_value = ho.content_hash()
        crc32_value = crc32("")
        deflated_content = zlib.compress("")
        deflated_size = len(deflated_content)

        mk = yield self.client.make_file(request.ROOT, self.root_id,
                                         "test_first" + suffix)
        yield self.client.put_content(
            request.ROOT, mk.new_id, NO_CONTENT_HASH, hash_value,
            crc32_value, 0, deflated_size, StringIO(deflated_content))

        for i in xrange(self.N):
            mk = yield self.client.make_file(request.ROOT, self.root_id,
                                             "test_%03x%s" % (i, suffix))
            yield self.client.put_content(request.ROOT, mk.new_id,
                                          NO_CONTENT_HASH, hash_value,
                                          crc32_value, 0, deflated_size,
                                          StringIO(deflated_content))
