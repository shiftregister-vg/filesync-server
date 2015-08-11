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

"""Tests the UDF sync functionality of sync daemon."""

import os
import subprocess

from cStringIO import StringIO

from twisted.internet import reactor, defer
from twisted.python.failure import Failure

from ubuntuone.storage.server.integtests import test_sync
from ubuntuone.storage.server.testing.aq_helpers import NO_CONTENT_HASH


class TestUDFSync(test_sync.TestSync):
    """Base class for UDF tests."""
    called = 0

    def handle_SYS_STATE_CHANGED(self, state):
        """We fire our callback shortly after the state arrives in IDLE."""
        if not self.called and state.name == 'QUEUE_MANAGER' \
                and state.queue_state.name == 'IDLE':
            self.called = 1
            # this is probably a hack:
            # let the other subscribers go first
            reactor.callLater(.1, self.deferred.callback, None)

    def handle_default(self, event_name, *args, **kwargs):
        """Stub implementation."""
        pass

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the tests."""
        # we are in the setUp, so we need to define some attributes.
        yield super(TestUDFSync, self).setUp()
        self.eq.subscribe(self)
        self.deferred = defer.Deferred()
        self.udf_source_dir = self.mktemp('source/udf')
        self.source_dir = self.udf_source_dir
        yield self.deferred
        # create a UDF for the tests
        yield self.wait_for_nirvana(.2)
        self.my_udf = yield self.create_udf('TestUDF')
        self.my_udf_id = self.my_udf.id
        self.my_udf_dir = self.my_udf.path

    @defer.inlineCallbacks
    def create_udf(self, name):
        """Create a UDF."""
        # do not loose the event, wait for it before creating the UDF
        wait_for_udf_created = self.wait_for('VM_UDF_CREATED')
        path = os.path.join(self.home_dir, name)
        self.main.vm.create_udf(path)
        yield wait_for_udf_created

        for udf in self.main.vm.udfs.values():
            if udf.path == path:
                defer.returnValue(udf)
        else:
            raise ValueError("No UDF created.")

    def compare_dirs(self):
        """Run rsync to compare directories, needs some work."""
        def _compare():
            """spwan rsync and compare"""
            out = StringIO()
            subprocess.call(["rsync", "-nric", self.my_udf_dir,
                             self.source_dir], stdout=out)
            if not out.getvalue():
                return True
            else:
                return False
        return test_sync.deferToThread(_compare)

    def upload_server(self):
        """Upload files in source to the test udf."""
        return super(TestUDFSync, self).upload_server(
            share=str(self.my_udf_id))

    def compare_server(self, dir_name='my_udf_dir',
                       udf_id_name='my_udf_id'):
        """Compare UDF with server."""
        return super(TestUDFSync, self).compare_server(
            share=str(getattr(self, udf_id_name)),
            target=getattr(self, dir_name))


class TestUDFBasic(TestUDFSync, test_sync.TestBasic):
    """UDF basic tests, download from the server."""

    def test_u1sync_failed_compare(self):
        """make sure compare fails if different"""
        open(self.source_dir + "/file", "w").close()
        d = self.compare_server("source_dir")
        d.addCallbacks(lambda _: Failure(Exception("dirs matched, they dont")),
                       lambda _: True)
        return d


class TestUDFBasic2(TestUDFSync, test_sync.TestBasic2):
    """Basic2 tests for UDFs."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set the root_dir = my_udf_dir."""
        yield super(TestUDFBasic2, self).setUp()
        self.root_dir = self.my_udf_dir

    def tearDown(self):
        """Cleanup the test."""
        return super(TestUDFBasic2, self).tearDown()


class TestUDFClientMove(TestUDFSync, test_sync.TestClientMove):
    """Move on the client (inside UDF)."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set the root_dir = my_udf_dir."""
        yield super(TestUDFClientMove, self).setUp()
        self.root_dir = self.my_udf_dir

    def tearDown(self):
        """ cleanup the test """
        return super(TestUDFClientMove, self).tearDown()


class TestUDFServerBase(TestUDFSync, test_sync.TestServerBase):
    """Base test case for server-side UDF related tests."""

    def make_file(self, udf_name, filename, parent):
        """Create a file in the server."""
        # data for putcontent
        hash_value, crc32_value, deflated_size, deflated_content = \
            self.get_put_content_data()

        volume_id = getattr(self, udf_name + '_id')
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(volume_id,
                                                      parent, filename))
        d.addCallback(lambda mk: self.client.put_content(
            volume_id, mk.new_id, NO_CONTENT_HASH, hash_value, crc32_value, 0,
            deflated_size, StringIO(deflated_content)))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check(udf_name + '_dir',
                                           udf_name + '_id'))
        return d

    def make_dir(self, udf_name, dirname, parent):
        """Create a dir in the server."""
        volume_id = getattr(self, udf_name + '_id')
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(volume_id,
                                                     parent, dirname))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check(udf_name + '_dir',
                                           udf_name + '_id'))
        return d

    def check(self, udf_dir, udf_id):
        """Compare against server."""
        d = self.main.wait_for_nirvana(last_event_interval=0.5)
        d.addCallback(lambda _: self.compare_server(udf_dir, udf_id))
        return d


class TestClientMoveMultipleUDFs(TestUDFServerBase):
    """Moves on the client (between UDFs), e.g:
        1) jack has two UDFs
        2) jack moves (on the filesystem) a file from udf1 to udf2
        3) jack moves (on the filesystem) a dir from udf1 to udf2

    """

    @defer.inlineCallbacks
    def setUp(self):
        """Create another UDF."""
        yield super(TestClientMoveMultipleUDFs, self).setUp()
        # Creates a extra UDF for the cross UDF tests
        yield self.wait_for_nirvana(.2)
        self.other_udf = yield self.create_udf('TestUDF2')
        self.other_udf_id = self.other_udf.id
        self.other_udf_dir = self.other_udf.path

    @defer.inlineCallbacks
    def test_simple_file_move(self):
        """Move a file inter-UDFs."""
        yield self.make_file('my_udf', 'test_file', self.my_udf.node_id)
        yield self.main.wait_for_nirvana(last_event_interval=0.3)
        # move a file between UDFs
        fname = self.my_udf_dir + "/test_file"
        dest_fname = self.other_udf_dir + "/test_file"
        os.rename(fname, dest_fname)
        yield self.check('my_udf_dir', 'my_udf_id')
        yield self.check('other_udf_dir', 'other_udf_id')

    @defer.inlineCallbacks
    def test_dir_move(self):
        """Move a directory inter-UDFs."""
        yield self.make_dir('my_udf', 'test_dir', self.my_udf.node_id)
        yield self.main.wait_for_nirvana(last_event_interval=0.3)
        # Move a dir between UDFs
        fname = self.my_udf_dir + "/test_dir"
        dest_fname = self.other_udf_dir + "/test_dir"
        os.rename(fname, dest_fname)

        yield self.check('my_udf_dir', 'my_udf_id')
        yield self.check('other_udf_dir', 'other_udf_id')


class TestUDFServerMove(TestUDFServerBase):
    """Server-side moves in UDFs."""

    @defer.inlineCallbacks
    def setUp(self):
        """Create another UDF."""
        yield super(TestUDFServerMove, self).setUp()
        # Creates a extra UDF for the cross UDF tests
        yield self.wait_for_nirvana(.2)
        self.other_udf = yield self.create_udf('TestUDF2')
        self.other_udf_id = self.other_udf.id
        self.other_udf_dir = self.other_udf.path

    def tearDown(self):
        """Cleanup the test."""
        return super(TestUDFServerMove, self).tearDown()

    @defer.inlineCallbacks
    def test_simple_move(self):
        """Server-side move of a file inside a UDF."""
        # data for putcontent
        hash_value, crc32_value, deflated_size, deflated_content = \
            self.get_put_content_data()

        yield self.get_client()
        req = yield self.client.make_file(self.my_udf_id,
                                          self.my_udf.node_id, "test_file")
        yield self.client.put_content(self.my_udf_id, req.new_id,
                                      NO_CONTENT_HASH, hash_value, crc32_value,
                                      0, deflated_size,
                                      StringIO(deflated_content))
        yield self.main.wait_for_nirvana(last_event_interval=.5)
        yield self.client.move(self.my_udf_id, req.new_id,
                               self.my_udf.node_id, "test_file_moved")
        yield self.check()

    @defer.inlineCallbacks
    def test_simple_dir_move(self):
        """Test rename dir."""
        yield self.get_client()
        d = self.client.make_dir(self.my_udf_id,
                                 self.my_udf.node_id, "test_dir")
        d.addCallback(self.save("request"))
        yield d
        yield self.main.wait_for_nirvana(last_event_interval=1)
        yield self.client.move(self.my_udf_id, self.request.new_id,
                               self.my_udf.node_id, "test_dir_moved")
        yield self.check()

    def check(self):
        """Compare against server."""
        return super(TestUDFServerMove, self).check('my_udf_dir', 'my_udf_id')
