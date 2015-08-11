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

"""Tests the share sync functionality of sync daemon."""

import os
import subprocess

from cStringIO import StringIO

from twisted.internet import reactor, defer
from twisted.python.failure import Failure

from ubuntuone.storage.server.integtests import test_sync
from ubuntuone.storage.server.testing.aq_helpers import NO_CONTENT_HASH
from ubuntuone.storageprotocol import request
from ubuntuone.syncdaemon import states


class TestSharesSync(test_sync.TestSync):
    """ Base class for shares tests. """
    called = 0

    def handle_SYS_STATE_CHANGED(self, state):
        """We fire our callback shortly after the state arrives in IDLE."""
        if not self.called and state.queue_state == states.QueueManager.IDLE:
            self.called = 1
            # this is probably a hack:
            # let the other subscribers go first
            reactor.callLater(.1, self.deferred.callback, None)

    def handle_default(self, event_name, **kwargs):
        """Stub implementation."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the tests."""
        # we are in the setUp, so we need to define some attributes.
        self.deferred = defer.Deferred()
        yield super(TestSharesSync, self).setUp()
        self.eq.subscribe(self)
        yield self.deferred
        self.source_dir = self.share_source_dir

        # create two shares more for the tests.
        self.jane_share_id, self.jane_share_subtree = self.create_share(
            shared_by='jane', dirname=u'TestSync', name=u'TestSyncShare')
        self.john_share_id, self.john_share_subtree = self.create_share(
            shared_by='john', dirname=u'TestSync2', name=u'TestSyncShare2')
        self.main.action_q.list_shares()
        yield self.wait_for_nirvana(.2)

        # get the share dir
        wait_share_ok = defer.DeferredList([
            self.wait_for('AQ_ANSWER_SHARE_OK', share_id=self.john_share_id),
            self.wait_for('AQ_ANSWER_SHARE_OK', share_id=self.jane_share_id)],
            fireOnOneErrback=True, consumeErrors=True)

        vm = self.main.vm
        vm.accept_share(self.jane_share_id, True)
        vm.accept_share(self.john_share_id, True)
        self.jane_share_dir = vm.shares[self.jane_share_id].path
        self.john_share_dir = vm.shares[self.john_share_id].path

        yield wait_share_ok
        yield vm.subscribe_share(self.jane_share_id)
        yield vm.subscribe_share(self.john_share_id)

    def create_share(self, shared_by, dirname, name, access_level='Modify'):
        """ Create the test share with Modify access_level """
        u = self.storage_users[shared_by]
        subtree = u.root.make_subdirectory(dirname)
        share = subtree.share(self.storage_users['jack'].id, name,
                              readonly=(access_level == 'View'))
        return str(share.id), subtree

    def compare_dirs(self):
        "run rsync to compare directories, needs some work"
        def _compare():
            """spwan rsync and compare"""
            out = StringIO()
            subprocess.call(["rsync", "-nric", self.jane_share_dir,
                             self.source_dir], stdout=out)
            if not out.getvalue():
                return True
            else:
                return False
        return test_sync.deferToThread(_compare)

    def upload_server(self):
        "upload files in source to the test share"
        return test_sync.TestSync.upload_server(self, share=self.jane_share_id)

    def compare_server(self, dir_name='jane_share_dir',
                       share_id_name='jane_share_id'):
        "compare share with server"
        return test_sync.TestSync.compare_server(
            self, share=getattr(self, share_id_name),
            target=getattr(self, dir_name))


class TestSharesBasic(TestSharesSync, test_sync.TestBasic):
    """Shares basic tests. download from the server."""

    def test_u1sync_failed_compare(self):
        """make sure compare fails if different"""
        open(self.source_dir + "/file", "w").close()
        d = self.compare_server("source_dir")
        d.addCallbacks(lambda _: Failure(Exception("dirs matched, they dont")),
                       lambda _: True)
        return d


class TestSharesBasic2(TestSharesSync, test_sync.TestBasic2):
    """ Basic2 tests for shares"""

    def setUp(self):
        """ Set the root_dir = share_dir"""
        d = super(TestSharesBasic2, self).setUp()

        def set_root_dir(result):
            self.root_dir = self.jane_share_dir

        d.addCallback(set_root_dir)
        return d

    def tearDown(self):
        """ cleanup the test """
        return super(TestSharesBasic2, self).tearDown()


class TestShareClientMove(TestSharesSync, test_sync.TestClientMove):
    """Move on the client (inside shares)."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set the root_dir = share_dir."""
        yield super(TestShareClientMove, self).setUp()
        self.root_dir = self.jane_share_dir

    def tearDown(self):
        """Cleanup the test."""
        return super(TestShareClientMove, self).tearDown()


class TestShareServerBase(TestSharesSync, test_sync.TestServerBase):
    """ Base test case for server-side share related tests. """

    def make_file__legacy(self, username, filename, parent):
        """ create a file in the server. """
        d = self.get_client_by_user(username)
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, parent, filename))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check(username + '_share_dir',
                                           username + '_share_id'))
        return d

    def make_file(self, username, filename, parent):
        """ create a file in the server. """
        # data for putcontent
        hash_value, crc32_value, deflated_size, deflated_content = \
            self.get_put_content_data()

        d = self.get_client_by_user(username)
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, parent, filename))
        d.addCallback(lambda mk: self.client.put_content(
            request.ROOT, mk.new_id, NO_CONTENT_HASH, hash_value,
            crc32_value, 0, deflated_size, StringIO(deflated_content)))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check(username + '_share_dir',
                                           username + '_share_id'))
        return d

    def make_dir(self, username, dirname, parent):
        """ create a dir in the server. """
        d = self.get_client_by_user(username)
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, parent, dirname))
        d.addCallback(
            lambda _: self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check(username + '_share_dir',
                                           username + '_share_id'))
        return d

    def check(self, share_dir, share_id):
        """compare against server."""
        d = self.main.wait_for_nirvana(last_event_interval=0.5)
        d.addCallback(lambda _: self.compare_server(share_dir, share_id))
        return d

    def get_client_by_user(self, username):
        """ returns the client for the user with token: username+'_token'. """
        return self.get_client(username=username,
                               root_id_name=username + '_root_id')


class TestClientMoveMultipleShares(TestShareServerBase):
    """Moves on the client (inside shares).

    E.g.:
        1) Jane shares share1 to jack, john shares share2 to jack.
        2) jack moves (on the filesystem) a file from share1 to share2
        3) jack moves (on the filesystem) a dir from share1 to share2
    """

    @defer.inlineCallbacks
    def test_simple_file_move(self):
        """Move a file inter-shares of two different users."""
        yield self.make_file('jane', 'test_file', self.jane_share_subtree.id)
        yield self.main.wait_for_nirvana(0.5)

        # move a file between shares
        fname = self.jane_share_dir + "/test_file"
        dest_fname = self.john_share_dir + "/test_file"
        os.rename(fname, dest_fname)

        yield self.check('john_share_dir', 'john_share_id')
        yield self.check('jane_share_dir', 'jane_share_id')

    @defer.inlineCallbacks
    def test_dir_move(self):
        """Move a directory inter-shares of two different users."""
        yield self.make_dir('jane', 'test_dir', self.jane_share_subtree.id)
        yield self.main.wait_for_nirvana(0.5)

        # move a dir between shares.
        fname = self.jane_share_dir + "/test_dir"
        dest_fname = self.john_share_dir + "/test_dir"
        os.rename(fname, dest_fname)

        yield self.check('john_share_dir', 'john_share_id')
        yield self.check('jane_share_dir', 'jane_share_id')


class TestReadOnlyShares(TestShareServerBase):
    """Tests for RO Shares."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup a ro share."""
        yield super(TestReadOnlyShares, self).setUp()

        # create another share (ro) for this tests
        self.jane_ro_share_id, self.jane_ro_share_subtree = self.create_share(
            shared_by='jane', dirname=u'TestJaneShareRO',
            name=u'TestJaneShareRO', access_level='View')
        d = self.wait_for('AQ_SHARES_LIST')
        self.main.action_q.list_shares()
        yield d

        # get the share dir
        d = self.wait_for('AQ_ANSWER_SHARE_OK')
        vm = self.main.vm
        vm.accept_share(str(self.jane_ro_share_id), True)
        yield d
        self.jane_ro_share_dir = vm.shares[str(self.jane_ro_share_id)].path
        yield vm.subscribe_share(self.jane_ro_share_id)

    def test_new_dir(self):
        """ adds a new (server-side) in a RO share (local)"""
        d = self.get_client_by_user('jane')
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.jane_ro_share_subtree.id, "test_dir"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.check('jane_ro_share_dir',
                                           'jane_ro_share_id'))
        return d
