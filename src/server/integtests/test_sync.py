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

"""Test the sync functionality of sync daemon."""

import logging
import os
import re
import stat
import shutil
import subprocess
import threading
import uuid
import zlib

from StringIO import StringIO

from twisted.internet import reactor, defer
from twisted.python.failure import Failure

import u1sync.client

from backends.filesync.data import model
from u1sync.main import do_diff, do_init, do_sync
from ubuntuone.storage.server import server
from ubuntuone.storageprotocol import client as protocol_client
from ubuntuone.storageprotocol import request
from ubuntuone.storageprotocol.client import (
    StorageClient,
    StorageClientFactory,
)
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32
from ubuntuone.storageprotocol import protocol_pb2
from ubuntuone.platform import tools
from ubuntuone.syncdaemon import REQUIRED_CAPS

from ubuntuone.storage.server.testing.aq_helpers import show_time  # NOQA
from ubuntuone.storage.server.testing.aq_helpers import (
    TestWithDatabase,
    NukeAQClient,
    MethodInterferer,
    FakeGetContent,
)
from ubuntuone.storage.server.testing.caps_helpers import required_caps
from ubuntuone.syncdaemon import hash_queue
from ubuntuone.syncdaemon.main import Main

U1SYNC_QUIET = True
#U1SYNC_QUIET = False

NO_CONTENT_HASH = ""


class ErrorHandler(logging.Handler):
    """A Log Handler to detect exceptions."""
    def __init__(self):
        """create it"""
        logging.Handler.__init__(self)
        self.errors = []

    def emit(self, record):
        """Add the record."""
        if record.levelno >= logging.ERROR:
            self.errors.append(record)


def deferToThread(func, *args, **kwargs):
    """glib reactor doesnt have this."""
    d = defer.Deferred()

    def inner():
        """inner"""
        try:
            r = func(*args, **kwargs)
        except Exception, e:
            reactor.callFromThread(d.errback, e)
        else:
            reactor.callFromThread(d.callback, r)

    t = threading.Thread(target=inner)
    t.start()
    return d


def filter_symlinks(root, listdir):
    """filter symlinks from a list of paths"""
    path_join = os.path.join
    islink = os.path.islink
    no_links = [d for d in listdir if not islink(path_join(root, d))]
    return no_links


class TestSync(TestWithDatabase):
    """Basic test setup"""
    # this all takes a lot of time
    timeout = 300
    _ignore_cancelled_downloads = True

    def no_op(self):
        """Do nothing."""

    @defer.inlineCallbacks
    def client_setup(self):
        """Crete the client."""
        # ensure a clean tmpdir
        if os.path.exists(self.tmpdir):
            self.rmtree(self.tmpdir)
        self.home_dir = self.mktemp('home_dir')
        self.root_dir = root_dir = os.path.join(self.home_dir, "root")
        self.shares_dir = shares_dir = self.mktemp("shares")
        data_dir = self.mktemp("data")
        partials_dir = self.mktemp("partials_dir")
        self.source_dir = self.mktemp("source/root")
        self.share_source_dir = self.mktemp("source/share")

        self.patch(Main, "start_status_listener", self.no_op)
        self.patch(hash_queue, "HASHQUEUE_DELAY", 0.1)
        self.main = Main(root_dir, shares_dir, data_dir, partials_dir,
                         "localhost", self.ssl_port, dns_srv=None, ssl=True,
                         disable_ssl_verify=True)
        self.addCleanup(self.main.shutdown)

        self.eq = self.main.event_q
        self.aq = self.main.action_q
        self.u1sync_init = True
        self.tool = tools.SyncDaemonTool(self.bus)

        # start and wait to be started
        d = self.wait_for('SYS_LOCAL_RESCAN_DONE')
        self.main.start()
        yield d

        d = self.wait_for('SYS_SERVER_RESCAN_DONE')
        self.eq.push('SYS_USER_CONNECT',
                     access_token=self.access_tokens['jack'])
        self.eq.push('SYS_NET_CONNECTED')
        yield d

        self.error_handler = ErrorHandler()
        logger = logging.getLogger("fsyncsrvr.SyncDaemon.sync")
        logger.addHandler(self.error_handler)
        logger = logging.getLogger("twisted")
        logger.addHandler(self.error_handler)

    def _wait_for_dead_nirvana(self):
        """Wait until it's disconnected."""
        d = defer.Deferred()
        state = self.main.state_manager.state

        def check():
            """Check that it's finished finishing."""
            if state.name in ("READY", "STANDOFF", "SHUTDOWN"):
                d.callback(True)
            else:
                reactor.callLater(.5, check)
        check()
        return d

    @defer.inlineCallbacks
    def tearDown(self):
        """Close the client."""
        shutil.rmtree(self.tmpdir)

        # cancel pending timer for FS notifications, if still waiting for
        # anything (if what is waiting is needed for the test, the test
        # should had handled it)
        timer = self.eq.monitor._processor.timer
        if timer is not None and timer.active():
            timer.cancel()

        try:
            yield super(TestSync, self).tearDown()
            yield self._wait_for_dead_nirvana()
        finally:
            # make sure no errors were reported
            logger = logging.getLogger("fsyncsrvr.SyncDaemon.sync")
            logger.removeHandler(self.error_handler)
            logger = logging.getLogger("twisted")
            logger.removeHandler(self.error_handler)
            if self.error_handler.errors:
                errs = "\n".join(e.getMessage()
                                 for e in self.error_handler.errors)
                raise Exception("Test ended with errors: \n" + errs)

    def compare_dirs(self):
        "run rsync to compare directories, needs some work"
        def _compare():
            """spwan rsync and compare"""
            cmd = ["rsync", "-nric", self.root_dir, self.source_dir]
            output = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                      env=os.environ).communicate()[0]
            if not output:
                return True
            else:
                print "****\n", output, "****"
                return False
        return deferToThread(_compare)

    def upload_server(self, share=None):
        """Upload self.source_dir to the server."""
        def _runit():
            """Worker."""
            c = self._u1sync_client()
            try:
                if self.u1sync_init:
                    do_init(
                        client=c, share_spec=share, directory=self.source_dir,
                        quiet=U1SYNC_QUIET, subtree_path=None)
                    self.u1sync_init = False
                do_sync(
                    client=c, directory=self.source_dir,
                    action="clobber-server", dry_run=False, quiet=U1SYNC_QUIET)
            finally:
                c.disconnect()
        return deferToThread(_runit)

    def compare_server(self, target=None, share=None):
        "compare target with server"
        if target is None:
            target = self.root_dir

        def _runit():
            """worker"""
            c = self._u1sync_client()
            try:
                do_diff(client=c, share_spec=share, directory=target,
                        quiet=U1SYNC_QUIET, subtree_path=None)
            finally:
                c.disconnect()

        return deferToThread(_runit)

    def _u1sync_client(self):
        """Create a u1sync client instance."""
        client = u1sync.client.Client()
        client.connect_ssl("localhost", self.ssl_port, True)
        client.set_capabilities()
        auth_info = self.access_tokens['jack']
        client.simple_auth(auth_info['username'], auth_info['password'])
        return client

    @defer.inlineCallbacks
    def sync_and_check(self):
        """Wait for synchronization and diff."""
        yield self.upload_server()
        yield self.main.wait_for_nirvana(0.5)
        yield self.compare_server()

    @defer.inlineCallbacks
    def check(self):
        """Compare against server."""
        yield self.main.wait_for_nirvana(last_event_interval=0)
        yield self.compare_server()


class TestBrokenNode(TestSync):
    """Tests for broken nodes in server."""

    @defer.inlineCallbacks
    def test_broken_node_in_server(self):
        """Test that current client doesn't delete local node."""
        with open(self.root_dir + "/test_file", "w") as f:
            f.write("foo")
            f.flush()
        yield self.main.wait_for_nirvana(0.5)

        # now, modify the file:
        #   - set the content_hash to None
        #   - increase the generation
        user = self.storage_users['jack']
        store = self.get_shard_store(user.shard_id)
        root = model.StorageObject.get_root(store, user.id)
        afile = root.get_child_by_name(u"test_file")
        # create a "invalid" content blob
        content = model.ContentBlob()
        content.hash = ""
        content.magic_hash = ""
        content.storage_key = uuid.uuid4()
        content.crc32 = 1
        content.size = 1
        content.deflated_size = 1
        content.status = model.STATUS_LIVE
        store.add(content)
        store.flush()
        # change the node
        afile._content_hash = ""
        afile.update_generation()
        store.commit()
        # reconnect the client and wait it to sync
        d = self.wait_for('SYS_SERVER_RESCAN_DONE')
        self.eq.push('SYS_NET_DISCONNECTED')
        self.eq.push('SYS_NET_CONNECTED')
        yield d
        yield self.main.wait_for_nirvana(0.5)
        # check that the file is still there, with the same contents.
        file_path = self.root_dir + "/test_file"
        self.assertTrue(os.path.exists(file_path))
        self.assertEqual(open(file_path).read(), "foo")
        # re-get the file and check the server content is fixed
        afile = root.get_child_by_name(u"test_file")
        self.assertTrue(afile.content.hash)
        self.assertTrue(afile.content.magic_hash)


class TestBasic(TestSync):
    """Basic tests"""

    def test_u1sync_compare(self):
        """everything matches on empty"""
        # wait for SD nirvana, as this test doesn't use it and we need
        # not to leave the reactor dirty with SD dance
        d = self.main.wait_for_nirvana(last_event_interval=0.5)
        d.addCallback(lambda _: self.compare_server())
        return d

    def test_u1sync_upload(self):
        """upload nothing and works"""
        # wait for SD nirvana, as this test doesn't use it and we need
        # not to leave the reactor dirty with SD dance
        d = self.main.wait_for_nirvana(last_event_interval=0.5)
        d.addCallback(lambda _: self.upload_server())
        return d

    def test_u1sync_failed_compare(self):
        """make sure compare fails if different"""
        open(self.source_dir + "/file", "w").close()
        d = self.compare_server(self.source_dir)
        d.addCallbacks(lambda _: Failure(Exception("dirs matched, they dont")),
                       lambda _: True)

        # wait for SD nirvana, as this test doesn't use it and we need
        # not to leave the reactor dirty with SD dance
        d.addCallback(lambda _: self.wait_for_nirvana(last_event_interval=0.5))
        return d

    def test_sync_a_file(self):
        """Sync one file."""
        open(self.source_dir + "/file", "w").close()
        return self.sync_and_check()

    def test_sync_a_strangely_named_file(self):
        """Sync one file with non-ascii name."""
        open(self.source_dir + u"/propósito".encode("utf8"), "w").close()
        return self.sync_and_check()

    @defer.inlineCallbacks
    def test_sync_a_file_with_content(self):
        """Sync one file with content."""
        open(self.source_dir + "/file", "w").close()
        yield self.sync_and_check()

        # put content on the file
        with open(self.source_dir + "/file", "w") as fd:
            fd.write("HELLO WORLD")

        yield self.sync_and_check()

    def test_sync_a_dir(self):
        """Sync one directory."""
        os.mkdir(self.source_dir + "/test_dir")
        return self.sync_and_check()

    def test_sync_a_dir_and_subdir(self):
        """Sync one directory and then a sub directory."""
        os.mkdir(self.source_dir + "/test_dir")
        d = self.sync_and_check()
        d.addCallback(lambda _: os.mkdir(
            self.source_dir + "/test_dir/test_dir2"))
        d.addCallback(lambda _: self.sync_and_check())
        return d

    def test_sync_a_dir_and_subfile(self):
        """Sync one directory and a sub file."""
        os.mkdir(self.source_dir + "/test_dir")
        d = self.sync_and_check()
        d.addCallback(lambda _: open(
            self.source_dir + "/test_dir/test_file", "w").close())
        d.addCallback(lambda _: self.sync_and_check())
        return d

    # we cant test with multiple files, becase each time u1sync creates
    # a file we get a new hash for the directory, and we need cancel
    # support for that


class TestBasic2(TestSync):
    """Phase 2 tests. Local changes, upload to the server."""

    def test_sync_a_dir(self):
        """Sync one directory."""
        os.mkdir(self.root_dir + "/test_dir")
        d = self.check()
        return d

    def test_sync_a_file(self):
        """Sync one file."""
        open(self.root_dir + "/test_file", "w").close()
        d = self.check()
        return d

    @defer.inlineCallbacks
    def test_sync_a_file_with_content(self):
        """Sync one file with content."""
        wait_for_hash1 = self.wait_for("AQ_UPLOAD_FINISHED")
        open(self.root_dir + "/test_file", "w").close()
        yield wait_for_hash1
        yield self.check()
        wait_for_hash2 = self.wait_for("AQ_UPLOAD_FINISHED")
        fd = open(self.root_dir + "/test_file", "w")
        fd.write("HELLO WORLD")
        fd.close()
        yield wait_for_hash2
        yield self.check()

    def test_sync_a_file_with_content_nowait(self):
        """Sync one file with content."""
        fd = open(self.root_dir + "/test_file", "w")
        fd.write("HELLO WORLD")
        fd.close()
        return self.check()

    def test_sync_a_file_with_content_nowait_twice(self):
        """Sync one file with content."""
        fd = open(self.root_dir + "/test_file", "w")
        fd.write("HELLO WORLD")
        fd.close()
        d = defer.Deferred()

        # we give the reactor some time to breathe
        # and read from filesystem
        # so we make sure we get to events
        def cont():
            """continue operation"""
            fd = open(self.root_dir + "/test_file", "w")
            fd.write("HELLO WORLD2")
            fd.close()
            d.callback(True)
        reactor.callLater(0, cont)

        d.addCallback(lambda _: self.check())
        return d

    def test_sync_a_file_with_content_same_nowait_twice(self):
        """Sync one file with content."""
        fd = open(self.root_dir + "/test_file", "w")
        fd.write("HELLO WORLD")
        fd.close()
        d = defer.Deferred()

        # we give the reactor some time to breathe
        # and read from filesystem
        # so we make sure we get to events
        def cont():
            """continue operation"""
            fd = open(self.root_dir + "/test_file", "w")
            fd.write("HELLO WORLD")
            fd.close()
            d.callback(True)
        reactor.callLater(0, cont)

        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_sync_a_file_with_content_same_revert_twice(self):
        """Sync one file with content."""
        fd = open(self.root_dir + "/test_file", "w")
        fd.write("HELLO WORLD" * 1000)
        fd.close()
        yield self.main.wait_for_nirvana(.5)

        fd = open(self.root_dir + "/test_file", "w")
        fd.write("X")
        fd.close()

    def test_sync_a_dir_and_subfile(self):
        """Sync one directory and a sub file."""
        os.mkdir(self.root_dir + "/test_dir")
        d = self.check()
        d.addCallback(lambda _:
                      open(self.root_dir + "/test_dir/test_file", "w").close())
        d.addCallback(lambda _: self.check())
        return d

    def test_sync_a_dir_and_subdir(self):
        """Sync one directory and a sub dir."""
        os.mkdir(self.root_dir + "/test_dir")
        d = self.check()
        d.addCallback(lambda _: os.mkdir(
            self.root_dir + "/test_dir/test_subdir"))
        d.addCallback(lambda _: self.check())
        return d

    def test_sync_a_symlink(self):
        """Fail to sync one symlink (sync nothing!)."""
        a_file = os.path.join(self.tmpdir, "file")
        open(a_file, "w").close()
        os.symlink(a_file, os.path.join(self.root_dir, 'a_symlink'))
        d = self.sync_and_check()

        def cleanup(r):
            """remove the file"""
            os.remove(a_file)
        d.addBoth(cleanup)
        return d

    @defer.inlineCallbacks
    def test_delete_dir_new_same_name(self):
        """Delete a dir and create other with the same name."""
        # create initial dir and wait until is fully scanned
        direct = os.path.join(self.root_dir, "direct")
        os.mkdir(direct)
        yield self.main.wait_for_nirvana(0.5)

        # delete it, create a new one with same name
        os.rmdir(direct)
        os.mkdir(direct)
        yield self.main.wait_for_nirvana(0.5)  # to LR not pick up the file
        open(os.path.join(direct, "testfile"), "w").close()

        # wait, and check
        yield self.main.wait_for_nirvana(0.5)
        yield self.compare_server()

    @defer.inlineCallbacks
    def test_rename_dir_new_same_name(self):
        """Rename a dir and create other with the same name."""
        # create initial dir and wait until is fully scanned
        dir1 = os.path.join(self.root_dir, "dir1")
        dir2 = os.path.join(self.root_dir, "dir2")
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(0.5)

        # rename it, create a new one with same name and file inside
        os.rename(dir1, dir2)
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(0.5)  # to LR not pick up the file
        open(os.path.join(dir1, "testfile"), "w").close()

        #  wait, and check
        yield self.main.wait_for_nirvana(0.5)
        yield self.compare_server()

    @defer.inlineCallbacks
    def test_moveout_dir_new_same_name(self):
        """Delete a dir and create other with the same name."""
        # create initial dir and wait until is fully scanned
        dir1 = os.path.join(self.root_dir, "direct")  # watched
        dir2 = os.path.join(self.home_dir, "direct")  # not watched
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(0.5)

        # move it out, create a new one with same name and file inside
        os.rename(dir1, dir2)
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(0.5)  # to LR not pick up the file
        open(os.path.join(dir1, "testfile"), "w").close()

        #  wait, and check
        yield self.main.wait_for_nirvana(0.5)
        yield self.compare_server()


class TestServerDelete(TestSync):
    """test deleting on the server and downloading the changes."""

    def test_sync_a_file_and_delete(self):
        """Sync one file with content."""
        open(self.source_dir + "/file", "w").close()
        d = self.sync_and_check()

        def remove(_):
            """put content on the file"""
            os.remove(self.source_dir + "/file")

        d.addCallback(remove)
        d.addCallback(lambda _: self.sync_and_check())
        return d

    def test_sync_a_dir_and_delete(self):
        """Sync one file with content."""
        os.mkdir(self.source_dir + "/test_dir")
        d = self.sync_and_check()

        def remove(_):
            """put content on the file"""
            os.rmdir(self.source_dir + "/test_dir")

        d.addCallback(remove)
        d.addCallback(lambda _: self.sync_and_check())
        return d

    def test_sync_a_dir_and_subfile_and_delete(self):
        """Sync one file with content."""
        os.mkdir(self.source_dir + "/test_dir")
        open(self.source_dir + "/test_dir/file", "w").close()
        d = self.sync_and_check()

        def remove(_):
            """put content on the file"""
            os.remove(self.source_dir + "/test_dir/file")
            os.rmdir(self.source_dir + "/test_dir")

        d.addCallback(remove)
        d.addCallback(lambda _: self.sync_and_check())
        return d


class TestClientDelete(TestSync):
    """Test deleting locally and uploading the changes"""

    def test_sync_a_file_and_delete(self):
        """Sync one file."""
        open(self.root_dir + "/test_file", "w").close()
        d = self.check()

        def remove(_):
            """remove it"""
            os.remove(self.root_dir + "/test_file")
        d.addCallback(remove)
        d.addCallback(lambda _: self.check())
        return d

    def test_sync_a_file_and_delete_nowait(self):
        """Sync one file."""
        open(self.root_dir + "/test_file", "w").close()
        os.remove(self.root_dir + "/test_file")
        return self.check()

    def test_sync_a_dir_and_delete(self):
        """Sync one dir."""
        os.mkdir(self.root_dir + "/test_dir")
        d = self.check()

        def remove(_):
            """remove it"""
            os.rmdir(self.root_dir + "/test_dir")
        d.addCallback(remove)
        d.addCallback(lambda _: self.check())
        return d

    def test_sync_a_dir_and_delete_nowait(self):
        """Sync one dir."""
        os.mkdir(self.root_dir + "/test_dir")
        os.rmdir(self.root_dir + "/test_dir")
        return self.check()

    def test_sync_a_dir_and_subfile_and_delete(self):
        """Sync one dir and subfile."""
        os.mkdir(self.root_dir + "/test_dir")
        open(self.root_dir + "/test_dir/file", "w").close()
        d = self.check()

        def remove(_):
            """put content on the file"""
            os.remove(self.root_dir + "/test_dir/file")
            os.rmdir(self.root_dir + "/test_dir")
        d.addCallback(remove)
        d.addCallback(lambda _: self.check())
        return d
    test_sync_a_dir_and_subfile_and_delete.skip = (
        'Fails in PQM with: '
        'ubuntuone.storageprotocol.errors.NotAvailableError: NOT_AVAILABLE')

    def test_sync_a_symlink_and_delete(self):
        """fail to sync+delete one symlink (sync/delete nothing!)"""
        a_file = os.path.join(self.tmpdir, "file")
        open(a_file, "w").close()
        os.symlink(a_file, os.path.join(self.root_dir, 'a_symlink'))
        d = self.check()

        def remove(_):
            """remove it"""
            os.remove(self.root_dir + "/test_file")
        d.addCallback(remove)
        d.addCallback(lambda _: self.check())

        def cleanup(r):
            """remove the file"""
            os.remove(a_file)
        d.addBoth(cleanup)
        return d

    def test_delete_check_middle_state(self):
        """MD should be in "deleted" state in the middle of the process.

        When a file is deleted, the MD must be marked as "deleted", and
        effectively removed only when the server answered.
        """
        filepath = os.path.join(self.root_dir, "test_file")
        _node = []

        def create_local(_):
            """Creates a file."""
            open(filepath, "w").close()
            return _

        def remove_local(_):
            """Removes the file."""
            os.remove(filepath)
            mdobj = self.main.fs.get_by_path(filepath)
            _node.append((mdobj.share_id, mdobj.node_id))
            return _

        def middle_check(*_):
            """Called before the delete gets to the server."""
            self.assertFalse(self.main.fs.has_metadata(path=filepath))
            self.assertTrue(_node[0] in self.main.fs.trash)

        def final_check(_):
            """Check that everything is ok"""
            # it should only be our file in the directory
            self.assertFalse(_node[0] in self.main.fs.trash)

        methinterf = MethodInterferer(self.main.fs, 'delete_to_trash')

        d = self.main.wait_for_nirvana()
        d.addCallback(create_local)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))
        d.addCallback(methinterf.insert_after, middle_check)
        d.addCallback(remove_local)
        d.addBoth(methinterf.restore)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))
        d.addCallback(final_check)
        return d


class Client(StorageClient):
    """Simple client that calls a callback on connection."""

    def connectionMade(self):
        """Setup and call callback."""
        StorageClient.connectionMade(self)
        self.factory.clientConnectionMade(self)


class ClientFactory(StorageClientFactory):
    """A test oriented protocol factory."""

    protocol = Client

    def __init__(self, defer):
        """create a factory."""
        self.defer = defer

    def clientConnectionMade(self, client):
        """on client connection made."""
        self.client = client
        self.defer.callback(client)

    def clientConnectionFailed(self, connector, reason):
        """We failed at connecting."""
        self.defer.errback(reason)


class TestServerBase(TestSync):
    """Utility for interacting with the server."""

    def save(self, name):
        "utility method to save deferred results"
        def saver(value):
            "inner"
            setattr(self, name, value)
            return value
        return saver

    @defer.inlineCallbacks
    def get_client(self, username='jack', root_id_name='root_id',
                   client_name='client'):
        """Return a deferred that will succeed with a connected client."""
        d = defer.Deferred()
        self.clients = []
        factory = ClientFactory(d)
        reactor.connectTCP("localhost", self.port, factory)
        client = yield d

        # connected!
        setattr(self, client_name, client)
        self.clients.append(client)
        yield client.set_caps(REQUIRED_CAPS)
        auth_info = self.access_tokens[username]
        yield client.simple_authenticate(auth_info['username'],
                                         auth_info['password'])
        root = yield client.get_root()
        setattr(self, root_id_name, root)

    @defer.inlineCallbacks
    def put_content(self, share_id, node_id, content, client_name='client'):
        """Upload content to the server."""
        client = getattr(self, client_name)
        req = yield client.get_delta(share_id, from_scratch=True)
        dt = [dt for dt in req.response
              if dt.node_id == node_id and dt.share_id == share_id][0]
        old_hash = dt.content_hash
        ho = content_hash_factory()
        ho.update(content)
        hash_value = ho.content_hash()
        crc32_value = crc32(content)
        deflated_content = zlib.compress(content)
        size = len(content)
        deflated_size = len(deflated_content)
        yield client.put_content(
            share_id, node_id, old_hash, hash_value,
            crc32_value, size, deflated_size, StringIO(deflated_content))

    def clean_partials_and_conflicts(self):
        """remove all partial and conflicted files"""
        def match(s):
            """are they partial or conflict?"""
            return (s.startswith(".u1partial.") or s.endswith("/.u1partial") or
                    re.search(r"\.u1conflict(?:\.\d+)?$", s))
        for root, dirs, files in os.walk(self.root_dir):
            for name in dirs:
                if match(name):
                    shutil.rmtree(os.path.join(root, name))
            for name in files:
                if match(name):
                    os.remove(os.path.join(root, name))

    def tearDown(self):
        "cleanup"
        if hasattr(self, "clients"):
            for client in self.clients:
                client.transport.loseConnection()
        return super(TestServerBase, self).tearDown()

    def get_put_content_data(self):
        """Returns the test data for put_content."""
        ho = content_hash_factory()
        hash_value = ho.content_hash()
        crc32_value = crc32("")
        deflated_content = zlib.compress("")
        deflated_size = len(deflated_content)
        return hash_value, crc32_value, deflated_size, deflated_content


class TestServerMove(TestServerBase):
    """move on the server"""

    def test_simple_move(self):
        """test rename"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request.new_id,
            self.root_id, "test_file_moved"))
        d.addCallback(lambda _: self.check())
        return d

    def test_simple_dir_move(self):
        """test rename dir"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request.new_id, self.root_id, "test_dir_moved"))
        d.addCallback(lambda _: self.check())
        return d

    def test_change_parent(self):
        """test rename"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir1"))
        d.addCallback(self.save("request1"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir2"))
        d.addCallback(self.save("request2"))
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.request1.new_id, "test_file"))
        d.addCallback(self.save("request3"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request3.new_id,
            self.request2.new_id, "test_file_moved"))
        d.addCallback(lambda _: self.check())
        return d


class TestServerCornerCases(TestServerBase):
    """Test corner cases."""

    def test_dir_reget(self):
        """Two simultaneous changes on a dir."""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir2"))
        d.addCallback(lambda _: self.check())
        return d

    def test_dir_reget_w_files(self):
        """Two simultaneous changes on a dir."""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file2"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _: self.check())
        return d

    # PQM fails on this, i dont know why
    def test_file_reget_content(self):
        """Test dont download data we dont need."""
        d = self.get_client()
        data_one = os.urandom(1000000)
        data_two = os.urandom(1000)
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.put_content(
            request.ROOT, self.req.new_id, data_one))
        d.addCallback(lambda _: self.put_content(
            request.ROOT, self.req.new_id, data_two))
        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_file_new_content_dont_reget(self):
        """Test roll back to previous version."""
        yield self.get_client()
        data_one = os.urandom(1000000)
        data_two = ""
        req = yield self.client.make_file(request.ROOT,
                                          self.root_id, "test_file")
        yield self.put_content(request.ROOT, req.new_id, data_one)
        yield self.put_content(request.ROOT, req.new_id, data_two)
        yield self.check()

    @defer.inlineCallbacks
    def test_file_upload_delete(self):
        """Upload a file and delete."""
        yield self.get_client()
        data_one = os.urandom(1000000)
        req = yield self.client.make_file(request.ROOT,
                                          self.root_id, "test_file")
        yield self.put_content(request.ROOT, req.new_id, data_one)
        yield self.client.unlink(request.ROOT, req.new_id)
        yield self.check()

    def test_dir_upload_delete(self):
        """Upload a dir and delete."""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.client.unlink(
            request.ROOT, self.req.new_id))
        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_quick_server_mess(self):
        """Server does some actions very quick, client gets late picture."""
        yield self.main.wait_for_nirvana()
        yield self.get_client()

        # create file "foo" with some content
        data = os.urandom(100)
        foo_path = os.path.join(self.root_dir, "foo")
        fh = open(foo_path, "w")
        fh.write(data)
        fh.close()
        yield self.main.wait_for_nirvana(.5)

        # move "foo" to "bar", create a new "foo" with other
        # content, delete "bar"
        md_foo = self.main.fs.get_by_path(foo_path)
        yield self.client.move(request.ROOT, md_foo.node_id,
                               self.root_id, "bar")
        req = yield self.client.make_file(request.ROOT, self.root_id, "foo")
        yield self.put_content(request.ROOT, req.new_id, ":)")
        yield self.client.unlink(request.ROOT,
                                 md_foo.node_id)  # foo is bar now
        yield self.main.wait_for_nirvana(.5)

        self.aq.rescan_from_scratch(request.ROOT)
        yield self.main.wait_for_nirvana(.5)

        # check locally and in the server
        # it should only be our file in the directory, with content ok
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["foo"])
        self.assertEqual(open(foo_path).read(), ":)")
        yield self.compare_server()


class TestServerMove2(TestServerBase):
    """move on the server"""

    def test_simple_move(self):
        """test rename"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request.new_id,
            self.root_id, "test_file_moved"))
        d.addCallback(lambda _: self.check())
        return d

    def test_simple_dir_move(self):
        """test rename dir"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request.new_id, self.root_id, "test_dir_moved"))
        d.addCallback(lambda _: self.check())
        return d

    def test_change_parent(self):
        """test rename"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir1"))
        d.addCallback(self.save("request1"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir2"))
        d.addCallback(self.save("request2"))
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.request1.new_id, "test_file"))
        d.addCallback(self.save("request3"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request3.new_id,
            self.request2.new_id, "test_file_moved"))
        d.addCallback(lambda _: self.check())
        return d


class TestClientMove(TestSync):
    """move on the server"""

    def test_simple_move_file(self):
        """Test rename a file."""
        fname = self.root_dir + "/test_file"
        open(fname, "w").close()

        def rename():
            "doit"
            os.rename(fname, fname + "_new")
        d = self.main.wait_for_nirvana(last_event_interval=0.2)
        d.addCallback(lambda _: rename())
        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_simple_move_nowait_file(self):
        """Test rename a file quickly."""
        fname = self.root_dir + "/test_file"
        open(fname, "w").close()
        os.rename(fname, fname + "_new")
        yield self.check()

    def test_simple_move_dir(self):
        """Test rename a dir."""
        fname = self.root_dir + "/test_dir"
        os.mkdir(fname)

        def rename():
            """do it"""
            os.rename(fname, fname + "_new")
        d = self.main.wait_for_nirvana(last_event_interval=0.2)
        d.addCallback(lambda _: rename())
        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_simple_move_nowait_dir(self):
        """Test rename a dir quickly."""
        fname = self.root_dir + "/test_dir"
        os.mkdir(fname)
        os.rename(fname, fname + "_new")
        yield self.check()

    @defer.inlineCallbacks
    def test_change_parent_move_dir(self):
        """Test rename dir"""
        dirname1 = os.path.join(self.root_dir, "test_dir_1")
        os.mkdir(dirname1)
        dirname2 = os.path.join(self.root_dir, "test_dir_2")
        os.mkdir(dirname2)
        yield self.main.wait_for_nirvana(.5)

        os.rename(dirname1, os.path.join(dirname2, 'newdirname'))
        yield self.main.wait_for_nirvana(.5)
        yield self.check()

    @defer.inlineCallbacks
    def test_change_parent_move_file(self):
        """Test rename file."""
        filename = os.path.join(self.root_dir, "test_file")
        open(filename, 'w').close()
        dirname = os.path.join(self.root_dir, "test_dir")
        os.mkdir(dirname)
        yield self.main.wait_for_nirvana(.5)

        os.rename(filename, os.path.join(dirname, 'newfilename'))
        yield self.main.wait_for_nirvana(.5)
        yield self.check()

    @defer.inlineCallbacks
    def test_hq_gets_a_file_in_moved_dir(self):
        """Check that HQ finally hashes something that was moved.

        Recipe:
          1. open a file, write it, close it, move it
          2. a HQ_HASH_ERROR should happen
          3. wait for nirvana
        """
        dir1 = os.path.join(self.root_dir, "dir1")
        dir2 = os.path.join(self.root_dir, "dir2")
        filepath1 = os.path.join(dir1, "test_file")
        filepath2 = os.path.join(dir2, "test_file")
        dfnew = self.wait_for("HQ_HASH_ERROR")

        # creates a dir.
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(.5)

        # creates a file in the dir, and moves the dir.
        fh = open(filepath1, "w")
        fh.write("foo")
        fh.close()
        os.rename(dir1, dir2)
        yield dfnew
        yield self.main.wait_for_nirvana(.5)

        # check that everything is ok: it should only be our file in the dir...
        l = os.listdir(dir2)
        self.assertEqual(l, ["test_file"])

        # ... and our file should have the correct content
        f = open(filepath2)
        content = f.read()
        f.close()
        self.assertEqual(content, "foo")

        yield self.compare_server()

    def test_syncs_get_file_hash_in_moved_dir(self):
        """At the time the hash arrives to Sync, the full dir was moved."""
        dir1 = os.path.join(self.root_dir, "dir1")
        dir2 = os.path.join(self.root_dir, "dir2")
        filepath1 = os.path.join(dir1, "test_file")
        filepath2 = os.path.join(dir2, "test_file")

        def create_testing_dir(_):
            """Creates a dir."""
            os.mkdir(dir1)
            return _

        def create_local(_):
            """Creates a file in the dir."""
            fh = open(filepath1, "w")
            fh.write("foo")
            fh.close()
            return _

        def move_dir(event, *args, **kwargs):
            """Moves the dir when HQ_HASH_NEW is received."""
            if event == "HQ_HASH_NEW":
                os.rename(dir1, dir2)
                methinterf.restore()

            # for the original function to be called
            return True

        def local_check(_):
            """Check that everything is ok"""
            # it should only be our file in the directory
            l = os.listdir(dir2)
            self.assertEqual(l, ["test_file"])

            # our file should have the correct content
            f = open(filepath2)
            content = f.read()
            f.close()
            self.assertEqual(content, "foo")

        #
        # Recipe:
        #   1. open a file, write it, close it
        #   2. a HQ_HASH_NEW should happen
        #   3. in the moment we see the OK, move the dir
        #   4. sync should miss the file
        #   5. wait for nirvana, it should be hashed ok
        #
        methinterf = MethodInterferer(self.main.event_q, 'push')
        methinterf.insert_before(None, move_dir)

        d = self.main.wait_for_nirvana()
        d.addCallback(create_testing_dir)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))
        d.addCallback(create_local)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))
        d.addCallback(local_check)
        d.addCallback(lambda _: self.compare_server())
        return d

    @defer.inlineCallbacks
    def test_syncs_get_changed_file_hash(self):
        """At the time the hash arrives to Sync, the file changed.

        Recipe:
          1. open a file, write it, close it
          2. a HQ_HASH_NEW should happen
          3. in the moment we see the OK getting into EQ, change the file
          4. sync will see the changed fail
          5. wait for nirvana, it should be hashed ok ("bar" in the file,
             and the same in the server)
        """
        filepath = os.path.join(self.root_dir, "test_file")

        def change_file(event, *args, **kwargs):
            """Changes the file permissions when HQ_HASH_NEW is received."""
            if event == "HQ_HASH_NEW":
                os.chmod(filepath, 0666)
                methinterf.restore()
            return True  # for the original function to be called

        methinterf = MethodInterferer(self.main.event_q, 'push')
        methinterf.insert_before(None, change_file)

        # wait to start, and create the file, that should trigger all events
        yield self.main.wait_for_nirvana()
        fh = open(filepath, "w")
        fh.write("foo")
        fh.close()

        # wait dust to settle, check locally and in the server
        yield self.main.wait_for_nirvana(.5)
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["test_file"])
        yield self.compare_server()

    @defer.inlineCallbacks
    def test_lr_gets_a_dir_in_moved_dir(self):
        """Check that LR finally scans something that was moved.

        Recipe:
          1. create a directory, move it
          2. a LR_ERROR should happen
          3. wait for nirvana
          4. check that a watch was added to it
        """
        dir1 = os.path.join(self.root_dir, "dir1")
        dir2 = os.path.join(self.root_dir, "dir2")

        # patch EQ to take note of the added watches
        added_watches = []
        original_add_watch = self.eq.add_watch

        def fake(path):
            """Fake add watch."""
            added_watches.append(path)
            original_add_watch(path)
        self.eq.add_watch = fake

        dfnew = self.wait_for("LR_SCAN_ERROR")
        yield self.main.wait_for_nirvana()
        os.mkdir(dir1)
        yield self.main.wait_for_nirvana(.5)

        # creates a file in the dir, and moves the dir
        os.mkdir(os.path.join(dir1, "testdir"))
        os.rename(dir1, dir2)
        yield dfnew
        yield self.main.wait_for_nirvana(.5)

        # check that the watch was added for the correct directory
        self.assertIn(os.path.join(dir2, "testdir"), added_watches)
        yield self.compare_server()


class TestTimings(TestServerBase):
    """Test specific timing behaviours"""

    @defer.inlineCallbacks
    def test_hq_takes_too_long(self):
        """Check that we don't accept a HQ result from an too old file.

        Recipe:
          1. do a put_content
          2. wait for SV_FILE_NEW
          3. inject the fake hash event

        so, this will trigger...
          4. the hash event will be processed
          5. later, it will arrive the FS_FILE_CREATE (from SV_FILE_NEW)
          6. DESPAIR! because we're receiving the FS_FILE_CREATE while in
             local state

        We're injecting an event and not generating it "naturally", because
        the hash generation is threaded, so it's too difficult to assure that
        the timing will be correct for the test case we created here
        """
        dfnew = self.wait_for("SV_FILE_NEW")
        yield self.get_client()
        req = yield self.client.make_file(request.ROOT, self.root_id,
                                          "test_file")
        yield self.put_content(request.ROOT, req.new_id, ":)")
        yield dfnew

        # inject a fake HQ_HASH_NEW
        filepath = self.root_dir + "/test_file"
        filedata = dict(
            hash='sha1:aaf4870935738945703957495349574987a9434d',
            crc32=939847398,
            size=10000,
            stat=os.stat("."),
        )
        self.eq.push('HQ_HASH_NEW', path=filepath, **filedata)
        yield self.main.wait_for_nirvana(0.5)

        # check that everything is ok
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["test_file"])
        f = open(self.root_dir + "/test_file")
        content = f.read()
        f.close()
        self.assertEqual(content, ":)")
        yield self.compare_server()


class TestConflict(TestServerBase):
    """Test corner cases."""

    def test_delete_conflict(self):
        """Delete locally while putting content in the server."""
        data_one = os.urandom(1000000)

        def change_local(_):
            """do the local change"""
            os.remove(self.root_dir + '/test_file')

        def content_check(_):
            """check that everything is in sync"""
            self.assertRaises(IOError, lambda:
                              open(self.root_dir + '/test_file'))

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))

        # let's put some content, for later have something to delete
        d.addCallback(lambda _: self.put_content(request.ROOT,
                                                 self.req.new_id, ":)"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))

        d.addCallback(lambda _: (
            self.put_content(request.ROOT, self.req.new_id, data_one),
            None)[1])
        d.addCallback(self.wait_for_cb("AQ_DELTA_OK"))
        d.addCallback(change_local)
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(content_check)
        return d

    def test_converge_conflict(self):
        """Write something locally while other content is put in server."""
        data_one = os.urandom(1000000)

        def change_local():
            """Do the local change."""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data_one)
            f.close()

        def content_check():
            """Check that everything is in sync."""
            data_one_local = open(self.root_dir + '/test_file').read()
            self.assertTrue(data_one_local == data_one)

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
                      request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(lambda _: (self.put_content(
                      request.ROOT, self.req.new_id, data_one), None)[1])
        d.addCallback(self.wait_for_cb("AQ_DELTA_OK"))
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(lambda _: content_check())
        return d

    @defer.inlineCallbacks
    def test_delete_changed_local_file_conflict(self):
        """Remove in the server a file that is locally modified."""
        data_conflict = "local" * 100000

        def change_local(req):
            """do the local change"""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data_conflict)
            f.close()
            d = self.client.unlink(request.ROOT, req.new_id)
            return d

        def content_check():
            """check that everything is in sync"""
            self.assertRaises(IOError,
                              open, self.root_dir + '/test_file')
            try:
                data_in_conflict = open(self.root_dir
                                        + '/test_file.u1conflict').read()
                self.assertTrue(data_in_conflict == data_conflict)
            except IOError:
                # sometimes the delete will take so long that the file will
                # be uploaded to the server before it happens.
                # this should still converge. nothing anywhere.
                return self.check()

        yield self.get_client()
        req = yield self.client.make_file(request.ROOT, self.root_id,
                                          "test_file")
        yield self.main.wait_for_nirvana(last_event_interval=0.5)
        yield change_local(req)
        yield self.main.wait_for_nirvana(last_event_interval=0.5)
        yield content_check()

    def test_simple_conflict(self):
        """Write something locally while other content is put in server.

        Here convergence is also checked.
        """
        data_one = os.urandom(1000000)
        data_conflict = "hello"

        def change_local():
            """do the local change"""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data_conflict)
            f.close()

        def content_check():
            """check that everything is in sync"""
            data_one_local = open(self.root_dir + '/test_file').read()
            try:
                data_conflict_local = open(
                    self.root_dir + '/test_file.u1conflict').read()
            except IOError:
                self.assertTrue(data_one_local == data_conflict)
            else:
                self.assertTrue(data_one_local == data_one)
                self.assertTrue(data_conflict_local == data_conflict)

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.put_content(
            request.ROOT, self.req.new_id, data_one))
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(lambda _: content_check())
        return d

    def test_double_make_no_conflict(self):
        """local change after server change waiting for download."""
        data = "server"

        def change_local():
            """do the local change"""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data)
            f.close()

        def local_check(_):
            """check that everything is in sync"""
            l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
            self.assertEqual(l, ["test_file"])

            final_data = open(self.root_dir + '/test_file').read()
            self.assertEqual(data, final_data)

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
                      request.ROOT, self.root_id, "test_file"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(local_check)
        d.addCallback(lambda _: self.compare_server())
        return d

    def test_double_make_conflict_w_dir_only(self):
        """Try to make dir in server when it's already there."""

        def change_local(_):
            """Do the local change."""
            os.mkdir(self.root_dir + '/test_dir')
            d = self.client.make_dir(request.ROOT, self.root_id, "test_dir")
            return d

        def content_check(_):
            """Check that everything is in sync."""
            l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
            self.assertEqual(l, ["test_dir"])
            self.assertTrue(stat.S_ISDIR(
                os.stat(self.root_dir + '/test_dir')[stat.ST_MODE]))

        d = self.get_client()
        d.addCallback(change_local)
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(content_check)
        return d

    def test_double_make_conflict_w_dir_and_file(self):
        """Make with same name a dir in server and file locally."""
        data_conflict = "local"

        def change_local():
            """do the local change"""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data_conflict)
            f.close()
            d = self.client.make_dir(request.ROOT, self.root_id, "test_file")
            return d

        def content_check():
            """check that everything is in sync"""
            self.assertTrue(stat.S_ISDIR(
                os.stat(self.root_dir + '/test_file')[stat.ST_MODE]))
            data_in_conflict = open(
                self.root_dir + '/test_file.u1conflict').read()
            self.assertEqual(data_in_conflict, data_conflict)

        d = self.get_client()
        d.addCallback(lambda _: change_local())
        d.addCallback(self.save("req"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(lambda _: content_check())
        return d
    test_double_make_conflict_w_dir_and_file.skip = (
        "Fails in PQM with ALREADY_EXISTS, see Bug #705655")

    @defer.inlineCallbacks
    def test_makefile_problem_retry(self):
        """Problem in make_file (but it got to the server), so retry it."""
        yield self.get_client()
        yield self.main.wait_for_nirvana(.5)

        # nuke the client's method
        nuker = NukeAQClient(self.aq, 'put_content_request')
        nuker.nuke(None)

        # create the file and wait to everything propagate
        with open(self.root_dir + "/test_file", "w") as fh:
            fh.write(os.urandom(100))
        yield self.wait_for_cb("HQ_HASH_NEW")

        # disconnect, restore the client's method, and connect again
        self.eq.push('SYS_NET_DISCONNECTED')
        nuker.restore(None)
        self.eq.push('SYS_NET_CONNECTED')
        yield self.main.wait_for_nirvana(.5)

        # it should only be our file in the directory
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["test_file"])

        # our file should have the full content
        filesize = os.stat(self.root_dir + "/test_file").st_size
        self.assertEqual(filesize, 100)

        yield self.compare_server()

    @defer.inlineCallbacks
    def _create_conflict(self):
        """Create a file with conflict."""
        data_local = ":)"
        data_server = "foobar"
        test_file = os.path.join(self.root_dir, 'test_file')
        yield self.get_client()
        d = defer.Deferred()

        orig_putcontent_class = protocol_client.PutContent
        test = self

        class FakePutContent(orig_putcontent_class):
            """Fake to do something before sending first message."""
            @defer.inlineCallbacks
            def _start(self):
                """Fake."""
                # restore
                protocol_client.PutContent = orig_putcontent_class

                # do something in the server
                req = yield test.client.make_file(request.ROOT,
                                                  test.root_id, "test_file")
                yield test.put_content(request.ROOT, req.new_id, data_server)

                # keep walking, and release for the test
                orig_putcontent_class._start(self)
                d.callback(True)

        # patch the client and do the change
        self.patch(protocol_client, 'PutContent', FakePutContent)
        with open(test_file, "w") as fh:
            fh.write(data_local)
        yield d
        yield self.main.wait_for_nirvana(.5)

        # we should have both files...
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), ["test_file", "test_file.u1conflict"])

        # ...with the respective content
        sanefile = open(test_file).read()
        self.assertEqual(sanefile, data_server)
        conflict = open(test_file + ".u1conflict").read()
        self.assertEqual(conflict, data_local)

        defer.returnValue((test_file, data_local, data_server))

    @defer.inlineCallbacks
    def test_move_conflict_over_file(self):
        """Move a conflict file over the file should solve it."""
        test_file, data_local, _ = yield self._create_conflict()

        # now we fix it!
        os.rename(test_file + ".u1conflict", test_file)
        yield self.main.wait_for_nirvana(.5)

        # finally, only one file with right content
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), ["test_file"])
        sanefile = open(test_file).read()
        self.assertEqual(sanefile, data_local)
        yield self.compare_server()

    @defer.inlineCallbacks
    def test_move_conflict_to_new(self):
        """Move a conflict file to a new one."""
        test_file, data_local, data_server = yield self._create_conflict()

        # now we fix it, and wait
        os.rename(test_file + ".u1conflict", test_file + "_new")
        yield self.main.wait_for_nirvana(.5)

        # finally, only one file with right content
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), ["test_file", "test_file_new"])
        srvfile = open(test_file).read()
        self.assertEqual(srvfile, data_server)
        newfile = open(test_file + "_new").read()
        self.assertEqual(newfile, data_local)

        yield self.compare_server()


class TestConflictOnServerSideDelete(TestServerBase):
    """Test corner cases on server side tree delete."""

    dir_name = u'foo'
    dir_name_conflict = dir_name + u'.u1conflict'
    file_name = u'bar.txt'

    def create_and_check(self, _):
        """Create initial directory hierarchy."""
        self.local_dir = os.path.join(self.root_dir, self.dir_name)
        self.local_dir_conflict = os.path.join(self.root_dir,
                                               self.dir_name_conflict)
        self.local_file = os.path.join(self.local_dir, self.file_name)

        os.mkdir(self.local_dir)
        assert os.path.exists(self.local_dir)

        open(self.local_file, 'w').close()  # touch local_file
        assert os.path.exists(self.local_file)

    def unlink_dir_tree(self, _):
        """Remove the tree below foo/ directly on server."""
        u = self.storage_users['jack']
        store = self.get_shard_store(u.shard_id)
        root = model.StorageObject.get_root(store, u.id)
        dir_tree = root.get_child_by_name(self.dir_name)

        # foo/bar.txt exists on local FS
        assert os.path.exists(self.local_dir)
        assert os.path.exists(self.local_file)

        # unlinkig the foo/ tree directly on the server database
        # does not propagate the removal to the client
        dir_tree.unlink_tree()
        store.commit()

    def update_state(self, _):
        """Update the local state by triggering a Query to the server."""
        # foo/bar.txt still exists on local FS
        assert os.path.exists(self.local_dir)
        assert os.path.exists(self.local_file)
        return self.aq.rescan_from_scratch(request.ROOT)

    def test_no_conflict_if_no_local_changes(self):
        """Don't conflict if no local changes after server tree deletion."""

        def no_conflict_check(_):
            """Check the absence of conflict, and the deletion of the node."""
            l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
            self.assertEqual(l, [])

        d = self.get_client()
        d.addCallback(self.create_and_check)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))

        d.addCallback(self.unlink_dir_tree)

        d.addCallback(self.update_state)
        d.addCallback(lambda _: self.main.wait_for_nirvana(.5))

        d.addCallback(no_conflict_check)
        return d

    @defer.inlineCallbacks
    def test_conflict_if_local_changes(self):
        """Conflict if there were local changes after server tree deletion."""
        client = yield self.get_client()
        self.create_and_check(client)
        yield self.main.wait_for_nirvana(.5)
        wait_hash = self.wait_for('HQ_HASH_NEW')

        self.unlink_dir_tree(None)

        former_size = os.path.getsize(self.local_file)
        assert former_size == 0

        with open(self.local_file, 'w') as f:
            f.write("this is something that wasn't there before!")

        assert former_size < os.path.getsize(self.local_file)

        yield wait_hash
        self.update_state(None)
        yield self.main.wait_for_nirvana(.5)

        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), [self.dir_name_conflict])


class TestConflict2(TestServerBase):
    """Test corner cases."""

    @defer.inlineCallbacks
    def test_make_makedir_fail(self):
        """local change after server change waiting for download."""
        yield self.get_client()
        req = yield self.client.make_dir(
            request.ROOT, self.root_id, "test_dir")
        yield self.main.wait_for_nirvana(0.5)
        os.mkdir(self.root_dir + "/test_dir/test_dir")
        try:
            yield self.client.unlink(request.ROOT, req.new_id)
        except:
            # ignore if the unlink fails
            pass
        yield self.main.wait_for_nirvana(0.5)
        yield self.clean_partials_and_conflicts()
        yield self.check()

    @defer.inlineCallbacks
    def test_local_delete_dir_while_downloading(self):
        """Remove tree locally when creating stuff in server."""
        yield self.get_client()
        req = yield self.client.make_dir(request.ROOT, self.root_id,
                                         "test_dir")
        req2 = yield self.client.make_file(request.ROOT, req.new_id,
                                           "test_file")
        yield self.main.wait_for_nirvana(last_event_interval=0.5)
        yield self.client.unlink(request.ROOT, req2.new_id)
        yield self.wait_for_cb("AQ_DELTA_OK")
        shutil.rmtree(self.root_dir + "/test_dir")
        yield self.main.wait_for_nirvana(last_event_interval=0.5)
        yield self.clean_partials_and_conflicts()
        yield self.check()

    @defer.inlineCallbacks
    def test_double_quick_putcontent(self):
        """Second putcontent has an old/false previous_hash."""
        fname = os.path.join(self.root_dir, 'testfile')

        class FakeProducer(object):
            """Just a holder."""
            finished = False

            def stopProducing(self):
                """Nothing."""

        fake_request = request.Request('protocol')
        fake_request.producer = FakeProducer()

        fake_putcontent_called = defer.Deferred()

        def fake_put_content(*args, **kwargs):
            """Fake put content that return the fake request."""
            fake_putcontent_called.callback((args, kwargs))
            return fake_request

        # nuke the real method to get in the middle
        mi = MethodInterferer(self.aq.client, 'put_content_request')
        mi.nuke(None, fake_put_content)

        # generate first change and wait for the fake put content to be called
        with open(fname, 'w') as fh:
            fh.write("foo")
        pc_args, pc_kwargs = yield fake_putcontent_called

        # restore the old (real) putcontent method
        mi.restore()

        # generate the real put content, and wait for it to finish (so the
        # server will have a different previous_hash than the next Upload)
        req = self.aq.client.put_content_request(*pc_args, **pc_kwargs)
        real_pc_result = yield req.deferred

        # mark the fake request's producer as finished, as it's important for
        # ActionQueue's Upload to know that it's too late to cancel
        fake_request.producer.finished = True

        # generate other FS_FILE_CLOSE_WRITE and wait for new command
        # in the queue
        wait_for_new_command = self.wait_for("SYS_QUEUE_ADDED")
        with open(fname, 'w') as fh:
            fh.write("other content")
        yield wait_for_new_command

        # release the internal deferred and let the first Upload continue
        fake_request.deferred.callback(real_pc_result)
        yield self.wait_for_nirvana(.5)

        # ask a delta from 1 (the make file), the server has gen=2, with older
        # hash, when downloaded it will generate the conflict
        self.aq.get_delta(request.ROOT, 1)
        yield self.wait_for_nirvana(.5)

        # no conflicts, and the server with correct data
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ['testfile'])
        self.assertEqual(open(fname).read(), "other content")
        yield self.check()

    @defer.inlineCallbacks
    def test_svfilenew_yetnonodeid_NONE(self):
        """Receive SV_FILE_NEW, node in NONE without node_id."""
        fcontent = "foobar"
        fname = os.path.join(self.root_dir, 'tfile')
        wait_for_svfilenew = self.wait_for("SV_FILE_NEW")

        # nuke client's MakeFile, that will trigger us later to continue
        d1 = defer.Deferred()
        mi = MethodInterferer(self.aq.client, 'make_file')
        play_makefile = mi.pause(lambda *a, **k: d1.callback(True))

        # nuke HQ insert, to simulate a later hashing
        d2 = defer.Deferred()
        mi = MethodInterferer(self.main.hash_q, 'insert')
        play_hash = mi.pause(lambda *a, **k: d2.callback(True))

        # write the file locally, and wait to be processed
        with open(fname, 'w') as fh:
            fh.write(fcontent)
        yield d1
        yield d2

        # write the file in the server and wait until SV_FILE_NEW arrives
        yield self.get_client()
        req = yield self.client.make_file(request.ROOT, self.root_id, "tfile")
        yield self.put_content(request.ROOT, req.new_id, fcontent)
        yield wait_for_svfilenew

        # release the interfered MakeFile, and wait for everything to finish
        play_makefile()
        play_hash()

        # check that we don't have conflict, and all is the same with server
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ['tfile'])
        self.assertEqual(open(fname).read(), fcontent)
        yield self.check()

    @defer.inlineCallbacks
    def test_svfilenew_yetnonodeid_LOCAL(self):
        """Receive SV_FILE_NEW, node in LOCAL without node_id."""
        fcontent = "foobar"
        fname = os.path.join(self.root_dir, 'tfile')
        wait_for_svfilenew = self.wait_for("SV_FILE_NEW")

        # nuke client's MakeFile, that will trigger us later to continue
        d1 = defer.Deferred()
        mi = MethodInterferer(self.aq.client, 'make_file')
        play_makefile = mi.pause(lambda *a, **k: d1.callback(True))

        # wait for the hashing to finish
        wait_for_hqhashnew = self.wait_for("HQ_HASH_NEW")

        # write the file locally, and wait to be processed
        with open(fname, 'w') as fh:
            fh.write(fcontent)
        yield d1
        yield wait_for_hqhashnew

        # write the file in the server and wait until SV_FILE_NEW arrives
        yield self.get_client()
        req = yield self.client.make_file(request.ROOT, self.root_id, "tfile")
        yield self.put_content(request.ROOT, req.new_id, fcontent)
        yield wait_for_svfilenew

        # release the interfered MakeFile, and wait for everything to finish
        play_makefile()
        yield self.wait_for_nirvana(.5)

        # check that we don't have conflict, and all is the same with server
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ['tfile'])
        self.assertEqual(open(fname).read(), fcontent)
        yield self.check()

    @defer.inlineCallbacks
    def test_svfilenew_yetnonodeid_differentpath(self):
        """Receive SV_FILE_NEW, match by path but to a different node!"""
        fcontent = "foobar"
        fname = os.path.join(self.root_dir, 'tfile')
        wait_for_svfilenew = self.wait_for("SV_FILE_NEW")

        # create a secondary node to later overwrite the testing one
        with open(fname + 'tobemoved', 'w') as fh:
            fh.write('foo')
        yield self.wait_for_nirvana(.5)

        # nuke client's MakeFile, that will trigger us later to continue
        d1 = defer.Deferred()
        mi = MethodInterferer(self.aq.client, 'make_file')
        play_makefile = mi.pause(lambda *a, **k: d1.callback(True))

        # write the file locally, and wait to be processed
        with open(fname, 'w') as fh:
            fh.write(fcontent)
        yield d1

        # overwrite the testing node, for "path" finding in _handle_SV_FILE_NEW
        # to retrieve other node
        os.rename(fname + 'tobemoved', fname)

        # write the file in the server and wait until SV_FILE_NEW arrives
        yield self.get_client()
        req = yield self.client.make_file(request.ROOT, self.root_id, "tfile")
        yield self.put_content(request.ROOT, req.new_id, fcontent)
        yield wait_for_svfilenew

        # release the interfered MakeFile, and wait for everything to finish
        play_makefile()
        yield self.wait_for_nirvana(.5)

        # check that we don't have conflict, and all is the same with server
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), ['tfile'])
        self.assertEqual(open(fname).read(), 'foo')
        yield self.check()


class TestPartialRecover(TestServerBase):
    """Recover from a removed partial."""

    @defer.inlineCallbacks
    def test_remove_partial(self):
        """Recover from a removed partial."""
        data_one = os.urandom(1000)
        commited = defer.Deferred()

        def remove_partial(*args):
            """Remove the node's .partial"""
            mdobj = self.main.fs.get_by_path(os.path.join(self.root_dir,
                                                          'test_file'))
            p_name = mdobj.mdid + '.u1partial.' + os.path.basename(mdobj.path)
            os.remove(os.path.join(self.main.fs.partials_dir, p_name))

            # don't call us again, call original func after this one
            nuker.restore()
            commited.callback(True)
            return True

        # get the client
        yield self.get_client()
        yield self.main.wait_for_nirvana(0)

        # set up the trap: remove the partial before commiting the download
        nuker = MethodInterferer(self.main.fs, 'commit_partial')
        nuker.insert_before(None, remove_partial)

        make_req = yield self.client.make_file(request.ROOT,
                                               self.root_id, "test_file")
        yield self.put_content(request.ROOT, make_req.new_id, data_one)
        yield commited
        yield self.main.wait_for_nirvana(0)

        # Sync will raise an exception, because commit_file is leaving the
        # node in a different state of what the spreadsheet indicated.
        # As this is ok, we're cleaning the error here
        self.error_handler.errors = []

        # Now, let LR fix the node, and SD to download it again
        yield self.main.lr.scan_dir("mdid", self.root_dir)
        yield self.main.wait_for_nirvana(0)

        content = open(self.root_dir + '/test_file').read()
        self.assertEqual(content, data_one)


class TestInotify(TestServerBase):
    """try to recover from a removed partial"""

    def test_add_file_in_server_folder(self):
        """add a file in a server created folder"""
        def change_local():
            """do the local change"""
            open(self.root_dir + '/test_dir/file', 'w').close()

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        d.addCallback(lambda _: self.check())
        return d


class TestStupendous(TestServerBase):
    """Tests with a lot of files.

    Create a stupendous amount of files on the server, and take it from there.
    """

    timeout = 3600  # yes, an hour

    def client_setup(self):
        """Do the file creation before hooking up the client."""
        d = self.get_client()
        for i in xrange(0x600):
            d.addCallback(lambda _, i=i: self.client.make_file(
                request.ROOT, self.root_id, 'test_%03x' % i))
            d.addCallback(lambda mk: self.put_content(request.ROOT,
                                                      mk.new_id,
                                                      'hola'))
        d.addCallback(lambda _: super(TestStupendous, self).client_setup())
        return d

    def test_survive_creating_a_bucketful_of_files_on_the_server_please(self):
        """Just check that we reach nirvana."""
        return self.main.wait_for_nirvana()
    test_survive_creating_a_bucketful_of_files_on_the_server_please.skip = \
        False if os.getenv('RUN_REALLY_SLOW_TESTS') else 'really slow test'


class TestMoveWhileInProgress(TestServerBase):
    """Handle new move cases."""

    @defer.inlineCallbacks
    def test_local_move_file_while_uploading(self):
        """Local change after server change waiting for upload."""
        data_conflict = os.urandom(1000000)
        wait_for_hash = self.wait_for_cb("HQ_HASH_NEW")

        # do the local change
        f = open(self.root_dir + "/test_file", "w")
        f.write(data_conflict)
        f.close()
        yield wait_for_hash

        # do the rename
        os.rename(self.root_dir + "/test_file",
                  self.root_dir + "/test_file_new")

        yield self.main.wait_for_nirvana(0.5)
        yield self.clean_partials_and_conflicts()
        yield self.check()

    def test_local_move_file_while_downloading(self):
        """Local change after server change waiting for download."""
        data_one = os.urandom(1000000)

        def change_local():
            """do the local change"""
            os.rename(self.root_dir + "/test_file",
                      self.root_dir + "/test_file_new")

        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))

        # let's put some content, for later have something to delete
        d.addCallback(lambda _: self.put_content(request.ROOT,
                                                 self.req.new_id, ":)"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))

        d.addCallback(lambda _: (self.put_content(
            request.ROOT, self.req.new_id, data_one), None)[1])
        d.addCallback(self.wait_for_cb("SV_VOLUME_NEW_GENERATION"))
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(lambda _: self.clean_partials_and_conflicts())
        d.addCallback(lambda _: self.check())

        return d

    def test_local_move_dir_while_downloading(self):
        """Local change after server change waiting for download."""
        def change_local():
            """do the local change"""
            os.rename(self.root_dir + "/test_dir",
                      self.root_dir + "/test_renamed")

        # let's build ROOT/test_dir/test_dir in the server
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("req"))
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.req.new_id, "inside_dir"))
        d.addCallback(self.wait_for_cb("AQ_DELTA_OK"))

        # change locally, wait, and compare
        d.addCallback(lambda _: change_local())
        d.addCallback(lambda _: self.main.wait_for_nirvana(0.5))
        d.addCallback(lambda _: self.clean_partials_and_conflicts())
        d.addCallback(lambda _: self.check())

        return d

    @defer.inlineCallbacks
    def test_server_move(self):
        """Receive a move from the server side."""
        waiting_for_event = self.wait_for_cb("AQ_DELTA_OK")

        # let's build ROOT/test_dir/test_dir_2 in the server
        yield self.get_client()
        req = yield self.client.make_dir(request.ROOT, self.root_id, "testdir")
        yield self.main.wait_for_nirvana(0.5)
        yield self.client.make_dir(request.ROOT, req.new_id, "test_dir_2")
        yield waiting_for_event
        yield self.main.wait_for_nirvana(0.5)

        # change locally, wait, and compare
        os.rename(self.root_dir + "/testdir", self.root_dir + "/testdir_new")
        yield self.main.wait_for_nirvana(0.5)
        yield self.check()

    def test_move_replace_file(self):
        """test rename"""
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file_moved"))
        d.addCallback(lambda _: self.client.make_file(
            request.ROOT, self.root_id, "test_file"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda r: self.put_content(request.ROOT, r.new_id, ""))
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=1))
        d.addCallback(lambda _: self.client.move(
            request.ROOT, self.request.new_id,
            self.root_id, "test_file_moved"))
        d.addCallback(lambda _: self.check())
        return d

    @defer.inlineCallbacks
    def test_move_while_hashing(self):
        """Test a rename while hashing the file."""
        yield self.get_client()

        # avoid make file to run
        make_nuker = MethodInterferer(self.aq.client, 'make_file')
        run_make_file = make_nuker.pause()

        # do a rename of the file before hashing
        dname_src = os.path.join(self.root_dir, 'dir_src')
        dname_dst = os.path.join(self.root_dir, 'dir_dst')

        def rename_it(*a):
            """Restore the proper hasher, and rename the file."""
            hash_nuker.restore(None)
            os.rename(dname_src, dname_dst)
            return True

        hash_nuker = MethodInterferer(self.main.hash_q, 'insert')
        hash_nuker.insert_before(None, rename_it)

        # need to wait the Upload to be executed before unleashing
        # the make file, to hit the bug
        def unleash_makefile(*a, **k):
            """Restore, unleash, and return True to continue all."""
            upload_nuker.restore()
            run_make_file()
            return True

        upload_nuker = MethodInterferer(self.aq, 'upload')
        upload_nuker.insert_after(None, unleash_makefile)

        # write the file, wait and check
        os.mkdir(dname_src)
        with open(os.path.join(dname_src, 'file.txt'), 'w') as fh:
            fh.write('hola')
        yield self.main.wait_for_nirvana(0.5)
        yield self.check()


class TestMoveWhileInProgress2(TestServerBase):
    """handle more new move cases."""
    def test_local_move_file_while_uploading_no_wait(self):
        """local change after server change waiting for upload no wait."""

        data_conflict = os.urandom(10000000)

        def change_local1():
            """do the local change"""
            f = open(self.root_dir + "/test_file", "w")
            f.write(data_conflict)
            f.close()

        def change_local2():
            """do the local change"""
            os.rename(self.root_dir + "/test_file",
                      self.root_dir + "/test_file_new")

        d = defer.succeed(True)
        d.addCallback(lambda _: change_local1())
        #d.addCallback(self.wait_for_cb("HQ_HASH_NEW"))
        d.addCallback(lambda _: change_local2())
        d.addCallback(lambda _:
                      self.main.wait_for_nirvana(last_event_interval=0.5))
        #d.addCallback(lambda _:
        #    self.clean_partials_and_conflicts())
        d.addCallback(lambda _: self.check())
        return d
    test_local_move_file_while_uploading_no_wait.skip = '20 (?)'


class TestLocalRescan(TestServerBase):
    """Test LR in some startup situations."""

    @defer.inlineCallbacks
    @required_caps(server.PREFERRED_CAP)
    def test_noncontent_file_generations(self):
        """Force LR on a non-content file.

        This is to reproduce the situation where the download gets
        interrupted in the middle because the client goes down, and then
        the client is executed again.
        """
        nuker = MethodInterferer(self.main.event_q, 'push')
        yield self.get_client()

        # create the file
        req = yield self.client.make_file(request.ROOT, self.root_id, "tfile")
        lr_def = defer.Deferred()

        def trigger_lr(event, *args, **kwargs):
            """Triggers a LR when the event is received."""
            if event != "AQ_DELTA_OK":
                return True

            # check if there's new info for this node
            deltas = [dt for dt in kwargs['delta_content']
                      if dt.node_id == req.new_id]
            if len(deltas) != 1:
                return True
            delta = deltas[0]

            # check if the putcontent finished
            if delta.generation < 2:
                return True

            # LR, and return False to stop event real propagation
            d = self.main.lr.scan_dir("mdid", self.root_dir)
            d.addCallback(lr_def.callback)
            return False

        # put content in the server, so it will start the download, but LR
        # will get in the middle
        nuker.insert_before(None, trigger_lr)
        try:
            yield self.put_content(request.ROOT, req.new_id, ":)")
            yield self.main.wait_for_nirvana(0)
            yield lr_def
        finally:
            nuker.restore(None)

        # issue a query on the node, to get the info from the server,
        # simulating the server_rescan...
        self.aq.get_delta(request.ROOT, 0)
        yield self.main.wait_for_nirvana(0)

        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["tfile"])
        yield self.check()

    @defer.inlineCallbacks
    def test_interrupted_upload(self):
        """The upload was interrupted, go LR and fix it."""
        yield self.get_client()

        # nuke AQ and create the file in disk, to simulate a broken upload
        nuker = MethodInterferer(self.aq, 'upload')
        nuker.nuke(None)
        fname = os.path.join(self.root_dir, 'file.txt')
        try:
            with open(fname, 'w') as fh:
                fh.write('hola')
            # need to wait until all is procesed before restoring the upload
            yield self.main.wait_for_nirvana(.5)
        finally:
            nuker.restore(None)

        # let LR run, and then SR, and wait
        yield self.main.lr.scan_dir('mdid', self.root_dir)
        vol = self.main.vm.get_volume(request.ROOT)
        self.aq.get_delta(request.ROOT, vol.generation)
        yield self.main.wait_for_nirvana(0)

        # check local and against the server
        self.assertEqual(open(fname).read(), 'hola')
        yield self.check()

    @defer.inlineCallbacks
    def test_interrupted_download(self):
        """The upload was interrupted, go LR and fix it."""
        yield self.get_client()

        # nuke AQ and create the file in server, to simulate a broken download
        nuker = MethodInterferer(self.aq, 'download')
        nuker.nuke(None)

        try:
            req = yield self.client.make_file(request.ROOT,
                                              self.root_id, 'file.txt')
            yield self.put_content(request.ROOT, req.new_id, 'hola')
            yield self.main.wait_for_nirvana(0)
        finally:
            nuker.restore(None)

        # let LR run, and then SR, and wait
        yield self.main.lr.scan_dir('mdid', self.root_dir)
        fname = os.path.join(self.root_dir, "file.txt")
        vol = self.main.vm.get_volume(request.ROOT)
        self.aq.get_delta(request.ROOT, vol.generation)
        yield self.main.wait_for_nirvana(0)

        # check local and against the server
        self.assertEqual(open(fname).read(), 'hola')
        yield self.check()


class TestDownloadError(TestServerBase):
    """test handling of AQ_DOWNLOAD_ERROR"""

    def test_not_available(self):
        """Tests that on a AQ_DOWNLOAD_ERROR (NOT_AVAILABLE) the .u1partial is
        deleted.
        """
        def get_content_request(share, node, hash, offset=0,
                                callback=None, node_attr_callback=None):
            """get_content_request that always fails with NOT_AVAILABLE"""
            message = protocol_pb2.Message()
            message.id = 10
            message.type = protocol_pb2.Message.ERROR
            message.error.type = protocol_pb2.Error.NOT_AVAILABLE
            failed = defer.fail(request.StorageRequestError(None, message))
            return FakeGetContent(failed, share, node, hash)

        nuker = NukeAQClient(self.aq, 'get_content_request')
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda _: nuker.nuke(_, get_content_request))
        d.addCallback(lambda _: self.wait_for_nirvana())

        def check_partial(_):
            """check if the partial is there"""
            self.assertFalse(os.path.exists(os.path.join(self.root_dir,
                                                         '.u1partial')))
        d.addCallback(check_partial)
        d.addCallback(nuker.restore)
        d.addCallback(lambda _: self.client.unlink(
            request.ROOT, self.request.new_id))
        d.addCallback(lambda _: self.check())
        return d

    def test_other_error(self):
        """Tests that on a AQ_DOWNLOAD_ERROR (misc error) the .u1partial isn't
        deleted.
        """
        def get_content_request(share, node, hash, offset=0,
                                callback=None, node_attr_callback=None):
            """get_content_request that always fails with PROTOCOL_ERROR"""
            message = protocol_pb2.Message()
            message.id = 10
            message.type = protocol_pb2.Message.ERROR
            message.error.type = protocol_pb2.Error.PROTOCOL_ERROR
            failed = defer.fail(request.StorageRequestError(None, message))
            return FakeGetContent(failed, share, node, hash)

        nuker = NukeAQClient(self.aq, 'get_content_request')
        d = self.get_client()
        d.addCallback(lambda _: self.client.make_dir(
            request.ROOT, self.root_id, "test_dir"))
        d.addCallback(self.save("request"))
        d.addCallback(lambda _: nuker.nuke(_, get_content_request))
        d.addCallback(lambda _: self.wait_for_nirvana())

        def check_partial(_):
            """check if the partial is there"""
            self.assertFalse(os.path.exists(
                os.path.join(self.root_dir, '.u1partial')))
        d.addCallback(check_partial)
        d.addCallback(nuker.restore)
        d.addCallback(lambda _: self.client.unlink(
            request.ROOT, self.request.new_id))
        d.addCallback(lambda _: self.check())
        return d


class TestOpenFiles(TestServerBase):
    """Stuff happen when we held the files opened."""

    class TestFileManager(object):
        """Handle the test file."""
        def __init__(self, filepath, test):
            self.filepath = filepath
            self.fh = None
            self.test = test

        @defer.inlineCallbacks
        def create(self):
            """Create a file in disk."""
            hashed = self.test.wait_for("HQ_HASH_NEW")
            fh = open(self.filepath, "w")
            fh.write("foo")
            fh.close()

            # wait to be hashed, and then all to settle down
            yield hashed
            yield self.test.main.wait_for_nirvana(0.5)

        @defer.inlineCallbacks
        def open_and_change(self):
            """Ope and write something to the file."""
            hashed = self.test.wait_for("HQ_HASH_NEW")
            self.fh = open(self.filepath, "a")
            self.fh.write("bar")
            yield hashed

        def open(self):
            """Just open the file."""
            self.fh = open(self.filepath)

        def close(self):
            """Close the file."""
            self.fh.close()

    @defer.inlineCallbacks
    def test_download_close_changes(self):
        """While open, a download happens, the file is closed, with changes."""
        filepath = os.path.join(self.root_dir, "test_file")
        tfm = self.TestFileManager(filepath, self)
        yield self.get_client()

        # create the file
        yield tfm.create()

        # open the file and change it
        tfm.open_and_change()

        # get the node id and change it in the server
        mdobj = self.main.fs.get_by_path(filepath)
        wait_download = self.wait_for("AQ_DOWNLOAD_FINISHED",
                                      node_id=mdobj.node_id)
        yield self.put_content(request.ROOT, mdobj.node_id, ":)")
        yield self.main.wait_for_nirvana(0.5)

        # close the file, wait for the download event to circulate
        tfm.close()
        yield wait_download

        # check the node is conflicted, both files with corerct content
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(sorted(l), ["test_file", "test_file.u1conflict"])
        with open(filepath) as f:
            self.assertEqual(f.read(), ":)")
        with open(filepath + ".u1conflict") as f:
            self.assertEqual(f.read(), "foobar")

    @defer.inlineCallbacks
    def test_download_close_nochanges(self):
        """While open, a download happens, the file is closed, no changes."""
        filepath = os.path.join(self.root_dir, "test_file")
        tfm = self.TestFileManager(filepath, self)
        yield self.get_client()

        # create and open the file
        yield tfm.create()
        tfm.open()

        # get the node id of created file and put content in the server to it
        mdobj = self.main.fs.get_by_path(filepath)
        yield self.put_content(request.ROOT, mdobj.node_id, ":)")
        yield self.main.wait_for_nirvana(0.5)
        tfm.close()
        yield self.main.wait_for_nirvana(0.5)

        # check that everything is ok
        l = filter_symlinks(self.root_dir, os.listdir(self.root_dir))
        self.assertEqual(l, ["test_file"])
        with open(filepath) as f:
            self.assertEqual(f.read(), ":)")


class TestStartup(TestSync):
    """Test startup of the syncdaemon """

    @defer.inlineCallbacks
    def test_connection_made(self):
        """Test that events are received after a SYS_CONNECTION_MADE."""
        yield self.main.wait_for_nirvana()

        # disconnect and wait to know that connection is done
        self.main.event_q.push('SYS_USER_DISCONNECT')
        yield self.main.wait_for('SYS_CONNECTION_LOST')

        # connect again and check
        self.main.event_q.push('SYS_USER_CONNECT',
                               auth_info=self.access_tokens['jack'])
        yield self.main.wait_for('SYS_ROOT_RECEIVED')
