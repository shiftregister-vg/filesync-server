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

"""Integration Tests for UDFs."""

import multiprocessing
import os
import random
import shutil
import signal
import subprocess
import sys
import time

from optparse import OptionParser

import dbus
import dbus.mainloop.glib  # this is black magic. DO NOT REMOVE!

from distutils.spawn import find_executable

# fix paths 1, to include lib for most basic stuff
LIB_DIR = os.path.abspath("lib")
sys.path.insert(0, LIB_DIR)

from twisted.internet import glib2reactor
glib2reactor.install()  # before any reactor import

from ubuntuone.storage.server.testing.testcase import create_test_user
from utilities import utils, dev_launcher
from ubuntuone.platform.linux import tools
from twisted.internet import reactor, defer

from ubuntuone.storage.tests.integration.helpers import debug, retryable

# to make dbus work
dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)


# this should be done manually before:
DEP_STARTOAUTH = """
make start-oauth
"""
DEP_BUILDCLIENT = """
cd sourcecode/ubuntuone-client/
./autogen.sh --with-protocol=../ubuntuone-storage-protocol/
make
"""


ROOT = utils.get_rootdir()
TMP_DIR = os.path.join(ROOT, 'tmp')
PID_FILENAME = os.path.join(TMP_DIR, 'syncdaemon-process-%d.pid')

_dbus = find_executable("dbus-daemon")
if not _dbus:
    raise RuntimeError("dbus-daemon was not found.")


def start_DBus():
    """Start our own session bus daemon for testing."""
    config_file = os.path.join(os.getcwd(), "configs", "dbus-session.conf")
    dbus_args = ["--fork",
                 "--config-file=" + config_file,
                 "--print-address=1",
                 "--print-pid=2"]
    p = subprocess.Popen([_dbus] + dbus_args, bufsize=4096,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    dbus_address = "".join(p.stdout.readlines()).strip()
    dbus_pid = int("".join(p.stderr.readlines()).strip())

    if dbus_address == "":
        os.kill(dbus_pid, signal.SIGKILL)
        raise Exception("There was a problem launching dbus-daemon.")

    return dbus_pid, dbus_address


def deps_missing():
    """Check if we're ready to go.

    This tests the best way we can that all dependencies are ready. It depends
    on how other parts of the projects do their stuff, so it may change.
    """
    couchdb_port = os.path.join(TMP_DIR, "couchdb-master0.port")
    if not os.path.exists(couchdb_port):
        print "Not ready! Hint: did you do...?:\n" + DEP_STARTOAUTH
        return True

    log_conf = os.path.join(ROOT, "sourcecode",
                            "ubuntuone-client", "data", "logging.conf")
    if not os.path.exists(log_conf):
        print "Not ready! Hint: did you do...?:\n" + DEP_BUILDCLIENT
        return True


def create_syncdaemon(username, procnum, homedir, pidfile):
    """Creates a SD with its DBus."""
    prefix = 'Create_syncdaemon:'
    # start dbus
    debug(prefix, 'Starting DBus...')
    (dbus_pid, dbus_address) = start_DBus()
    debug(prefix, 'DBus started with (dbus_pid, dbus_address):',
          dbus_pid, dbus_address)

    # run the client
    env = dict(DBUS_SESSION_BUS_ADDRESS=dbus_address,
               PYTHONPATH=LIB_DIR,
               XDG_CACHE_HOME="tmp/xdg_cache%d" % procnum)
    value = (dbus_pid, dbus_address)
    debug(prefix, "Putting in pidfile %s values %s" % (pidfile, value))
    with open(pidfile, 'w') as f:
        f.write(' '.join(map(str, value)))
        f.write('\n')

    debug(prefix, "Launching SD with dbus", dbus_address)
    dev_launcher.launch("sourcecode/ubuntuone-client/bin/ubuntuone-syncdaemon",
                        username, params=("--send_events_over_dbus",
                                          "--udf_autosubscribe=true"),
                        environ=env, homedir=homedir, verbose=True)


class SyncDaemonToolProxy(object):
    """Call SDT methods, but retrying."""

    def __init__(self, conn, event_rcvr):
        self._sdt = tools.SyncDaemonTool(conn)
        self._sdt.bus.add_signal_receiver(event_rcvr, signal_name='Event')

    def __getattr__(self, name):
        """Return SDT method wrapped in retryable."""
        original_method = getattr(self._sdt, name)
        return retryable(original_method)


class SyncDaemon(object):
    """SyncDaemon abstraction."""

    def __init__(self, username, procnum, timestamp, verbose=False):
        self._proc = None
        self._dbus_pid = None
        self._dbus_address = None
        self.username = username
        self.procnum = procnum
        self.verbose = verbose
        self.prefix = 'SyncDaemon %d:' % procnum
        sd_home = 'syncdaemon-home-%d-%s' % (procnum, timestamp)
        self.homedir = os.path.join(TMP_DIR, sd_home, username)
        self.rootdir = os.path.join(self.homedir, 'Ubuntu One')
        self.pidfile = PID_FILENAME % procnum
        self.events = []
        self.sdt = None

        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def debug(self, msg, *args):
        """Print debug messages."""
        if self.verbose:
            debug(self.prefix, msg, *args)

    def parse_daemon_data(self):
        """Get dbus_pid, dbsu_address and homedir for process."""
        for i in range(10):
            if os.path.exists(self.pidfile):
                break
            self.debug("Process' pid file doesn't exist, sleeping 2 second",
                       self.pidfile)
            time.sleep(2)
        else:
            ValueError("ERROR: couldn't see the pidfile: %r" % (self.pidfile,))

        with open(self.pidfile) as f:
            content = f.read()

        return content.split()

    def record_event(self, event_dict):
        """Store events for further analysis."""
        self.events.append(event_dict)

    @defer.inlineCallbacks
    def start(self):
        """Connects the syncdaemon."""
        self.debug("Starting SyncDaemon", self.procnum)
        args = (self.username, self.procnum, self.homedir, self.pidfile)
        self._proc = multiprocessing.Process(target=create_syncdaemon,
                                             args=args)
        self.debug("Process created:", self._proc)
        self._proc.start()
        self.debug("Process started.")

        self._dbus_pid, self._dbus_address = self.parse_daemon_data()
        self._dbus_pid = int(self._dbus_pid)
        self.debug("Daemon data parsed, values:",
                   self._dbus_pid, self._dbus_address)

        conn = dbus.bus.BusConnection(self._dbus_address)
        self.debug("dbus.bus.BusConnection coon:", conn)
        self.sdt = SyncDaemonToolProxy(conn, self.record_event)
        self.debug("tools.SyncDaemonTool:", self.sdt._sdt)

        # let's wait for it to start, connect and wait for nirvana
        self.debug("Let's connect! events so far", self.events)
        yield self.sdt.connect()
        self.debug("Connected, waiting for nirvana now.")
        yield self.sdt.wait_for_nirvana(1)

    def stop(self):
        """Kills the running syncdaemon."""
        self.debug("Stopping SyncDaemon", self.procnum)
        assert self.sdt._sdt is not None
        d = self.sdt.quit()
        d.addCallback(lambda _: os.kill(self._dbus_pid, signal.SIGKILL))
        d.addCallback(lambda _: os.remove(self.pidfile))
        return d

    def wait_for_event(self, event_name, **event_args):
        """Wait for a given event."""
        self.debug('Waiting for event', event_name)

        def filter(event):
            """Filter an event."""
            if event['event_name'] != event_name:
                return False
            for key, value in event_args:
                if event[key] != value:
                    return False
            return True

        return self.sdt.wait_for_signal('Event', filter)


def get_all_tests(test_filter):
    """Collect all tests to execute."""
    # convert the test filter to an usable dict
    testsok = {}
    for t in test_filter:
        if "." in t:
            fmod, ftest = t.split(".")
            testsok.setdefault(fmod, []).append(ftest)
        else:
            testsok[t] = None

    base = os.path.dirname(__file__)
    test_files = [x for x in os.listdir(base)
                  if x.startswith('integtests_') and x.endswith('.py')]

    all_funcs = []
    for fname in test_files:
        modname = fname[:-3]
        mod = __import__(modname)
        funcnames = [x for x in dir(mod) if x.startswith('test_')]

        # check if in asked for tests
        funcnames_filtered = []
        if testsok == {}:
            # nothing required, all in
            funcnames_filtered = funcnames
        else:
            if modname in testsok:
                tests = testsok[modname]
                if tests is None:
                    # all the module is ok
                    funcnames_filtered = funcnames
                else:
                    funcnames_filtered = [n for n in funcnames if n in tests]

        funcs = [getattr(mod, x) for x in funcnames_filtered]
        all_funcs.extend(funcs)

    random.shuffle(all_funcs)
    return all_funcs


@defer.inlineCallbacks
def execute_tests(all_tests, sd1, sd2, sd3):
    """Execute the whole suite, please."""
    prefix = 'Execute tests:'

    debug(prefix, 'Starting')
    assert os.path.exists(sd1.rootdir), sd1.rootdir + ' must exist'

    # create directory and wait for it to go all places
    dir_in_sd1 = sd1.wait_for_event('AQ_DIR_NEW_OK')
    dir_in_sd2 = sd1.wait_for_event('FS_DIR_CREATE')
    os.mkdir(os.path.join(sd1.rootdir, ".mark"))
    yield dir_in_sd1
    debug(prefix, 'AQ_DIR_NEW_OK in sd1 done.')
    yield dir_in_sd2
    debug(prefix, 'FS_DIR_CREATE in sd2 done.')

    # calm down
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, 'Nirvana reached for sd1 and sd2.')

    len_tests = len(all_tests)
    for i, test in enumerate(all_tests):
        test_name = test.func_name
        testprefix = ' '.join(("===", test_name, "==="))

        skipit = getattr(test, 'skip', None)
        if skipit:
            debug(testprefix, 'Skipping test!', skipit, previous_newline=True)
            continue

        debug(testprefix, 'Starting test %d of %d' % (i + 1, len_tests),
              previous_newline=True)

        try:
            yield test(test_name, sd1, sd2, sd3, testprefix)
        except Exception, e:
            debug(testprefix, 'Crushing failure and despair, :( -- ', str(e))
            raise
        else:
            debug(testprefix, 'Test finished ok! :)')
        finally:
            debug(testprefix, 'Cleaning up.')

            # wait for SDs to finish
            yield sd1.sdt.wait_for_nirvana(.5)
            yield sd2.sdt.wait_for_nirvana(.5)
            yield sd3.sdt.wait_for_nirvana(.5)

            # clean up UDFs (removing from 1 should remove it from all)
            debug(testprefix, 'Removing old folders.')
            folders = yield sd1.sdt.get_folders()
            for udf in folders:
                vid = udf['volume_id']
                debug(testprefix, 'Deleting UDF with id:', vid)
                yield sd1.sdt.delete_folder(folder_id=vid)

            # wait for SDs to finish
            yield sd1.sdt.wait_for_nirvana(.5)
            yield sd2.sdt.wait_for_nirvana(.5)
            yield sd3.sdt.wait_for_nirvana(.5)

            folders = yield sd1.sdt.get_folders()
            assert folders == []
            folders = yield sd2.sdt.get_folders()
            assert folders == []
            folders = yield sd3.sdt.get_folders()
            assert folders == []

            debug(testprefix, 'Removing old data in home.')
            for sd in sd1, sd2, sd3:
                for something in os.listdir(sd.homedir):
                    if something not in ('Ubuntu One', '.local', '.config'):
                        fullpath = os.path.join(sd.homedir, something)
                        if os.path.isdir(fullpath):
                            shutil.rmtree(fullpath)
                        else:
                            os.remove(fullpath)

            debug(testprefix, 'Removing old data in client main dir.')
            for sd in sd1, sd2, sd3:
                for something in os.listdir(sd.rootdir):
                    if something != 'Shared With Me':
                        fullpath = os.path.join(sd.rootdir, something)
                        if os.path.isdir(fullpath):
                            shutil.rmtree(fullpath)
                        else:
                            os.remove(fullpath)

            # wait for SDs to finish these last changes
            yield sd1.sdt.wait_for_nirvana(.5)
            yield sd2.sdt.wait_for_nirvana(.5)
            yield sd3.sdt.wait_for_nirvana(.5)

            # clean up events
            sd1.events = []
            sd2.events = []
            sd3.events = []

            debug(testprefix, 'All done')

    debug(prefix, "All tests passed ok!", previous_newline=True)


@defer.inlineCallbacks
def main(test_filter, repeat=False):
    """Main function."""
    all_tests = get_all_tests(test_filter)

    prefix = 'main:'
    timestamp = time.strftime("%Y%m%d%M%H%S")

    # create user
    user1 = "integtest" + timestamp
    user2 = "integotro" + timestamp
    for u in (user1, user2):
        create_test_user(u)
        debug(prefix, 'User created:', u)

    debug(prefix, 'Content blobs created')

    sd1 = SyncDaemon(user1, 1, timestamp, verbose=True)
    debug(prefix, 'SyncDaemon 1 created.')
    sd2 = SyncDaemon(user1, 2, timestamp, verbose=True)
    debug(prefix, 'SyncDaemon 2 created.')
    sd3 = SyncDaemon(user2, 3, timestamp, verbose=True)
    debug(prefix, 'SyncDaemon 2 created.')

    yield sd1.start()
    debug(prefix, 'SyncDaemon 1 started.')
    yield sd2.start()
    debug(prefix, 'SyncDaemon 2 started.')
    yield sd3.start()
    debug(prefix, 'SyncDaemon 3 started.')

    try:
        if repeat:
            i = 0
            while True:
                i += 1
                debug(prefix, 'Executing tests, run', i)
                yield execute_tests(all_tests, sd1, sd2, sd3)
        else:
            yield execute_tests(all_tests, sd1, sd2, sd3)
            debug(prefix, 'Tests executed.')
    except Exception, e:
        print '\n', '!' * 20, 'There was a problem. Failure below.', '!' * 20
        print e
        print '!' * 20, 'There was a problem. Failure above.', '!' * 20

    debug(prefix, 'End.')
    yield sd1.stop()
    debug(prefix, 'sd1 stopped.')
    yield sd2.stop()
    debug(prefix, 'sd2 stopped.')
    yield sd3.stop()
    debug(prefix, 'sd3 stopped.')
    reactor.stop()
    debug(prefix, 'reactor stopped.')


if __name__ == "__main__":
    # check we're ready to go
    if not deps_missing():
        parser = OptionParser()
        parser.add_option("-r", "--repeat", action="store_true", dest="repeat",
                          help="repeat the test(s) until failure")
        (options, args) = parser.parse_args()
        main(args, options.repeat)
        reactor.run()
