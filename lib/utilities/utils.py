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

"""Miscelaneous functions generically useful."""

import os
import sys
import signal
import socket

from utilities.localendpoints import get_local_port, get_rootdir


def check_process(proc_name, pid_file):
    """Check if a process is already running."""
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.readline())
        proc_stat = '/proc/%s/stat' % (pid,)
        with open(proc_stat, 'r') as f:
            if proc_name in f.read():
                return True
    except:
        # ignore all errors
        pass
    return False


def is_running(proc_name, pidfile, service, timeout=1):
    """Check if we have a postgres instance running."""
    if not check_process(proc_name, pidfile):
        return False
    try:
        port = get_local_port(service)
        s = socket.create_connection(("127.0.0.1", port), timeout)
    except (IOError, socket.error):
        return False
    else:
        s.close()
    return True


def os_exec(*args):
    """Wrapper for os.execv() that catches execution errors """
    try:
        os.execv(args[0], args)
        os._exit(1)
    except OSError:
        sys.stderr.write("\nERROR:\nCould not exec: %s\n" % (args,))
    # if we reach here, it's an error anyway
    os._exit(-1)


def get_tmpdir():
    """Compose the temp dir path."""
    return os.path.join(get_rootdir(), 'tmp')


def kill_group(signum=None, frame=None, exitcode=0, pid=0):
    """Kill the whole process group, optionally exiting

    This can be called as a signal handler (hence the first two
    parameters), but also standalone.

    @param signum: The signal that is being handled, if any
    @param frame: The current stack frame when the signal was delivered
    @param exitcode: If not None, exit with the given exitcode.
    @param pid: the process id of a member of the process group
                to be killed
    """
    os.killpg(os.getpgid(pid), signal.SIGTERM)
    if exitcode is not None:
        sys.exit(exitcode)
