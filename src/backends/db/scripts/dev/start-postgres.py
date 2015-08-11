#!/usr/bin/python

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

"""Script to start Postgres."""

import os
import sys

import utilities.localendpoints as local

from utilities.utils import os_exec, is_running, get_tmpdir


def start():
    """Start Postgres."""
    sock_dir = os.path.join(get_tmpdir(), "db1")
    data_dir = os.path.join(sock_dir, "data")
    pidfile = os.path.join(data_dir, "postmaster.pid")

    if is_running("postgres", pidfile, "postgres"):
        print "Postgres already up & running."
        return 0

    pg_bin = None
    for path in ("/usr/lib/postgresql/9.1/bin",
                 "/usr/lib/postgresql/8.4/bin"):
        if os.path.isdir(path):
            pg_bin = path
            break

    if pg_bin is None:
        print "Cannot find valid parent for PGBINDIR"
        return 1

    pg_port = local.allocate_ports(1)[0]
    local.register_local_port("postgres", pg_port)
    os_exec(os.path.join(pg_bin, "postgres"),
            "-D", data_dir,
            "-k", sock_dir,
            "-i", "-h", "127.0.0.1",
            "-p", str(pg_port))

if __name__ == '__main__':
    sys.exit(start())
