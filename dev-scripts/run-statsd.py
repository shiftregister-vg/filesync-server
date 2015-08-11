#!/usr/bin/env python

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

"""Start txstatsd."""

import sys
import os
import socket
import time
import platform

from utilities.utils import get_tmpdir
import utilities.localendpoints as local


TMP_DIR = get_tmpdir()
statsd_log = os.path.join(TMP_DIR, "statsd.log")

# allocate and register port
statsd_port, = local.allocate_ports(1)
local.register_local_port("statsd", statsd_port)

# wait for graphite to be done
carbon_port = local.get_local_port("carbon_pickle_receiver")
s = socket.socket()
for i in range(30):
    try:
        s.connect(("localhost", carbon_port))
        break
    except socket.error:
        time.sleep(2)
else:
    print "Carbon isn't up"
    sys.exit(1)
s.close()

twistd = "/usr/bin/twistd"

cmdline = [twistd, "--reactor=epoll", "--logfile=%s" % statsd_log,
           "-n", "statsd",
           "--carbon-cache-port", str(carbon_port),
           "--carbon-cache-name", "a",
           "--listen-port", str(statsd_port),
           "-m", "Hakuna", "-o", "Matata",
           "-s", "0", "-x", "statsd",
           "-N", "%s.development" % platform.node(),
           "-r", "process", "-i", "60000"]
print cmdline
os.execv(twistd, cmdline)
