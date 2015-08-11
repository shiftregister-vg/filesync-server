#!/usr/bin/python -Wignore

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

"""Script starting the squid server."""

import os
import sys
import subprocess

import _pythonpath  # NOQA

from config import config
from utilities import utils
from utilities.localendpoints import allocate_ports, register_local_port


def main():
    """Start the squid service."""
    service_name, config_template = sys.argv[1:3]
    squid_bin = "/usr/sbin/squid3"
    port = config.storage_proxy.port
    if not port:
        port = allocate_ports()[0]
        register_local_port(service_name, port, ssl=False)

    tmp_dir = utils.get_tmpdir()
    conffile_path = os.path.join(tmp_dir, '%s.conf' % service_name)
    with open(conffile_path, 'w') as config_out:
        with open(config_template, 'r') as config_in:
            config_out.write(config_in.read().format(
                s3_dstdomain=config.aws_s3.host,
                s3_dstport=config.aws_s3.port,
                s3_dstssl=int(config.aws_s3.port) == 443 and "ssl" or "",
                swift_dstdomain=config.keystone.host,
                swift_dstport=config.keystone.port,
                swift_dstssl=int(config.keystone.port) == 443 and "ssl" or "",
                service_name=service_name,
                tmpdir=tmp_dir,
                port=port))

    subprocess.call([squid_bin, '-f', conffile_path, '-z'])
    os.execvp(squid_bin, [squid_bin, '-f', conffile_path,
                          '-N', '-Y', '-C'])

if __name__ == '__main__':
    main()
