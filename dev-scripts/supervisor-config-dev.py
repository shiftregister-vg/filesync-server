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

"""Script to generate supervisor config for many dev services and workers."""

import os
import socket

import _pythonpath  # NOQA

from ubuntuone.supervisor.config_helpers import generate_server_config
from utilities import utils

from supervisor_templates import TEMPLATES

ROOTDIR = utils.get_rootdir()
TMPDIR = os.path.join(ROOTDIR, 'tmp')

config_spec = {
    "basepath": ROOTDIR,
    "log_folder": TMPDIR,
    "pid_folder": TMPDIR,
}

workers = {}
services = {
    "development": {
        "services": {}
    }
}

if __name__ == '__main__':
    hostname = socket.gethostname()
    config_content = generate_server_config(
        hostname, services, config_spec, TEMPLATES, None,
        with_heartbeat=False, with_stats_worker=False, with_header=False)
    with open(os.path.join(TMPDIR, 'services-supervisor.conf'), 'w') as f:
        f.write(config_content)
