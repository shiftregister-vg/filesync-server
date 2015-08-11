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

"""Development configuration."""

from utilities import localendpoints


def development_ports(config):
    """Augment the configuration with branch-local port numbers."""
    config.statsd.servers = localendpoints.get_local_server('statsd')

    config.aws_s3.port = localendpoints.get_local_port('s4')
    config.aws_s3.proxy_host = 'localhost'
    config.aws_s3.proxy_port = localendpoints.get_local_port('storage-proxy')

    config.keystone.port = localendpoints.get_local_port('s4')
    config.keystone.proxy_host = 'localhost'
    config.keystone.proxy_port = localendpoints.get_local_port('storage-proxy')
