# -*- python -*-

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

import os

from twisted.application import service
from twisted.internet import reactor

import oops_timeline

from config import config
from ubuntuone.storage.server import server, auth

oops_config = server.configure_oops()
# Should probably be an option to configure_oops?
oops_timeline.install_hooks(oops_config)


s3_host = config.aws_s3.host
s3_ssl = config.aws_s3.use_ssl

# if neither env nor config has it, we try the port file
if s3_ssl:
    s3_port = os.getenv('S4SSLPORT', config.aws_s3.port)
    if s3_port:
        s3_port = int(s3_port)
else:
    s3_port = os.getenv('S4PORT', config.aws_s3.port)
    if s3_port:
        s3_port = int(s3_port)

s3_key = config.secret.aws_access_key_id or os.environ["S3_KEY"]
s3_secret = config.secret.aws_secret_access_key or os.environ["S3_SECRET"]

application = service.Application('StorageServer')
storage = server.create_service(s3_host, s3_port, s3_ssl, s3_key, s3_secret,
                                s3_proxy_host=config.aws_s3.proxy_host,
                                s3_proxy_port=config.aws_s3.proxy_port,
                                status_port=config.api_server.status_port,
                                auth_provider_class=auth.SimpleAuthProvider,
                                oops_config=oops_config)
storage.setServiceParent(service.IServiceCollection(application))

# install signal handlers
server.install_signal_handlers()
