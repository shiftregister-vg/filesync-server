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

"""An (non) RPC layer."""

import logging
import time

from twisted.internet import defer, threads

from ubuntuone.storage.rpcdb import (
    AUTH_BACKEND,
    DAL_BACKEND,
    auth_backend,
    dal_backend,
)


# log setup
logger = logging.getLogger("storage.server.rpc")
logger.setLevel(logging.DEBUG)


class ThreadedNonRPC(object):
    """A threaded way to call endpoints, not really an RPC."""

    services = {
        DAL_BACKEND: dal_backend.DAL(),
        AUTH_BACKEND: auth_backend.Auth(),
    }

    def __init__(self, service_name):
        self.service_name = service_name
        self.service = self.services[service_name]

    @defer.inlineCallbacks
    def call(self, funcname, **kwargs):
        """Call the method in the service."""
        servname = self.service_name
        user_id = kwargs.get('user_id')
        logger.info("Call to %s.%s (user=%s) started",
                    servname, funcname, user_id)

        start_time = time.time()
        try:
            method = getattr(self.service, funcname)
            result = yield threads.deferToThread(method, **kwargs)
        except Exception as exc:
            time_delta = time.time() - start_time
            logger.info("Call %s.%s (user=%s) ended with error: %s (%s) "
                        "- time: %s", servname, funcname, user_id,
                        exc.__class__.__name__, exc, time_delta)
            raise

        time_delta = time.time() - start_time
        logger.info("Call to %s.%s (user=%s) ended OK - time: %s",
                    servname, funcname, user_id, time_delta)
        defer.returnValue(result)
