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

"""Authentication backends for the storage server."""

__metaclass__ = type
__all__ = ["AuthenticationProvider", "DummyAuthProvider", "SimpleAuthProvider"]

import logging

from twisted.internet import defer

from backends.filesync.data.errors import DoesNotExist
from ubuntuone.storage.rpcdb import auth_backend


logger = logging.getLogger("storage.server")


class AuthenticationProvider:
    """Provides authentication services to the server."""

    def __init__(self, factory):
        """Create a provider.

        @param factory: the Storage server's factory.
        """
        super(AuthenticationProvider, self).__init__()
        self.factory = factory

    def authenticate(self, auth_parameters, protocol):
        """Talk with the service provider to get the access token
        if we get a valid access token, we return a StorageUser.

        @param auth_parameters: the signed request token.
        @param protocol: the protocol object.
        @returns: a deferred that results in a StorageUser.
        """
        msg = "AuthenticationProvider doesn't implement authenticate."
        raise NotImplementedError(msg)


class DummyAuthProvider(AuthenticationProvider):
    """Dummy authentication provider."""

    # request_token: user_id
    _allowed = {
        "open sesame": 0,
        "friend": 1,          # say 'friend', and enter
        "usr3": 3,
    }

    @defer.inlineCallbacks
    def authenticate(self, auth_parameters, protocol):
        """Authenticate users if request_token is self._request_token."""
        request_token = auth_parameters.get('dummy_token')
        if request_token not in self._allowed:
            logger.debug("AUTH: request_token not in _allowed")
            return

        session_id = protocol.session_id if protocol else None
        user_id = self._allowed[request_token]
        get_user_by_id = self.factory.content.get_user_by_id
        try:
            user = yield get_user_by_id(user_id, session_id=session_id,
                                        required=True)
        except DoesNotExist:
            logger.debug("AUTH: missing user (id=%s)", user_id)
            return

        logger.debug("AUTH: authenticated user %s OK", user.username)
        user.register_protocol(protocol)
        defer.returnValue(user)


class SimpleAuthProvider(AuthenticationProvider):
    """Simple authentication implementation for Storage Server."""

    @defer.inlineCallbacks
    def authenticate(self, auth_parameters, protocol):
        """See `AuthenticationProvider`"""
        rpc_client = self.factory.rpcauth_client
        try:
            resp = yield rpc_client.call('get_userid_from_token',
                                         auth_parameters=auth_parameters)
        except auth_backend.FailedAuthentication, exc:
            logger.info("Failed auth: %s", exc)
            return

        user_id = resp['user_id']
        logger.info("AUTH: valid tokens (id=%s)", user_id)

        session_id = protocol.session_id if protocol else None
        get_user_by_id = self.factory.content.get_user_by_id
        try:
            user = yield get_user_by_id(user_id, session_id=session_id,
                                        required=True)
        except DoesNotExist:
            # if there is no user, they haven't subscribed
            logger.warning("AUTH: missing user (id=%s)", user_id)
            return
        logger.info("AUTH: authenticated user %s (id=%s) OK",
                    user.username, user.id)
        user.register_protocol(protocol)
        defer.returnValue(user)
