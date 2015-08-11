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

"""The Auth entry point as a service."""

from django.contrib.auth import authenticate


class FailedAuthentication(Exception):
    """Generic error for auth problems."""


class Auth(object):
    """The entry point for the authentication process."""

    def ping(self):
        """Used for a simple liveness check."""
        return dict(response="pong")

    def get_userid_from_token(self, auth_parameters):
        """Get the user_id for the token."""
        username = auth_parameters.get('username')
        password = auth_parameters.get('password')
        user = authenticate(username=username, password=password)

        if user is None:
            raise FailedAuthentication(
                "Bad parameters: %s" % repr(auth_parameters))

        return dict(user_id=user.id)
