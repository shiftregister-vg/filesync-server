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

"""Tests for the Auth entry point."""

from backends.filesync.data.testing.testcase import StorageDALTestCase
from ubuntuone.storage.rpcdb import auth_backend
from ubuntuone.storage.server.testing.testcase import create_test_user


class AuthTestCase(StorageDALTestCase):
    """Tests for the Auth service specifically."""

    def setUp(self):
        """Set up."""
        res = super(AuthTestCase, self).setUp()
        self.auth = auth_backend.Auth()
        self.auth_parameters = dict(username=u"user", password='testpass')
        self.usr = create_test_user(**self.auth_parameters)
        return res

    def test_ping(self):
        """Ping pong."""
        res = self.auth.ping()
        self.assertEqual(res, {'response': 'pong'})

    def test_get_user_id_ok(self):
        """Get user id, all ok."""
        result = self.auth.get_userid_from_token(self.auth_parameters)
        self.assertEqual(result, dict(user_id=self.usr.id))

    def test_get_user_id_bad_auth(self):
        """Bad parameters in the oauth request."""
        bad_parameters = self.auth_parameters.copy()
        bad_parameters['password'] = "bad"
        try:
            self.auth.get_userid_from_token({})
        except auth_backend.FailedAuthentication as exc:
            self.assertEqual(str(exc), "Bad parameters: {}")
        else:
            self.fail("Should have raised an exception.")

    def test_auth_parameters_not_dict(self):
        bad_params = (1, 2, object(), 'foo')
        self.assertRaises(
            AttributeError, self.auth.get_userid_from_token, bad_params)

    def test_auth_parameters_empty_dict(self):
        bad_params = dict()
        with self.assertRaises(auth_backend.FailedAuthentication) as ctx:
            self.auth.get_userid_from_token(bad_params)
        self.assertEqual(
            str(ctx.exception), 'Bad parameters: {}')
