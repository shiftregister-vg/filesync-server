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

"""Test authentication."""

import logging

from twisted.internet import defer

from ubuntuone.devtools.handlers import MementoHandler

from backends.filesync.data.errors import DoesNotExist
from ubuntuone.storage.server.auth import (
    AuthenticationProvider,
    DummyAuthProvider,
    SimpleAuthProvider,
)
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storageprotocol import errors as protocol_errors
from ubuntuone.storageprotocol import request


class AuthenticationBaseTestCase(TestWithDatabase):

    def do_auth(self, client, credentials, **kwargs):
        if not isinstance(credentials, dict):
            auth_d = client.dummy_authenticate(credentials, **kwargs)
        else:
            username = credentials['username']
            password = credentials['password']
            auth_d = client.simple_authenticate(username, password, **kwargs)

        return auth_d


class AuthenticationProviderTests(AuthenticationBaseTestCase):
    """Fixture for authentication provider tests."""

    auth_provider_class = AuthenticationProvider

    @defer.inlineCallbacks
    def setUp(self):
        yield super(AuthenticationProviderTests, self).setUp()
        self.provider = self.auth_provider_class(self.service.factory)


class DummyProviderTests(AuthenticationProviderTests):
    """Tests for the dummy authentication provider."""

    auth_provider_class = DummyAuthProvider

    @defer.inlineCallbacks
    def test_authenticate(self):
        """The dummy authentication provider succeeds with a valid token."""
        auth_params = {"dummy_token": "open sesame"}
        user = yield self.provider.authenticate(auth_params, None)
        self.assertEqual(user.id, 0)
        # the same user is returned by repeated calls
        user2 = yield self.provider.authenticate(auth_params, None)
        self.assertEqual(user.id, user2.id)

    @defer.inlineCallbacks
    def test_authenticate_fail(self):
        """The dummy authentication provider fails with an invalid token."""
        auth_params = {"dummy_token": "wrong password"}
        user = yield self.provider.authenticate(auth_params, None)
        self.assertEqual(user, None)

    @defer.inlineCallbacks
    def test_authenticate_no_parameters(self):
        """The dummy authentication provider fails with no parameters."""
        user = yield self.provider.authenticate({}, None)
        self.assertEqual(user, None)


class SimpleAuthProviderTests(AuthenticationProviderTests):
    """Tests for the simple authentication provider."""

    auth_provider_class = SimpleAuthProvider

    @defer.inlineCallbacks
    def setUp(self):
        yield super(SimpleAuthProviderTests, self).setUp()
        self.creds = {
            "username": self.usr0.username,
            "password": self.usr0.password,
        }

    @defer.inlineCallbacks
    def test_authenticate(self):
        """The OAuth provider succeeds with a valid PLAINTEXT signature."""
        user = yield self.provider.authenticate(self.creds, None)
        self.assertEqual(user.id, self.usr0.id)

        # the same user is returned by repeated calls
        user2 = yield self.provider.authenticate(self.creds, None)
        self.assertEqual(user.id, user2.id)

    @defer.inlineCallbacks
    def test_authenticate_failure(self):
        """The OAuth provider succeeds with an invalid PLAINTEXT signature."""
        auth_parameters = self.creds.copy()
        auth_parameters['password'] = 'invalid'

        user = yield self.provider.authenticate(auth_parameters, None)
        self.assertEqual(user, None)

    @defer.inlineCallbacks
    def test_authenticate_no_parameters(self):
        """The OAuth provider fails with no parameters."""
        user = yield self.provider.authenticate({}, None)
        self.assertEqual(user, None)


class ClientDummyAuthTests(AuthenticationBaseTestCase):
    """Client authentication tests using the dummy auth provider."""

    auth_provider_class = DummyAuthProvider

    @defer.inlineCallbacks
    def setUp(self):
        yield super(ClientDummyAuthTests, self).setUp()
        self.creds = 'open sesame'
        self.bad_creds = 'not my secret'
        self.handler = MementoHandler()
        logger = logging.getLogger('storage.server')
        logger.addHandler(self.handler)
        self.addCleanup(logger.removeHandler, self.handler)
        self.handler.setLevel(logging.DEBUG)

    def assert_auth_ok_logging(self):
        self.assertTrue(self.handler.check_debug(
            "authenticated user", "OK", self.usr0.username))
        self.assertFalse(self.handler.check_warning("missing user"))

    def assert_auth_ok_missing_user(self):
        self.assertTrue(self.handler.check_debug(
                        "missing user", "(id=%s)" % self.usr0.id))
        self.assertFalse(self.handler.check_info("authenticated user"))

    @defer.inlineCallbacks
    def test_auth_ok_user_ok(self):
        """Correct authentication must succeed."""
        yield self.callback_test(
            self.do_auth, credentials=self.creds, add_default_callbacks=True)
        self.assert_auth_ok_logging()

    @defer.inlineCallbacks
    def test_auth_ok_bad_user(self):
        """Non existing user must fail authentication."""
        # make the user getter fail
        self.patch(self.service.factory.content, 'get_user_by_id',
                   lambda *a, **k: defer.fail(DoesNotExist()))

        d = self.callback_test(
            self.do_auth, credentials=self.creds, add_default_callbacks=True)
        yield self.assertFailure(d, protocol_errors.AuthenticationFailedError)

        self.assert_auth_ok_missing_user()

    @defer.inlineCallbacks
    def test_auth_ok_with_session_id(self):
        """Correct authentication must succeed and include the session_id."""
        auth_request = yield self.callback_test(
            self.do_auth, credentials=self.creds, add_default_callbacks=True)

        protocol = self.service.factory.protocols[0]
        self.assertEqual(auth_request.session_id, str(protocol.session_id))

    @defer.inlineCallbacks
    def test_auth_ok_with_metadata(self):
        """Correct authentication must succeed and include metadata."""
        m_called = []
        self.service.factory.metrics.meter = lambda *a: m_called.append(a)

        metadata = {u"platform": u"linux2", u"version": u"1.0", u"foo": u"bar"}
        yield self.callback_test(
            self.do_auth, credentials=self.creds, metadata=metadata,
            add_default_callbacks=True)

        self.assertTrue(
            self.handler.check_info("Client metadata: %s" % metadata))
        self.assertIn(("client.platform.linux2", 1), m_called)
        self.assertIn(("client.version.1_0", 1), m_called)
        self.assertNotIn(("client.foo.bar", 1), m_called)

    def test_auth_fail(self):
        """Wrong secret must fail."""

        def test(client, **kwargs):
            d = self.do_auth(client, credentials=self.bad_creds)
            d.addCallbacks(
                lambda _: client.test_fail(Exception("Should not succeed.")),
                lambda _: client.test_done("ok"))

        return self.callback_test(test)

    def test_get_root(self):
        """Must receive the root after authentication."""

        @defer.inlineCallbacks
        def test(client, **kwargs):
            yield self.do_auth(client, credentials=self.creds)
            root_id = yield client.get_root()
            self.assertIsNotNone(root_id)

        return self.callback_test(test, add_default_callbacks=True)

    def test_get_root_twice(self):
        """Get root must keep the root id."""

        @defer.inlineCallbacks
        def test(client, **kwargs):
            yield self.do_auth(client, credentials=self.creds)
            root_id1 = yield client.get_root()
            root_id2 = yield client.get_root()
            self.assertEqual(root_id1, root_id2)

        return self.callback_test(test, add_default_callbacks=True)

    def test_user_becomes_inactive(self):
        """After StorageUser authentication ok it becomes inactive."""

        @defer.inlineCallbacks
        def test(client):
            """Test."""
            yield self.do_auth(client, credentials=self.creds)
            root_id = yield client.get_root()

            # create one file, should be ok
            yield client.make_file(request.ROOT, root_id, "f1")

            # cancel user subscription, so it needs
            # to get it again from the DB
            self.usr0.update(subscription=False)

            # create second file, should NOT be ok
            try:
                yield client.make_file(request.ROOT, root_id, "f2")
            except protocol_errors.DoesNotExistError:
                pass  # failed as we expected
            else:
                client.test_fail("It should have failed!")

        return self.callback_test(test, add_default_callbacks=True)


class ClientSimpleAuthTests(ClientDummyAuthTests):
    """Client authentication tests using the OAuth provider."""

    auth_provider_class = SimpleAuthProvider

    @defer.inlineCallbacks
    def setUp(self):
        yield super(ClientSimpleAuthTests, self).setUp()
        self.creds = {
            "username": self.usr0.username,
            "password": self.usr0.password,
        }
        self.bad_creds = {
            "username": self.usr0.username,
            "password": 'invalid',
        }

    def assert_auth_ok_logging(self):
        self.assertTrue(self.handler.check_info(
            "authenticated user", "OK", self.usr0.username,
            "(id=%s)" % self.usr0.id))
        self.assertTrue(self.handler.check_info(
            "valid tokens", "(id=%s)" % self.usr0.id))
        self.assertFalse(self.handler.check_warning("missing user"))

    def assert_auth_ok_missing_user(self):
        self.assertTrue(self.handler.check_info(
                        "valid tokens", "(id=%s)" % self.usr0.id))
        self.assertTrue(self.handler.check_warning(
                        "missing user", "(id=%s)" % self.usr0.id))
        self.assertFalse(self.handler.check_info("authenticated user"))
