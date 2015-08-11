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

"""Tests for the capabilities handling."""

from twisted.internet import defer

from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from ubuntuone.storage.server import server as server_module


class QuerySetHelper(TestWithDatabase):
    '''Helping methods.'''

    # just to restore original values
    _original_supported_caps = server_module.SUPPORTED_CAPS
    _original_suggested_redirs = server_module.SUGGESTED_REDIRS
    _original_min_caps = server_module.MIN_CAP

    def tearDown(self):
        '''after test run'''
        server_module.SUPPORTED_CAPS = self._original_supported_caps
        server_module.SUGGESTED_REDIRS = self._original_suggested_redirs
        server_module.MIN_CAP = self._original_min_caps
        return super(QuerySetHelper, self).tearDown()

    def check_answer(self, req, operation, sent, should_accept, redirs=None):
        '''validate response'''
        # hack to test how the server worked
        test_server = self.service.factory.protocols[0]

        # accepted or not as it should?
        self.assertEqual(should_accept, req.accepted)

        if operation == "query":
            # query operations should not set capabilities!
            self.assertEqual(test_server.working_caps, server_module.MIN_CAP)
        elif operation == "set":
            # if the server accepted, it should set the capabilities
            if should_accept:
                self.assertEqual(test_server.working_caps, set(sent))

        # let's see if it suggested correctly where to redirect
        if not should_accept and redirs is not None:
            self.assertEqual(req.redirect_hostname, redirs.get("hostname", ""))
            self.assertEqual(req.redirect_port, redirs.get("port", ""))
            self.assertEqual(req.redirect_srvrecord,
                             redirs.get("srvrecord", ""))

    def send(self, operation, to_send, should_accept, redirs=None):
        '''Will send a query, and validate response.'''

        def handy(client):
            '''easies how this all is called'''
            op = getattr(client, operation + "_caps")
            d = op(to_send)
            d.addCallback(lambda r: self.check_answer(
                          r, operation, to_send, should_accept, redirs))
            d.addCallbacks(client.test_done, client.test_fail)
            return client
        return handy


class QueryCapabilitiesTest(QuerySetHelper):
    """Client querying the server in different ways."""

    def test_query_empty(self):
        """Simple query, no capabilities, always supported default cap."""
        server_module.SUPPORTED_CAPS = set([frozenset([])])
        return self.callback_test(self.send("query", [], True), caps=None)

    def test_query_one_present_ok(self):
        """Simple query, one capability present, ok."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        return self.callback_test(self.send("query", ["foo"], True), caps=None)

    def test_query_one_present_bad(self):
        """Simple query, one capability present, bad."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        return self.callback_test(self.send("query", ["bar"], False),
                                  caps=None)

    def test_query_two_present_ok(self):
        """Simple query, two capabilities present, ok."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])
        return self.callback_test(self.send("query", ["bar", "foo"], True),
                                  caps=None)

    def test_query_two_present_bad(self):
        """Simple query, two capabilities present, bad."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])
        return self.callback_test(self.send("query", ["foo", "rab"], False),
                                  caps=None)

    def test_query_insistent(self):
        """Repeated query."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])

        def queries(client):
            '''some sequential queries'''
            d = defer.succeed(None)
            # nop
            d.addCallback(lambda _: client.query_caps(["foo", "rab"]))
            d.addCallback(lambda r: self.check_answer(r, "query", None, False))

            # ok!
            d.addCallback(lambda _: client.query_caps(["foo", "bar"]))
            d.addCallback(lambda r: self.check_answer(r, "query", None, True))

            # nop
            d.addCallback(lambda _: client.query_caps(["foo"]))
            d.addCallback(lambda r: self.check_answer(r, "query", None, False))

            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(queries, caps=None)


class SetCapabilitiesTest(QuerySetHelper):
    """Client setting the server capabilities in different ways."""

    def test_set_empty(self):
        """Simple set, no capabilities."""
        server_module.SUPPORTED_CAPS = set([frozenset([])])
        return self.callback_test(self.send("set", [], True), caps=None)

    def test_set_one_present_ok(self):
        """Simple set, one capability present, ok."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        return self.callback_test(self.send("set", ["foo"], True), caps=None)

    def test_set_one_present_bad(self):
        """Simple set, one capability present, bad."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        return self.callback_test(self.send("set", ["bar"], False), caps=None)

    def test_set_two_present_ok(self):
        """Simple set, two capabilities present, ok."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])
        return self.callback_test(self.send("set", ["bar", "foo"], True),
                                  caps=None)

    def test_set_two_present_bad(self):
        """Simple set, two capabilities present, bad."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])
        return self.callback_test(self.send("set", ["foo", "rab"], False),
                                  caps=None)

    def test_set_insistent_ok(self):
        """Repeated set, allowed."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])

        def queries(client):
            '''some sequential queries'''
            d = defer.succeed(None)
            # nop
            d.addCallback(lambda _: client.set_caps(["foo", "rab"]))
            d.addCallback(lambda r: self.check_answer(r, "set", [], False))

            # nop
            d.addCallback(lambda _: client.set_caps(["foo"]))
            d.addCallback(lambda r: self.check_answer(r, "set", [], False))

            # ok!
            d.addCallback(lambda _: client.set_caps(["foo", "bar"]))
            d.addCallback(lambda r: self.check_answer(r, "set",
                                                      ["foo", "bar"], True))

            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(queries, caps=None)

    def test_set_insistent_nop(self):
        """Repeated set, forbidden."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo", "bar"])])

        def queries(client):
            '''some sequential queries'''
            d = defer.succeed(None)
            # nop
            d.addCallback(lambda _: client.set_caps(["foo", "rab"]))
            d.addCallback(lambda r: self.check_answer(r, "set", [], False))

            # ok!
            d.addCallback(lambda _: client.set_caps(["foo", "bar"]))
            d.addCallback(lambda r: self.check_answer(r, "set",
                                                      ["foo", "bar"], True))

            # after a succesful one, all others must fail!
            d.addCallback(lambda _: client.set_caps(["foo"]))
            d.addCallback(lambda r: self.check_answer(r, "set", [], False))
            d.addCallback(lambda _: client.set_caps(["foo", "bar"]))
            d.addCallback(lambda r: self.check_answer(r, "set", [], False))

            d.addCallbacks(client.test_done, client.test_fail)
            return d
        return self.callback_test(queries, caps=None)


class RedirectsTest(QuerySetHelper):
    """Testing that the server redirects as it should."""

    def test_redirect_nothing_emptiness(self):
        """Don't get redirection because of emptiness."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        server_module.SUGGESTED_REDIRS = {}
        return self.callback_test(self.send("set", ["bar"], False, {}),
                                  caps=None)

    def test_redirect_nothing_mismatch(self):
        """Don't get redirection because of mismatch."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir = dict(hostname='a', port='b', srvrecord='c')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir}
        return self.callback_test(self.send("set", ["baz"], False, {}),
                                  caps=None)

    def test_redirect_ok_set(self):
        """Get a redirection in a set operation."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir = dict(hostname='a', port='b', srvrecord='c')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir}
        return self.callback_test(self.send("set", ["bar"], False, redir),
                                  caps=None)

    def test_redirect_ok_query(self):
        """Get a redirection in a set operation."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir = dict(hostname='a', port='b', srvrecord='c')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir}
        return self.callback_test(self.send("query", ["bar"], False, redir),
                                  caps=None)

    def test_redirect_combination_onlyhost(self):
        """Get a redirection to a host only."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir = dict(hostname='a', port='b')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir}
        return self.callback_test(self.send("set", ["bar"], False, redir),
                                  caps=None)

    def test_redirect_combination_onlysrv(self):
        """Get a redirection to a srv record."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir = dict(srvrecord='c')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir}
        return self.callback_test(self.send("set", ["bar"], False, redir),
                                  caps=None)

    def test_redirect_more_options(self):
        """Get a redirection even having more opptions."""
        server_module.SUPPORTED_CAPS = set([frozenset(["foo"])])
        redir1 = dict(hostname='a', port='b', srvrecord='c')
        redir2 = dict(hostname='t', port='y', srvrecord='u')
        server_module.SUGGESTED_REDIRS = {frozenset(["bar"]): redir1,
                                          frozenset(["gol"]): redir2}
        return self.callback_test(self.send("set", ["bar"], False, redir1),
                                  caps=None)
