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

"""Unit tests for the txutils.retry."""

from ubuntuone import txutils
from twisted.internet import defer
from twisted.trial.unittest import TestCase as TwistedTestCase


class TestNetworkRetry(TwistedTestCase):
    """Tests for the NetworkRetry."""

    class TestException(Exception):
        """An exception only for this tests."""

    class MostlyFailingCallable:
        """A callable that only succeeds after a given number of calls."""

        def __init__(self, success_on_try=None):
            """Initialize this instance with the number of retries."""
            self.count = 0
            self.success_on_try = success_on_try

        def __call__(self):
            """The call that will mostly fail."""
            self.count += 1
            if self.count == self.success_on_try:
                return defer.succeed("OK")
            return defer.fail(TestNetworkRetry.TestException())

    @defer.inlineCallbacks
    def test_no_problems(self):
        """This will succeed on the first try."""
        retrier = txutils.NetworkRetry(catch=TestNetworkRetry.TestException)
        f = TestNetworkRetry.MostlyFailingCallable(success_on_try=1)
        ret = yield retrier(f)
        self.assertEquals(ret, "OK")
        self.assertEquals(f.count, 1)

    @defer.inlineCallbacks
    def test_some_retries(self):
        """This will succeed after a few tries."""
        retrier = txutils.NetworkRetry(
            catch=TestNetworkRetry.TestException, retries=3, retry_wait=0.001)
        f = TestNetworkRetry.MostlyFailingCallable(success_on_try=3)
        ret = yield retrier(f)
        self.assertEquals(ret, "OK")
        self.assertEquals(f.count, 3)

    @defer.inlineCallbacks
    def test_too_many_retries(self):
        """This will try a few times, and fail."""
        retrier = txutils.NetworkRetry(
            catch=TestNetworkRetry.TestException, retries=3, retry_wait=0.001)
        f = TestNetworkRetry.MostlyFailingCallable(success_on_try=10)
        yield self.assertFailure(retrier(f), TestNetworkRetry.TestException)
        self.assertEquals(f.count, 3)

    @defer.inlineCallbacks
    def test_other_exception(self):
        """If the exception is not the expected one, we should not retry."""
        retrier = txutils.NetworkRetry(catch=None, retries=3, retry_wait=0.001)
        f = TestNetworkRetry.MostlyFailingCallable(success_on_try=3)
        yield self.assertFailure(retrier(f), TestNetworkRetry.TestException)
        self.assertEquals(f.count, 1)
