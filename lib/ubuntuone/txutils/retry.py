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

"""Retry utilities."""

from twisted.internet import reactor, defer


def async_sleep(seconds, reactor=reactor):
    """Fire the deferred returned after the specified time.

    @param seconds: the seconds to wait
    @param reactor: the reactor (only used in tests)
    """
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, seconds)
    return d


class NetworkRetry(object):
    """Retry a given twisted function."""

    def __init__(self, catch, retries=3, retry_wait=10):
        """Initialize this instance.

        @param retries: the number of retries.
        @param retry_wait: the time we async_sleep between retries
        @param catch: the error or tuple of errors that will be retried
        """
        self.retries = retries
        self.retry_wait = retry_wait
        self.catch = catch

    def _handle_retried_exc(self, exc, retry_count, func, args, kwargs):
        """Handle a exception inside the retry loop. (do nothing by default).

        It's always called from a try/except.
        """
        pass

    def _handle_exc(self, exc):
        """Handle a execption raised by the last retry."""
        raise

    @defer.inlineCallbacks
    def __call__(self, f, *args, **kwargs):
        """Call the provided function, retrying if it fails."""
        for n in range(self.retries - 1):
            try:
                ret = yield f(*args, **kwargs)
                defer.returnValue(ret)
            except self.catch, e:
                self._handle_retried_exc(e, n, f, args, kwargs)
                yield async_sleep(self.retry_wait)
        # try one last time
        try:
            ret = yield f(*args, **kwargs)
            defer.returnValue(ret)
        except self.catch, e:
            yield defer.maybeDeferred(self._handle_exc, e)
