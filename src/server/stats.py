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

"""Storage server stats helpers."""

import logging
import socket

from twisted.application.internet import TCPServer, SSLServer
from twisted.internet import defer, reactor
from twisted.web import server, resource

from metrics.metricsconnector import MetricsConnector
from s3lib import producers
from s3lib.contrib import http_client
from txstatsd.process import report_reactor_stats
from ubuntuone.storage.server import upload
from ubuntuone.monitoring import dump

logger = logging.getLogger("storage.server.stat")
status_log = logging.getLogger("storage.server.status")

# list of buffers to watch, we will use
BUFFERS = (upload.MultipartUploadFactory,
           http_client.HTTPProducer,
           producers.S3Producer)


class MeliaeResource(resource.Resource):
    """The Statistics Resource."""

    def render_GET(self, request):
        """Handle GET."""
        return dump.meliae_dump()


class GCResource(resource.Resource):
    """Working with the garbage collector."""

    def render_GET(self, request):
        """Handle GET."""
        return dump.gc_dump()


class StatsWorker(object):
    """Execute actions and log the results at the specified interval."""

    logger = logger

    def __init__(self, service, interval, servername=None):
        self.service = service
        if servername is None:
            self.servername = socket.gethostname()
        else:
            self.servername = servername
        self.interval = interval
        self.next_loop = None
        self._get_reactor_stats = report_reactor_stats(reactor)

    @property
    def metrics(self):
        """Return the metrics instance for self.service."""
        return MetricsConnector.get_metrics("root")

    def callLater(self, seconds, func, *args, **kwargs):
        """Wrap reactor.callLater to simplify testing."""
        reactor.callLater(seconds, func, *args, **kwargs)

    def log(self, msg, *args, **kwargs):
        """Wrap logger.info call to simplify testing."""
        self.logger.info(msg, *args, **kwargs)

    def start(self):
        """Start rolling."""
        if self.interval:
            self.next_loop = self.callLater(0, self.work)

    def stop(self):
        """Stop working, cancel delayed calls if active."""
        if self.next_loop is not None and self.next_loop.active():
            self.next_loop.cancel()

    def work(self):
        """Call the methods that do the real work."""
        self.runtime_info()
        self.next_loop = self.callLater(self.interval, self.work)

    def runtime_info(self):
        """Log runtime info.

        This includes: reactor readers/writers and buffers size.
        """
        reactor_report = self._get_reactor_stats()
        for key, value in reactor_report.iteritems():
            self.metrics.gauge(key, value)

        self.log("reactor readers: %(reactor.readers)s "
                 "writers: %(reactor.writers)s", reactor_report)

        # send a metric for current buffers size
        for clazz in BUFFERS:
            size = clazz.buffers_size
            name = clazz.__name__
            self.metrics.gauge("buffers_size.%s" % (name,), size)


class _Status(resource.Resource):
    """The Status Resource."""

    def __init__(self, server, user_id):
        """Create the Resource."""
        resource.Resource.__init__(self)
        self.storage_server = server
        self.user_id = user_id

    @property
    def content(self):
        """A property to get the content manager."""
        return self.storage_server.factory.content

    def render_GET(self, request):
        """Handle GET."""
        d = self._check()

        def on_success(result):
            """Success callback"""
            request.write(result)
            request.finish()

        def on_error(failure):
            """Error callback"""
            status_log.error("Error while getting status. %s",
                             failure.getTraceback())
            request.setResponseCode(500)
            request.write(failure.getErrorMessage() + "\n")
            request.finish()
        d.addCallbacks(on_success, on_error)
        return server.NOT_DONE_YET

    @defer.inlineCallbacks
    def _check(self):
        """The check for Alive/Dead.

        Get the user and it root object.
        """
        user = yield self.content.get_user_by_id(self.user_id, required=True)
        yield user.get_root()
        defer.returnValue('Status OK\n')


def create_status_service(storage, parent_service, port,
                          user_id=0, ssl_context_factory=None):
    """Create the status service."""
    root = resource.Resource()
    root.putChild('status', _Status(storage, user_id))
    root.putChild('+meliae', MeliaeResource())
    root.putChild('+gc-stats', GCResource())
    site = server.Site(root)
    if ssl_context_factory is None:
        service = TCPServer(port, site)
    else:
        service = SSLServer(port, site, ssl_context_factory)
    service.setServiceParent(parent_service)
    return service
