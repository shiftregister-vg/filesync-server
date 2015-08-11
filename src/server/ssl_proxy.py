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

"""An SSL proxy/unwrapper for the api/filesync server."""

import logging

from OpenSSL import SSL, crypto

import twisted

from twisted.application.service import MultiService
from twisted.application.internet import TCPServer, SSLServer
from twisted.internet import defer, address, protocol, reactor, error, stdio
from twisted.internet.interfaces import ITCPTransport
from twisted.protocols import portforward, basic
from twisted.web import server, resource
from zope.component.interfaces import ComponentLookupError

from config import config
from metrics.metricsconnector import MetricsConnector
from ubuntuone.storage.server.logger import configure_logger
from ubuntuone.storage.server.server import get_service_port
from ubuntuone.storage.server.ssl import disable_ssl_compression
from ubuntuone.supervisor import utils as supervisor_utils

logger = logging.getLogger('ssl_proxy')


def get_transport_info(t):
    """Return a str representation of the transport"""
    if ITCPTransport.providedBy(t):
        return get_transport_info(t.getHost())
    elif isinstance(t, address.IPv4Address):
        return "%s:%d" % (t.host, t.port)
    else:
        return repr(t)


class ProxyClient(portforward.ProxyClient):
    """ProxyClient subclass that adds logging and metrics."""

    source = None
    local = None
    dest = None

    def connectionMade(self):
        self.peer.metrics.meter("backend_connection_made", 1)
        self.source = get_transport_info(self.peer.transport.getPeer())
        self.local = get_transport_info(self.transport.getHost())
        self.dest = get_transport_info(self.transport.getPeer())
        logger.debug("Backend connection made: %s -> %s -> %s",
                     self.source, self.local, self.dest)

        if twisted.version.major < 11:
            # see http://twistedmatrix.com/trac/ticket/3350
            # Wire this and the peer transport together to enable
            # flow control (this stops connections from filling
            # this proxy memory when one side produces data at a
            # higher rate than the other can consume).
            self.transport.registerProducer(self.peer.transport, True)
            self.peer.transport.registerProducer(self.transport, True)
        portforward.ProxyClient.connectionMade(self)

    def connectionLost(self, reason=None):
        if reason and reason.check(error.ConnectionDone):
            self.peer.metrics.meter("backend_connection_done", 1)
            logger.debug("Backend connection done: %s -> %s -> %s",
                         self.source, self.local, self.dest)
        else:
            self.peer.metrics.meter("backend_connection_lost", 1)
            logger.debug("Backend connection lost: %s -> %s -> %s",
                         self.source, self.local, self.dest)
        portforward.ProxyClient.connectionLost(self, reason=reason)


class ProxyClientFactory(portforward.ProxyClientFactory):
    """ProxyClientFactory subclass that adds logging and metrics."""

    noisy = False
    protocol = ProxyClient

    def clientConnectionFailed(self, connector, reason):
        self.server.metrics.meter("backend_connection_failed", 1)
        logger.warning('Backend connection failed: %s - %s', connector, reason)
        portforward.ProxyClientFactory.clientConnectionFailed(
            self, connector, reason)


class ProxyServer(portforward.ProxyServer):
    """ProxyServer subclass that adds logging and metrics."""

    clientProtocolFactory = ProxyClientFactory
    metrics = None

    def connectionMade(self):
        self.metrics.meter("frontend_connection_made", 1)
        logger.debug('Frontend connection made: %s',
                     get_transport_info(self.transport.getPeer()))
        portforward.ProxyServer.connectionMade(self)

    def connectionLost(self, reason=None):
        self.metrics.meter("frontend_connection_lost", 1)
        logger.debug('Frontend connection lost: %s',
                     get_transport_info(self.transport.getPeer()))
        portforward.ProxyServer.connectionLost(self, reason=reason)


class SSLProxyFactory(portforward.ProxyFactory):
    """Factory of the ssl proxy.

    Simple subclass that adds logging and metrics support.
    """

    protocol = ProxyServer

    def __init__(self, listen_port, remote_host, remote_port,
                 server_name='ssl-proxy'):
        portforward.ProxyFactory.__init__(self, remote_host, remote_port)
        self.listen_port = listen_port
        self.server_name = server_name
        self.metrics = None

    def startFactory(self):
        """Start any other stuff we need."""
        logger.info("listening on %d -> %s:%d",
                    self.listen_port, self.host, self.port)
        try:
            self.metrics = MetricsConnector.get_metrics("ssl-proxy")
        except ComponentLookupError:
            namespace = config.ssl_proxy.metrics_namespace
            MetricsConnector.register_metrics("ssl-proxy", namespace)
            self.metrics = MetricsConnector.get_metrics("ssl-proxy")
        self.metrics.meter("server_start", 1)

    def stopFactory(self):
        """Shutdown everything."""
        self.metrics.meter("server_stop", 1)
        if self.metrics.connection:
            self.metrics.connection.disconnect()

    def buildProtocol(self, *args, **kw):
        prot = portforward.ProxyFactory.buildProtocol(self, *args, **kw)
        prot.metrics = self.metrics
        return prot

    def check_remote_host(self, timeout=30):
        """Check that the remote host is alive."""
        factory = RemoteHostCheckerFactory()
        reactor.connectTCP(self.host, self.port, factory, timeout=timeout)
        return factory.deferred


class RemoteHostChecker(basic.LineReceiver):
    """A LineReceiver to check if the remote host is alive."""

    def lineReceived(self, line):
        """Handle a single line received."""
        if "filesync server revision" in line:
            self.factory.deferred.callback(line)
        else:
            self.factory.deferred.errback(ValueError(line))
        self.transport.loseConnection()


class RemoteHostCheckerFactory(protocol.ClientFactory):
    """A factory for the RemoteHostChecker."""

    protocol = RemoteHostChecker

    def __init__(self):
        self.deferred = defer.Deferred()

    def buildProtocol(self, addr):
        p = protocol.ClientFactory.buildProtocol(self, addr)
        return p

    def clientConnectionFailed(self, connector, reason):
        self.deferred.errback(reason)


class ProxyContextFactory:
    """An SSL Context Factory."""

    def __init__(self, cert, key, cert_chain):
        """Create the factory.

        @param cert: the certificate text
        @param key: the key text
        """
        self.cert = cert
        self.key = key
        self.cert_chain = cert_chain
        self._context = None

    def getContext(self):
        """Get the SSL Context."""
        if self._context is None:
            ctx = SSL.Context(SSL.SSLv23_METHOD)
            ctx.use_certificate(self.cert)
            if self.cert_chain:
                ctx.add_extra_chain_cert(self.cert_chain)
            ctx.use_privatekey(self.key)
            return ctx
        else:
            return self._context


class ProxyService(MultiService):
    """A class wrapping the whole things a s single twisted service."""

    def __init__(self, ssl_cert, ssl_key, ssl_cert_chain, ssl_port,
                 dest_host, dest_port, server_name, status_port):
        """ Create a rageServerService.

        @param ssl_cert: the certificate text.
        @param ssl_key: the key text.
        @param ssl_port: the port to listen on with ssl.
        @param dest_host: destination hostname.
        @param dest_port: destination port.
        @param server_name: name of this server.
        """
        MultiService.__init__(self)
        self.heartbeat_writer = None
        if server_name is None:
            server_name = "anonymous_instance"
        self.server_name = server_name
        self.factory = SSLProxyFactory(ssl_port, dest_host, dest_port,
                                       self.server_name)
        ssl_context_factory = ProxyContextFactory(ssl_cert, ssl_key,
                                                  ssl_cert_chain)
        self.ssl_service = SSLServer(ssl_port, self.factory,
                                     ssl_context_factory)
        self.ssl_service.setName("SSL")
        self.ssl_service.setServiceParent(self)
        # setup the status service
        self.status_service = create_status_service(self.factory, status_port)
        self.status_service.setServiceParent(self)
        # disable ssl compression
        if config.ssl_proxy.disable_ssl_compression:
            disable_ssl_compression(logger)

    @property
    def port(self):
        """The port with ssl."""
        return get_service_port(self.ssl_service)

    @property
    def status_port(self):
        """The status service port."""
        return get_service_port(self.status_service)

    @defer.inlineCallbacks
    def startService(self):
        """Start listening on two ports."""
        logger.info("- - - - - SERVER STARTING")
        # setup stats in the factory
        yield MultiService.startService(self)
        # only start the HeartbeatWriter if the interval is > 0
        heartbeat_interval = float(config.ssl_proxy.heartbeat_interval)
        if heartbeat_interval > 0:
            self.heartbeat_writer = stdio.StandardIO(
                supervisor_utils.HeartbeatWriter(heartbeat_interval, logger))

    @defer.inlineCallbacks
    def stopService(self):
        """Stop listening on both ports."""
        logger.info("- - - - - SERVER STOPPING")
        yield MultiService.stopService(self)
        if self.heartbeat_writer:
            self.heartbeat_writer.loseConnection()
            self.heartbeat_writer = None
        logger.info("- - - - - SERVER STOPPED")


def create_service():
    """Create the service instance."""
    configure_logger(logger=logger, filename=config.ssl_proxy.log_filename,
                     level=logging.DEBUG, propagate=False, start_observer=True)
    server_key = config.secret.api_server_key
    server_crt = config.secret.api_server_crt
    server_crt_chain = config.secret.api_server_crt_chain

    ssl_cert = crypto.load_certificate(crypto.FILETYPE_PEM, server_crt)
    ssl_cert_chain = crypto.load_certificate(crypto.FILETYPE_PEM,
                                             server_crt_chain)
    ssl_key = crypto.load_privatekey(crypto.FILETYPE_PEM, server_key)

    ssl_proxy = ProxyService(ssl_cert, ssl_key, ssl_cert_chain,
                             config.ssl_proxy.port,
                             '127.0.0.1',
                             config.api_server.tcp_port,
                             config.ssl_proxy.server_name,
                             config.ssl_proxy.status_port)
    return ssl_proxy


class _Status(resource.Resource):
    """The Status Resource."""

    def __init__(self, service):
        """Create the Resource."""
        resource.Resource.__init__(self)
        self.service = service

    def render_GET(self, request):
        """Handle GET."""
        d = self.service.check_remote_host()

        def on_success(result):
            """Success callback"""
            request.write('OK')
            request.finish()

        def on_error(failure):
            """Error callback"""
            logger.error("Error while checking remote host: %s",
                         failure.getBriefTraceback())
            request.setResponseCode(503)
            request.write(failure.getErrorMessage() + "\n")
            request.finish()

        d.addCallbacks(on_success, on_error)
        return server.NOT_DONE_YET


def create_status_service(proxy_server, port):
    """Create the status service."""
    root = resource.Resource()
    root.putChild('status', _Status(proxy_server))
    site = server.Site(root)
    service = TCPServer(port, site)
    return service
