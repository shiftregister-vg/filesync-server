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

"""Report metric samples to statsd."""

from txstatsd.client import (
    StatsDClientProtocol, TwistedStatsDClient,
    UdpStatsDClient, ConsistentHashingClient)
from txstatsd.metrics.extendedmetrics import ExtendedMetrics
from txstatsd.metrics.imetrics import IMetrics

from config import config

from zope.component import globalSiteManager as gsm


class MetricsConnector(object):
    """Obtain a Metrics instance for reporting events to statsd."""

    @classmethod
    def new_metrics(cls, connection=None, namespace=""):
        """Returns a Metrics (configured with the statsd connection)."""
        return cls._metrics(connection, namespace, False)

    @classmethod
    def new_txmetrics(cls, connection=None, namespace=""):
        """Returns a Metrics (configured with the statsd connection)."""
        return cls._metrics(connection, namespace, True)

    @classmethod
    def register_metrics(cls, key, namespace=None, instance=None):
        """Registers a metrics instance as a zope named utility."""
        if namespace is None:
            namespace = key
        if instance is None:
            instance = cls.new_txmetrics(namespace=namespace)
        gsm.registerUtility(instance, IMetrics, key)

    @classmethod
    def unregister_metrics(cls):
        """Unregisters all previously registered metrics."""
        for key, _ in gsm.getUtilitiesFor(IMetrics):
            gsm.unregisterUtility(None, IMetrics, key)

    @classmethod
    def get_metrics(cls, key):
        """Retrieves an ExtendedMetrics for the given key."""
        return gsm.getUtility(IMetrics, key)

    @classmethod
    def _metrics(cls, connection, namespace, async):
        """Returns a Metrics (configured with the statsd connection).

        Raises an Exception if missing statsd configuration.
        """

        if connection is None:
            connection = cls._realise_connection(async)
        return ExtendedMetrics(connection, namespace)

    @classmethod
    def _realise_connection(cls, async):
        """Return a configured statsd client connection."""
        servers = config.statsd.servers
        if servers is None:
            raise LookupError('Unable to obtain the statsd configuration')

        servers = map(None, [server.strip() for server in servers.split(";")])
        if not servers:
            raise LookupError('Unable to obtain the statsd configuration')

        connections = []
        for server in servers:
            statsd_host, statsd_port = server.split(":")
            statsd_port = int(statsd_port)

            if async:
                connection = TwistedStatsDClient.create(statsd_host,
                                                        statsd_port)
                connection.disconnect_callback = \
                    lambda: cls._disconnect_connection(connection)
                protocol = StatsDClientProtocol(connection)

                from twisted.internet import reactor
                reactor.listenUDP(0, protocol)
            else:
                connection = UdpStatsDClient(statsd_host, statsd_port)
                connection.connect()

            connections.append(connection)

        if len(connections) == 1:
            return connections[0]

        return ConsistentHashingClient(connections)

    @classmethod
    def _disconnect_connection(cls, connection):
        """Disconnect the supplied connection."""
        if connection is not None and \
                connection.transport is not None:
            connection.transport.stopListening()
