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

"""Tests for the metrics connector."""

import os

from metrics.metricsconnector import MetricsConnector

from mocker import Mocker

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from txstatsd.client import TwistedStatsDClient
from txstatsd.metrics.extendedmetrics import ExtendedMetrics

from zope.component import ComponentLookupError

from config import config

from utilities import utils


class TestMetricsConnector(TestCase):
    """Test MetricsConnector."""

    package_dir = os.path.dirname(__file__)

    def setUp(self):
        """Save the existing global config."""
        self.metrics = None

    def tearDown(self):
        """Restore the existing global config."""
        if (self.metrics is not None and
                self.metrics.connection is not None):
            if isinstance(self.metrics.connection, TwistedStatsDClient):
                protocol = self.metrics.connection.transport
                protocol.stopListening()

    @inlineCallbacks
    def test_configured_statsd(self):
        """Test configuring of Metrics using supplied configuration."""
        self.patch(config.statsd, 'servers', "localhost:8125")

        metrics = MetricsConnector.new_metrics()
        self.failIf(metrics.connection is None)
        self.failIf(metrics.connection.socket is None)

        self.metrics = MetricsConnector.new_txmetrics()
        yield self.metrics.connection.resolve_later
        self.failIf(self.metrics.connection is None)
        self.failIf(self.metrics.connection.transport is None)
        self.assertEqual(self.metrics.connection.port, 8125)

    def ensure_port_tmpdir(self):
        """Ensure that tmp dir exists."""
        tmpdir = utils.get_tmpdir()
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)

        return tmpdir


class TestMetricsAdapter(TestCase):
    """Test registering and lookup of zope metrics component."""

    def setUp(self):
        """Set up a fake MetricsConnector."""
        super(TestMetricsAdapter, self).setUp()

        self.orig_new_txmetrics = MetricsConnector.new_txmetrics
        # Mock out the new_txmetrics call
        MetricsConnector.new_txmetrics = classmethod(
            lambda _, connection=None, namespace="": "fake_" + namespace)

    def tearDown(self):
        """Reset new_metrics() back to its original value."""
        MetricsConnector.new_txmetrics = self.orig_new_txmetrics

    def test_lookup_without_registering(self):
        """If we don't register an instance, then lookup fails."""
        self.assertRaises(ComponentLookupError,
                          MetricsConnector.get_metrics, "one")

    def test_register_and_lookup(self):
        """Test that we can look up different instances and record metrics."""
        MetricsConnector.register_metrics("one")
        MetricsConnector.register_metrics("two")

        self.assertEqual("fake_one", MetricsConnector.get_metrics("one"))
        self.assertEqual("fake_two", MetricsConnector.get_metrics("two"))
        self.assertIdentical(MetricsConnector.get_metrics("one"),
                             MetricsConnector.get_metrics("one"))

        self.assertRaises(ComponentLookupError,
                          MetricsConnector.get_metrics, "three")

        MetricsConnector.unregister_metrics()
        self.assertRaises(ComponentLookupError,
                          MetricsConnector.get_metrics, "one")
        self.assertRaises(ComponentLookupError,
                          MetricsConnector.get_metrics, "two")

    def test_register_with_different_namespace(self):
        """Register an instance with an aribrary lookup key."""
        MetricsConnector.register_metrics("abc", "root.a.b.c")
        self.assertEqual("fake_root.a.b.c",
                         MetricsConnector.get_metrics("abc"))

    def test_register_fake_metrics(self):
        """Test that we can switch out a different metrics instance."""
        MetricsConnector.register_metrics("abc", instance="totally_fake")
        self.assertEqual("totally_fake", MetricsConnector.get_metrics("abc"))

    def test_register_mock_metrics(self):
        """Test that we can register a mock for testing."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        mock_metrics.meter("abc", 123)
        MetricsConnector.register_metrics("x", instance=mock_metrics)
        with mocker:
            MetricsConnector.get_metrics("x").meter("abc", 123)
