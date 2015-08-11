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

"""Tests for the stats helpers."""

import mocker

from twisted.internet import defer, task

from metrics.metricsconnector import MetricsConnector
from s3lib import producers
from s3lib.contrib import http_client
from ubuntuone.storage.server import upload
from ubuntuone.storage.server.stats import StatsWorker
from ubuntuone.storage.server.testing.testcase import TestWithDatabase


class FakeMetrics(object):
    """Fake Metrics object that records calls."""
    connection = None

    def __init__(self):
        """Initialize calls."""
        self.calls = []

    def meter(self, name, count):
        """Record call to meter()."""
        self.calls.append(("meter", name, count))

    def gauge(self, name, value):
        """Record call to gauge()."""
        self.calls.append(("gauge", name, value))


class TestStats(TestWithDatabase):
    """Test stats stuff."""

    @defer.inlineCallbacks
    def setUp(self):
        """Setup the test."""
        yield super(TestStats, self).setUp()
        self.obj_factory.make_user(999, u'test user999', u'', 2 ** 20)
        self.metrics = FakeMetrics()
        MetricsConnector.register_metrics("root", instance=self.metrics)
        self.service.stats_worker.stop()

    def tearDown(self):
        """Tear down the test."""
        # restore the original instance
        MetricsConnector.register_metrics(
            "root", instance=self.service.factory.metrics)
        return super(TestStats, self).tearDown()

    def test_stats_loop(self):
        """Test that the StatsWorker loop works as expected."""
        stats_worker = StatsWorker(self.service, 2)
        clock = task.Clock()
        stats_worker.callLater = clock.callLater
        failure = defer.failure.Failure(ValueError('error!'))
        stats_worker.deferToThread = lambda _: defer.fail(failure)
        stats_worker.start()
        clock.advance(1)
        delayed_calls = clock.getDelayedCalls()
        self.assertEqual(len(delayed_calls), 1)
        self.assertIn(stats_worker.next_loop, delayed_calls)
        stats_worker.stop()

    def test_runtime_info(self):
        """Make sure we add runtime info."""
        stats_worker = StatsWorker(self.service, 10)
        # get the reactor
        from twisted.internet import reactor
        stats_worker.runtime_info()
        # check the reactor data
        self.assertIn(('gauge', 'reactor.readers',
                       len(reactor.getReaders())), self.metrics.calls)
        self.assertIn(('gauge', 'reactor.writers',
                       len(reactor.getWriters())), self.metrics.calls)

    def test_buffers_runtime_info(self):
        """Make sure we send the buffers size info."""
        stats_worker = StatsWorker(self.service, 10)
        # add some info the all the buffers we know
        mock = mocker.Mocker()
        producer = mock.mock()
        upload.MultipartUploadFactory.buffers_size = 0
        mp = upload.MultipartUploadFactory(None, producer, 1024, 2048,
                                           0, 1042, 10)
        mp.dataReceived('a' * 10)
        client = mock.mock()
        http_client.HTTPProducer.buffers_size = 0
        http_prod = http_client.HTTPProducer(client)
        self.addCleanup(http_prod.timeout_call.cancel)
        http_prod.dataReceived('a' * 20)
        producers.S3Producer.buffers_size = 0
        s3_prod = producers.S3Producer(1024)
        s3_prod.dataReceived('a' * 30)

        stats_worker.runtime_info()
        self.assertIn(('gauge', 'buffers_size.HTTPProducer', 20),
                      self.metrics.calls)
        self.assertIn(('gauge', 'buffers_size.MultipartUploadFactory', 10),
                      self.metrics.calls)
        self.assertIn(('gauge', 'buffers_size.S3Producer', 30),
                      self.metrics.calls)
