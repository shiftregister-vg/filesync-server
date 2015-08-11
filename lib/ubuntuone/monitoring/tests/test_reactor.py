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

"""Tests for the ReactorInspector."""

import time
import logging
import threading

from twisted.trial.unittest import TestCase as TwistedTestCase
from twisted.internet import reactor, defer

from metrics.metricsconnector import MetricsConnector

from ubuntuone.storage.server.logger import TRACE
from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.monitoring.reactor import ReactorInspector


class ReactorInspectorTestCase(TwistedTestCase):
    """Test the ReactorInspector class."""

    def setUp(self):
        """Set up."""
        class Helper(object):
            """Fake object with a controllable call."""
            def __init__(self):
                self.call_count = 1
                self.calls = []
                self.ri = None

            def call(self, func):
                """Call function when counter is 0, then stop running."""
                self.call_count -= 1
                self.calls.append(func)
                if self.call_count == 0:
                    for f in self.calls:
                        f()
                if self.call_count <= 0:
                    self.ri.stop()

        class FakeMetrics(object):
            """Fake Metrics object that records calls."""
            def __init__(self):
                """Initialize calls."""
                self.calls = []

            def meter(self, name, count):
                """Record call to meter()."""
                self.calls.append(("meter", name, count))

            def gauge(self, name, val):
                """Record call to gauge()."""
                self.calls.append(("gauge", name, val))

        logger = logging.getLogger("storage.server")
        logger.propagate = False
        logger.setLevel(TRACE)
        self.handler = MementoHandler()
        self.handler.setLevel(TRACE)
        logger.addHandler(self.handler)
        self.addCleanup(logger.removeHandler, self.handler)
        self.helper = Helper()
        self.fake_metrics = FakeMetrics()
        MetricsConnector.register_metrics("reactor_inspector",
                                          instance=self.fake_metrics)
        self.addCleanup(MetricsConnector.unregister_metrics)
        self.ri = ReactorInspector(logger, self.helper.call, loop_time=.1)
        self.helper.ri = self.ri

    def run_ri(self, call_count=None, join=True):
        """Set the call count and then run the ReactorInspector."""
        if call_count is not None:
            self.helper.call_count = call_count
        self.start_ts = time.time()
        self.ri.start()
        # Reactor will stop after call_count calls, thanks to helper
        if join:
            self.ri.join()

    def test_stop(self):
        """It stops."""
        self.run_ri(1000, join=False)
        assert self.ri.is_alive()
        self.ri.stop()
        self.ri.join()
        self.assertFalse(self.ri.is_alive())

    @defer.inlineCallbacks
    def test_dump_frames(self):
        """Test how frames are dumped.

        Rules:
        - own frame must not be logged
        - must log all other threads
        - main reactor thread must have special title
        """
        # other thread, whose frame must be logged
        waitingd = defer.Deferred()

        def waiting_function():
            """Function with funny name to be checked later."""
            reactor.callFromThread(waitingd.callback, True)
            # wait have a default value
            event.wait()

        event = threading.Event()
        threading.Thread(target=waiting_function).start()
        # Make sure the thread has entered the waiting_function
        yield waitingd

        # Set reactor_thread since we're not starting the ReactorInspector
        # thread here.
        self.ri.reactor_thread = threading.currentThread().ident

        # dump frames in other thread, also
        def dumping_function():
            """Function with funny name to be checked later."""
            time.sleep(.1)
            self.ri.dump_frames()
            reactor.callFromThread(d.callback, True)

        d = defer.Deferred()
        threading.Thread(target=dumping_function).start()
        yield d
        event.set()

        # check
        self.assertFalse(self.handler.check_debug("dumping_function"))
        self.assertTrue(self.handler.check_debug("Dumping Python frame",
                                                 "waiting_function"))
        self.assertTrue(self.handler.check_debug("Dumping Python frame",
                                                 "reactor main thread"))

    def test_reactor_ok(self):
        """Reactor working fast."""
        self.run_ri()
        ok_line = self.handler.check(TRACE, "ReactorInspector: ok")
        self.assertTrue(ok_line)
        self.assertTrue(ok_line.args[-1] >= 0)  # Should be near zero delay
        # Check the metrics
        expected_metric = ("gauge", "delay", ok_line.args[-1])
        self.assertEqual([expected_metric], self.fake_metrics.calls)
        self.assertTrue(self.ri.last_responsive_ts >= self.start_ts)

    @defer.inlineCallbacks
    def test_reactor_blocked(self):
        """Reactor not working fast."""
        dump_frames_called = defer.Deferred()
        self.ri.dump_frames = lambda: dump_frames_called.callback(True)
        self.run_ri(0)
        yield dump_frames_called
        log_line = self.handler.check(logging.CRITICAL, "ReactorInspector",
                                      "detected unresponsive")
        self.assertTrue(log_line)
        self.assertTrue(log_line.args[-1] >= .1)  # waited for entire loop time
        # Check the metrics
        expected_metric = ("gauge", "delay", log_line.args[-1])
        self.assertEqual([expected_metric], self.fake_metrics.calls)

        self.assertTrue(self.ri.last_responsive_ts < self.start_ts)

    def test_reactor_back_alive(self):
        """Reactor resurrects after some loops."""
        self.run_ri(3)
        late_line = self.handler.check_warning("ReactorInspector: late",
                                               "got: 0")
        self.assertTrue(late_line)
        self.assertTrue(late_line.args[-1] >= .2)  # At least 2 cycles of delay
        # Check the metrics
        expected_metric = ("gauge", "delay", late_line.args[-1])
        self.assertEqual(expected_metric, self.fake_metrics.calls[-1])

        self.assertTrue(self.ri.queue.empty())
        # A late reactor is not considered responsive (until a successful loop)
        self.assertTrue(self.ri.last_responsive_ts < self.start_ts)
