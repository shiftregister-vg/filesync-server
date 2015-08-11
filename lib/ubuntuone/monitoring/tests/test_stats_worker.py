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

"""Tests for the stats worker."""

import logging

from mocker import Mocker, expect
from supervisor import states
from twisted.trial.unittest import TestCase

from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.monitoring import stats_worker

RUNNING = states.ProcessStates.RUNNING
STARTING = states.ProcessStates.STARTING


class FakeData(object):
    """Data repository that holds it in attributes."""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __eq__(self, other):
        return self.__dict__ == other


class StatsWorkerTestCase(TestCase):
    """Tests for StatsWorker class."""

    def setUp(self):
        super(StatsWorkerTestCase, self).setUp()
        self.mocker = Mocker()
        self.rpc = self.mocker.mock()
        self.worker = stats_worker.StatsWorker(10, '', self.rpc)

        # logging setup
        self.handler = MementoHandler()
        self.worker.logger.addHandler(self.handler)
        self.addCleanup(self.worker.logger.removeHandler, self.handler)
        self.worker.logger.setLevel(logging.DEBUG)
        self.handler.setLevel(logging.DEBUG)
        self.worker.logger.propagate = False
        self.handler.debug = True

    def test_collect_stats(self):
        """Test the collect_stats method."""
        called = []
        self.worker._collect_process = \
            lambda p, n: called.append(('proc', p, n)) or {}
        self.worker._collect_machine = lambda: called.append('machine') or {}
        processes = [dict(name="bar", group="foo", pid="42", state=RUNNING)]
        expect(self.rpc.supervisor.getAllProcessInfo()).result(processes)
        with self.mocker:
            self.worker.collect_stats()
        self.assertEqual(called, ['machine', ('proc', 42, 'bar')])
        self.assertTrue(self.handler.check_info("Collecting machine stats"))
        self.assertTrue(self.handler.check_info("Collecting stats for proc",
                                                "pid=42", "name=bar"))

    def test_collect_stats_not_running(self):
        """Test the collect_stats method if the proccess isn't running."""
        called = []
        self.worker._collect_process = \
            lambda p, n: called.append(('proc', p, n)) or {}
        self.worker._collect_machine = lambda: called.append('machine') or {}
        processes = [dict(name="bar", group="foo", pid="42", state=STARTING)]
        expect(self.rpc.supervisor.getAllProcessInfo()).result(processes)
        with self.mocker:
            self.worker.collect_stats()
        self.assertEqual(called, ['machine'])
        self.assertTrue(self.handler.check_info("Collecting machine stats"))
        self.assertTrue(self.handler.check_info("Ignoring process",
                                                "pid=42", "name=bar",
                                                "state=%s" % STARTING))

    def test_collect_stats_no_data(self):
        """Test the collect_stats method with no data of a process."""
        called = []
        self.worker._collect_process = \
            lambda p, n: called.append(('proc', p, n)) or {}
        self.worker._collect_machine = lambda: called.append('machine') or {}
        expect(self.rpc.supervisor.getAllProcessInfo()).result([])
        with self.mocker:
            self.worker.collect_stats()
        self.assertEqual(called, ['machine'])
        self.assertTrue(self.handler.check_info("Collecting machine stats"))

    def test_collect_process_info_new_report(self):
        """Check how the process info is collected first time."""
        mocker = Mocker()
        assert not self.worker.process_cache

        # patch Process to return our mock for test pid
        Process = mocker.mock()
        self.patch(stats_worker.psutil, 'Process', Process)
        proc = mocker.mock()
        pid = 1234
        expect(Process(pid)).result(proc)

        # patch ProcessReport to return or mock for given proc
        ProcessReport = mocker.mock()
        self.patch(stats_worker, 'ProcessReport', ProcessReport)
        proc_report = mocker.mock()
        expect(ProcessReport(proc)).result(proc_report)

        # expect to get called with some info, return some results
        name = 'test_proc'
        result = object()
        expect(proc_report.get_memory_and_cpu(prefix=name)).result(result)

        with mocker:
            real = self.worker._collect_process(pid, name)
        self.assertIdentical(real, result)

    def test_collect_process_info_old_report(self):
        """Check how the process info is collected when cached."""
        mocker = Mocker()

        # put it in the cache
        pid = 1234
        proc_report = mocker.mock()
        self.worker.process_cache[pid] = proc_report

        # expect to get called with some info, return some results
        name = 'test_proc'
        result = object()
        expect(proc_report.get_memory_and_cpu(prefix=name)).result(result)

        with mocker:
            real = self.worker._collect_process(pid, name)
        self.assertIdentical(real, result)

    def test_collect_system_info(self):
        """Check how the system info is collected."""
        mocker = Mocker()

        # change the constant to assure it's used as we want
        result1 = dict(a=3, b=5)
        result2 = dict(c=7)
        fake = (lambda: result1, lambda: result2)
        self.patch(stats_worker, 'SYSTEM_STATS', fake)

        with mocker:
            result = self.worker._collect_machine()

        should = {}
        should.update(result1)
        should.update(result2)
        self.assertEqual(result, should)

    def test_informed_metrics(self):
        """Check how stats are reported."""
        # prepare a lot of fake info that will be "collected"
        machine_info = dict(foo=3, bar=5)
        process_info = {
            1: dict(some=1234, other=4567),
            2: dict(some=9876, other=6543),
        }
        self.worker._collect_process = lambda pid, name: process_info[pid]
        self.worker._collect_machine = lambda: machine_info
        processes = [
            dict(name="proc1", group="", pid="1", state=RUNNING),
            dict(name="proc2", group="", pid="2", state=RUNNING),
        ]
        expect(self.rpc.supervisor.getAllProcessInfo()).result(processes)

        # patch the metric reporter to see what is sent
        reported = set()
        self.worker.metrics.gauge = lambda *a: reported.add(a)

        # what we should get is...
        should = set([
            ('foo', 3),
            ('bar', 5),
            ('some', 1234),
            ('other', 4567),
            ('some', 9876),
            ('other', 6543),
        ])
        with self.mocker:
            self.worker.collect_stats()
        self.assertEqual(reported, should)
