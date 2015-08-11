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

"""Process that inform metrics about other processes and the machine."""

from __future__ import with_statement

import os
import time
import logging
import platform

import psutil

from supervisor import childutils, states

from metrics.metricsconnector import MetricsConnector
from txstatsd.process import (
    report_file_stats, report_counters, report_system_stats,
    parse_meminfo, parse_loadavg, parse_netdev, ProcessReport)

from ubuntuone.storage.server.logger import configure_logger

from config import config


SYSTEM_STATS = (
    report_file_stats("/proc/meminfo", parse_meminfo),
    report_file_stats("/proc/loadavg", parse_loadavg),
    report_counters(report_file_stats("/proc/net/dev", parse_netdev)),
    report_counters(report_system_stats, percpu=True),
)


class StatsWorker(object):
    """Get stats from different processes and the machine."""

    def __init__(self, interval, namespace_prefix, supervisor_rpc):
        self.interval = interval
        self.rpc = supervisor_rpc
        self.process_cache = {}

        self.metrics = MetricsConnector.new_metrics(
            namespace="%s.resources" % (namespace_prefix))
        self.logger = logging.getLogger("resources")

    def run_forever(self):
        """Infinite loop to do the work."""
        while True:
            tini = time.time()
            try:
                self.collect_stats()
            except:
                self.logger.exception("Problem collecting stats!")

            elapsed = time.time() - tini
            to_wait = self.interval - elapsed
            if to_wait > 0:
                time.sleep(to_wait)

    def _collect_machine(self):
        """Collect stats for the machine."""
        d = {}
        for report in SYSTEM_STATS:
            result = report()
            d.update(result)
        return d

    def _collect_process(self, pid, name):
        """Collect stats for the process with the given pid."""
        try:
            proc_report = self.process_cache[pid]
        except KeyError:
            proc = psutil.Process(pid)
            proc_report = ProcessReport(proc)
            self.process_cache[pid] = proc_report

        result = proc_report.get_memory_and_cpu(prefix=name)
        return result

    def collect_stats(self):
        """Collect stats from all processes and the machine itself."""
        self.logger.info("Collecting machine stats")
        info = self._collect_machine()
        for key, value in info.iteritems():
            self.metrics.gauge(key, value)

        infos = self.rpc.supervisor.getAllProcessInfo()
        self.logger.info("Found %d processes", len(infos))

        for info in infos:
            pid = info['pid']
            name = info['name']
            state = info['state']
            if not pid or state != states.ProcessStates.RUNNING:
                # nothing to check, as there is no pid yet for this process
                self.logger.info("Ignoring process (pid=%s, name=%s, "
                                 "state=%s)", pid, name, state)
                continue

            self.logger.info("Collecting stats for proc: pid=%s, name=%s",
                             pid, name)
            info = self._collect_process(int(pid), name)
            for key, value in info.iteritems():
                self.metrics.gauge(key, value)


default_config = """
[__main__]
interval.default = 60
interval.parser = int
interval.help = Interval to check for all resources info.
metric_namespace_prefix.default =
metric_namespace_prefix.help = namespace prefix to be used in the metric.
log_file.default = tmp/stats_worker.log
log_file.help = The path to the log file.
log_level.default = INFO
log_level.help = The logging level (DEBUG, INFO, WARNING, ERROR)
log_format.default = %(asctime)s %(levelname)-8s %(name)s: %(message)s
log_format.help = The logging format
"""


def _comma_separated_list_parser(value):
    """A configglue parser for comma separated list."""
    if not value:
        return []
    return value.split(",")


if __name__ == '__main__':
    import sys
    from StringIO import StringIO
    from configglue import configglue
    config_file = StringIO()
    config_file.write(default_config)
    config_file.seek(0)

    (parser, options, argv) = configglue(
        config_file,
        args=sys.argv[1:],
        usage="%prog [options]",
        extra_parsers=[("comma_separated_list", _comma_separated_list_parser)])

    # configure the logger
    try:
        level = logging._levelNames[options.log_level]
    except KeyError:
        # if level doesn't exist fallback to DEBUG
        level = logging.DEBUG

    configure_logger(name="resources", filename=options.log_file,
                     level=level, log_format=options.log_format,
                     propagate=False)

    if not options.metric_namespace_prefix:
        options.metric_namespace_prefix = "%s.%s" % (
            config.general.environment_name, platform.node())

    # start the process
    rpc = childutils.getRPCInterface(os.environ)
    sw = StatsWorker(options.interval, options.metric_namespace_prefix, rpc)
    sw.run_forever()
