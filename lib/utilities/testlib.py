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

"""Library of functions used in tests."""


import os
import re
import unittest
import trace

from twisted.internet.base import DelayedCall
from twisted.python.components import registerAdapter
from twisted.trial.itrial import ITestCase
from twisted.trial.reporter import (
    TreeReporter, TestResultDecorator, SubunitReporter)
from twisted.trial.runner import TrialRunner
from twisted.trial.unittest import TestDecorator

import testresources

COVERAGE_DIR = "./tmp/coverage"
WORKING_DIR = "./tmp/trial"

import logging
logging.setLoggerClass(logging.Logger)


class UnsortedOptimisingTestSuite(testresources.OptimisingTestSuite):
    """A different kind of sort."""
    def sortTests(self):
        """Sort the tests by .id() to ensure test order determinism."""
        # remove the runTest spurious entries as well
        result = [t for t in self._tests if not t.id().endswith(".runTest")]
        result.sort(key=lambda t: t.id())
        self._tests = result


class StopOnFailureDecorator(TestResultDecorator):
    """TestResult decorator that stop the test on the first failure/error."""

    def addFailure(self, test, fail):
        """
        Forward the failure to TreeReporter and stop the test if
        stop_early is True
        """
        self._originalReporter.addFailure(test, fail)
        self._originalReporter.stop()

    def addError(self, test, error):
        """
        Forward the error to TreeReporter and stop the test if
        stop_early is True
        """
        self._originalReporter.addError(test, error)
        self._originalReporter.stop()


class LogsOnFailureDecorator(TestResultDecorator):
    """Show some logs to stdout on failure/error."""

    _logs_to_show = [
        'tmp/filesync-server-tests.log',
        'tmp/xdg_cache/ubuntuone/log/syncdaemon.log',
    ]

    def __init__(self, *a, **k):
        self._logs_positions = {}
        cwd = os.getcwd()
        self._abspath_logs = [os.path.join(cwd, l) for l in self._logs_to_show]
        TestResultDecorator.__init__(self, *a, **k)

    def startTest(self, test):
        """Record the logs positions when the test starts."""
        for log in self._abspath_logs:
            if os.path.exists(log):
                self._logs_positions[log] = os.stat(log).st_size
            else:
                self._logs_positions[log] = 0
        TestResultDecorator.startTest(self, test)

    def _show_the_logs(self):
        """Show the logs to stdout since the failed test started."""
        for log in self._abspath_logs:
            prev_pos = self._logs_positions[log]
            print "\n-------- Dumping log:", repr(log)
            if os.path.exists(log):
                with open(log) as fh:
                    fh.seek(prev_pos)
                    print fh.read(),
                print "------------ end log:", repr(log)
            else:
                print "------------ log not found!"

    def addFailure(self, test, fail):
        """Show the log and forward the failure."""
        self._show_the_logs()
        TestResultDecorator.addFailure(self, test, fail)

    def addError(self, test, error):
        """Show the log and forward the error."""
        self._show_the_logs()
        TestResultDecorator.addError(self, test, error)


class ResourcedTestCaseAdapter(TestDecorator):
    """An `ITestCase` adapter for `ResourcedTestCase` instances.

    This is required in order for `OptimisingTestSuite` to function
    correctly with the Trial test runner.  See
    U{http://twistedmatrix.com/trac/ticket/3535} for details.
    """
    @property
    def resources(self):
        """Forward the resources attribute from the original test case."""
        return self._originalTest.resources


registerAdapter(
    ResourcedTestCaseAdapter, testresources.ResourcedTestCase, ITestCase)


def collect_tests(topdir, testdirs, testpaths=()):
    """Collect the tests to run into a single test suite.

    @param testpaths: If provided, only tests in the given sequence will
                      be considered.  If not provided, all tests are
                      considered.
    @return: a TestSuite instance containing all the test cases.
    """

    # convert symlinks to real directory names that will be found by
    # os.walk() later on
    _testpaths = set()
    for tp in testpaths:
        rp = tp
        if os.path.exists(tp):
            rp = os.path.realpath(tp)
            if rp.startswith(topdir):
                rp = rp[len(topdir) + 1:]
        if tp != rp:
            print "WARNING: using %s instead of %s" % (rp, tp)
        _testpaths.add(rp)

    suite = unittest.TestSuite()
    for testdir in testdirs:
        suite.addTest(collect_tests_inner(testdir, testpaths, topdir))

    return suite


def load_unittest(relpath):
    """Load unit tests from a Python module with the given relative path."""
    assert relpath.endswith(".py"), (
        "%s does not appear to be a Python module" % relpath)
    modpath = relpath.replace(os.path.sep, ".")[:-3]
    module = __import__(modpath, None, None, [""])

    # If the module has a 'suite' or 'test_suite' function, use that
    # to load the tests.
    if hasattr(module, "suite"):
        return module.suite()
    elif hasattr(module, "test_suite"):
        return module.test_suite()
    return unittest.defaultTestLoader.loadTestsFromModule(module)


def collect_tests_inner(testdir, testpaths, topdir):
    """Helper for collect_tests; searches a particular testdir for tests.

    @param testdir: The directory to search for tests.
    @param testpaths: If provided, only tests in this sequence will
                      be considered.  If not provided, all tests are
                      considered.
    @param topdir: the top-level source directory
    @return: {unittest.TestSuite} will all the tests
    """
    suite = unittest.TestSuite()
    follow_links = os.environ.get('TESTING_FOLLOW_LINKS')
    follow_links = follow_links is not None

    for root, dirnames, filenames in os.walk(testdir,
                                             followlinks=follow_links):
        # Only process files found within directories named "tests".
        if not os.path.basename(root).endswith('tests'):
            continue

        for filename in filenames:
            filepath = os.path.join(root, filename)
            relpath = filepath[len(testdir) + 1:]

            if testpaths:
                top_relpath = os.path.abspath(filepath)[len(topdir) + 1:]
                # Skip any tests not in testpaths.
                for testpath in testpaths:
                    if top_relpath.startswith(testpath):
                        break
                else:
                    continue

            if filename.endswith(".py"):
                if filename.startswith("test_"):
                    suite.addTest(load_unittest(relpath))

    return suite


def flatten_suite(source_suite, target_suite):
    """Flatten test cases into test suites."""
    for case in source_suite:
        if isinstance(case, unittest.TestSuite):
            flatten_suite(case, target_suite)
        else:
            target_suite.addTest(case)


def test_with_trial(options, topdir, testdirs, testpaths):
    """The main testing entry point."""
    # parse arguments

    # Ensure that the database watcher is installed early, so that
    # unexpected database access can be blocked.
    from backends.testing.resources import DatabaseResource
    watcher = DatabaseResource.get_watcher()
    watcher.enable('account')

    reporter_decorators = []
    if options.one:
        reporter_decorators.append(StopOnFailureDecorator)
    if options.logs_on_failure:
        reporter_decorators.append(LogsOnFailureDecorator)

    def factory(*args, **kwargs):
        """Custom factory tha apply the decorators to the TreeReporter"""
        if options.subunit:
            return SubunitReporter(*args, **kwargs)
        else:
            result = TreeReporter(*args, **kwargs)
            for decorator in reporter_decorators:
                result = decorator(result)
            return result
    reporterFactory = factory
    runner = TrialRunner(reporterFactory=reporterFactory,
                         realTimeErrors=True, workingDirectory=WORKING_DIR)

    suite = UnsortedOptimisingTestSuite()
    suite.adsorbSuite(collect_tests(topdir, testdirs, testpaths))

    if options.test:
        old_suite = suite
        suite = UnsortedOptimisingTestSuite()
        pattern = re.compile('.*%s.*' % options.test)
        for test in iter(old_suite):
            if pattern.match(test.id()):
                suite.addTest(test)

    if options.ignore:
        old_suite = suite
        suite = UnsortedOptimisingTestSuite()
        pattern = re.compile('.*%s.*' % options.ignore)
        for test in iter(old_suite):
            if not pattern.match(test.id()):
                suite.addTest(test)

    if options.loops > 1:
        # we could loop the .run() call, but creating a suite that contains
        # duplicate tests is more efficient and more likely to expose
        # test-to-test issues. Plus, the testrun summary reports are nicer
        # this way --gafton
        old_suite = suite
        suite = UnsortedOptimisingTestSuite()
        for x in xrange(options.loops):
            suite.addTest(old_suite)

    if options.debug:
        DelayedCall.debug = True

    watcher.disable('account')
    if options.coverage:
        tracer = trace.Trace(trace=False, count=True)
        tracer.runctx('runner.run(suite)', globals=globals(), locals=vars())
        r = tracer.results()
        if not os.path.exists(COVERAGE_DIR):
            os.mkdir(COVERAGE_DIR)
        r.write_results(show_missing=True, summary=True, coverdir=COVERAGE_DIR)
    else:
        result = runner.run(suite)
        return not result.wasSuccessful()
