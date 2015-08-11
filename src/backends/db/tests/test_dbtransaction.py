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

"""Tests for dbtransaction."""

import threading
import unittest
import logging

import mocker
import transaction

from mock import patch
from psycopg2.extensions import TransactionRollbackError
from psycopg2 import InternalError, IntegrityError
from storm import tracer
from storm.exceptions import DisconnectionError, TimeoutError, ProgrammingError
from transaction.interfaces import IDataManager
from zope.interface import implements
from zope.interface.verify import verifyObject

from ubuntuone.devtools.handlers import MementoHandler

from backends.testing import testcase

from backends.db import errors

from backends.db.store import get_store
from backends.db.dbtransaction import (
    _check_stores_and_invalidate,
    retryable_transaction,
    get_storm_commit,
    get_storm_readonly,
    RetryLimitReached,
    TransactionTimer,
    disable_timeout_tracer,
    enable_timeout_tracer,
    current_timeout_tracer,
    db_timeout,
    timer,
    StorageTimeoutTracer,
    on_timeout,
    storage_zstorm
)

storm_commit = get_storm_commit(transaction)
storm_readonly = get_storm_readonly(transaction)
storm_readonly_ro_store = get_storm_readonly(transaction, use_ro_store=True)


class LoggingDataManager(object):
    """An IDataManager implementation that logs calls."""
    implements(IDataManager)

    def __init__(self, transaction_manager):
        super(LoggingDataManager, self).__init__()
        self.transaction_manager = transaction_manager
        self.calls = []

    def abort(self, txn):
        """See IDataManager."""
        self.calls.append('abort')

    def tpc_begin(self, txn):
        """See IDataManager."""
        self.calls.append('tpc_begin')

    def commit(self, txn):
        """See IDataManager."""
        self.calls.append('commit')

    def tpc_vote(self, txn):
        """See IDataManager."""
        self.calls.append('tpc_vote')

    def tpc_finish(self, txn):
        """See IDataManager."""
        self.calls.append('tpc_finish')

    def tpc_abort(self, txn):
        """See IDataManager."""
        self.calls.append('tpc_abort')

    def sortKey(self):
        """See IDataManager."""
        return '%s:%d' % (self.__class__.__name__, id(self))


class DecoratorTests(mocker.MockerTestCase):
    """Tests for the transaction decorators."""

    def setUp(self):
        super(DecoratorTests, self).setUp()
        transaction.begin()

    def tearDown(self):
        transaction.abort()
        super(DecoratorTests, self).tearDown()

    def test_LoggingDataManager(self):
        """The LoggingDataManager class implements IDataManager correctly."""
        data_manager = LoggingDataManager(transaction)
        verifyObject(IDataManager, data_manager)

    def test_storm_commit(self):
        """storm_commit commits on success."""
        data_manager = LoggingDataManager(transaction)

        @storm_commit
        def function():
            """Join the transaction."""
            transaction.get().join(data_manager)
        function()
        self.assertEqual(function.__doc__, "Join the transaction.")
        self.assertEqual(data_manager.calls,
                         ['tpc_begin', 'commit', 'tpc_vote', 'tpc_finish'])

    def test_storm_commit_failure(self):
        """storm_commit aborts on failure."""
        data_manager = LoggingDataManager(transaction)

        @storm_commit
        def function():
            """Join the transaction then fail with RuntimeError."""
            transaction.get().join(data_manager)
            raise RuntimeError
        self.assertRaises(RuntimeError, function)
        self.assertEqual(data_manager.calls, ['abort'])

    def test_storm_commit_integrityerror(self):
        """This postgresql exception is translated to one of our own."""
        data_manager = LoggingDataManager(transaction)

        @storm_commit
        def function():
            """Join the transaction then fail."""
            transaction.get().join(data_manager)
            raise IntegrityError()
        self.assertRaises(errors.IntegrityError, function)
        self.assertEqual(data_manager.calls, ['abort'])

    def test_storm_readonly(self):
        """storm_readonly aborts on failure."""
        data_manager = LoggingDataManager(transaction)

        @storm_readonly
        def function():
            """Join the transaction."""
            transaction.get().join(data_manager)
        function()
        self.assertEqual(function.__doc__, "Join the transaction.")
        self.assertEqual(data_manager.calls, ['abort'])

    def test_storm_readonly_failure(self):
        """storm_readonly aborts on failure."""
        data_manager = LoggingDataManager(transaction)

        @storm_readonly
        def function():
            """Join the transaction then fail with RuntimeError."""
            transaction.get().join(data_manager)
            raise RuntimeError
        self.assertRaises(RuntimeError, function)
        self.assertEqual(data_manager.calls, ['abort'])

    def test_storm_readonly_use_ro_store(self):
        """storm_readonly set_ro_store decorator."""

        @storm_readonly
        def function1():
            """Join the transaction then fail with RuntimeError."""
            txn = transaction.get()
            self.assertFalse(txn.use_ro_store)
            return True
        self.assertTrue(function1())

        @storm_readonly_ro_store
        def function2():
            """Join the transaction then fail with RuntimeError."""
            txn = transaction.get()
            self.assertTrue(txn.use_ro_store)
            return True
        self.assertTrue(function2())

    def test_storm_readonly_single_DisconnectionError(self):
        """storm_readonly retries on DisconnectionError."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(2)
        self.mocker.replay()

        calls = []

        @storm_readonly
        def function():
            """Fail with DisconnectionError once."""
            calls.append('function')
            if len(calls) < 2:
                raise DisconnectionError
        function()
        self.assertEqual(calls, ['function'] * 2)

    def test_retryable_transaction(self):
        """retryable_transaction runs the function once."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(1)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Test function that succeeds."""
            calls.append('function')
        function()
        self.assertEqual(calls, ['function'])

    def test_retryable_transaction_exception(self):
        """retryable_transaction passes through exceptions."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(1)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Fail with RuntimeError."""
            calls.append('function')
            raise RuntimeError
        self.assertRaises(RuntimeError, function)
        self.assertEqual(calls, ['function'])

    def test_retryable_transaction_single_DisconnectionError(self):
        """retryable_transaction retries on DisconnectionError."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(2)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Fail with DisconnectionError once."""
            calls.append('function')
            if len(calls) < 2:
                raise DisconnectionError
        function()
        self.assertEqual(calls, ['function'] * 2)

    def test_retryable_transaction_single_TransactionRollbackError(self):
        """retryable_transaction retries on TransactionRollbackError."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(2)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Fail with TransactionRollbackError once."""
            calls.append('function')
            if len(calls) < 2:
                raise TransactionRollbackError
        function()
        self.assertEqual(calls, ['function'] * 2)

    def test_retryable_transaction_does_not_retry_TimeoutError(self):
        """retryable_transaction does not retry on TimeoutError."""
        calls = []

        @retryable_transaction()
        def function():
            """Fail with TimeoutError once."""
            calls.append('function')
            if len(calls) < 2:
                raise TimeoutError("fake-message", "fake statement", 'args')
        self.assertRaises(TimeoutError, function)

    def test_retryable_transaction_max_retries(self):
        """retryable_transaction fails on too many retries."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(3)
        self.mocker.replay()

        calls = []

        @retryable_transaction(max_retries=3)
        def function():
            """Fail with TransactionRollbackError."""
            calls.append('function')
            raise TransactionRollbackError("message")
        try:
            function()
        except RetryLimitReached, exc:
            # The original exception message is preserved.
            self.assertEqual(
                exc.args[0],
                "Maximum retries (3) reached. Please try again. "
                "(Original error: TransactionRollbackError: message)")
            self.assertEqual(exc.extra_info,
                             "TransactionRollbackError: message")
            self.assertEqual(str(exc),
                             "Maximum retries (3) reached. Please try again. "
                             "(Original error: TransactionRollbackError: "
                             "message)")

        else:
            self.fail("RetryLimitReached exception not raised.")
        self.assertEqual(calls, ['function'] * 3)

    def test_retryable_transaction_default_max_retries(self):
        """retryable_transaction defaults to 3 retries."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(3)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Fail with TransactionRollbackError."""
            calls.append('function')
            raise TransactionRollbackError
        self.assertRaises(RetryLimitReached, function)
        self.assertEqual(calls, ['function'] * 3)

    def test_retryable_transaction_original_type(self):
        """Retry failure keeps original type as well."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(3)
        self.mocker.replay()

        calls = []

        @retryable_transaction()
        def function():
            """Fail with TransactionRollbackError."""
            calls.append('function')
            raise TransactionRollbackError
        self.assertRaises(TransactionRollbackError, function)

    def test_retryable_transaction_InternalError(self):
        """Retryable_transaction retry and log on InternalError."""
        sleep = self.mocker.replace('time.sleep')
        self.expect(sleep(mocker.ANY)).count(2)
        self.mocker.replay()

        logger = logging.getLogger('storage.server.txn')
        h = MementoHandler()
        logger.addHandler(h)

        calls = []

        @retryable_transaction()
        def function():
            """Fail with InternalError."""
            if len(calls) < 1:
                calls.append('function')
                raise InternalError('internal error')
            else:
                calls.append('function')
        function()
        logger.removeHandler(h)
        self.assertEqual(calls, ['function'] * 2)
        self.assertEqual(1, len(h.records))
        self.assertIn(
            'Got an InternalError, retrying', h.records[0].getMessage())
        self.assertEqual('internal error', h.records[0].exc_info[1].message)


class TransactionTimerTests(unittest.TestCase):
    """Tests for the TransactionTimer class."""

    def test_TransactionTimer(self):
        """Test basic interface of TransactionTimer."""
        transaction_timer = TransactionTimer()
        self.assertEqual(transaction_timer.get_transaction_time(), None)
        transaction_timer.start_transaction()
        self.assertNotEqual(transaction_timer.get_transaction_time(), None)
        transaction_timer.end_transaction()
        self.assertEqual(transaction_timer.get_transaction_time(), None)

    def test_thread_isolation(self):
        """Different threads do not interfere with each other."""
        transaction_timer = TransactionTimer()
        event1 = threading.Event()
        event2 = threading.Event()

        def worker():
            """Start and finish a transaction in a thread."""
            self.assertEqual(transaction_timer.get_transaction_time(), None)
            transaction_timer.start_transaction()
            event1.set()
            event2.wait()
            self.assertNotEqual(transaction_timer.get_transaction_time(), None)
            transaction_timer.end_transaction()

        transaction_timer.start_transaction()
        thread = threading.Thread(target=worker)
        thread.start()
        event1.wait()
        transaction_timer.end_transaction()
        event2.set()
        thread.join()


class TimeoutTracerTests(mocker.MockerTestCase):
    """Tests for the timeout tracer."""

    def tearDown(self):
        disable_timeout_tracer()
        timer.end_transaction()
        super(TimeoutTracerTests, self).tearDown()

    def test_enable_timeout_tracer(self):
        """enable_timeout_tracer adds the tracer."""
        enable_timeout_tracer()
        timeout_tracers = [t for t in tracer.get_tracers()
                           if isinstance(t, StorageTimeoutTracer)]
        self.assertNotEqual(timeout_tracers, [])
        self.assertEqual(timeout_tracers[0].max_time, 20)

    def test_enable_timeout_tracer_max_time(self):
        """The max_time argument is passed to the tracer instance."""
        enable_timeout_tracer(max_time=42)
        timeout_tracer = current_timeout_tracer()
        self.assertEqual(timeout_tracer.max_time, 42)

    def test_disable_timeout_tracer(self):
        """disable_timeout_tracer removes the tracer."""
        enable_timeout_tracer()
        disable_timeout_tracer()
        timeout_tracers = [t for t in tracer.get_tracers()
                           if isinstance(t, StorageTimeoutTracer)]
        self.assertEqual(timeout_tracers, [])

    def test_get_remaining_time(self):
        """get_remaining_time() decreases as the transaction prgresses."""
        time = self.mocker.replace('time.time')
        with self.mocker.order():
            self.expect(time()).result(50)
            self.expect(time()).result(100)
            self.expect(time()).result(150)
        self.mocker.replay()

        timeout_tracer = StorageTimeoutTracer(max_time=75)
        timer.start_transaction()
        # 50 seconds have elapsed.
        self.assertEqual(timeout_tracer.get_remaining_time(), 25)
        # Another 50 seconds elapse.  The remaining time is clamped at zero.
        self.assertEqual(timeout_tracer.get_remaining_time(), 0)

    def test_get_remaining_time_outside_transaction(self):
        """Outside of a transaction get_remaining_time() returns max_time."""
        timeout_tracer = StorageTimeoutTracer(max_time=75)
        # Outside of transaction, return max_time.
        self.assertEqual(timeout_tracer.get_remaining_time(), 75)

    def test_gets_current_timeout_tracer(self):
        """Test that we're able to get the current timeout tracer."""
        enable_timeout_tracer()
        timeout_tracers = [t for t in tracer.get_tracers()
                           if isinstance(t, StorageTimeoutTracer)]
        expected_tracer = timeout_tracers[0]
        actual_tracer = current_timeout_tracer()

        self.assertEqual(actual_tracer, expected_tracer)

    def test_raises_exception_if_no_current_timeout_tracer_exists(self):
        """Test that calling current_timeout_tracer without a current timeout
        having been registered raises NoTimeoutRegistered."""

        self.assertRaises(errors.NoTimeoutTracer, current_timeout_tracer)

    @patch.object(timer, 'get_transaction_time')
    def test_changes_timeout_within_context_manager(self, mock_gettt):
        """Timing out changes and reverts to the previously set timeout."""
        old_timeout = 123
        old_remaining_time = 113
        inner_timeout = 50
        inner_remaining_time = 16
        transaction_time = old_timeout - old_remaining_time
        new_remaining_time = (old_remaining_time -
                              (inner_timeout -
                               inner_remaining_time))
        enable_timeout_tracer(max_time=old_timeout)
        mock_gettt.return_value = transaction_time

        with db_timeout(max_time=inner_timeout):
            self.assertEqual(current_timeout_tracer().get_remaining_time(),
                             inner_timeout)
            mock_gettt.return_value += inner_timeout - inner_remaining_time

        self.assertEqual(current_timeout_tracer().get_remaining_time(),
                         new_remaining_time)

    @patch.object(timer, 'get_transaction_time')
    def test_forces_change_timeout_within_context_manager(self, mock_gettt):
        """Test that forcing a timeout higher than the currently allowed budget
        is possible and that current budget is not changed, but the current
        remaining time is subtracted from the requested max_time.
        """
        old_timeout = 123
        old_remaining_time = 113
        inner_timeout = 300
        inner_remaining_time = 16
        transaction_time = old_timeout - old_remaining_time
        enable_timeout_tracer(max_time=old_timeout)
        mock_gettt.return_value = transaction_time

        with db_timeout(max_time=inner_timeout, force=True):
            self.assertEqual(current_timeout_tracer().get_remaining_time(),
                             inner_timeout - old_remaining_time)
            mock_gettt.return_value += inner_timeout - inner_remaining_time

        self.assertEqual(current_timeout_tracer().get_remaining_time(),
                         old_remaining_time)

    @patch.object(timer, 'get_transaction_time')
    def test_restore_tracer_on_exception(self, mock_gettt):
        """Test that exception in context manager reverts to the previously set
        timeout.
        """

        old_timeout = 123
        old_remaining_time = 113
        inner_timeout = 50
        inner_remaining_time = 16
        new_remaining_time = (old_remaining_time -
                              (inner_timeout - inner_remaining_time))
        transaction_time = old_timeout - old_remaining_time
        enable_timeout_tracer(max_time=old_timeout)
        mock_gettt.return_value = transaction_time

        class ExpectedError(Exception):
            pass

        self.assertEqual(old_remaining_time,
                         current_timeout_tracer().get_remaining_time())

        try:
            with db_timeout(max_time=inner_timeout):
                self.assertEqual(current_timeout_tracer().get_remaining_time(),
                                 inner_timeout)
                mock_gettt.return_value += (
                    inner_timeout - inner_remaining_time)
                raise ExpectedError()
        except ExpectedError:
            pass

        self.assertEqual(current_timeout_tracer().get_remaining_time(),
                         new_remaining_time)

    def test_does_nothing_if_only_exists_in_ctx_manager(self):
        """Test that no timeout tracer is installed if not previously set."""

        new_timeout = 234

        with db_timeout(max_time=new_timeout):
            self.assertRaises(errors.NoTimeoutTracer, current_timeout_tracer)

        self.assertRaises(errors.NoTimeoutTracer, current_timeout_tracer)

    @patch.object(timer, 'get_transaction_time')
    def test_uses_old_remaining_time_if_lower_than_new_max_time(
            self, mock_gettt):
        """Uses the remaining time from the older tracer if it's lower than the
        new max_time."""

        old_timeout = 123
        old_remaining_time = 20
        inner_timeout = 50
        inner_remaining_time = 5
        new_remaining_time = (old_remaining_time -
                              (old_remaining_time - inner_remaining_time))
        transaction_time = old_timeout - old_remaining_time
        enable_timeout_tracer(max_time=old_timeout)
        mock_gettt.return_value = transaction_time
        self.assertEqual(old_remaining_time,
                         current_timeout_tracer().get_remaining_time())

        with db_timeout(max_time=inner_timeout):
            self.assertEqual(current_timeout_tracer().get_remaining_time(),
                             old_remaining_time)
            mock_gettt.return_value += (
                old_remaining_time - inner_remaining_time)

        self.assertEqual(current_timeout_tracer().get_remaining_time(),
                         new_remaining_time)

    @patch.object(timer, 'get_transaction_time')
    def test_reset_timeout(self, mock_gettt):
        """Reset local timeout on _reset_timeout_tracer_remaining_time."""

        old_timeout = 123
        inner_timeout = 50
        transaction_time = 42
        mock_gettt.return_value = transaction_time
        enable_timeout_tracer(max_time=old_timeout)
        with db_timeout(max_time=inner_timeout):
            self.assertEqual(current_timeout_tracer().get_remaining_time(),
                             inner_timeout)
            self.assertEqual(inner_timeout + transaction_time,
                             current_timeout_tracer().local.remaining_time)

        self.assertEqual(old_timeout,
                         current_timeout_tracer().local.remaining_time)

        class Connection(object):
            _timeout_tracer_remaining_time = 0

        connection = Connection()
        current_timeout_tracer().connection_commit(connection)

        self.assertIsNone(current_timeout_tracer().local.remaining_time)
        self.assertEqual(current_timeout_tracer().get_remaining_time(),
                         old_timeout - transaction_time)


class TestOnTimeout(unittest.TestCase):
    """Test on_db_timeout context manager."""

    def test_no_timeout(self):
        """Test no timeout."""
        called = set()

        def handler():
            """handler."""
            called.add('called')

        with on_timeout(handler):
            pass

        self.assertFalse(called)

    def test_retrylimit(self):
        """Test retry limit case."""
        called = set()

        def handler():
            """handler."""
            called.add('called')

        with on_timeout(handler):
            raise RetryLimitReached("a msg", extra_info="orginal exception")

        self.assertTrue(called)

    def test_pg_timeout(self):
        """Test ProgrammingError pg timeout."""
        called = set()

        def handler():
            """handler."""
            called.add('called')

        with on_timeout(handler):
            raise ProgrammingError("timeout")

        self.assertTrue(called)

    def test_storm_timeout(self):
        """Test storm TimeoutError."""
        called = set()

        def handler():
            """handler."""
            called.add('called')

        with on_timeout(handler):
            raise TimeoutError("fake-message", "fake statement", 'args')

        self.assertTrue(called)

    def test_non_timeout_error(self):
        """Test other errors propagate."""
        failing = set()
        called = set()

        def handler():
            """handler."""
            called.add('called')

        try:
            with on_timeout(handler):
                raise ValueError()
        except ValueError:
            failing.add('failing')

        self.assertFalse(called)
        self.assertTrue(failing)

    def test_non_timeout_programming_error(self):
        """Test other errors propagate."""
        failing = set()
        called = set()

        def handler():
            """handler."""
            called.add('called')

        try:
            with on_timeout(handler):
                raise ProgrammingError("err")
        except ProgrammingError:
            failing.add('failing')

        self.assertFalse(called)
        self.assertTrue(failing)

    def test_raising_handler(self):
        """Raising in the timeout handler works as expected."""
        raised = set()
        called = set()

        class HandlerExc(Exception):
            """Handler raised exception."""
            pass

        def handler():
            """handler."""
            called.add('called')
            raise HandlerExc()

        try:
            with on_timeout(handler):
                raise ProgrammingError("timed out")
        except HandlerExc:
            raised.add('raised')

        self.assertTrue(called)
        self.assertTrue(raised)


class TestCheckStoresAndInvalidate(testcase.DatabaseResourceTestCase):
    """Test _check_stores_and_invalidate."""

    def setUp(self):
        self._sto = None

    def tearDown(self):
        if self._sto is not None:
            storage_zstorm.remove(self._sto)

    def test__check_stores_and_invalidate(self):
        """Test _check_stores_and_invalidate invalidate case."""
        from backends.filesync.data.services import make_storage_user
        from backends.filesync.data.model import StorageObject

        logger = logging.getLogger('storage.server.noninv')

        h = MementoHandler()
        logger.addHandler(h)

        make_storage_user(1, u'foo', u'foo', 10000, u'shard2')
        sto = get_store('shard2', storage_zstorm)
        self._sto = sto  # for later cleanup
        obj = StorageObject(1, u'foo', u'File')
        sto.add(obj)
        sto.flush()
        self.assertFalse(obj.__storm_object_info__.get("invalidated", False))
        _check_stores_and_invalidate(storage_zstorm)
        self.assertTrue(obj.__storm_object_info__.get("invalidated", False))
        self.assertEqual(1, len(h.records))
        self.assertEqual((obj,), h.records[0].args)
