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

"""Transaction management."""

import contextlib
import logging
import math
import random
import sys
import threading
import time

from functools import wraps

import psycopg2
import transaction

from psycopg2.extensions import TransactionRollbackError
from storm.databases.postgres import PostgresTimeoutTracer
from storm.exceptions import DisconnectionError, TimeoutError, ProgrammingError
from storm.tracer import install_tracer, remove_tracer_type, get_tracers
from storm.zope.zstorm import ZStorm

from backends.db.errors import (
    IntegrityError,
    NoTimeoutTracer,
    RetryLimitReached,
)

#
# Since some of the account data is maintained by django, the basic
# transaction manager is used as this is what storm.django uses
#
account_tm = transaction
account_zstorm = ZStorm()
account_zstorm.transaction_manager = account_tm

storage_tm = transaction.ThreadTransactionManager()
storage_zstorm = ZStorm()
storage_zstorm.transaction_manager = storage_tm

# these are the default retryable exceptions
RETRYABLE_EXCEPTIONS = (
    DisconnectionError,
    TransactionRollbackError,
    psycopg2.InternalError,
    psycopg2.OperationalError,
)

# This is the maximum time a transaction can take before pgkillactive kills it.
TRANSACTION_MAX_TIME = 600


def retryable_transaction(max_time=4.0, max_retries=3, variance=0.5,
                          exceptions=RETRYABLE_EXCEPTIONS):
    """Make sure that transactions are retried after conflicts.

    This function builds a decorator to be used.
    """
    # XXX("lucio.torre", "we need to measure here to find reasonable " \
    #                    "default parameters for this")
    def inner(function):
        """The actual decorator"""
        # we need to define scale so that it makes delay_time never add
        # to more than max_time given max_retries
        scale = 1
        _variance = 0

        def delay_time(step):
            """generates a delay for each step"""
            # delay_time(0) == 0
            return (((math.exp(step) - 1) / scale) /
                    (1 + random.random() * _variance))

        total_time = sum(delay_time(i) for i in range(max_retries))
        _variance = variance
        scale = total_time / max_time

        @wraps(function)
        def decorated(*args, **kwargs):
            """the new decorated function"""
            count = 0
            while True:
                time.sleep(delay_time(count))
                try:
                    value = function(*args, **kwargs)
                except exceptions, e:
                    info = sys.exc_info()
                    try:
                        if isinstance(e, psycopg2.InternalError):
                            logger = logging.getLogger('storage.server.txn')
                            logger.exception("Got an InternalError, retrying. "
                                             "(count: %s, max_retries: %s)",
                                             str(count), str(max_retries))
                        count += 1
                        if count >= max_retries:
                            # include the original error name in the new
                            # exception
                            msg = ("Maximum retries (%i) reached. "
                                   "Please try again. (Original error: %s: %s)"
                                   % (count, e.__class__.__name__, e.message))
                            # and add the detailed error as extra_info
                            extra = "%s: %s" % (e.__class__.__name__, str(e))

                            class RetryLimitExceeded(RetryLimitReached,
                                                     info[0]):
                                """A dynamic exception type which preserves
                                the original type as well."""

                            raise RetryLimitExceeded(msg, extra_info=extra)
                    finally:
                        # We clear this variable to avoid creating a
                        # reference cycle between traceback and this
                        # frame.
                        info = None
                else:
                    break
            return value
        return decorated
    return inner


class TransactionTimer(threading.local):
    """A thread local storage class to time transactions."""
    def __init__(self):
        super(TransactionTimer, self).__init__()
        self._start_time = None

    def start_transaction(self):
        """Record that the transaction has started."""
        self._start_time = time.time()

    def end_transaction(self):
        """Record that the transaction has completed."""
        self._start_time = None

    def get_transaction_time(self):
        """Get the length of time that the transaction has run.

        If no transaction is running, return None.
        """
        if self._start_time is None:
            return None
        return time.time() - self._start_time


timer = TransactionTimer()


def _check_stores_and_invalidate(zstorm):
    """Check stores for non-invalidated objs and invalidate as needed.

       Also report.
    """
    logger = None
    for _, store in zstorm.iterstores():
        for obj_info in store._iter_alive():
            if not obj_info.get("invalidated", False):
                if logger is None:
                    logger = logging.getLogger('storage.server.noninv')
                logger.error("NON-INVALIDATED OBJ %r", obj_info.get_obj())
                store.invalidate()
                break


def get_storm_commit(tx_manager):
    """Get a decorator that starts and commits a transaction

    Will get the store and commit the transaction when function is done.
    If the function raises any exception, the transaction will be rolled back.
    """
    def wrapper(function):
        """The wrapper."""
        @wraps(function)
        def decorated(*args, **kwargs):
            """the decorated function"""
            if tx_manager is storage_tm:
                _check_stores_and_invalidate(storage_zstorm)
            timer.start_transaction()
            tx_manager.begin()
            try:
                result = function(*args, **kwargs)
            except psycopg2.IntegrityError, e:
                tx_manager.abort()
                raise IntegrityError(str(e))
            except:
                tx_manager.abort()
                raise
            else:
                tx_manager.commit()
            finally:
                timer.end_transaction()
            return result
        return decorated
    return wrapper


def get_storm_readonly(tx_manager, use_ro_store=False):
    """Get a decorator that starts and ends a read only transaction.

    Will get the store and rollback the transaction when function is done.
    The transaction will be retried if there is a DisconnectionError.
    """
    def wrapper(function):
        """The wrapper."""
        @wraps(function)
        @retryable_transaction(
            exceptions=(DisconnectionError, psycopg2.InternalError))
        def decorated(*args, **kwargs):
            """the decorated function"""
            timer.start_transaction()
            tx_manager.begin()
            tx = tx_manager.get()
            tx.use_ro_store = use_ro_store
            try:
                result = function(*args, **kwargs)
            finally:
                tx_manager.abort()
                timer.end_transaction()
            return result
        return decorated
    return wrapper


class RemainingTime(threading.local):
    """Time remaining for local thread."""
    remaining_time = None


class StorageTimeoutTracer(PostgresTimeoutTracer):
    """A tracer that enforces a maximum time limit on transactions."""

    def __init__(self, max_time):
        super(StorageTimeoutTracer, self).__init__()
        self.max_time = max_time
        self.local = RemainingTime()

    def get_remaining_time(self):
        """See TimeoutTracer."""
        remaining_time = self.local.remaining_time
        if remaining_time is None:
            remaining_time = self.max_time
        transaction_time = timer.get_transaction_time()
        if transaction_time is None:
            return remaining_time
        return max(remaining_time - transaction_time, 0)

    def set_remaining_time(self, remaining_time):
        """Set remaining time for current thread."""
        self.local.remaining_time = remaining_time

    def _reset_timeout_tracer_remaining_time(self, connection):
        super(StorageTimeoutTracer, self)._reset_timeout_tracer_remaining_time(
            connection)
        self.set_remaining_time(None)


def disable_timeout_tracer():
    """Disable transaction timeouts."""
    remove_tracer_type(StorageTimeoutTracer)


def enable_timeout_tracer(max_time=20):
    """Enable transaction timeouts."""
    disable_timeout_tracer()
    install_tracer(StorageTimeoutTracer(max_time))


def current_timeout_tracer():
    """Get the current timeout tracer."""
    timeout_tracers = [t for t in get_tracers()
                       if isinstance(t, StorageTimeoutTracer)]
    try:
        return timeout_tracers[0]
    except IndexError:
        raise NoTimeoutTracer('No timeout tracer registered')


@contextlib.contextmanager
def db_timeout(max_time=20, force=False):
    """
    Enables transaction timeouts to a certain value and then returns to the
    previous state.

    If force=False the requested timeout is only set for the duration of the
    call and deducted from the previously available budget.

    If force=True the requested timeout is set for the duration of the call but
    not deducted from the previously available budget.
    """
    transaction_time = timer.get_transaction_time()
    try:
        tracer = current_timeout_tracer()
        old_remaining_time = tracer.get_remaining_time()
        if not force:
            # Requested time must not be higher than current available
            # budget. If it's higher, then we force it into the available
            # budget.
            max_time = min(max_time, old_remaining_time)
        else:
            max_time = max_time - old_remaining_time
    except NoTimeoutTracer:
        # Can't do anything about it.
        yield
    else:
        max_time = (transaction_time or 0) + max_time
        assert isinstance(tracer, StorageTimeoutTracer)
        tracer.set_remaining_time(max_time)
        try:
            yield
        finally:
            if not force:
                # Deduct time spent from previosly allowed budget.
                inner_remaining_time = tracer.get_remaining_time()
                new_remaining_time = (old_remaining_time -
                                      (max_time -
                                       (transaction_time or 0) -
                                       inner_remaining_time))
            else:
                # Restore previously remaining time.
                new_remaining_time = old_remaining_time
            transaction_time = timer.get_transaction_time()
            tracer.set_remaining_time((transaction_time or 0) +
                                      new_remaining_time)


def _is_pg_timeout(programming_error):
    """Detect programming errors that are timeouts."""
    msg = str(programming_error)
    return "timed out" in msg or "timeout" in msg


@contextlib.contextmanager
def on_timeout(handler):
    """Context manager invoking a handler in various cases of db timeout."""
    try:
        yield
    except ProgrammingError, e:
        if _is_pg_timeout(e):
            handler()
        else:
            raise
    except (RetryLimitReached, TimeoutError):
        handler()
