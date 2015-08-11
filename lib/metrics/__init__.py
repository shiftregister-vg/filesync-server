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

"""Support for getting metrics sent over to statsd."""

import os
import platform
import time

from functools import wraps
from threading import Lock
from Queue import Full

from config import config


def get_service_name():
    """Get the service name."""
    return os.environ.get("FSYNC_SERVICE_NAME", config.general.service_name)


def get_instance_id():
    """Get the instance id."""
    inst_id = os.environ.get("FSYNC_INSTANCE_ID", config.general.instance_id)
    return "%03d" % int(inst_id)


class MeterUtility(object):
    """The meter utility."""

    def __init__(self):
        self.instance_meter = None
        self.service_meter = None
        self.activity_meter = None
        self.beacon_meter = None
        self.worker_meter = None
        self.lock = Lock()

    def get_service_meter(self):
        """Return the meter for current environment and service."""
        if self.service_meter is None:
            with self.lock:
                # Re-check, since another thread might have gotten the
                # lock first.
                if self.service_meter is None:
                    from metrics.metricsconnector import MetricsConnector

                    self.service_meter = MetricsConnector.new_metrics(
                        namespace=get_service_meter_name())
        return self.service_meter


METER_UTILITY = MeterUtility()
get_service_meter = METER_UTILITY.get_service_meter


def get_meter(scope=None):
    """Get a meter for the given scope.

    Defaults to instance scope, which generates a metric containing the
    instance id as part of it's namespace.
    """
    if scope is None:
        scope = "instance"
    return getattr(METER_UTILITY, "get_%s_meter" % scope)()


def get_resources_meter_name():
    """Return the meter name for reporting resources info."""
    assert config.general.environment_name, "Missing environment_name setting"
    return "%s.%s.resources" % (config.general.environment_name,
                                platform.node())


def get_instance_meter_name():
    """Return the meter name for current environment and service."""
    return "%s.%s" % (get_service_meter_name(), get_instance_id())


def get_service_meter_name():
    """Return the meter name for current environment and service."""
    assert config.general.environment_name, "Missing environment_name setting"
    assert config.general.service_group, "Missing service_group setting"
    return "%s.%s.%s" % (config.general.environment_name,
                         config.general.service_group,
                         get_service_name())


def timed_call(scope=None):
    """A decorator for callables that measures time with txstatsd.

    Use it like this::

        @timed_call(scope='instance')
        def view(request, *args):
            code

    or::

        @timed_call(scope='service')
        def my_callable(*args):
            code

    It will generate a timing metric (durations and rates) in a path
    that looks like this for Django views::

        <module_path>.<func_name>.<result_code>

    or this for other callables::

        <module_path>.<func_name>

    or::

        <module_path>.<func_name>.error

    if an exception was raised.
    """
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            meter = get_meter(scope=scope)
            start_time = time.time()
            try:
                result = view(*args, **kwargs)
            except:
                meter.timing(view.__module__ + "." + view.func_name + ".error",
                             time.time() - start_time)
                raise
            else:
                status_code = getattr(result, "status_code", None)
                if status_code is not None:
                    meter.timing(view.__module__ + "." + view.func_name
                                 + "." + str(status_code),
                                 time.time() - start_time)
                else:
                    meter.timing(
                        '.'.join([view.__module__, view.func_name, 'success']),
                        time.time() - start_time)

            return result

        return wrapper

    return decorator


def timed_invoke(func, *args, **kwargs):
    """Invoke function timing execution."""
    t0 = time.time()
    label = kwargs.pop('_timing_label', None)
    scope = kwargs.pop('_timing_scope', None)
    meter = get_meter(scope=scope)
    if label is None:
        label = "%s.%s" % (func.__module__, func.__name__)
    try:
        res = func(*args, **kwargs)
    except:
        meter.timing(label + ".error", time.time() - t0)
        raise
    else:
        meter.timing(label + ".success", time.time() - t0)
    return res


class NamespaceMeter(object):
    """A namespace meter."""

    def __init__(self, namespace, scope=None):
        self.namespace = namespace
        self.scope = scope

    def gauge(self, name, value, sample_rate=1):
        """Record an absolute reading for C{name} with C{value}."""
        return get_meter(scope=self.scope).gauge(
            self.namespace + "." + name,
            value, sample_rate)

    def increment(self, name, value=1, sample_rate=1):
        """Increment counter C{name} by C{count}."""
        return get_meter(scope=self.scope).increment(
            self.namespace + "." + name,
            value, sample_rate)

    def decrement(self, name, value=1, sample_rate=1):
        """Decrement counter C{name} by C{count}."""
        return get_meter(scope=self.scope).decrement(
            self.namespace + "." + name,
            value, sample_rate)

    def timing(self, name, duration=None, sample_rate=1):
        """Report that C{name} took C{duration} seconds."""
        return get_meter(scope=self.scope).timing(
            self.namespace + "." + name,
            duration, sample_rate)

    def meter(self, name, value=1, sample_rate=1):
        """Mark the occurrence of a given number of events."""
        return get_meter(scope=self.scope).meter(
            self.namespace + "." + name,
            value, sample_rate)

    def report(self, name, value, metric_type, sample_rate=1):
        """Report a generic metric.

        Used for server side plugins without client support.
        """
        return get_meter(scope=self.scope).report(
            self.namespace + "." + name,
            value, metric_type, sample_rate)

    def sli(self, *args, **kwargs):
        """Report a service level metric."""
        return get_meter(scope=self.scope).sli(*args, **kwargs)

    def sli_error(self, *args, **kwargs):
        """Report an error for a service level metric."""
        return get_meter(scope=self.scope).sli_error(*args, **kwargs)


class QueueMeter(object):
    """A queue meter."""

    def __init__(self, queue, block=False, on_full=None):
        self.queue = queue
        self.block = block
        self.on_full = on_full

    def _queue_call(self, method_name, *args):
        """Queue the call."""
        try:
            self.queue.put((method_name,) + args, block=self.block)
        except Full:
            if self.on_full is not None:
                self.on_full()

    def gauge(self, name, value, sample_rate=1):
        """Record an absolute reading for C{name} with C{value}."""
        self._queue_call("gauge", name, value, sample_rate)

    def increment(self, name, value=1, sample_rate=1):
        """Increment counter C{name} by C{count}."""
        self._queue_call("increment", name, value, sample_rate)

    def decrement(self, name, value=1, sample_rate=1):
        """Decrement counter C{name} by C{count}."""
        self._queue_call("decrement", name, value, sample_rate)

    def timing(self, name, duration=None, sample_rate=1):
        """Report that C{name} took C{duration} seconds."""
        self._queue_call("timing", name, duration, sample_rate)

    def meter(self, name, value=1, sample_rate=1):
        """Mark the occurrence of a given number of events."""
        self._queue_call("meter", name, value, sample_rate)

    def report(self, name, value, metric_type, sample_rate=1):
        """Report a generic metric.

        Used for server side plugins without client support.
        """
        self._queue_call("report", name, value, metric_type, sample_rate)
