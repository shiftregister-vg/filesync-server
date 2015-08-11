# -*- coding: utf-8 -*-

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

"""Test Storage Server requests/responses."""

import collections
import datetime
import logging
import os
import types
import time
import uuid
import weakref

from config import config
from metrics.metricsconnector import MetricsConnector
from mocker import expect, Mocker, MockerTestCase, ARGS, KWARGS, ANY

from twisted.python.failure import Failure
from twisted.python import log
from twisted.internet import defer, task, error as txerror
from twisted.trial.unittest import TestCase as TwistedTestCase

from txstatsd.metrics.extendedmetrics import ExtendedMetrics
from txstatsd.metrics.gaugemetric import GaugeMetric
from txstatsd.metrics.metermetric import MeterMetric

from s3lib import s3lib

from backends.filesync.data import errors as dataerror
from ubuntuone.devtools.handlers import MementoHandler
from ubuntuone.storage.server import errors, server
from ubuntuone.storage.server.server import (
    StorageServer, StorageServerRequestResponse, SimpleRequestResponse,
    ListShares, ShareAccepted, CreateShare, DeleteShare, TRACE,
    CreateUDF, DeleteVolume, ListVolumes, Unlink, BytesMessageProducer,
    GetContentResponse, PutContentResponse, StorageServerFactory,
    GetDeltaResponse, RescanFromScratchResponse, configure_oops,
    QuerySetCapsResponse, MoveResponse, MakeResponse, FreeSpaceResponse,
    AccountResponse, AuthenticateResponse, Action, LoopingPing,
)
from ubuntuone.storage.server.testing import testcase
from ubuntuone.storageprotocol import protocol_pb2, request


noop = lambda *args, **kwargs: None


class FakeNode(object):
    """A fake node."""
    id = 123
    generation = 0
    is_live = False
    is_file = False
    name = u"name"
    parent_id = None
    content_hash = None
    crc32 = 12123
    size = 45325
    last_modified = 2334524
    is_public = False
    path = u"path"


class FakeUser(object):
    """A fake user."""
    id = 42
    username = 'username'
    get_root = lambda self: (123, 456)  # root_id, gen
    set_client_caps = lambda self, caps: None


class FakeProducer(object):
    """A fake producer."""
    resumeProducing = stopProducing = pauseProducing = lambda s: None


class FakedStats(object):
    """A faked statsmeter"""

    def __init__(self):
        self.informed = []

    def hit(self, *args):
        """Inform stats."""
        self.informed.append(args)


class FakeStatsDClient(object):
    """A faked StatsD client"""

    def __init__(self):
        self.data = None

    def connect(self):
        """Connect to the StatsD server."""
        pass

    def disconnect(self):
        """Disconnect from the StatsD server."""
        pass

    def write(self, data):
        """Send the metric to the StatsD server."""
        self.data = data


class FakedFactory(object):
    """A faked factory."""

    def __init__(self):
        self.oops_config = configure_oops()
        self.oopses = []

        def publish(report):
            self.oopses.append(report)

        self.oops_config.publishers = [publish]

        self.stats = FakedStats()
        connection = FakeStatsDClient()
        self.metrics = ExtendedMetrics(connection, "server.test")
        self.user_metrics = ExtendedMetrics(connection, "server.test")
        self.sli_metrics = ExtendedMetrics(connection, "server.test")
        self.servername = "fakeservername"
        self.trace_users = []


class FakedPeer(object):
    """A faked peer."""

    def __init__(self):
        self.host = 'localhost'
        self.port = 0


class FakedTransport(object):
    """A faked transport."""

    def __init__(self):
        self.registerProducer = noop
        self.unregisterProducer = noop
        self.loseConnection = noop
        self.getPeer = lambda *_: FakedPeer()


class BaseStorageServerTestCase(TwistedTestCase):
    """Test the StorageServer class.

    This is just a base class with a lot of functionality for other TestCases.
    """

    @defer.inlineCallbacks
    def setUp(self):
        """Init."""
        yield super(BaseStorageServerTestCase, self).setUp()
        self.last_msg = None
        self.restore = {}
        self.server = StorageServer()
        self.patch(self.server, 'sendMessage',
                   lambda msg: setattr(self, 'last_msg', msg))
        self.patch(self.server, 'factory', FakedFactory())
        self.patch(self.server, 'transport', FakedTransport())

        self.handler = MementoHandler()
        self.handler.setLevel(TRACE)
        self.server.logger.addHandler(self.handler)

    @defer.inlineCallbacks
    def tearDown(self):
        """Clean up."""
        self.server.logger.removeHandler(self.handler)
        self.server = None
        self.last_msg = None
        yield super(BaseStorageServerTestCase, self).tearDown()
        MetricsConnector.unregister_metrics()

    @property
    def shutdown(self):
        """Property to access self.server.shutting_down attribute."""
        return self.server.shutting_down

    @property
    def msg(self):
        """A per-test message to raise exceptions."""
        return 'Some message for a failure while executing %s.' % self

    def assert_correct_comment(self, comment, msg):
        """Ckeck that error sent had `msg' as comment."""
        self.assertTrue(comment is not None)
        self.assertTrue(len(comment) > 0)
        error_msg = 'msg ("%s") must be in comment field ("%s").'
        self.assertTrue(unicode(msg) in comment, error_msg % (msg, comment))

    def fail_please(self, failure):
        """Return a function that raises 'failure'."""

        def inner(*args, **kwargs):
            """Do nothing but fail."""
            raise failure

        return inner

    def just_return(self, result):
        """Return a function that returns 'result'."""

        def inner(*args, **kwargs):
            """Do nothing but return a value."""
            return result

        return inner

    def assert_oopsless(self):
        """Make sure we have no oopses."""
        self.assertTrue(len(self.server.factory.oopses) == 0,
                        'Must not have any oops')

    def assert_oopsing(self, failure):
        """Check oopsing."""
        if not isinstance(failure, Failure):
            failure = Failure(failure)
        oopses = self.server.factory.oopses
        self.assertTrue(len(oopses) > 0, 'must have at least one oops')
        self.assertTrue(str(failure.value) in oopses[-1]["value"])
        self.assertTrue(str(failure.type.__name__) in oopses[-1]["type"])


class StorageServerTestCase(BaseStorageServerTestCase):
    """Test the StorageServer class.

    Here are the tests specific for the StorageServer class... other TestCase
    classes should not inherit this.
    """

    def test_log_error_and_oops(self):
        """Test server._log_error_and_oops."""
        req = request.Request(protocol=None)
        failure = Failure(Exception(self.msg))
        self.server._log_error_and_oops(failure, req.__class__)

        self.assertTrue(self.handler.check_error(
                        req.__class__.__name__, str(failure)))
        self.assert_oopsing(failure)

    def test_schedule_request(self):
        """Test schedule_request adds a logging errback to the request."""
        self.patch(self.server, 'execute_next_request', lambda: None)

        req = request.Request(protocol=self.server)
        req.id = 42
        self.server.requests[req.id] = req
        self.server.schedule_request(request=req, callback=None)

        # assert proper errback was chained
        self.assertEqual(len(req.deferred.callbacks), 1)
        self.assertEqual(req.deferred.callbacks[0][1][0],
                         self.server._log_error_and_oops,
                         'errback must be correct')
        self.assertEqual(req.deferred.callbacks[0][1][1],
                         (req.__class__,),
                         'errback parameter is correct')

        # the logging callback actually works!
        failure = Failure(Exception(self.msg))
        req.error(failure)
        self.assertTrue(self.handler.check_error(
                        str(failure), req.__class__.__name__))

    def test_schedule_request_head(self):
        """Test schedule_request to the left of the deque."""
        self.patch(self.server, 'execute_next_request', lambda: None)

        req1 = request.Request(protocol=self.server)
        req1.id = 42
        self.server.requests[req1.id] = req1
        self.server.schedule_request(req1, None)

        req2 = request.Request(protocol=self.server)
        req2.id = 43
        self.server.requests[req2.id] = req2
        self.server.schedule_request(req2, None, head=True)
        # check that req2 is at the head of the deque
        expected_deque = collections.deque([(req2, None), (req1, None)])
        self.assertEqual(expected_deque, self.server.pending_requests)
        self.assertEqual(req2.id, self.server.pending_requests.popleft()[0].id)

    def test_handle_PROTOCOL_VERSION_when_version_too_low(self):
        """handle_PROTOCOL_VERSION when unsupported version."""
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PROTOCOL_VERSION
        message.protocol.version = self.server.VERSION_REQUIRED - 1

        self.server.handle_PROTOCOL_VERSION(message)

        self.assertTrue(self.shutdown)
        self.assertTrue(self.last_msg is not None)
        self.assertEqual(protocol_pb2.Message.ERROR, self.last_msg.type)
        self.assertEqual(protocol_pb2.Error.UNSUPPORTED_VERSION,
                         self.last_msg.error.type)

        self.assert_correct_comment(comment=self.last_msg.error.comment,
                                    msg=message.protocol.version)

    def test_handle_PROTOCOL_VERSION_when_version_too_high(self):
        """handle_PROTOCOL_VERSION when unsupported version."""
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PROTOCOL_VERSION
        message.protocol.version = self.server.PROTOCOL_VERSION + 1

        self.server.handle_PROTOCOL_VERSION(message)

        self.assertTrue(self.shutdown)
        self.assertTrue(self.last_msg is not None)
        self.assertEqual(protocol_pb2.Message.ERROR, self.last_msg.type)
        self.assertEqual(protocol_pb2.Error.UNSUPPORTED_VERSION,
                         self.last_msg.error.type)

        self.assert_correct_comment(comment=self.last_msg.error.comment,
                                    msg=message.protocol.version)

    def test_data_received(self):
        """Test error handling on server.dataReceived method."""
        failure = Exception(self.msg)
        self.patch(self.server, 'buildMessage', self.fail_please(failure))
        self.server.dataReceived(data=None)
        self.assertTrue(self.handler.check_exception(failure))
        self.assert_oopsing(failure)

    def test_execute_next_request(self):
        """Test error handling for execute_next_request."""
        failure = Exception(self.msg)
        next_req = (request.Request(None), self.fail_please(failure))
        self.server.pending_requests = collections.deque([next_req])
        self.server.execute_next_request()

        self.assertTrue(self.handler.check_exception(
            failure,
            next_req[0].__class__.__name__))
        self.assert_oopsing(failure)

    def test_process_message_logs_and_oops_on_error(self):
        """Test error handling for processMessage."""
        failure = Exception(self.msg)
        self.patch(request.RequestHandler, 'processMessage',
                   self.fail_please(failure))
        self.server.processMessage(protocol_pb2.Message())
        self.assertTrue(self.handler.check_exception(
            failure, self.server.processMessage.__name__))
        self.assert_oopsing(failure)

    def test_protocol_ref_enabled(self):
        """Test that protocol weakref is disabled in tests."""
        try:
            _server = StorageServer()
            old = config.api_server.protocol_weakref
            config.api_server['protocol_weakref'] = True
            response = StorageServerRequestResponse(
                protocol=_server, message=protocol_pb2.Message())
            self.assertEqual(_server, response._protocol_ref())
            self.assertEqual(weakref.ref, type(response._protocol_ref))
        finally:
            config.api_server['protocol_weakref'] = old

    def test_protocol_ref_disabled(self):
        """Test that protocol weakref is disabled in tests."""
        try:
            _server = StorageServer()
            old = config.api_server.protocol_weakref
            config.api_server['protocol_weakref'] = False
            response = StorageServerRequestResponse(
                protocol=_server, message=protocol_pb2.Message())
            self.assertEqual(_server, response._protocol_ref)
        finally:
            config.api_server['protocol_weakref'] = old

    def test_looping_ping_enabled(self):
        """Test that the server instantiates the looping ping."""
        self.assertTrue(isinstance(self.server.ping_loop, LoopingPing))

    def test_looping_ping_interval(self):
        """Test the looping ping interval set from the server."""
        self.assertEqual(self.server.ping_loop.interval, 120)

    def test_looping_ping_timeout(self):
        """Test the looping ping timeout set from the server."""
        self.assertEqual(self.server.ping_loop.timeout, 480)

    def test_setuser_set_user(self):
        """Check the user is set."""
        assert self.server.user is None
        user = FakeUser()
        self.server.set_user(user)
        self.assertEqual(self.server.user, user)

    def test_setuser_fix_logger_yes(self):
        """If the user is special, fix the logger."""
        # set up
        user = FakeUser()
        assert user.username == 'username'
        standard_name = config.api_server.logger_name
        assert self.server.logger.name == standard_name
        self.server.factory.trace_users = ['username']

        # call and check
        self.server.set_user(user)
        self.assertEqual(self.server.logger.name, standard_name + '.hackers')

    def test_setuser_fix_logger_no(self):
        """If the user is not special, fix the logger."""
        # set up
        user = FakeUser()
        assert user.username == 'username'
        previous_logger = self.server.logger
        self.server.factory.trace_users = []

        # call and check
        self.server.set_user(user)
        self.assertIdentical(self.server.logger, previous_logger)

    def test_handle_PING(self):
        """Handle PING."""
        # get the response
        response = []
        self.server.sendMessage = lambda r: response.append(r)

        # build the msg
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PING

        # try it
        self.server.handle_PING(message)

        # check response and logging
        self.assertEqual(response[0].type, protocol_pb2.Message.PONG)
        self.handler.debug = True
        self.assertTrue(self.handler.check(TRACE, "ping pong"))


class ActionTestCase(BaseStorageServerTestCase):
    """Test the Action class."""

    @defer.inlineCallbacks
    def setUp(self):
        """Init."""
        yield super(ActionTestCase, self).setUp()
        # create a request-response to use in the tests.
        self.response = StorageServerRequestResponse(
            protocol=self.server, message=protocol_pb2.Message())
        self.response.id = 42
        self.response.protocol.requests[self.response.id] = self.response
        self.callable_deferred = defer.Deferred()
        self.action = Action(self.response, self._callable)
        self.increments = []
        self.decrements = []
        self.patch(self.response.protocol.factory.metrics, 'increment',
                   self.increments.append)
        self.patch(self.response.protocol.factory.metrics, 'decrement',
                   self.decrements.append)

    def _callable(self):
        """Do nothing."""
        return self.callable_deferred

    def test_start(self):
        """Test the start method."""
        self.action.start()
        self.assertTrue(self.action.started)
        self.assertEqual(["action_instances." + Action.__name__],
                         self.increments)
        self.assertTrue(self.handler.check_debug(
            "Action being scheduled (%s)" % (self._callable,)))
        self.assertTrue(self.handler.check_debug(
            "Action being started, working on: %s" % (self._callable,)))

    def test_start_tail(self):
        """Test the start method, scheduled in the tail."""
        # get the request lock
        self.server.request_locked = self.response
        action = Action(self.response, self._callable)
        action.start()
        self.action.start(False)
        self.assertEqual(action, self.server.pending_requests.popleft()[0])
        self.assertEqual(self.action,
                         self.server.pending_requests.popleft()[0])

    def test_start_head(self):
        """Test the start method, scheduled in the head."""
        # get the request lock
        self.server.request_locked = self.response
        self.action.start()
        self.assertFalse(self.action.started)
        action = Action(self.response, self._callable)
        action.start()
        self.action.start()
        self.assertEqual(self.action,
                         self.server.pending_requests.popleft()[0])
        self.assertEqual(action, self.server.pending_requests.popleft()[0])

    def test_schedule_start(self):
        """Test the start method."""
        self.server.request_locked = self.response
        self.action.start()
        self.assertEqual(["action_instances." + Action.__name__],
                         self.increments)
        self.assertTrue(self.handler.check_debug(
            "Action being scheduled (%s)" % (self._callable,)))

    def test__start(self):
        """Test the _start method."""
        def _callable():
            """Do nothing."""
            self.callable_deferred.callback(None)
            return self.callable_deferred
        self.action._callable = _callable
        self.action.start()
        self.assertTrue(self.callable_deferred.called)

    def test_cleanup(self):
        """Test the cleanup method."""
        self.action.start()
        self.callable_deferred.callback(None)
        self.assertTrue(self.action.finished)
        self.assertFalse(self.action.started)
        self.assertEqual(["action_instances." + Action.__name__],
                         self.increments)
        self.assertEqual(["action_instances." + Action.__name__],
                         self.decrements)

    def test_done(self):
        """Test the done method."""
        self.action.start()
        self.callable_deferred.callback(None)
        self.assertTrue(self.handler.check_debug("Action done (%s)" %
                                                 (self._callable,)))

    def test_done_fails(self):
        """Test that error is called if done fails."""
        # patch base class done method, and make it fail.
        exc = RuntimeError("fail!")

        def bad_cleanup(_):
            """cleanup method that always fails."""
            raise exc
        self.patch(Action, 'cleanup', bad_cleanup)
        # patch error method to know it was called
        called = []

        def error(_, e):
            """Collect the errors."""
            called.append(e)
        self.patch(Action, 'error', error)
        self.action.start()
        self.callable_deferred.callback(None)
        self.assertTrue(self.handler.check_debug("Action done (%s)" %
                                                 (self._callable,)))
        # done was called, check if error too.
        self.assertEqual(1, len(called))
        self.assertEqual(exc, called[0].value)

    def test_action_deferred_called(self):
        """Test the action.deferred callback chain."""
        self.action.start()
        result = object()
        self.callable_deferred.callback(result)
        self.assertTrue(self.action.deferred.called)
        self.assertEqual(result, self.action.deferred.result)

    def test_action_deferred_errback_called(self):
        """Test the action.deferred errback chain."""
        self.action.start()
        failure = Failure(RuntimeError("fail!"))
        self.callable_deferred.errback(failure)
        self.assertTrue(self.action.deferred.called)

        def check(f):
            """Check the failure."""
            self.assertEqual(failure, f)
        self.action.deferred.addErrback(check)


class StorageServerRequestResponseTestCase(BaseStorageServerTestCase):
    """Test the StorageServerRequestResponse class."""

    response_class = StorageServerRequestResponse

    @defer.inlineCallbacks
    def setUp(self):
        """Init."""
        yield super(StorageServerRequestResponseTestCase, self).setUp()
        self.errors = []
        self.last_msg = None

        def sendError(myself, msg, comment=None, free_space_info=None):
            """Fake sendError."""
            self.errors.append((msg, comment, free_space_info))
        self.patch(self.response_class, 'sendError', sendError)
        self.patch(self.response_class, 'sendMessage',
                   lambda s, m: setattr(self, 'last_msg', m))
        self.response = self.response_class(protocol=self.server,
                                            message=protocol_pb2.Message())
        self.response.id = 42
        self.response.protocol.requests[self.response.id] = self.response
        factory = self.response.protocol.factory
        self.increments = []
        self.decrements = []
        self.patch(factory.metrics, 'increment', self.increments.append)
        self.patch(factory.metrics, 'decrement', self.decrements.append)

    @defer.inlineCallbacks
    def tearDown(self):
        """Clean up."""
        self.errors = None
        self.last_msg = None
        yield super(StorageServerRequestResponseTestCase, self).tearDown()

    @property
    def last_error(self):
        """Return the last error."""
        return self.errors[-1] if self.errors else None

    def assert_comment_present(self, msg):
        """Ckeck that error sent had `msg' as comment."""
        self.assertTrue(self.last_error is not None)
        self.assertEqual(3, len(self.last_error))
        if self.last_error[2] is not None:
            free_space_info = self.last_error[2]
            self.assertEquals(dict, type(free_space_info))

        comment = self.last_error[1]
        self.assert_correct_comment(comment, msg)

    def test_error_doesnt_fail(self):
        """Test response.error doesn't fail, always log."""
        failure = Exception(self.msg)
        self.patch(request.RequestResponse, 'error', self.fail_please(failure))
        self.response.error(failure=failure)
        self.assertTrue(self.handler.check_error(
                        str(failure), self.response.error.__name__))

    def test_process_message_returns_result_when_started(self):
        """Test response.processMessage erturns the result."""
        expected = object()
        self.patch(self.response_class, '_processMessage',
                   self.just_return(expected))
        # create a new response with the patched class
        response = self.response_class(protocol=self.server,
                                       message=protocol_pb2.Message())
        response.id = 43
        response.protocol.requests[self.response.id] = self.response
        # check
        message = protocol_pb2.Message()
        response.started = True
        actual = response.processMessage(message=message)
        self.assertEqual(expected, actual,
                         'processMessage must return _processMessage result')

    def test_protocol_gone_raise_error(self):
        """Test that ProtocolReferenceError is raised."""
        # patch _protocol_ref to return None, and fake a gc'ed protocol.
        self.response.use_protocol_weakref = True
        self.response._protocol_ref = lambda: None
        self.assertRaises(errors.ProtocolReferenceError,
                          self.response._get_protocol)

    @defer.inlineCallbacks
    def test_queue_action(self):
        """Test queue_action"""
        result = object()

        def to_do():
            """Just succeed."""
            return defer.succeed(result)
        r = yield self.response.queue_action(to_do)
        self.assertEqual(result, r)

    def test_request_logger_id_updated(self):
        """Test that StorageRequestLogger.request_id is updated."""
        response = self.response_class(protocol=self.server,
                                       message=protocol_pb2.Message())
        self.assertEqual(None, response.id)
        self.assertEqual(None, response.log.request_id)
        response.id = 42
        self.assertEqual(42, response.id)
        self.assertEqual(42, response.log.request_id)

    def test_stop_if_started(self):
        """Cancel the request if already started."""
        called = []
        self.patch(self.response_class, 'cancel',
                   lambda _: called.append(True))
        self.response.started = True
        self.response.stop()
        self.assertTrue(called)

    def test_stop_if_not_started(self):
        """Log and cleanup the request if not started."""
        called = []
        self.patch(self.response_class, 'cleanup',
                   lambda _: called.append(True))
        assert not self.response.started
        self.response.stop()
        self.assertTrue(called)
        self.assertTrue(self.handler.check_debug(
                        "Request being released before start"))


class SSRequestResponseSpecificTestCase(StorageServerRequestResponseTestCase):
    """Test the StorageServerRequestResponse class, not all inherited ones."""

    def test_done_user_activity_yes(self):
        """Report the request's user activity string."""
        # put a user_activity in the class
        self.response_class.user_activity = 'test-activity'
        self.addCleanup(delattr, self.response_class, 'user_activity')

        # set a user
        self.response.protocol.user = FakeUser()

        # record what is measured
        informed = []
        self.patch(self.response.protocol.factory.user_metrics, 'report',
                   lambda *a: informed.extend(a))

        # execute and test
        self.response.done()
        self.assertEqual(informed, ['test-activity', '42', 'd'])

    def test_done_user_activity_no_activity(self):
        """Don't report the request's user activity, as there is no string."""
        # assure there's no activity
        assert not hasattr(self.response_class, 'user_activity')

        # record what is measured
        informed = []
        self.patch(self.response.protocol.factory.user_metrics, 'report',
                   lambda *a: informed.extend(a))

        # execute and test
        self.response.done()
        self.assertEqual(informed, [])

    def test_done_user_activity_no_user_name(self):
        """Report the request's user activity, but still no user."""
        # put a user_activity in the class
        self.response_class.user_activity = 'test-activity'
        self.addCleanup(delattr, self.response_class, 'user_activity')

        # assure there's no user
        assert self.response.protocol.user is None

        # record what is measured
        informed = []
        self.patch(self.response.protocol.factory.user_metrics, 'report',
                   lambda *a: informed.extend(a))

        # execute and test
        self.response.done()
        self.assertEqual(informed, ['test-activity', '', 'd'])

    def test_get_extension_valids(self):
        """Get a 2 chars extension."""
        ext = self.response._get_extension("code.c")
        self.assertEqual(ext, "c")
        ext = self.response._get_extension("binary.db")
        self.assertEqual(ext, "db")
        ext = self.response._get_extension("image.png")
        self.assertEqual(ext, "png")
        ext = self.response._get_extension("image.jpeg")
        self.assertEqual(ext, "jpeg")

    def test_get_extension_toolong(self):
        """Get an extension from something that has too long similar one."""
        ext = self.response._get_extension("document.personal")
        self.assertEqual(ext, None)

    def test_get_extension_path(self):
        """Get an extension from a big path."""
        ext = self.response._get_extension("/foo/bar/etc/image.png")
        self.assertEqual(ext, "png")

    def test_get_extension_several_dots(self):
        """Get an extension from a path with several dots."""
        ext = self.response._get_extension("/foo/bar.etc/image.stuff.png")
        self.assertEqual(ext, "png")

    def test_get_extension_small_name(self):
        """Get an extension from a small name."""
        ext = self.response._get_extension("do")
        self.assertEqual(ext, None)

    def test_get_extension_nothing(self):
        """Get an extension not finding one."""
        ext = self.response._get_extension("alltogetherjpg")
        self.assertEqual(ext, None)


class SimpleRequestResponseTestCase(StorageServerRequestResponseTestCase):
    """Test the SimpleRequestResponse class."""

    response_class = SimpleRequestResponse

    def test_retry_limit_reached_sends_try_again(self):
        """When RetryLimitReached is raised TRY_AGAIN is sent."""
        self.assertIn(dataerror.RetryLimitReached,
                      self.response.try_again_errors)

    def test_tcp_timeout_sends_try_again(self):
        """When TCPTimedOutError is raised TRY_AGAIN is sent."""
        self.assertIn(txerror.TCPTimedOutError,
                      self.response.try_again_errors)

    def test_translation_does_not_exist(self):
        """DoesNotExist is properly translated."""
        e = self.response.protocol_errors[dataerror.DoesNotExist]
        self.assertEquals(protocol_pb2.Error.DOES_NOT_EXIST, e)

    def test_send_protocol_error_handles_retry_limit_reached(self):
        """_send_protocol_error handles the RetryLimitReached."""
        failure = Failure(dataerror.RetryLimitReached(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is not None)
        self.assertEquals(protocol_pb2.Error.TRY_AGAIN, self.last_error[0])
        self.assertEquals("TryAgain (RetryLimitReached: %s)" % self.msg,
                          self.last_error[1])

    def test_tcp_timeout_handled_as_try_again(self):
        """_send_protocol_error handles TCPTimedOutError as TRY_AGAIN."""
        failure = Failure(txerror.TCPTimedOutError())
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is not None)
        self.assertEquals(protocol_pb2.Error.TRY_AGAIN, self.last_error[0])

    def test_send_protocol_error_handles_does_not_exist(self):
        """_send_protocol_error handles the DoesNotExist."""
        failure = Failure(dataerror.DoesNotExist(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is not None)
        self.assertEquals(protocol_pb2.Error.DOES_NOT_EXIST,
                          self.last_error[0])
        self.assertEquals(self.msg, self.last_error[1])

    def test_send_protocol_error_sends_comment(self):
        """_send_protocol_error sends the optional comment on errors."""
        self.response.__class__.expected_errors = \
            self.response.protocol_errors.keys()
        for error in self.response.protocol_errors:
            msg = 'Failing with %s' % error
            if error == dataerror.QuotaExceeded:
                failure = Failure(error(msg, uuid.uuid4(), 10))
            else:
                failure = Failure(error(msg))
            self.response._send_protocol_error(failure=failure)
            self.assert_comment_present(msg)

        # any error not in protocol_errors
        msg = u"ñoño message with non ascii chars"
        failure = Failure(Exception(msg.encode('utf8')))
        self.response._send_protocol_error(failure=failure)
        self.assert_comment_present(msg)

    def test_send_protocol_error_dont_shutdown(self):
        """_send_protocol_error don't shutdown the StorageServer instance."""
        failure = Failure(ValueError(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is not None)
        self.assertEquals(protocol_pb2.Error.INTERNAL_ERROR,
                          self.last_error[0])
        self.assertEquals(self.msg, self.last_error[1])
        self.assertFalse(self.shutdown)

    def test_send_protocol_error_try_again_is_metered(self):
        """_send_protocol_error sends metrics on TryAgain errors."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        self.response.protocol.factory.metrics = mock_metrics
        mock_metrics.meter("TRY_AGAIN.ValueError", 1)
        with mocker:
            failure = Failure(errors.TryAgain(ValueError(self.msg)))
            self.response._send_protocol_error(failure=failure)

    def test_send_protocol_error_converted_try_again_is_metered(self):
        """_send_protocol_error sends metrics on convertd TryAgain errors."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        self.response.protocol.factory.metrics = mock_metrics
        error = self.response.try_again_errors[0]
        mock_metrics.meter("TRY_AGAIN.%s" % error.__name__, 1)
        with mocker:
            failure = Failure(error(self.msg))
            self.response._send_protocol_error(failure=failure)

    def test_send_protocol_error_not_s3_error(self):
        """No metrics are sent for s3 timeout if the error is different."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        self.response.protocol.factory.metrics = mock_metrics
        expect(mock_metrics.meter(ANY)).count(0)
        with mocker:
            failure = Failure(NameError())
            self.response._send_protocol_error(failure=failure)

    def test_send_protocol_error_locked_user(self):
        """_send_protocol_error handles the LockedUserError"""
        called = []
        self.patch(self.response.protocol, 'shutdown',
                   lambda: called.append(1))
        failure = Failure(dataerror.LockedUserError())
        self.response._send_protocol_error(failure=failure)
        self.assertEqual(self.last_error, None)
        self.assertEqual(called, [1])

    def fake_reactor_inspector(self, last_responsive_ts):
        """Instance that fakes the last_responsive_ts attribute."""
        class FakeReactorInspector:
            """Just fakes the last_responsive_ts field."""
            def __init__(self, last_responsive_ts):
                """Pass in the fake last_responsive_ts value."""
                self.last_responsive_ts = last_responsive_ts
        return FakeReactorInspector(last_responsive_ts)

    def test_send_protocol_error_s3_client_timeout(self):
        """Sends metrics for a S3 client timeout."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        self.response.protocol.factory.metrics = mock_metrics
        # Reactor was responsive since last operation, so blame the client
        self.response.last_good_state_ts = 12345
        ri = self.fake_reactor_inspector(12399)
        self.response.protocol.factory.reactor_inspector = ri
        operation = self.response.__class__.__name__
        msg = "%s.request_error.s3_timeout.client" % operation
        mock_metrics.meter(msg, 1)
        expect(mock_metrics.meter(ANY, 1)).count(0, None)
        with mocker:
            failure = Failure(errors.S3Error(ValueError("help")))
            self.response._send_protocol_error(failure=failure)

    def test_send_protocol_error_s3_server_timeout(self):
        """Sends metrics for a S3 server timeout."""
        mocker = Mocker()
        mock_metrics = mocker.mock(ExtendedMetrics)
        self.response.protocol.factory.metrics = mock_metrics
        # Reactor was not responsive since last operation, blame the server
        self.response.last_good_state_ts = 12345
        ri = self.fake_reactor_inspector(12300)
        self.response.protocol.factory.reactor_inspector = ri
        operation = self.response.__class__.__name__
        msg = "%s.request_error.s3_timeout.server" % operation
        mock_metrics.meter(msg, 1)
        expect(mock_metrics.meter(ANY, 1)).count(0, None)
        with mocker:
            failure = Failure(errors.S3Error(ValueError("help")))
            self.response._send_protocol_error(failure=failure)

    def test_start_sends_comment_on_error(self):
        """_start sends the optional comment on errors."""
        self.patch(self.response_class, 'authentication_required', True)
        self.response.protocol.user = None
        self.response._start()
        self.assert_comment_present(self.response.auth_required_error)

    def test_done_never_fails_if_inner_done_fails(self):
        """_start never fails even if done() fails."""
        failure = Exception(self.msg)
        self.patch(request.RequestResponse, 'done', self.fail_please(failure))
        self.response.done()
        self.assertTrue(self.response.deferred.called,
                        'request.deferred was fired.')
        # Commented see Bug LP:890246
        #self.assertTrue(self.handler.check_exception(
        #                    failure,
        #                    self.response.__class__.__name__))
        self.assert_oopsing(failure)

    def test_get_node_info(self):
        """Test the correct info generation."""
        self.response = self.response_class(protocol=self.server,
                                            message=protocol_pb2.Message())

    def test_log_working_on_nothing(self):
        """Log working on without specifications."""
        message = protocol_pb2.Message()
        req = self.response_class(self.server, message)
        req.start()
        self.assertTrue(self.handler.check_debug("Request being started"))

    def test_request_instances_metric(self):
        """request_instances.<request> is updated."""
        message = protocol_pb2.Message()
        req = self.response_class(self.server, message)
        req.start()
        self.assertIn("request_instances." + self.response_class.__name__,
                      self.increments)
        self.assertIn("request_instances." + self.response_class.__name__,
                      self.decrements)

    def test_log_working_on_something(self):
        """Log working on something."""
        message = protocol_pb2.Message()
        self.patch(self.response_class, '_get_node_info', lambda _: 'FOO')
        req = self.response_class(self.server, message)
        req.start()
        self.assertTrue(self.handler.check_debug(
                        "Request being started, working on: FOO"))

    def test_log_operation_data(self):
        """Log data operation."""
        message = protocol_pb2.Message()
        req = self.response_class(self.server, message)
        req.operation_data = "some=stuff bar=foo"
        req.done()
        self.assertTrue(self.handler.check_info(
            "Request done: some=stuff bar=foo"))

    def test_log_request_process(self):
        """Log correctly the life of a request."""
        # setup the message
        message = protocol_pb2.Message()
        message.id = 42
        self.patch(self.response_class, '_process', lambda _: None)
        req = self.response_class(self.server, message)
        req.start()

        # assert log order
        msgs = [(r.levelno, r.msg) for r in self.handler.records]

        def index(level, text):
            """Return the position of first message where text is included."""
            for i, (msglevel, msgtext) in enumerate(msgs):
                if text in msgtext and level == msglevel:
                    return i
            raise ValueError("msg not there!")
        pos_sched = index(logging.INFO, '42 - Request being scheduled')
        pos_start = index(logging.DEBUG, '42 - Request being started')
        pos_done = index(logging.INFO, '42 - Request done')
        self.assertTrue(pos_sched < pos_start < pos_done)

    @defer.inlineCallbacks
    def test_internal_error(self):
        """Test for the internal_error method."""
        failure = Failure(ValueError(self.msg))
        self.response.internal_error(failure=failure)
        try:
            yield self.response.deferred
        except ValueError:
            pass
        else:
            self.fail('Should get a ValueError.')
        self.assertTrue(self.response.finished)
        self.assertTrue(self.shutdown)

    def test_internal_error_after_shutdown(self):
        """Test for getting internal errors after shutdown."""
        # shutdown the server, just like if another request
        # failed with internal error
        self.server.shutdown()
        self.assertTrue(self.shutdown)
        self.assertTrue(self.response.finished)
        # now, make this one fail with internal error
        called = []
        self.patch(self.server, 'shutdown', lambda: called.append(1))
        failure = Failure(ValueError(self.msg))
        # the request is already finished
        self.response.internal_error(failure=failure)
        self.assertTrue(self.response.finished)
        self.assertFalse(called)

    def test_cancel_filter(self):
        """Test the cancel_filter decorator."""
        self.response_class.fakefunction = \
            server.cancel_filter(lambda *a: 'hi')
        self.assertEquals(self.response.fakefunction(self.response), "hi")
        self.response.cancelled = True
        self.assertRaises(request.RequestCancelledError,
                          self.response.fakefunction, self.response)

    def test_requests_leak(self):
        """Test that the server shutdown remove non-started requests."""
        # remove the default request
        del self.server.requests[42]
        # set a fake user.
        mocker = Mocker()
        self.server.user = mocker.mock(count=False)
        expect(self.server.user.username).result("username")
        expect(self.server.user.id).result(1)
        expect(self.server.user.reuse_content).result(noop)
        expect(self.server.user.get_upload_job).result(
            lambda *a, **k: defer.Deferred())
        expect(self.server.user.get_node).result(
            lambda *a, **k: defer.Deferred())
        mocker.replay()

        cleaned = []
        orig_cleanup = self.response_class.cleanup

        def fake_cleanup(response):
            """Clean up the request, but flag it here for the tests."""
            cleaned.append(response.id)
            orig_cleanup(response)

        # patch the _start method to avoid real Response execution
        start_deferred = defer.Deferred()

        @defer.inlineCallbacks
        def fake_start(r):
            """Fake start."""
            yield start_deferred

            # call done() only if it's not a PutContent response, as it handles
            # its own termination in its own _cancel() method (that is called
            # for the running instance at shutdown() time).
            if not isinstance(r, PutContentResponse):
                r.done()

        self.patch(self.response_class, 'cleanup', fake_cleanup)
        self.patch(self.response_class, '_start', fake_start)
        for i in range(5):
            response = self.response_class(protocol=self.server,
                                           message=protocol_pb2.Message())
            response.source_message.id = i
            response.start()
        self.assertTrue(self.server.pending_requests)

        # we should have 4 pending_requests
        self.assertEqual(len(self.server.pending_requests), 4,
                         self.server.pending_requests)

        # the first request should be executing
        running_request = self.server.requests[0]
        self.assertTrue(running_request.started)

        # shutdown and check that pending_requests is clean
        self.server.shutdown()
        self.assertFalse(self.server.pending_requests,
                         self.server.pending_requests)

        # trigger the executing request _process deferred, for
        # it to finish, the requests should be clean now
        start_deferred.callback(True)
        self.assertFalse(self.server.requests, self.server.requests)

        # verify that all the requests were properly cleaned
        self.assertEqual(sorted(cleaned), range(5), cleaned)

    def test_sli_informed_on_done_default(self):
        """The SLI is informed when all ok."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        expect(mock_metrics.sli(self.response_class.__name__, ANY, 1))
        self.response.start_time = time.time()
        with mocker:
            self.response.done()

    def test_sli_informed_on_done_zero_value(self):
        """The SLI is informed when all ok."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        op_length = 0
        expect(mock_metrics.sli(self.response_class.__name__, ANY, op_length))
        self.response.start_time = time.time()
        with mocker:
            self.response.length = op_length
            self.response.done()

    def test_sli_informed_on_done_some_value(self):
        """The SLI is informed when all ok."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        op_length = 12345
        expect(mock_metrics.sli(self.response_class.__name__, ANY, op_length))
        self.response.start_time = time.time()
        with mocker:
            self.response.length = op_length
            self.response.done()

    def test_sli_informed_on_error(self):
        """The SLI is informed after a problem."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        expect(mock_metrics.sli_error(self.response_class.__name__))
        with mocker:
            self.response.error(ValueError())


class ListSharesTestCase(SimpleRequestResponseTestCase):
    """Test the ListShares class."""

    response_class = ListShares

    @defer.inlineCallbacks
    def test_process_set_length(self):
        """Set length attribute while processing."""
        mocker = Mocker()

        # fake share
        share = dict(id=None, from_me=None, to_me=None, root_id=None,
                     name=u'name', shared_by_username=u'sby', accepted=False,
                     shared_to_username=u'sto', shared_by_visible_name=u'vby',
                     shared_to_visible_name=u'vto', access='View')

        # fake user
        user = mocker.mock()
        shared_by = [share] * 3
        shared_to = [share] * 2
        expect(user.list_shares()
               ).result((shared_by, shared_to))
        expect(user.get_volume_id(None)
               ).count(3).result(None)
        expect(user.root_volume_id).count(3).result('')
        self.response.protocol.user = user

        with mocker:
            yield self.response._process()

        self.assertEqual(self.response.length, 5)


class ShareAcceptedTestCase(SimpleRequestResponseTestCase):
    """Test the ShareAccepted class."""

    response_class = ShareAccepted


class CreateShareTestCase(SimpleRequestResponseTestCase):
    """Test the CreateShare class."""

    response_class = CreateShare

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'create_share')


class DeleteShareTestCase(SimpleRequestResponseTestCase):
    """Test the DeleteShare class."""

    response_class = DeleteShare


class CreateUDFTestCase(SimpleRequestResponseTestCase):
    """Test the CreateUDF class."""

    response_class = CreateUDF

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')


class DeleteVolumeTestCase(SimpleRequestResponseTestCase):
    """Test the DeleteVolume class."""

    response_class = DeleteVolume

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')


class ListVolumesTestCase(SimpleRequestResponseTestCase):
    """Test the ListVolumes class."""

    response_class = ListVolumes

    @defer.inlineCallbacks
    def test_process_set_length(self):
        """Set length attribute while processing."""
        mocker = Mocker()

        # fake share
        share = dict(id=None, root_id=None, name=u'name', path=u"somepath",
                     shared_by_username=u'sby', accepted=False,
                     shared_by_visible_name=u'vby', access='View',
                     generation=9, free_bytes=123)

        # fake user
        user = mocker.mock()
        root = share
        shares = [share] * 3
        udfs = [share] * 2
        expect(user.list_volumes()
               ).result((root, shares, udfs, 123))
        self.response.protocol.user = user

        with mocker:
            yield self.response._process()

        self.assertEqual(self.response.length, 6)


class UnlinkTestCase(SimpleRequestResponseTestCase):
    """Test the Unlink class."""

    response_class = Unlink

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')


class GetContentResponseTestCase(SimpleRequestResponseTestCase):
    """Test the GetContentResponse class."""

    response_class = GetContentResponse

    def test_download_last_good_state(self):
        """Test that last_good_state_ts gets updated properly."""
        before = time.time()
        time.sleep(.1)
        self.response.start()
        after = time.time()
        self.assertTrue(before < self.response.last_good_state_ts <= after)
        time.sleep(.1)

        class FakeProducer:
            """Fake source of data to download."""
            def __init__(self):
                """Just setup the expecte attributes."""
                self.deferred = defer.Deferred()
                self.consumer = None

            def resumeProducing(self):
                """Do nothing."""
            def pauseProducing(self):
                """Do nothing."""
            def stopProducing(self):
                """Do nothing."""

        fake_producer = FakeProducer()
        self.response.send(fake_producer)
        fake_producer.consumer.write("abc")
        self.assertTrue(self.response.last_good_state_ts > after)

    def test_start_sends_comment_on_error(self):
        """_start sends the optional comment on errors."""
        self.response.protocol.user = None
        self.response._start()
        self.assert_comment_present(self.response.auth_required_error)

    def test_on_request_cancelled_error_with_cancel_message(self):
        """_send_protocol_error sends CANCELLED when RequestCancelledError.

        self.response.cancel_message is not None.

        """
        self.response.cancel_message = protocol_pb2.Message()
        self.response.cancel_message.id = 1
        assert not self.response.cancelled

        failure = Failure(request.RequestCancelledError(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is None)
        self.assertTrue(self.last_msg is not None)
        self.assertEqual(protocol_pb2.Message.CANCELLED, self.last_msg.type)

    def test_on_request_cancelled_error_without_cancel_message(self):
        """_send_protocol_error logs warning.

        self.response.cancel_message is None.

        """
        self.response.cancel_message = None  # no cancel_message

        failure = Failure(request.RequestCancelledError(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is None)
        self.assertTrue(self.last_msg is None)

        self.assertTrue(self.handler.check_warning(
                        str(failure), 'cancel_message is None'))

    def test_on_producerstopped_and_cancelled_with_cancel_message(self):
        """_send_protocol_error handles ProducerStopped when cancelled.

        self.response.cancel_message is not None.

        """
        self.response.cancel_message = protocol_pb2.Message()
        self.response.cancel_message.id = 1
        self.response.cancel()
        assert self.response.cancelled

        failure = Failure(s3lib.ProducerStopped(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is None)
        self.assertTrue(self.last_msg is not None)
        self.assertEqual(protocol_pb2.Message.CANCELLED, self.last_msg.type)

    def test_on_producerstopped_and_cancelled_without_cancel_message(self):
        """_send_protocol_error handles ProducerStopped when cancelled.

        self.response.cancel_message is None.

        """
        self.response.cancel_message = None
        self.response.cancel()
        assert self.response.cancelled

        failure = Failure(s3lib.ProducerStopped(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assertTrue(self.last_error is None)
        self.assertTrue(self.last_msg is None)
        self.assertTrue(self.handler.check_warning(
                        str(failure), 'cancel_message is None'))

    def test_on_producerstopped_and_not_cancelled(self):
        """_send_protocol_error handles ProducerStopped when not cancelled."""
        assert not self.response.cancelled

        failure = Failure(s3lib.ProducerStopped(self.msg))
        self.response._send_protocol_error(failure=failure)
        self.assert_comment_present(self.msg)
        self.assertEqual(protocol_pb2.Error.TRY_AGAIN, self.last_error[0])
        self.assertTrue(self.handler.check_warning(str(failure), 'TRY_AGAIN'))

    def test__init__(self):
        """Test __init__."""
        message = protocol_pb2.Message()
        response = GetContentResponse(self.server, message)
        self.assertEqual(response.cancel_message, None)
        self.assertEqual(response.message_producer, None)
        self.assertEqual(response.transferred, 0)

    def test_transferred_informed_on_done(self):
        """The transferred quantity is informed when all ok."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.metrics)
        expect(mock_metrics.gauge('GetContentResponse.transferred', 123))
        with mocker:
            self.response.transferred = 123
            self.response.done()

    def test_transferred_informed_on_error(self):
        """The transferred quantity is informed after a problem."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.metrics)
        expect(mock_metrics.gauge('GetContentResponse.transferred', 123))
        with mocker:
            self.response.transferred = 123
            self.response.error(ValueError())

    def test_sli_informed_on_done_default(self):
        """The SLI is NOT informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.done()

    def test_sli_informed_on_done_some_value(self):
        """The SLI is informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.transferred = 12345
        self.response.done()

    def test_sli_informed_on_done_zero_value(self):
        """The SLI is informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.transferred = 0
        self.response.done()

    def test_sli_informed_on_init(self):
        """The SLI is informed after the operation init part."""
        mocker = Mocker()

        # fake producer
        producer = mocker.mock()
        expect(producer.deferred).result(defer.Deferred())

        # some node
        node = mocker.mock()
        expect(node.deflated_size).result(3)
        expect(node.size).count(2).result(2)
        expect(node.content_hash).count(2).result('hash')
        expect(node.crc32).result(123)
        expect(node.get_content(KWARGS)).result(defer.succeed(producer))

        # the user
        fake_user = mocker.mock()
        expect(fake_user.get_node(None, '', '')
               ).result(defer.succeed(node))
        expect(fake_user.username).count(5).result('username')
        self.patch(self.response.protocol, 'user', fake_user)

        # the metric itself
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        expect(mock_metrics.sli('GetContentResponseInit', ANY))

        with mocker:
            self.response._start()


class PutContentResponseTestCase(SimpleRequestResponseTestCase,
                                 MockerTestCase):
    """Test the PutContentResponse class."""

    # subclass PutContentResponse so we have a __dict__ and can patch it.
    response_class = types.ClassType(PutContentResponse.__name__,
                                     (PutContentResponse,), {})

    class FakeUploadJob(object):
        """Fake an UploadJob."""

        def __init__(self):
            self.bytes = ''
            self.inflated_size_hint = 1000
            self.deferred = defer.Deferred()
            self.called = []

        def stop(self):
            """Fake."""

        def connect(self):
            """Flag the call."""
            self.called.append('connect')

        def cancel(self):
            """Flag the call."""
            self.called.append('cancel')

        def add_data(self, bytes):
            """Add data."""
            self.bytes += bytes

        def registerProducer(self, producer):
            """Register the producer."""
            self.called.append('registerProducer')

        def unregisterProducer(self):
            """Unregister the producer."""
            self.called.append('unregisterProducer')

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')

    def test__init__(self):
        """Test __init__."""
        message = protocol_pb2.Message()
        response = PutContentResponse(self.server, message)
        self.assertEqual(response.cancel_message, None)
        self.assertEqual(response.upload_job, None)
        self.assertEqual(response.source_message, message)
        self.assertEqual(response.protocol, self.server)
        self.assertEqual(response.transferred, 0)

    def test__get_node_info(self):
        """Test _get_node_info."""
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PUT_CONTENT
        message.put_content.node = 'abc'
        response = PutContentResponse(self.server, message)
        node_info = response._get_node_info()
        self.assertEqual(node_info, "node: 'abc'")

    def test_transferred_informed_on_done(self):
        """The transferred quantity is informed when all ok."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.metrics)
        expect(mock_metrics.gauge('PutContentResponse.transferred', 123))
        with mocker:
            self.response.transferred = 123
            self.response.done()

    def test_transferred_informed_on_error(self):
        """The transferred quantity is informed after a problem."""
        mocker = Mocker()
        mock_metrics = mocker.patch(self.response.protocol.factory.metrics)
        expect(mock_metrics.gauge('PutContentResponse.transferred', 123))
        with mocker:
            self.response.transferred = 123
            self.response.error(ValueError())

    def test_sli_informed_on_done_default(self):
        """The SLI is informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.done()

    def test_sli_informed_on_done_some_value(self):
        """The SLI is informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.transferred = 12345
        self.response.done()

    def test_sli_informed_on_done_zero_value(self):
        """The SLI is informed when all ok."""
        self.patch(self.response.protocol.factory.sli_metrics,
                   'sli', lambda *a: self.fail("Must not be called"))
        self.response.start_time = time.time()
        self.response.transferred = 0
        self.response.done()

    @defer.inlineCallbacks
    def test_sli_informed_on_init(self):
        """The SLI is informed after the operation init part."""
        mocker = Mocker()

        # fake uploadjob
        uploadjob = mocker.mock()
        expect(uploadjob.deferred).result(defer.Deferred())
        expect(uploadjob.registerProducer(ANY))
        expect(uploadjob.connect()).result(defer.succeed(None))
        expect(uploadjob.stop()).result(defer.succeed(None))
        expect(uploadjob.unregisterProducer())
        self.patch(self.response, '_get_upload_job',
                   lambda: defer.succeed(uploadjob))

        # the user
        fake_user = mocker.mock()
        expect(fake_user.username).count(7).result('username')
        self.patch(self.response.protocol, 'user', fake_user)

        # the metric itself
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        expect(mock_metrics.sli('PutContentResponseInit', ANY))

        with mocker:
            yield self.response._start()

    @defer.inlineCallbacks
    def test_sli_informed_on_commit(self):
        """The SLI is informed after the operation commit part."""
        mocker = Mocker()
        self.response.state = PutContentResponse.states.commiting
        self.patch(self.response, 'queue_action', lambda _: defer.succeed(0))

        # the metric itself
        mock_metrics = mocker.patch(self.response.protocol.factory.sli_metrics)
        expect(mock_metrics.sli('PutContentResponseCommit', ANY))

        with mocker:
            yield self.response._commit_uploadjob('result')

    def test_start_authentication_required(self):
        """Test that _start sends the optional comment on errors."""
        assert self.response.protocol.user is None

        response = self.mocker.patch(self.response)
        expect(response._log_start())
        expect(response.sendError(protocol_pb2.Error.AUTHENTICATION_REQUIRED,
                                  comment=self.response.auth_required_error))
        expect(response.done())

        self.mocker.replay()
        response._start()
        self.assertEqual(response.state, PutContentResponse.states.done)

    def test_start_upload_started_ok(self):
        """Test _start starts an upload."""
        self.response.protocol.user = 'user'

        response = self.mocker.patch(self.response)
        expect(response._log_start())
        expect(response._start_upload()).result(defer.Deferred())

        self.mocker.replay()
        response._start()
        self.assertEqual(response.state, PutContentResponse.states.init)

    def test_start_upload_started_error(self):
        """Test _start calls to generic error after a failing upload start."""
        self.response.protocol.user = 'user'

        response = self.mocker.patch(self.response)
        expect(response._log_start())
        failure = Failure(NameError('foo'))
        expect(response._start_upload()).result(defer.fail(failure))
        expect(response._generic_error(failure))

        self.mocker.replay()
        response._start()

    def test_start_upload_register_producer(self):
        """Test _start starts an upload."""
        upload_job = self.FakeUploadJob()
        upload_job.offset = 0
        upload_job.upload_id = str(uuid.uuid4())
        user = self.mocker.mock()
        self.response.protocol.user = user
        transport = self.mocker.mock()
        self.server.transport = transport
        response = self.mocker.patch(self.response)

        expect(response._log_start())
        expect(response.upload_job).result(upload_job).count(6)
        expect(response.protocol.transport).result(transport).count(1)
        expect(transport.getPeer()).result(FakedPeer()).count(1)
        expect(user.username).result('username').count(1)
        expect(user.get_upload_job(ARGS, KWARGS)).result(upload_job)
        expect(upload_job.registerProducer(transport))

        self.mocker.replay()
        response._start()
        self.assertEqual(response.state, PutContentResponse.states.uploading)

    def test_upload_last_good_state(self):
        """Test that last_good_state_ts gets updated as expected."""
        upload_job = self.FakeUploadJob()
        upload_job.offset = 0
        upload_job.upload_id = str(uuid.uuid4())
        user = self.mocker.mock()
        self.response.protocol.user = user
        transport = self.mocker.mock()
        self.server.transport = transport
        response = self.mocker.patch(self.response)

        expect(response._log_start())
        expect(response.upload_job).result(upload_job).count(1, None)
        expect(response.protocol.transport).result(transport).count(1)
        expect(transport.getPeer()).result(FakedPeer()).count(1, None)
        expect(user.username).result('username').count(1, None)
        expect(user.get_upload_job(ARGS, KWARGS)).result(upload_job)
        expect(upload_job.registerProducer(transport))

        self.mocker.replay()
        before = time.time()
        time.sleep(.1)
        response.start()
        after = time.time()
        self.assertTrue(before < response.last_good_state_ts <= after)
        time.sleep(.1)

        bytes_msg = protocol_pb2.Message()
        bytes_msg.type = protocol_pb2.Message.BYTES
        bytes_msg.bytes.bytes = "123"

        response.processMessage(bytes_msg)

        self.assertTrue(response.last_good_state_ts > after)
        self.assertEqual("123", upload_job.bytes)
        self.assertEqual(response.state, PutContentResponse.states.uploading)

    def test__cancel_uploadjob_cancelled(self):
        """Test cancel cancelling the upload_job."""
        self.response.state = PutContentResponse.states.canceling
        cancel_message = protocol_pb2.Message()
        cancel_message.id = 123
        self.response.cancel_message = cancel_message

        mocker = Mocker()
        upload_job = mocker.mock()
        self.response.upload_job = upload_job
        expect(upload_job.cancel())

        with mocker:
            self.response._cancel()
        self.assertTrue(self.handler.check_debug("Canceling the upload job"))

    def test__cancel_uploadjob_cancel_None(self):
        """Test cancel not having an upload_job."""
        self.response.state = PutContentResponse.states.canceling
        cancel_message = protocol_pb2.Message()
        cancel_message.id = 123
        self.response.cancel_message = cancel_message

        assert self.response.upload_job is None
        self.response._cancel()
        self.assertFalse(self.handler.check_debug("Canceling the upload job"))

    def test__cancel_uploadjob_stopped(self):
        """Test cancel cancelling the upload_job."""
        assert self.response.state != PutContentResponse.states.canceling
        mocker = Mocker()
        upload_job = mocker.mock()
        self.response.upload_job = upload_job
        expect(upload_job.stop())

        with mocker:
            self.response._cancel()
        self.assertTrue(self.handler.check_debug(
                        "Stoping the upload job after a cancel"))

    def test__cancel_uploadjob_stop_None(self):
        """Test cancel not having an upload_job."""
        assert self.response.state != PutContentResponse.states.canceling
        assert self.response.upload_job is None
        self.response._cancel()
        self.assertFalse(self.handler.check_debug(
                         "Stoping the upload job after a cancel"))

    def test__cancel_answer_client_yes(self):
        """Test answer is sent to the client because canceling."""
        self.response.state = PutContentResponse.states.canceling

        # set up the original message
        cancel_message = protocol_pb2.Message()
        cancel_message.id = 123
        self.response.cancel_message = cancel_message

        # be sure to close the request
        called = []
        self.response.done = lambda: called.append(True)

        # call and check
        self.response._cancel()
        self.assertTrue(called)
        self.assertEqual(self.response.state, PutContentResponse.states.done)
        self.assertEqual(self.last_msg.type, protocol_pb2.Message.CANCELLED)
        self.assertEqual(self.last_msg.id, 123)

    def test__cancel_answer_client_no(self):
        """Test answer is not sent to the client if not canceling."""
        assert self.response.state != PutContentResponse.states.canceling

        # be sure to close the request
        called = []
        self.response.done = lambda: called.append(True)

        # call and check
        self.response._cancel()
        self.assertTrue(called)
        self.assertEqual(self.response.state, PutContentResponse.states.done)

    def test__cancel_always_move_to_canceling(self):
        """Test that we always move to canceling state."""
        assert self.response.state == PutContentResponse.states.init

        mocker = Mocker()
        upload_job = mocker.mock()
        self.response.upload_job = upload_job

        # be sure to close the request
        called = []
        self.response.done = lambda: called.append(True)

        def save_state():
            """Save current state."""
            called.append(self.response.state)

        expect(upload_job.stop()).call(save_state).count(1)
        # call and check
        with mocker:
            self.response._cancel()

        self.assertTrue(self.handler.check_debug("Request canceled (in INIT)"))
        self.assertEqual(len(called), 2)
        self.assertEqual(called[0], PutContentResponse.states.canceling)
        self.assertEqual(called[1], True)
        self.assertEqual(self.response.state, PutContentResponse.states.done)

    def test_genericerror_log_error(self):
        """Generic error logs when called with an error."""
        assert self.response.state == PutContentResponse.states.init
        self.response._generic_error(NameError('foo'))
        self.assertTrue(self.handler.check_warning("Error while in INIT",
                                                   "NameError", "foo"))

    def test_genericerror_log_failure(self):
        """Generic error logs when called with a failure."""
        assert self.response.state == PutContentResponse.states.init
        self.response._generic_error(Failure(NameError('foo')))
        self.assertTrue(self.handler.check_warning("Error while in INIT",
                                                   "NameError", "foo"))

    def test_genericerror_already_in_error(self):
        """Just log if already in error."""
        self.response.state = PutContentResponse.states.error
        called = []
        self.response._send_protocol_error = called.append
        self.response._generic_error(NameError('foo'))
        self.assertFalse(called)
        self.assertTrue(self.handler.check_warning("Error while in ERROR",
                                                   "NameError", "foo"))

    def test_genericerror_already_in_done(self):
        """Just log if already in done."""
        self.response.state = PutContentResponse.states.done
        called = []
        self.response._send_protocol_error = called.append
        self.response._generic_error(NameError('foo'))
        self.assertFalse(called)
        self.assertTrue(self.handler.check_warning("Error while in DONE",
                                                   "NameError", "foo"))

    def test_genericerror_no_uploadjob(self):
        """Don't stop the upload job if doesn't have one."""
        assert self.response.upload_job is None
        self.response._generic_error(NameError('foo'))
        self.assertFalse(self.handler.check_debug(
                         "Stoping the upload job after an error"))

    def test_genericerror_stop_uploadjob(self):
        """Stop the upload job if has one."""
        mocker = Mocker()
        upload_job = mocker.mock()
        self.response.upload_job = upload_job
        expect(upload_job.stop())
        expect(upload_job.inflated_size_hint)
        expect(upload_job.unregisterProducer())

        with mocker:
            self.response._generic_error(NameError('foo'))
        self.assertTrue(self.handler.check_debug(
                        "Stoping the upload job after an error"))

    def test_genericerror_try_again_ok(self):
        """Test how a TRY_AGAIN error is handled."""
        # several patches
        self.response.upload_job = self.FakeUploadJob()
        called = []
        self.response._log_exception = lambda *a: called.extend(a)
        self.response.done = lambda: called.append('done')
        self.patch(PutContentResponse, '_try_again_errors', (NameError,))

        # call and test
        exc = NameError('foo')
        self.response._generic_error(exc)
        e, done = called
        self.assertEqual(e.__class__, errors.TryAgain)
        self.assertEqual(e.orig_error, exc)
        self.assertEqual(done, 'done')

        self.assertIn('unregisterProducer', self.response.upload_job.called)
        self.assert_oopsless()

    def test_genericerror_try_again_handling_error(self):
        """Test how a TRY_AGAIN error is handled."""
        # several patches
        self.response.upload_job = self.FakeUploadJob()
        called = []
        exc = ValueError("test error")

        def fake(*a):
            """Fake function to raise an error."""
            raise exc

        self.response._log_exception = fake
        self.response.done = lambda: called.append('done')
        self.response.internal_error = lambda f: called.append(('error', f))
        self.patch(PutContentResponse, '_try_again_errors', (NameError,))

        # call and test
        self.response._generic_error(NameError('foo'))
        called, = called
        self.assertEqual(called[0], 'error')
        self.assertEqual(called[1].value, exc)
        self.assertIn('unregisterProducer', self.response.upload_job.called)

    def test_genericerror_try_again_done_error(self):
        """Test how a TRY_AGAIN error is handled."""
        # several patches
        self.response.upload_job = self.FakeUploadJob()
        called = []
        exc = ValueError("test error")

        def fake_done(*a):
            """Fake function to raise an error."""
            raise Exception("Unexpected done call")

        self.response._log_exception = lambda *args: called.append('handle')
        self.response.done = fake_done
        self.response.internal_error = lambda f: called.append(('error', f))
        self.patch(PutContentResponse, '_try_again_errors', (NameError,))

        # call and test
        self.response._generic_error(exc)
        handle, done = called
        self.assertEqual(handle, 'handle')
        self.assertEqual(done[0], 'error')
        self.assertEqual(done[1].value, exc)
        self.assertIn('unregisterProducer', self.response.upload_job.called)

    def test_try_again_handling(self):
        """Test how a TRY_AGAIN error is handled."""
        # several patches
        self.response.upload_job = self.FakeUploadJob()
        size_hint = self.response.upload_job.inflated_size_hint
        mocker = Mocker()
        metrics = mocker.mock()
        self.response.protocol.factory.metrics = metrics
        # expect(metrics.gauge("upload_error.TRY_AGAIN.NameError", size_hint))

        # call and test
        with mocker:
            self.response._log_exception(errors.TryAgain(NameError('foo')))
        self.assertTrue(self.handler.check_debug("TryAgain", "NameError",
                                                 str(size_hint)))

    def test_try_again_on_what(self):
        """Assure the list of retryable errors is ok."""
        tryagain = PutContentResponse._try_again_errors
        expected_try_again = (s3lib.ProducerStopped,
                              dataerror.RetryLimitReached,
                              errors.BufferLimit)
        self.assertEqual(tryagain, expected_try_again)

    def test_genericerror_requestcancelled_canceling(self):
        """Test how a RequestCancelledError error is handled when canceling."""
        self.response.state = PutContentResponse.states.canceling
        called = []
        self.response._send_protocol_error = called.append
        self.response.done = called.append
        self.response._generic_error(request.RequestCancelledError('message'))
        self.assertFalse(called)
        self.assertTrue(self.handler.check_debug("Request cancelled: message"))

    def test_genericerror_requestcancelled_other(self):
        """Test how a RequestCancelledError error is handled in other state."""
        assert self.response.state != PutContentResponse.states.canceling
        self.response.upload_job = self.FakeUploadJob()
        response = self.mocker.patch(self.response)
        failure = Failure(request.RequestCancelledError("foo"))
        # expect(response.protocol.factory.metrics.gauge(
        #     "upload_error.RequestCancelledError", 1000))
        expect(response._send_protocol_error(failure))
        expect(response.done())

        self.mocker.replay()
        response._generic_error(failure)
        self.assertEqual(response.state, PutContentResponse.states.error)
        self.assertTrue(self.handler.check_debug("RequestCancelledError",
                                                 str(1000)))

    def test_genericerror_other_errors_ok(self):
        """Generic error handling."""
        self.response.upload_job = self.FakeUploadJob()
        response = self.mocker.patch(self.response)
        failure = Failure(NameError("foo"))
        # expect(response.protocol.factory.metrics.gauge(
        #     "upload_error.NameError", 1000))
        expect(response._send_protocol_error(failure))
        expect(response.done())

        self.mocker.replay()
        response._generic_error(failure)
        self.assertEqual(response.state, PutContentResponse.states.error)
        self.assertIn('unregisterProducer', self.response.upload_job.called)
        self.assertTrue(self.handler.check_debug("NameError", str(1000)))

    def test_genericerror_other_errors_problem_sendprotocolerror(self):
        """Handle problems in the _send_protocol_error() call."""
        response = self.mocker.patch(self.response)
        expect(response._send_protocol_error(ARGS)).throw(Exception("broken"))
        internal = []
        self.response.internal_error = internal.append

        self.mocker.replay()
        response._generic_error(ValueError('error'))
        self.assertEqual(response.state, PutContentResponse.states.error)
        error = internal[0].value
        self.assertTrue(isinstance(error, Exception))
        self.assertEqual(str(error), "broken")

    def test_genericerror_other_errors_problem_done(self):
        """Handle problems in the done() call."""
        response = self.mocker.patch(self.response)
        expect(response._send_protocol_error(ARGS)).result(defer.succeed(True))
        expect(response.done()).throw(Exception("broken"))
        internal = []
        self.response.internal_error = internal.append

        self.mocker.replay()
        response._generic_error(ValueError('error'))
        self.assertEqual(response.state, PutContentResponse.states.error)
        error = internal[0].value
        self.assertTrue(isinstance(error, Exception))
        self.assertEqual(str(error), "broken")

    @defer.inlineCallbacks
    def test__get_upload_job(self):
        """Test get_upload_job."""
        share_id = uuid.uuid4()
        upload_id = str(uuid.uuid4())
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PUT_CONTENT
        message.put_content.share = str(share_id)
        message.put_content.node = 'abc'
        message.put_content.previous_hash = 'p_hash'
        message.put_content.hash = 'hash'
        message.put_content.crc32 = 1
        message.put_content.size = 2
        message.put_content.deflated_size = 3
        message.put_content.magic_hash = 'magic'
        message.put_content.upload_id = upload_id
        response = PutContentResponse(self.server, message)
        response.protocol.working_caps = []
        response.protocol.session_id = 'abc'
        response.protocol.user = self.mocker.mock()
        response.protocol.user.get_upload_job(
            share_id, 'abc', 'p_hash', 'hash', 1, 2, 3,
            session_id='abc', magic_hash='magic', upload_id=upload_id)
        self.mocker.result('TheUploadJob')
        self.mocker.replay()
        uploadjob = yield response._get_upload_job()
        self.assertEqual(uploadjob, 'TheUploadJob')

    def test_processmessage_uploading_ok(self):
        """Process a message while uploading, all ok."""
        self.response.state = PutContentResponse.states.uploading
        response = self.mocker.patch(self.response)

        # all message types
        all_msgs = []
        for mtype in "CANCEL_REQUEST EOF BYTES".split():
            message = protocol_pb2.Message()
            message.type = getattr(protocol_pb2.Message, mtype)
            expect(response._process_while_uploading(message))
            all_msgs.append(message)

        self.mocker.replay()
        for msg in all_msgs:
            response._processMessage(msg)

    def test_processmessage_uploading_error(self):
        """Process a message while uploading, explodes."""
        self.response.state = PutContentResponse.states.uploading
        response = self.mocker.patch(self.response)

        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.BYTES
        failure = Exception('foo')
        expect(response._process_while_uploading(message)).throw(failure)
        expect(response._generic_error(failure))

        self.mocker.replay()
        response._processMessage(message)

    def test_processmessage_uploading_bad_message(self):
        """Process a bad message while uploading."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        mtyp = protocol_pb2.Message.PUT_CONTENT
        message.type = mtyp
        self.response._processMessage(message)
        self.assertTrue(self.handler.check_error("unknown message", str(mtyp)))

    def test_processmessage_init_cancel_ok(self):
        """Process a cancel request while in init, all ok."""
        self.response.state = PutContentResponse.states.init
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.CANCEL_REQUEST
        response = self.mocker.patch(self.response)
        expect(response.cancel())

        self.mocker.replay()
        response._processMessage(message)
        self.assertEqual(response.state, PutContentResponse.states.canceling)
        self.assertEqual(response.cancel_message, message)

    def test_processmessage_init_cancel_error(self):
        """Process a cancel request while in init, explodes."""
        self.response.state = PutContentResponse.states.init
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.CANCEL_REQUEST
        response = self.mocker.patch(self.response)
        failure = Exception('foo')
        expect(response.cancel()).throw(failure)
        expect(response._generic_error(failure))

        self.mocker.replay()
        response._processMessage(message)

    def test_processmessage_init_not_cancel(self):
        """Process other requests while in init."""
        self.response.state = PutContentResponse.states.init
        cancel_called = []
        self.response.cancel = lambda: cancel_called.append(True)

        # all message types except cancel
        all_msgs = []
        for mtype in "EOF BYTES".split():
            message = protocol_pb2.Message()
            message.type = getattr(protocol_pb2.Message, mtype)
            self.response._processMessage(message)
            all_msgs.append(message.type)

        for mtype in all_msgs:
            self.assertTrue(self.handler.check_warning("Received out-of-order",
                                                       "INIT", str(mtype)))
        self.assertFalse(cancel_called)

    def test_processmessage_error(self):
        """Process all requests while in error."""
        self.response.state = PutContentResponse.states.error

        # all message types
        for mtype in "CANCEL_REQUEST EOF BYTES".split():
            message = protocol_pb2.Message()
            message.type = getattr(protocol_pb2.Message, mtype)
            self.response._processMessage(message)

        self.assertFalse(self.handler.check_warning("Received out-of-order"))

    def test_processmessage_otherstates(self):
        """Process all requests while in other states."""
        for state in "commiting canceling done".split():
            for mtype in "CANCEL_REQUEST EOF BYTES".split():
                self.response.state = getattr(PutContentResponse.states, state)
                message = protocol_pb2.Message()
                message.type = getattr(protocol_pb2.Message, mtype)
                self.response._processMessage(message)
                chk = "Received out-of-order", state.upper(), str(message.type)
                self.assertTrue(self.handler.check_warning(*chk))

    def test_processwhileuploading_cancel(self):
        """Got a cancel request while uploading."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.CANCEL_REQUEST
        cancel_called = []
        self.response.cancel = lambda: cancel_called.append(True)

        self.response._process_while_uploading(message)
        self.assertEqual(self.response.cancel_message, message)
        self.assertEqual(self.response.state,
                         PutContentResponse.states.canceling)
        self.assertTrue(cancel_called)

    def test_processwhileuploading_eof_ok(self):
        """Got an eof while uploading, all finished ok."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.EOF
        self.response.upload_job = self.FakeUploadJob()

        # check what is called
        called = []
        self.response._commit_uploadjob = lambda r: called.append(('commt', r))
        self.response._generic_error = lambda _: called.append('error')

        # call, it should change the state and set up the callbacks
        self.response._process_while_uploading(message)
        self.assertEqual(self.response.state,
                         PutContentResponse.states.commiting)

        # trigger the deferred ok, it should just commit
        result = object()
        self.response.upload_job.deferred.callback(result)
        self.assertEqual(called, [('commt', result)])

    def test_processwhileuploading_eof_error_uploadjob(self):
        """Got an eof while uploading, but it ended in error."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.EOF
        self.response.upload_job = self.FakeUploadJob()

        # check what is called
        called = []
        self.response._commit_uploadjob = lambda _: called.append('commit')
        self.response._generic_error = lambda f: called.append(('error', f))

        # call, it should change the state and set up the callbacks
        self.response._process_while_uploading(message)
        self.assertEqual(self.response.state,
                         PutContentResponse.states.commiting)

        # trigger the deferred with a failure, it will not
        # commit, but handle the error
        failure = Failure(Exception())
        self.response.upload_job.deferred.errback(failure)
        self.assertEqual(called, [('error', failure)])

    def test_processwhileuploading_eof_error_commiting(self):
        """Got an eof while uploading, got an error while commiting."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.EOF
        self.response.upload_job = self.FakeUploadJob()

        # check what is called
        called = []
        failure = Failure(Exception())
        self.response._commit_uploadjob = \
            lambda r: called.append(('commit', r)) or failure
        self.response._generic_error = lambda f: called.append(('error', f))

        # call, it should change the state and set up the callbacks
        self.response._process_while_uploading(message)
        self.assertEqual(self.response.state,
                         PutContentResponse.states.commiting)

        # trigger the deferred with a failure, it will not
        # commit, but handle the error
        result = object()
        self.response.upload_job.deferred.callback(result)
        self.assertEqual(called, [('commit', result), ('error', failure)])

    def test_processwhileuploading_bytes(self):
        """Got some bytes while uploading."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.BYTES
        message.bytes.bytes = "foobar"
        self.response.upload_job = self.FakeUploadJob()
        prv_transferred = self.response.transferred

        self.response._process_while_uploading(message)
        self.assertEqual(self.response.transferred, prv_transferred + 6)
        self.assertEqual(self.response.state,
                         PutContentResponse.states.uploading)
        self.assertEqual(self.response.upload_job.bytes, "foobar")

    def test_processwhileuploading_strange(self):
        """Got other message while uploading."""
        self.response.state = PutContentResponse.states.uploading
        message = protocol_pb2.Message()
        message.type = protocol_pb2.Message.PUT_CONTENT
        self.response._process_while_uploading(message)
        self.assertTrue(self.handler.check_error("Received unknown message",
                                                 str(message.type)))

    def test_commituploadjob_not_commiting(self):
        """Assure we're still commiting when we reach this."""
        self.response.state = PutContentResponse.states.error
        called = []
        self.response.queue_action = lambda *a, **kw: called.append(True)
        self.response._commit_uploadjob('result')
        self.assertFalse(called)

    def test_commituploadjob_all_ok(self):
        """Normal commiting behaviour."""
        self.response.state = PutContentResponse.states.commiting
        response = self.mocker.patch(self.response)
        expect(response.queue_action(ARGS)).result(defer.succeed(35))
        expect(response.done())

        self.mocker.replay()
        response._commit_uploadjob('result')
        self.assertEqual(response.state, PutContentResponse.states.done)
        self.assertEqual(self.last_msg.type, protocol_pb2.Message.OK)
        self.assertEqual(self.last_msg.new_generation, 35)

    def test_commituploadjob_ok_but_canceled_by_framework(self):
        """Commit started but was canceled while waiting for queued commit."""
        self.response.state = PutContentResponse.states.commiting
        response = self.mocker.patch(self.response)
        node = FakeNode()

        def state_changed_to_cancel(response):
            """Change state to cancel before proceeding."""
            self.response.state = PutContentResponse.states.canceling
            return defer.succeed(node)
        expect(response.queue_action(ARGS)).call(state_changed_to_cancel)
        # Don't expect done() or any response to client
        expect(response.done()).count(0)
        expect(response.sendMessage(ANY)).count(0)

        self.mocker.replay()
        response._commit_uploadjob('result')
        self.assertEqual(response.state, PutContentResponse.states.canceling)

    def test_commit_canceled_in_queued_job(self):
        """Commit called but canceled before queued job executes."""
        self.response.upload_job = self.mocker.mock()
        # Actual commit will not be called
        expect(self.response.upload_job.commit(ANY)).count(0)
        self.response.state = PutContentResponse.states.commiting
        response = self.mocker.patch(self.response)
        # Patched queue_action changes state then runs the function

        def cancel_then_run_callback(f):
            """Change state to cancel, then call the function."""
            self.response.state = PutContentResponse.states.canceling
            f()
        expect(response.queue_action(ARGS)).call(cancel_then_run_callback)
        # Don't expect done() or any response to client
        expect(response.done()).count(0)
        expect(response.sendMessage(ANY)).count(0)

        self.mocker.replay()
        response._commit_uploadjob('result')
        self.assertEqual(response.state, PutContentResponse.states.canceling)

    def test_startupload_normal(self):
        """Normal behaviour for the start upload."""
        self.response.state = PutContentResponse.states.init
        response = self.mocker.patch(self.response)
        upload_job = self.FakeUploadJob()
        expect(response._get_upload_job()).result(upload_job)
        expect(response.upload_job.deferred.addErrback(
            self.response._generic_error))
        expect(response.upload_job.connect()).result(defer.succeed(True))
        expect(response.protocol.release(self.response))
        expect(response._send_begin())

        self.mocker.replay()
        response._start_upload()
        self.assertEqual(response.state, PutContentResponse.states.uploading)
        self.assertEqual(['registerProducer'], upload_job.called)

    def _test_startupload_canceling_while_getting_uploadjob(self, state):
        """State changes while waiting for the upload job."""
        self.response.state = PutContentResponse.states.init
        d = defer.Deferred()
        self.response._get_upload_job = lambda: d
        self.response._start_upload()

        # before releasing the deferred, change the state
        self.response.state = state
        upload_job = self.FakeUploadJob()
        d.callback(upload_job)
        self.assertEqual(upload_job.called, ['cancel'])   # not connect
        self.assertTrue(self.handler.check_debug(
                        "Manually canceling the upload job (in %s)" % state))

    def test_startupload_done(self):
        """State changes to done while getting the upload job."""
        state = PutContentResponse.states.done
        self._test_startupload_canceling_while_getting_uploadjob(state)

    def test_startupload_canceling(self):
        """State changes to canceling while getting the upload job."""
        state = PutContentResponse.states.canceling
        self._test_startupload_canceling_while_getting_uploadjob(state)

    def test__send_begin(self):
        """Test sendbegin."""
        self.response.upload_job = self.FakeUploadJob()
        self.response.upload_job.offset = 10
        self.response.upload_job.upload_id = 12
        self.response._send_begin()
        self.assertEqual(self.last_msg.type,
                         protocol_pb2.Message.BEGIN_CONTENT)
        self.assertEqual(self.last_msg.begin_content.offset, 10)
        self.assertEqual(self.last_msg.begin_content.upload_id, '12')

        metrics = self.response.protocol.factory.metrics
        upload_type = self.response.upload_job.__class__.__name__
        name = "%s.upload.begin" % (upload_type,)
        name = metrics.fully_qualify_name(name)
        self.assertTrue(isinstance(metrics._metrics[name],
                        MeterMetric))
        name = "%s.upload" % (upload_type,)
        name = metrics.fully_qualify_name(name)
        self.assertTrue(isinstance(metrics._metrics[name],
                        GaugeMetric))
        self.assertTrue(self.handler.check_debug(upload_type, "begin content",
                                                 "from offset 10"))

    def test__send_begin_new_upload_id(self):
        """Test sendbegin when the upload_id received is invalid."""
        self.response.upload_job = self.FakeUploadJob()
        self.response.upload_job.offset = 0
        self.response.upload_job.upload_id = 12
        # the client sent an upload_id, but it's different from the one we got
        # from content.py
        self.response.source_message.put_content.upload_id = '11'
        self.response._send_begin()
        self.assertEqual(self.last_msg.type,
                         protocol_pb2.Message.BEGIN_CONTENT)
        self.assertEqual(self.last_msg.begin_content.offset, 0)
        self.assertEqual(self.last_msg.begin_content.upload_id, '12')

        upload_type = self.response.upload_job.__class__.__name__
        self.assertTrue(self.handler.check_debug(upload_type, "begin content",
                                                 "from offset 0"))

    def test_putcontent_double_done(self):
        """Double call to self.done()."""
        self.response.state = PutContentResponse.states.init
        d = defer.Deferred()
        self.response._get_upload_job = lambda: d
        self.response._start_upload()

        # before releasing the deferred, change the state
        upload_job = self.FakeUploadJob()
        upload_job.offset = 1
        upload_job.upload_id = 1
        d.callback(upload_job)
        self.response.done()
        called = []
        self.response.error = called.append
        self.response.done()
        self.assertEqual(called, [])
        msg = ("runWithWarningsSuppressed -> test_method_wrapper -> "
               "test_putcontent_double_done: called done() finished=True")
        self.assertTrue(self.handler.check_warning(msg))

    def test_putcontent_done_after_error(self):
        """Double call to self.done()."""
        self.response.state = PutContentResponse.states.init
        d = defer.Deferred()
        self.response._get_upload_job = lambda: d
        self.response._start_upload()

        # before releasing the deferred, change the state
        upload_job = self.FakeUploadJob()
        upload_job.offset = 1
        upload_job.upload_id = 1
        d.callback(upload_job)
        self.response.error(Failure(ValueError("foo")))
        called = []
        self.response.error = called.append
        self.response.done()
        self.assertEqual(called, [])
        msg = ("runWithWarningsSuppressed -> test_method_wrapper -> "
               "test_putcontent_done_after_error: called done() finished=True")
        self.assertTrue(self.handler.check_warning(msg))


class QuerySetCapsResponseTestCase(SimpleRequestResponseTestCase):
    """Test the QuerySetCapsResponse class."""

    response_class = QuerySetCapsResponse


class MoveResponseTestCase(SimpleRequestResponseTestCase):
    """Test the MoveResponse class."""

    response_class = MoveResponse

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')


class MakeResponseTestCase(SimpleRequestResponseTestCase):
    """Test the MakeResponse class."""

    response_class = MakeResponse

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'sync_activity')


class FreeSpaceResponseTestCase(SimpleRequestResponseTestCase):
    """Test the FreeSpaceResponse class."""

    response_class = FreeSpaceResponse


class AccountResponseTestCase(SimpleRequestResponseTestCase):
    """Test the AccountResponse class."""

    response_class = AccountResponse


class AuthenticateResponseTestCase(SimpleRequestResponseTestCase):
    """Test the AuthenticateResponse class."""

    response_class = AuthenticateResponse

    def test_user_activity_indicator(self):
        """Check the user_activity value."""
        self.assertEqual(self.response_class.user_activity, 'connected')

    def test_set_user(self):
        """Check that the user is set after auth."""
        user = FakeUser()
        mocker = Mocker()

        # set up a fake auth
        auth_provider = mocker.mock()
        expect(auth_provider.authenticate).result(lambda *a, **kw: user)
        self.response.protocol.factory.auth_provider = auth_provider

        called = []
        self.response.protocol.set_user = lambda *a, **kw: called.append(a)

        with mocker:
            self.response._process()

        self.assertEqual(called, [(user,)])

    def _test_client_metadata(self, expected, expected_metrics):
        """Test client metadata handling in AuthenticateResponse."""
        user = FakeUser()
        mocker = Mocker()
        # set up a fake auth
        auth_provider = mocker.mock()
        expect(auth_provider.authenticate).result(lambda *a, **kw: user)
        self.response.protocol.factory.auth_provider = auth_provider
        metrics_called = []
        self.patch(self.response.protocol.factory.metrics, 'meter',
                   lambda *a: metrics_called.append(a))
        with mocker:
            self.response._process()
        self.assertEqual(metrics_called, expected_metrics)

    def test_client_metadata_valid(self):
        """Test client metadata handling in AuthenticateResponse."""
        md = self.response.source_message.metadata.add()
        md.key = "platform"
        md.value = "linux"
        md = self.response.source_message.metadata.add()
        md.key = "version"
        md.value = "42"
        expected = [("client.platform.linux",), ("client.version.42",)]
        expected_metrics = [
            ("client.platform.linux", 1),
            ("client.version.42", 1)
        ]
        self._test_client_metadata(expected, expected_metrics)

    def test_client_metadata_invalid_value(self):
        """Test client metadata handling in AuthenticateResponse."""
        md = self.response.source_message.metadata.add()
        md.key = "platform"
        md.value = "Windows XP-SP3 6.1.2008"
        md = self.response.source_message.metadata.add()
        md.key = "version"
        md.value = "1.42"
        expected = [("client.platform.Windows_XP_SP3_6_1_2008",),
                    ("client.version.1_42",)]
        expected_metrics = [
            ("client.platform.Windows_XP_SP3_6_1_2008", 1),
            ("client.version.1_42", 1)
        ]
        self._test_client_metadata(expected, expected_metrics)


class GetDeltaResponseTestCase(SimpleRequestResponseTestCase):
    """Test the GetDeltaResponse class."""

    response_class = GetDeltaResponse

    def test_cooperative_send_delta_info(self):
        """Test that send_delta_info isn't blocking."""
        d = self.response.send_delta_info([], '')
        self.assertTrue(isinstance(d, defer.Deferred))
        # check if _send_delta_info returns a generator
        gen = self.response._send_delta_info([], '')
        self.assertTrue(hasattr(gen, 'next'))
        # check if send_delta_info use the cooperator
        called = []
        real_cooperate = task.cooperate

        def cooperate(iterator):
            """Intercept the call to task.cooperate."""
            called.append(iterator)
            return real_cooperate(iterator)
        self.patch(task, 'cooperate', cooperate)
        self.response.send_delta_info([], '')
        self.assertEqual(len(called), 1)
        self.assertTrue(hasattr(called[0], 'next'))

    def test_reset_send_delta_info_counter(self):
        """Test that the count is reset on each iteration."""
        old_max_delta_info = config.api_server.max_delta_info
        config.api_server['max_delta_info'] = 5
        self.addCleanup(setattr, config.api_server,
                        'max_delta_info', old_max_delta_info)
        # create a few fake nodes
        nodes = []
        now = datetime.datetime.utcnow()
        for i in range(10):
            node = FakeNode()
            node.id = str(uuid.uuid4())
            node.parent_id = str(uuid.uuid4())
            node.generation = 100
            node.name = u"node_%s" % i
            node.is_live = True
            node.is_file = True
            node.is_public = True
            node.content_hash = 'sha1:foo'
            node.crc32 = 10
            node.size = 1024
            node.last_modified = int(time.mktime(now.timetuple()))
            nodes.append(node)
        gen = self.response._send_delta_info(nodes, 'share_id')
        gen.next()
        self.assertEqual(gen.gi_frame.f_locals['count'], 0)

    @defer.inlineCallbacks
    def test_process_set_length(self):
        """Set length attribute while processing."""
        mocker = Mocker()

        # fake message
        message = mocker.mock()
        expect(message.get_delta.share).count(2).result('')
        expect(message.get_delta.from_generation).result(10)
        self.response.source_message = message

        # fake user
        user = mocker.mock()
        nodes = [FakeNode(), FakeNode()]
        expect(user.get_delta(None, 10, KWARGS)).result((nodes, 12, 123))
        self.response.protocol.user = user

        with mocker:
            yield self.response._process()

        self.assertEqual(self.response.length, 2)


class RescanFromScratchResponseTestCase(SimpleRequestResponseTestCase):
    """Test the RescanFromScratchResponse class."""

    # subclass so we have a __dict__ and can patch it.
    response_class = types.ClassType(RescanFromScratchResponse.__name__,
                                     (RescanFromScratchResponse,), {})

    def test_cooperative_send_delta_info(self):
        """Test that send_delta_info isn't blocking."""
        d = self.response.send_delta_info([], '')
        self.assertTrue(isinstance(d, defer.Deferred))
        # check if _send_delta_info returns a generator
        gen = self.response._send_delta_info([], '')
        self.assertTrue(hasattr(gen, 'next'))
        # check if send_delta_info use the cooperator
        called = []
        real_cooperate = task.cooperate

        def cooperate(iterator):
            """Intercept the call to task.cooperate."""
            called.append(iterator)
            return real_cooperate(iterator)
        self.patch(task, 'cooperate', cooperate)
        self.response.send_delta_info([], '')
        self.assertEqual(len(called), 1)
        self.assertTrue(hasattr(called[0], 'next'))

    @defer.inlineCallbacks
    def test_chunked_get_from_scratch(self):
        """Get the nodes list in chunks."""
        config.api_server['get_from_scratch_limit'] = 5
        # build fake nodes
        nodes = []
        now = datetime.datetime.now()
        for i in range(20):
            node = FakeNode()
            node.id = str(uuid.uuid4())
            node.parent_id = str(uuid.uuid4())
            node.path = u"/"
            node.generation = i
            node.name = u"node_%s" % i
            node.is_live = True
            node.is_file = True
            node.is_public = True
            node.content_hash = 'sha1:foo'
            node.crc32 = 10
            node.size = 1024
            node.last_modified = int(time.mktime(now.timetuple()))
            nodes.append(node)
        # set required caps
        self.response.protocol.working_caps = server.PREFERRED_CAP
        mocker = Mocker()
        user = mocker.mock()
        self.patch(self.response, "send_delta_info", lambda *a: None)
        self.response.protocol.user = user
        # expect 3 calls to get_from_scratch
        expect(user.get_from_scratch(
            None, limit=5)
        ).count(1).result((nodes[:10], 20, 100))
        expect(user.get_from_scratch(
            None, limit=5, max_generation=20,
            start_from_path=("/", "node_9"))
        ).count(1).result((nodes[10:], 20, 100))
        expect(user.get_from_scratch(
            None, limit=5, max_generation=20,
            start_from_path=("/", "node_19"))
        ).count(1).result(([], 20, 100))

        mocker.replay()
        yield self.response._process()

    @defer.inlineCallbacks
    def test_process_set_length(self):
        """Set length attribute while processing."""
        mocker = Mocker()

        # fake message
        message = mocker.mock()
        expect(message.get_delta.share).count(2).result('')
        self.response.source_message = message

        # fake user
        user = mocker.mock()
        nodes = [[FakeNode(), FakeNode()], []]
        action = expect(user.get_from_scratch(None, KWARGS))
        action.count(2).call(lambda *a, **k: (nodes.pop(0), 12, 123))
        self.response.protocol.user = user

        with mocker:
            yield self.response._process()

        self.assertEqual(self.response.length, 2)


class NodeInfoLogsTests(BaseStorageServerTestCase):
    """Check that operations return correct node info."""

    def check(self, response, mes_type, klass=None, mes_name=None, **attrs):
        """Check that get_node_info returns correctly for the message."""
        # build the message
        message = protocol_pb2.Message()
        message.type = getattr(protocol_pb2.Message, mes_type)

        # optionally, has content!
        if mes_name is not None:
            inner = getattr(message, mes_name)
            for name, value in attrs.iteritems():
                setattr(inner, name, value)

        # put it in the request, get node info, and check
        if klass is None:
            klass = StorageServerRequestResponse
        req = klass(self.server, message)
        req.source_message = message
        self.assertEqual(req._get_node_info(), response)

    def test_simple_ones(self):
        """Test all messages without node info."""
        names = (
            'PROTOCOL_VERSION', 'PING', 'AUTH_REQUEST', 'CREATE_UDF',
            'CREATE_SHARE', 'QUERY_CAPS', 'SET_CAPS', 'FREE_SPACE_INQUIRY',
            'ACCOUNT_INQUIRY', 'LIST_VOLUMES', 'LIST_SHARES',
        )
        for name in names:
            self.check(None, name)

    def test_with_nodes(self):
        """Test messages that have node info."""
        data = [
            ('GET_CONTENT', GetContentResponse),
            ('PUT_CONTENT', PutContentResponse),
            ('UNLINK', Unlink),
            ('MOVE', MoveResponse),
        ]
        for name, klass in data:
            self.check("node: 'foo'", name, klass, name.lower(), node='foo')

    def test_with_parent(self):
        """Test messages where the node is the parent."""
        data = [
            ('MAKE_FILE', MakeResponse),
            ('MAKE_DIR', MakeResponse),
        ]
        for name, klass in data:
            self.check("parent: 'foo'", name, klass, 'make', parent_node='foo')

    def test_with_shares(self):
        """Test messages that work on shares."""
        data = [
            ('SHARE_ACCEPTED', ShareAccepted),
            ('DELETE_SHARE', DeleteShare),
        ]
        for name, klass in data:
            impl = os.environ.get('PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION')
            if impl == 'cpp':
                # the cpp implementation always return unicode.
                self.check("share: u'foo'", name, klass,
                           name.lower(), share_id='foo')
            else:
                self.check("share: 'foo'", name, klass,
                           name.lower(), share_id='foo')

    def test_with_volumes(self):
        """Test messages that work on volumes."""
        self.check("volume: 'foo'", 'DELETE_VOLUME', DeleteVolume,
                   'delete_volume', volume='foo')
        self.check("volume: 'foo'", 'GET_DELTA', GetDeltaResponse,
                   'get_delta', share='foo')


class TestLoopingPing(BaseStorageServerTestCase):
    """LoopingPing tests."""

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestLoopingPing, self).setUp()
        self.patch(self.server, 'ping', lambda: defer.succeed(None))

    def test_idle_timeout_enabled(self):
        """Test that idle_timeout is enabled and works."""
        self.server.ping_loop.idle_timeout = 0.1
        self.server.ping_loop.pong_count = 2
        self.server.ping_loop.schedule()
        self.assertTrue(self.shutdown)

    def test_idle_timeout_disabled(self):
        """Test that disbaled idle_timeout."""
        self.server.ping_loop.idle_timeout = 0
        self.server.ping_loop.schedule()
        self.assertFalse(self.shutdown)


class StorageServerFactoryTests(TwistedTestCase):
    """Test the StorageServerFactory class."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set up."""
        yield super(StorageServerFactoryTests, self).setUp()

        MetricsConnector.register_metrics("root", instance=ExtendedMetrics())
        MetricsConnector.register_metrics("user", instance=ExtendedMetrics())
        MetricsConnector.register_metrics("sli", instance=ExtendedMetrics())
        self.factory = StorageServerFactory(None, None, None, None, None)
        self.handler = MementoHandler()
        self.handler.setLevel(logging.DEBUG)
        self.factory.logger.addHandler(self.handler)
        self.addCleanup(self.factory.logger.removeHandler, self.handler)

    def tearDown(self):
        """Tear down."""
        MetricsConnector.unregister_metrics()

    def test_observer_added(self):
        """Test that the observer was added to Twisted logging."""
        self.assertIn(self.factory._deferror_handler,
                      log.theLogPublisher.observers)

    def test_noerror(self):
        """No error, no action."""
        self.factory._deferror_handler(dict(isError=False, message=''))
        self.assertFalse(self.handler.check_error("error"))

    def test_message(self):
        """Just a message."""
        self.factory._deferror_handler(dict(isError=True, message="foobar"))
        self.assertTrue(self.handler.check_error(
            "Unhandled error in deferred", "foobar"))

    def test_failure(self):
        """Received a full failure."""
        f = Failure(ValueError('foobar'))
        self.factory._deferror_handler(dict(isError=True,
                                            failure=f, message=''))
        self.assertTrue(self.handler.check_error(
                        "Unhandled error in deferred",
                        "ValueError", "foobar"))

    def test_trace_users(self):
        """Check trace users are correctly set."""
        # set a specific config to test
        old = config.api_server.trace_users
        config.api_server.trace_users = ['foo', 'bar', 'baz']
        self.addCleanup(setattr, config.api_server, 'trace_users', old)
        factory = StorageServerFactory(None, None, None, None, None)
        self.assertEqual(factory.trace_users, set(['foo', 'bar', 'baz']))


class BytesMessageProducerTests(TwistedTestCase):
    """Test the BytesMessageProducer class."""

    @defer.inlineCallbacks
    def setUp(self):
        """Set up."""
        yield super(BytesMessageProducerTests, self).setUp()
        server = StorageServer()
        req = GetContentResponse(protocol=server,
                                 message=protocol_pb2.Message())
        self.patch(GetContentResponse, 'sendMessage', lambda *a: None)
        self.producer = FakeProducer()
        self.bmp = BytesMessageProducer(self.producer, req)

        self.logger = logging.getLogger("storage.server")
        self.handler = MementoHandler()
        self.handler.setLevel(TRACE)
        self.logger.addHandler(self.handler)

    @defer.inlineCallbacks
    def tearDown(self):
        """Tear down."""
        self.logger.removeHandler(self.handler)
        yield super(BytesMessageProducerTests, self).tearDown()

    def test_resume_log(self):
        """Log when resumed."""
        self.bmp.resumeProducing()
        self.assertTrue(self.handler.check(TRACE,
                        "BytesMessageProducer resumed", str(self.producer)))

    def test_stop_log(self):
        """Log when stopped."""
        self.bmp.stopProducing()
        self.assertTrue(self.handler.check(TRACE,
                        "BytesMessageProducer stopped", str(self.producer)))

    def test_pause_log(self):
        """Log when paused."""
        self.bmp.pauseProducing()
        self.assertTrue(self.handler.check(TRACE,
                        "BytesMessageProducer paused", str(self.producer)))

    def test_transferred_counting(self):
        """Keep count of transferred data."""
        assert self.bmp.request.transferred == 0
        self.bmp.write("foobar")
        self.assertEqual(self.bmp.request.transferred, 6)


class TestLoggerSetup(testcase.TestWithDatabase):
    """Tests for the logging setup."""

    def test_server_logger(self):
        """Test the storage server logger."""
        self.assertEqual(self.service.logger.name,
                         "storage.server")
        self.assertFalse(self.service.logger.propagate)


class TestMetricsSetup(testcase.TestWithDatabase):
    """Tests that metrics are setup from configs properly"""

    @defer.inlineCallbacks
    def tearDown(self):
        """Unregister metrics that the server set up."""
        yield super(TestMetricsSetup, self).tearDown()
        MetricsConnector.unregister_metrics()

    def test_metrics_from_config(self):
        """Test that metrics names get set from the config properly"""
        self.assertEqual("development.filesync.server",
                         MetricsConnector.get_metrics("root").namespace)
        self.assertEqual("development.storage.user_activity",
                         MetricsConnector.get_metrics("user").namespace)
