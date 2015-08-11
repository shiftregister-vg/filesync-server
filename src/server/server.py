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

"""The Storage network server.

Handles all the networking part of the server.
All code here must be non blocking and the messages should never leave
the server's scope.
"""

import collections
import inspect
import logging
import os
import re
import signal
import sys
import time
import urllib
import weakref

from functools import wraps

import psycopg2
import twisted
import twisted.web.error
import oops
import oops_datedir_repo

import metrics.services
import versioninfo
import timeline

from twisted.application.service import MultiService, Service
from twisted.application.internet import TCPServer
from twisted.internet.defer import maybeDeferred, inlineCallbacks
from twisted.internet.protocol import Factory
from twisted.internet import defer, reactor, error, task, stdio
from twisted.python.failure import Failure

from s3lib.s3lib import S3, ProducerStopped

import uuid

from metrics import get_meter
from metrics.metricsconnector import MetricsConnector
from backends.filesync.data import errors as dataerror
from backends.filesync.notifier import notifier
from ubuntuone.storage.server.logger import configure_logger, TRACE
from config import config
from ubuntuone.monitoring.reactor import ReactorInspector
from ubuntuone.storage.rpcdb import inthread
from ubuntuone.storage.server import auth, content, errors, stats
from ubuntuone.storageprotocol import protocol_pb2, request, sharersp
from ubuntuone.supervisor import utils as supervisor_utils

# this is the minimal cap we support (to avoid hardcoding it in the code)
MIN_CAP = frozenset(["no-content", "account-info", "resumable-uploads",
                     "fix462230", "volumes", "generations"])

# these are the capabilities combinations that we support
SUPPORTED_CAPS = set([MIN_CAP])

# this is the capabilities combination that we prefer, the latest one
PREFERRED_CAP = MIN_CAP

# these is where we suggest to reconnect in case we deny the capabilities
SUGGESTED_REDIRS = {
    # frozenset(["example1"]): dict(hostname="fs-3.server.com", port=443)
    # frozenset(["example2"]): dict(srv_record="_https._tcp.fs.server.com")
}

MAX_OOPS_LINE = 300


def install_signal_handlers():
    """Install custom SIGUSR2 handler."""
    reactor.callWhenRunning(signal.signal, signal.SIGUSR2, sigusr2_handler)


def sigusr2_handler(signum, frame):
    """Handle SIGUSR2 to reload the config."""
    import config
    logger = logging.getLogger("storage.server")
    logger.info('Reloading config file')
    config.config = config._Config()


def loglevel(lvl):
    """Make a function that logs at lvl log level."""
    def level_log(self, message, *args, **kwargs):
        """inner."""
        self.log(lvl, message, *args, **kwargs)
    return level_log


def trace_message(function):
    """A decorator to trace incoming messages."""
    def decorator(self, message):
        """inner."""
        self.log.trace_message("IN: ", message)
        function(self, message)
    return decorator


def _prettify_traceback(report, context):
    """Make the traceback nicer."""
    if 'tb_text' in report:
        tb = "Traceback (most recent call last):\n"
        tb += report['tb_text']
        tb += "%s: '%s'" % (report['type'], report['value'])
        report['tb_text'] = tb


def configure_oops():
    """Configure the oopses."""
    oops_config = oops.Config()
    oops_config.on_create.append(_prettify_traceback)
    vers_info = dict(branch_nick=versioninfo.version_info['branch_nick'],
                     revno=versioninfo.version_info['revno'])
    oops_config.template.update(vers_info)
    datedir_repo = oops_datedir_repo.DateDirRepo(config.oops.path,
                                                 inherit_id=True)

    oops_config.publisher = oops.publishers.publish_to_many(
        datedir_repo.publish)
    return oops_config


class StorageLogger(object):
    """Create logs for the server.

    The log format is:
    session_id remote_ip:remote_port username request_type request_id \
    message

    Unknown fields are replaced with a '-'.

    Plus whatever the log handler prepends to the line, normally a timestamp
    plus level name.
    """

    def __init__(self, protocol):
        """Create the logger."""
        self.protocol = protocol

    def log(self, lvl, message, *args, **kwargs):
        """Log."""
        if self.protocol.logger.isEnabledFor(lvl):
            self._log(lvl, message, *args, **kwargs)

    def _log(self, lvl, message, *args, **kwargs):
        """Actually do the real log"""
        msg_format = "%(uuid)s %(remote)s %(userid)s %(message)s"
        extra = {"message": message}
        if self.protocol.user is not None:
            extra["userid"] = urllib.quote(self.protocol.user.username,
                                           ":~/").replace('%', '%%')
        else:
            extra["userid"] = "-"

        # be robust in case we have no transport
        if self.protocol.transport is not None:
            peer = self.protocol.transport.getPeer()
            extra["remote"] = peer.host + ":" + str(peer.port)
        else:
            extra["remote"] = '%s.transport is None' % self.__class__.__name__

        extra["uuid"] = self.protocol.session_id

        message = msg_format % extra
        self.protocol.logger.log(lvl, message, *args, **kwargs)

    def trace_message(self, text, message):
        """Log a message with some pre processing."""
        if self.protocol.logger.isEnabledFor(TRACE):
            if message.type != protocol_pb2.Message.BYTES:
                self.trace(text + (str(message).replace("\n", " ")))

    critical = loglevel(logging.CRITICAL)
    error = loglevel(logging.ERROR)
    warning = loglevel(logging.WARNING)
    info = loglevel(logging.INFO)
    debug = loglevel(logging.DEBUG)
    trace = loglevel(TRACE)


class StorageServerLogger(StorageLogger):
    """Logger for the server."""

    def log(self, lvl, message, *args, **kwargs):
        """Log."""
        message = "- - " + message
        super(StorageServerLogger, self).log(lvl, message, *args, **kwargs)


class StorageRequestLogger(StorageLogger):
    """Logger for requests."""

    def __init__(self, protocol, request):
        """Create the logger."""
        super(StorageRequestLogger, self).__init__(protocol)
        self.request_class_name = request.__class__.__name__
        self.request_id = request.id

    def log(self, lvl, message, *args, **kwargs):
        """Log."""
        message = "%s %s - %s" % (self.request_class_name,
                                  self.request_id, message)
        super(StorageRequestLogger, self).log(lvl, message, *args, **kwargs)


class NotificationRPCTimeoutError(Exception):
    """RPCTimeoutError but during a notification."""


class PoisonException(Exception):
    """An exception class for poison errors."""


class LoopingPing(object):
    """Execute Ping requests in a given interval and expect a response.

    Shutdown the request.RequestHandler if the request takes longer than
    the specified timeout.

    @param interval: the seconds between each Ping
    @param timeout: the seconds to wait for a response before shutting down
                    the request handler
    @param request_handler: a request.RequestHandler instance
    """

    def __init__(self, interval, timeout, idle_timeout, request_handler):
        self.interval = interval
        self.timeout = timeout
        self.idle_timeout = idle_timeout
        self.request_handler = request_handler
        self.shutdown = None
        self.next_loop = None
        self.running = False
        self.pong_count = 0

    def start(self):
        """Create the DelayedCall instances."""
        self.running = True

        self.shutdown = reactor.callLater(
            self.timeout, self._shutdown, reason='No Pong response.')

        self.next_loop = reactor.callLater(self.interval, self.schedule)

    def reset(self):
        """Reset pong count and reschedule."""
        self.pong_count = 0
        self.reschedule()

    def reschedule(self):
        """Reset all delayed calls."""
        if self.shutdown is not None and self.shutdown.active():
            self.shutdown.reset(self.timeout)
        elif self.running:
            self.shutdown = reactor.callLater(
                self.timeout, self._shutdown, reason='No Pong response.')
        if self.next_loop is not None and self.next_loop.active():
            self.next_loop.reset(self.interval)
        elif self.running:
            self.next_loop = reactor.callLater(self.interval, self.schedule)

    @defer.inlineCallbacks
    def schedule(self):
        """Request a Ping and reset the shutdown timeout."""
        yield self.request_handler.ping()
        self.pong_count += 1
        # check if the client is idling for too long
        # if self.idle_timeout is 0,  do nothing.
        idle_time = self.pong_count * self.interval
        if self.idle_timeout != 0 and idle_time >= self.idle_timeout:
            self._shutdown(reason="idle timeout %s" % (idle_time,))
        else:
            self.reschedule()

    def _shutdown(self, reason):
        """Shutdown the request_handler."""
        self.request_handler.log.info("Disconnecting - %s", reason)
        if not self.request_handler.shutting_down:
            self.request_handler.shutdown()

    def stop(self):
        """Stop all the delayed calls."""
        self.running = False
        if self.shutdown is not None and self.shutdown.active():
            self.shutdown.cancel()
        if self.next_loop is not None and self.next_loop.active():
            self.next_loop.cancel()
        for req in self.request_handler.requests.values():
            # cleanup stalled Ping requests
            if req.started and isinstance(req, request.Ping):
                req.done()


class StorageServer(request.RequestHandler):
    """The Storage network server."""

    # the version the client must have (minimum)
    VERSION_REQUIRED = 3
    PING_TIMEOUT = 480
    PING_INTERVAL = 120

    def __init__(self):
        """Create a network server. The factory does this."""
        request.RequestHandler.__init__(self)
        self.user = None
        self.factory = None
        self.session_id = uuid.uuid4()
        self.logger = logging.getLogger(config.api_server.logger_name)
        self.log = StorageServerLogger(self)
        self.shutting_down = False
        self.request_locked = False
        self.pending_requests = collections.deque()
        self.ping_loop = LoopingPing(StorageServer.PING_INTERVAL,
                                     StorageServer.PING_TIMEOUT,
                                     config.api_server.idle_timeout,
                                     self)

        # capabilities that the server is working with
        self.working_caps = MIN_CAP
        self.poisoned = []
        self.waiting_on_poison = []
        self.connection_time = None
        self._metadata_count = set()

    def set_user(self, user):
        """Set user and adjust values that depend on which user it is."""
        self.user = user
        if user.username in self.factory.trace_users:
            # set up the logger to use the hackers' one
            hackers_logger = config.api_server.logger_name + '.hackers'
            self.logger = logging.getLogger(hackers_logger)

    def poison(self, tag):
        """Inject a failure in the server. Works with check_poison."""
        self.poisoned.append(tag)

    def check_poison(self, tag):
        """Fail if poisoned with tag.

        Will raise a PoisonException when called and tag matches the poison
        condition.
        """
        if not self.poisoned:
            return

        poison = self.poisoned[-1]
        if poison == "*" or poison == tag:
            self.poisoned.pop()
            reactor.callLater(0, self.callback_on_poison, poison)
            raise PoisonException("Service was poisoned with: %s" % poison)

    def callback_on_poison(self, poison):
        """Call all the deferreds waiting on poison."""
        for d in self.waiting_on_poison:
            d.callback(poison)
        self.waiting_on_poison = []

    def wait_for_poison(self):
        """Return a deferred that will be called when we are poisoned."""
        d = defer.Deferred()
        self.waiting_on_poison.append(d)
        return d

    def _log_error_and_oops(self, failure, where, tl=None, exc_info=None):
        """Auxiliar method to log error and build oops."""
        self.log.error("Unhandled %s when calling '%s'", failure,
                       where.__name__, exc_info=exc_info)
        del exc_info

        if not isinstance(failure, Failure):
            failure = Failure(failure)
        oops = self.build_oops(failure, tl)
        self.save_oops(oops)

    def schedule_request(self, request, callback, head=False):
        """Schedule this request to run."""
        tl = getattr(request, "timeline", None)
        if head:
            self.pending_requests.appendleft((request, callback))
        else:
            self.pending_requests.append((request, callback))
        # we do care if it fails, means no one handled the failure before
        if not isinstance(request, Action):
            request.deferred.addErrback(self._log_error_and_oops,
                                        request.__class__, tl=tl)
        if not self.request_locked:
            self.execute_next_request()

    def release(self, request):
        """Request 'request' finished, others may start."""
        if self.request_locked is not request:
            # a release from someone not holding the lock does nothing
            return

        self.request_locked = False
        if self.pending_requests and not self.shutting_down:
            self.execute_next_request()

    def execute_next_request(self):
        """Read the queue and execute a request."""
        request, callback = self.pending_requests.popleft()
        tl = getattr(request, "timeline", None)

        if tl is not None:
            class_name = request.__class__.__name__
            tl.start("REQ-" + class_name,
                     "EXECUTE %s[%s]" % (class_name, request.id)).finish()

        try:
            self.request_locked = request
            callback()
        except Exception as e:
            self._log_error_and_oops(e, request.__class__, tl=tl,
                                     exc_info=sys.exc_info())
            self.release(request)

    def connectionLost(self, reason=None):
        """Unregister this connection and potentially remove user binding."""
        if not self.shutting_down:
            self.shutdown()
        if self.user is not None:
            self.user.unregister_protocol(self)

        self.factory.protocols.remove(self)
        self.log.info("Connection Lost: %s", reason.value)

    def connectionMade(self):
        """Called when a connection is made."""
        request.RequestHandler.connectionMade(self)
        self.factory.protocols.append(self)
        self.log.info("Connection Made")
        self.transport.write("%d filesync server revision %s.\r\n" %
                             (self.PROTOCOL_VERSION,
                              versioninfo.version_info['revno']))
        self.ping_loop.start()
        self.factory.metrics.meter("connection_made", 1)
        self.factory.metrics.increment("connections_active")
        self.connection_time = time.time()

    def shutdown(self):
        """Lose connection and abort requests."""
        self.log.debug("Shutdown Request")
        self.shutting_down = True
        self.ping_loop.stop()
        self.transport.loseConnection()
        self.pending_requests.clear()

        # stop all the pending requests
        for req in self.requests.values():
            req.stop()

        if self.connection_time is not None:
            delta = time.time() - self.connection_time
            self.factory.metrics.timing("connection_closed", delta)
            self.factory.metrics.decrement("connections_active")

    def wait_for_shutdown(self):
        """Wait until the server has stopped serving this client."""
        if not self.factory.graceful_shutdown:
            return defer.succeed(True)
        d = defer.Deferred()

        def wait_for_shutdown_worker():
            """Check, wait and recurse."""
            if self.requests:
                reactor.callLater(0.1, wait_for_shutdown_worker)
            else:
                d.callback(True)

        wait_for_shutdown_worker()
        return d

    def dataReceived(self, data):
        """Handle new data."""
        try:
            self.buildMessage(data)
        except Exception as e:
            # here we handle and should log all errors
            self.shutdown()

            if isinstance(e, request.StorageProtocolErrorSizeTooBig):
                self.log.warning("---- garbage in, garbage out")
                return

            self._log_error_and_oops(e, self.dataReceived,
                                     exc_info=sys.exc_info())

    def build_oops(self, failure, tl=None):
        """Create an oops entry to log the failure."""
        context = {"exc_info": (failure.type, failure.value, failure.tb)}
        del failure

        if tl is not None:
            context["timeline"] = tl

        report = self.factory.oops_config.create(context)
        del context

        if self.user is not None:
            # The 'username' key is parsed as (login, user_id, display_name).
            report["username"] = "%s,%s,%s" % (self.user.id, self.user.id,
                                               self.user.username)

        return report

    def save_oops(self, report):
        """save the oops entry."""
        self.factory.oops_config.publish(report)

    def processMessage(self, message):
        """Log errors from requests created by incoming messages."""
        # reset the ping loop if the message isn't a PING or a PONG
        if message.type not in (protocol_pb2.Message.PING,
                                protocol_pb2.Message.PONG):
            self.ping_loop.reset()
        try:
            result = request.RequestHandler.processMessage(self, message)
        except Exception as e:
            self._log_error_and_oops(e, self.processMessage,
                                     exc_info=sys.exc_info())
            return

        if isinstance(result, defer.Deferred):
            result.addErrback(self._log_error_and_oops, result.__class__)
        elif isinstance(result, request.Request):
            tl = getattr(result, "timeline", None)
            result.deferred.addErrback(self._log_error_and_oops,
                                       result.__class__, tl=tl)

    def handle_PROTOCOL_VERSION(self, message):
        """Handle PROTOCOL_VERSION message.

        If the version is less or more than whats allowed, we send an error
        and drop the connection.
        """
        version = message.protocol.version
        response = protocol_pb2.Message()
        response.id = message.id
        if self.VERSION_REQUIRED <= version <= self.PROTOCOL_VERSION:
            response.type = protocol_pb2.Message.PROTOCOL_VERSION
            response.protocol.version = self.PROTOCOL_VERSION
            self.sendMessage(response)
            self.log.debug("client requested protocol version %d", version)
        else:
            msg = "client requested invalid version %s" % (version,)
            response.type = protocol_pb2.Message.ERROR
            response.error.type = protocol_pb2.Error.UNSUPPORTED_VERSION
            response.error.comment = msg
            self.sendMessage(response)

            # wrong protocol version is no longer an error.
            self.log.info(msg)
            self.shutdown()

    def handle_PING(self, message):
        """handle an incoming ping message."""
        self.check_poison("ping")
        response = protocol_pb2.Message()
        response.id = message.id
        response.type = protocol_pb2.Message.PONG
        self.log.trace("ping pong")
        self.sendMessage(response)

    def handle_AUTH_REQUEST(self, message):
        """Handle AUTH_REQUEST message.

        If already logged in, send an error.

        """
        request = AuthenticateResponse(self, message)
        request.start()

    def handle_MAKE_DIR(self, message):
        """Handle MAKE_DIR message."""
        request = MakeResponse(self, message)
        request.start()

    def handle_MAKE_FILE(self, message):
        """Handle MAKE_FILE message."""
        request = MakeResponse(self, message)
        request.start()

    def handle_GET_DELTA(self, message):
        """Handle GET_DELTA message."""
        if message.get_delta.from_scratch:
            request = RescanFromScratchResponse(self, message)
        else:
            request = GetDeltaResponse(self, message)
        request.start()

    def handle_GET_CONTENT(self, message):
        """Handle GET_CONTENT message."""
        request = GetContentResponse(self, message)
        request.start()

    def handle_MOVE(self, message):
        """Handle MOVE message."""
        request = MoveResponse(self, message)
        request.start()

    def handle_PUT_CONTENT(self, message):
        """Handle PUT_CONTENT message."""
        request = PutContentResponse(self, message)
        request.start()

    def handle_UNLINK(self, message):
        """Handle UNLINK message."""
        request = Unlink(self, message)
        request.start()

    def handle_CREATE_UDF(self, message):
        """Handle CREATE_UDF message."""
        request = CreateUDF(self, message)
        request.start()

    def handle_DELETE_VOLUME(self, message):
        """Handle DELETE_VOLUME message."""
        request = DeleteVolume(self, message)
        request.start()

    def handle_LIST_VOLUMES(self, message):
        """Handle LIST_VOLUMES message."""
        request = ListVolumes(self, message)
        request.start()

    def handle_CREATE_SHARE(self, message):
        """Handle CREATE_SHARE message."""
        request = CreateShare(self, message)
        request.start()

    def handle_SHARE_ACCEPTED(self, message):
        """Handle SHARE_ACCEPTED message."""
        request = ShareAccepted(self, message)
        request.start()

    def handle_LIST_SHARES(self, message):
        """Handle LIST_SHARES message."""
        request = ListShares(self, message)
        request.start()

    def handle_DELETE_SHARE(self, message):
        """Handle DELETE_SHARE message."""
        request = DeleteShare(self, message)
        request.start()

    def handle_CANCEL_REQUEST(self, message):
        """Ignores this misreceived cancel."""
        # if this arrives to this Request Handler, it means that what
        # we're trying to cancel already finished (and the client will
        # receive or already received the ok for that), so we ignore it

    def handle_BYTES(self, message):
        """Ignores this misreceived message."""
        # if this arrives to this Request Handler, it means that what
        # we're receiving messages that hit the network before
        # everything is properly cancelled, so we ignore it
    handle_EOF = handle_BYTES

    def handle_QUERY_CAPS(self, message):
        """Handle QUERY_CAPS message."""
        request = QuerySetCapsResponse(self, message)
        request.start()

    def handle_SET_CAPS(self, message):
        """Handle SET_CAPS message."""
        request = QuerySetCapsResponse(self, message, set_mode=True)
        request.start()

    def handle_FREE_SPACE_INQUIRY(self, message):
        """Handle FREE_SPACE_INQUIRY message."""
        request = FreeSpaceResponse(self, message)
        request.start()

    def handle_ACCOUNT_INQUIRY(self, message):
        """Handle account inquiry message."""
        request = AccountResponse(self, message)
        request.start()


class BaseRequestResponse(request.RequestResponse):
    """Base RequestResponse class.

    It keeps a weak reference of the protocol instead of the real ref.
    """

    __slots__ = ('use_protocol_weakref', '_protocol_ref', 'timeline')

    def __init__(self, protocol, message):
        """Create the request response."""
        self.use_protocol_weakref = config.api_server.protocol_weakref
        self._protocol_ref = None
        self.timeline = timeline.Timeline(format_stack=None)
        super(BaseRequestResponse, self).__init__(protocol, message)

    def context(self):
        """Get the context of this request, to be passed down the RPC layer."""
        return {"timeline": self.timeline}

    def _get_protocol(self):
        """Return the protocol instance."""
        if self.use_protocol_weakref:
            protocol = self._protocol_ref()
            if protocol is None:
                # protocol reference gone,
                raise errors.ProtocolReferenceError(str(self._protocol_ref))
            return protocol
        else:
            return self._protocol_ref

    def _set_protocol(self, protocol):
        """Set the weak ref. to the protocol instance."""
        if self.use_protocol_weakref:
            self._protocol_ref = weakref.ref(protocol)
        else:
            self._protocol_ref = protocol

    protocol = property(fget=_get_protocol, fset=_set_protocol)

    def _start(self):
        """Override this method to start the request."""
        raise NotImplementedError("request needs to do something")


class StorageServerRequestResponse(BaseRequestResponse):
    """Base class for all server request responses."""

    __slots__ = ('_id', 'log', 'start_time', 'last_good_state_ts',
                 'length', 'operation_data')

    def __init__(self, protocol, message):
        """Create the request response. Setup logger."""
        self._id = None
        super(StorageServerRequestResponse, self).__init__(protocol, message)
        self.log = StorageRequestLogger(self.protocol, self)
        self.start_time = None
        self.last_good_state_ts = None
        self.length = 1
        self.operation_data = None

    def _get_id(self):
        """Return this request id."""
        return self._id

    def _set_id(self, id):
        """Set this request id."""
        # check if self.log is defined and isn't None
        if getattr(self, 'log', None) is not None:
            # update the request_id in self.log
            self.log.request_id = id
        self._id = id

    # override self.id with a property
    id = property(fget=_get_id, fset=_set_id)

    def _get_node_info(self):
        """Return node info from the message.

        This should be overwritten by our children, but it's not mandatory.
        """

    def start(self):
        """Schedule the request start for later.

        It will be executed when the server has no further pending requests.

        """
        def _scheduled_start():
            """The scheduled call."""
            self.start_time = time.time()
            self.last_good_state_ts = time.time()
            super(StorageServerRequestResponse, self).start()
            self.protocol.check_poison("request_start")

        self.protocol.addProducer(self)

        # we add the id to the request here, even if it happens again on the
        # super() call in _scheduled_start(), but it's good to have the id
        # while scheduled, before really started
        self.id = self.source_message.id
        self.protocol.requests[self.source_message.id] = self

        class_name = self.__class__.__name__
        self.timeline.start(
            "REQ-" + class_name,
            "SCHEDULE %s[%s:%s] CAPS %r WITH %r" % (
                class_name, self.protocol.session_id,
                self.id, self.protocol.working_caps,
                str(self.source_message)[:MAX_OOPS_LINE])).finish()

        self.log.info("Request being scheduled")
        self.log.trace_message("IN: ", self.source_message)
        self.protocol.factory.metrics.increment("request_instances.%s" %
                                                (self.__class__.__name__,))
        self.protocol.schedule_request(self, _scheduled_start)
        self.protocol.check_poison("request_schedule")

    def stop(self):
        """Stop the request."""
        if self.started:
            # cancel the request
            self.cancel()
        else:
            # not even started! just log and cleanup
            self.log.debug("Request being released before start")
            self.cleanup()

    def cleanup(self):
        """remove the reference to self from the request handler"""
        super(StorageServerRequestResponse, self).cleanup()
        self.protocol.factory.metrics.decrement("request_instances.%s" %
                                                (self.__class__.__name__,))
        self.protocol.release(self)
        self.timeline = None

    def error(self, failure):
        """Overrided error to add logging. Never fail, always log."""
        class_name = self.__class__.__name__
        try:
            if isinstance(failure, Exception):
                failure = Failure(failure)
            self.log.error("Request error",
                           exc_info=(failure.type, failure.value, None))
            o = self.protocol.build_oops(failure, self.timeline)
            self.protocol.save_oops(o)
            request.RequestResponse.error(self, failure)
        except Exception as e:
            msg = 'error() crashed when processing %s: %s'
            # we shouldn't create an oops here since we just failed
            self.log.error(msg, failure, e, exc_info=sys.exc_info())
        else:
            self.protocol.factory.sli_metrics.sli_error(class_name)
            if self.start_time is not None:
                delta = time.time() - self.start_time
                self.protocol.factory.metrics.timing(
                    "%s.request_error" % (class_name,), delta)
                self.protocol.factory.metrics.timing("request_error", delta)

            transferred = getattr(self, 'transferred', None)
            if transferred is not None:
                msg = "%s.transferred" % (class_name,)
                self.protocol.factory.metrics.gauge(msg, transferred)

    def done(self, _=None):
        """Overrided done to add logging.

        Added an ignored parameter so we can hook this to the defered chain.
        """
        try:
            if self.operation_data is None:
                self.log.info("Request done")
            else:
                self.log.info("Request done: %s", self.operation_data)
            request.RequestResponse.done(self)
        except Exception as e:
            self.error(Failure(e))
        else:
            class_name = self.__class__.__name__
            factory = self.protocol.factory
            transferred = getattr(self, 'transferred', None)

            if self.start_time is not None:
                delta = time.time() - self.start_time
                factory.metrics.timing("%s.request_finished" % (class_name,),
                                       delta)
                factory.metrics.timing("request_finished", delta)

                # inform SLI metric here if operation has a valid length (some
                # operations change it, default is 1 for other operations, and
                # Put/GetContentResponse set it to None to not inform *here*)
                if self.length is not None:
                    self.protocol.factory.sli_metrics.sli(class_name, delta,
                                                          self.length)

            if transferred is not None:
                msg = "%s.transferred" % (class_name,)
                factory.metrics.gauge(msg, transferred)

            # measure specific "user's client" activity
            user_activity = getattr(self, 'user_activity', None)
            if user_activity is not None:
                user_id = getattr(self.protocol.user, 'id', '')
                factory.user_metrics.report(user_activity, str(user_id), 'd')

    def sendMessage(self, message):
        """Send a message and trace it."""
        self.log.trace_message("OUT: ", message)
        request.RequestResponse.sendMessage(self, message)

    def _processMessage(self, message):
        """Locally overridable part of processMessage."""

    def processMessage(self, message):
        """Process the received messages.

        If the request already started, really process the message rightaway;
        if not, just schedule the processing for later.
        """
        result = None
        if self.started:
            result = self._processMessage(message)
        else:
            def _process_later():
                """Deferred call to process the message."""
                self._processMessage(message)
            self.protocol.schedule_request(self, _process_later)
        return result

    def convert_share_id(self, share_id):
        """Convert a share_id from a message to a UUID or None.

        @param share_id: a str representation of a share id
        @return: uuid.UUID or None
        """
        if share_id != request.ROOT:
            return uuid.UUID(share_id)
        else:
            return None

    def queue_action(self, callable_action, head=True):
        """Queue a callable action.

        Return a deferred that will be fired when the action finish.
        """
        action = Action(self, callable_action)
        action.start(head=head)
        return action.deferred

    def _get_extension(self, path):
        """Return an extension from the name/path."""
        parts = path.rsplit(".", 1)
        if len(parts) == 2:
            endpart = parts[1]
            if len(endpart) <= 4:
                return endpart


class SimpleRequestResponse(StorageServerRequestResponse):
    """Common request/response bits."""

    __slots__ = ()

    authentication_required = True

    # a list of foreign (non protocol) errors to be filled by heirs
    expected_foreign_errors = []

    # a list of foreing errors that will always be handled by this super class
    # because all operations could generate them (RetryLimitReached because of
    # access to the DB, and DoesNotExist because user account becoming
    # disabled while connected)
    generic_foreign_errors = [
        dataerror.RetryLimitReached,
        dataerror.DoesNotExist,
        dataerror.LockedUserError,
        psycopg2.DataError,
    ]

    # convert "foreign" errors directly to protocol error codes;
    # StorageServerError and TryAgain (and heirs) will be handled automatically
    # because Failure.check compares exceptions using isinstance().
    protocol_errors = {
        dataerror.NotEmpty: protocol_pb2.Error.NOT_EMPTY,
        dataerror.NotADirectory: protocol_pb2.Error.NOT_A_DIRECTORY,
        dataerror.NoPermission: protocol_pb2.Error.NO_PERMISSION,
        dataerror.DoesNotExist: protocol_pb2.Error.DOES_NOT_EXIST,
        dataerror.AlreadyExists: protocol_pb2.Error.ALREADY_EXISTS,
        dataerror.QuotaExceeded: protocol_pb2.Error.QUOTA_EXCEEDED,
        dataerror.InvalidFilename: protocol_pb2.Error.INVALID_FILENAME,
        psycopg2.DataError: protocol_pb2.Error.INVALID_FILENAME,

        # violation of primary key, foreign key, or unique constraints
        dataerror.IntegrityError: protocol_pb2.Error.ALREADY_EXISTS,
    }
    # These get converted to TRY_AGAIN
    try_again_errors = [
        dataerror.RetryLimitReached,
        error.TCPTimedOutError,
    ]

    auth_required_error = 'Authentication required and the user is None.'
    upload_not_accepted_error = (
        'This upload has not been accepted yet content is being sent.')

    def _process(self):
        """Process the request."""

    def _meter_s3_timeout(self, failure):
        """Send metrics on S3 timeouts."""
        if failure.check(errors.S3Error):
            factory = self.protocol.factory
            last_responsive = factory.reactor_inspector.last_responsive_ts
            last_good = self.last_good_state_ts
            # Unresponsive server or client not reading/writing fast enough?
            reason = "server" if last_responsive <= last_good else "client"

            class_name = self.__class__.__name__
            metric_name = "%s.request_error.s3_timeout.%s"
            factory.metrics.meter(metric_name % (class_name, reason), 1)

    def _send_protocol_error(self, failure):
        """Convert the failure to a protocol error.

        Send it if possible; otherwise, continue propagating it.

        """
        # Convert try_again_errors that weren't converted at lower levels
        if failure.check(*self.try_again_errors):
            failure = Failure(errors.TryAgain(failure.value))

        # Send metrics if we have an S3 error
        self._meter_s3_timeout(failure)

        foreign_errors = (self.generic_foreign_errors +
                          self.expected_foreign_errors)
        error = failure.check(errors.StorageServerError, *foreign_errors)
        comment = failure.getErrorMessage().decode('utf8')
        if failure.check(errors.TryAgain):
            orig_error_name = failure.value.orig_error.__class__.__name__
            comment = u"%s: %s" % (orig_error_name, comment)
            msg = 'Sending TRY_AGAIN to client because we got: %s', comment
            self.log.warning(msg, Failure(failure.value.orig_error))
            comment = u"TryAgain (%s)" % comment

            metric_name = "TRY_AGAIN.%s" % orig_error_name
            self.protocol.factory.metrics.meter(metric_name, 1)

        if error == dataerror.QuotaExceeded:
            # handle the case of QuotaExceeded
            # XXX: check Bug #605847
            protocol_error = self.protocol_errors[error]
            free_bytes = failure.value.free_bytes
            volume_id = str(failure.value.volume_id or '')
            free_space_info = {'share_id': volume_id, 'free_bytes': free_bytes}
            self.sendError(protocol_error, comment=comment,
                           free_space_info=free_space_info)
            return  # the error has been consumed
        elif error in self.protocol_errors:
            # an error we expect and can convert to a protocol error
            protocol_error = self.protocol_errors[error]
            self.sendError(protocol_error, comment=comment)
            return  # the error has been consumed
        elif (error == errors.StorageServerError and
                not failure.check(errors.InternalError)):
            # an error which corresponds directly to a protocol error
            self.sendError(failure.value.errno, comment=comment)
            return  # the error has been consumed
        elif error == dataerror.LockedUserError:
            self.log.warning("Shutting down protocol: user locked")
            if not self.protocol.shutting_down:
                self.protocol.shutdown()
            return  # the error has been consumed
        else:
            # an unexpected or unconvertable error
            self.sendError(protocol_pb2.Error.INTERNAL_ERROR,
                           comment=comment)
            return failure  # propagate the error

    def internal_error(self, failure):
        """Handle a failure that caused an INTERNAL_ERROR."""
        # only call self.error, if I'm not finished
        if not self.finished:
            self.error(failure)
        # only shutdown the protocol if isn't already doing the shutdown
        if not self.protocol.shutting_down:
            self.protocol.shutdown()

    def _log_start(self):
        """Log that the request started."""
        txt = "Request being started"
        on_what = self._get_node_info()
        if on_what:
            txt += ", working on: " + str(on_what)
        self.log.debug(txt)

    def _start(self):
        """Kick off request processing."""
        self._log_start()

        if self.authentication_required and self.protocol.user is None:
            self.sendError(protocol_pb2.Error.AUTHENTICATION_REQUIRED,
                           comment=self.auth_required_error)
            return self.done()
        else:
            d = maybeDeferred(self._process)
            d.addErrback(self._send_protocol_error)
            d.addCallbacks(self.done, self.internal_error)


class ListShares(SimpleRequestResponse):
    """LIST_SHARES Request Response."""

    __slots__ = ()

    @inlineCallbacks
    def _process(self):
        """List shares for the client."""
        user = self.protocol.user
        shared_by, shared_to = yield user.list_shares()
        self.length = len(shared_by) + len(shared_to)

        for share in shared_by:
            # get the volume_id of the shared node.
            volume_id = yield user.get_volume_id(share['root_id'])
            volume_id = str(volume_id) if volume_id else ''
            if volume_id == user.root_volume_id:
                # return the protocol root instead of users real root
                volume_id = request.ROOT
            share_resp = sharersp.ShareResponse.from_params(
                share['id'], "from_me", share['root_id'], share['name'],
                share['shared_to_username'] or u"",
                share['shared_to_visible_name'] or u"",
                share['accepted'], share['access'],
                subtree_volume_id=volume_id)
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.SHARES_INFO
            share_resp.dump_to_msg(response.shares)
            self.sendMessage(response)

        for share in shared_to:
            share_resp = sharersp.ShareResponse.from_params(
                share['id'], "to_me", share['root_id'], share['name'],
                share['shared_by_username'] or u"",
                share['shared_by_visible_name'] or u"",
                share['accepted'], share['access'])
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.SHARES_INFO
            share_resp.dump_to_msg(response.shares)
            self.sendMessage(response)

        # we're done!
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.SHARES_END
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "shared_by=%d shared_to=%d" % (
            len(shared_by), len(shared_to))


class ShareAccepted(SimpleRequestResponse):
    """SHARE_ACCEPTED Request Response."""

    __slots__ = ()

    # these are the valid access levels and their translation from the
    # protocol message
    _answer_prot2nice = {
        protocol_pb2.ShareAccepted.YES: "Yes",
        protocol_pb2.ShareAccepted.NO: "No",
    }

    def _get_node_info(self):
        """Return node info from the message."""
        share_id = self.source_message.share_accepted.share_id
        return "share: %r" % (share_id,)

    @inlineCallbacks
    def _process(self):
        """Mark the share as accepted."""
        mes = self.source_message.share_accepted
        answer = self._answer_prot2nice[mes.answer]

        accept_share = self.protocol.user.share_accepted
        share_id = self.convert_share_id(mes.share_id)
        yield accept_share(share_id, answer)

        # send the ok to the requesting client
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.OK
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s answer=%s" % (share_id, answer)


class CreateShare(SimpleRequestResponse):
    """CREATE_SHARE Request Response."""

    __slots__ = ()

    expected_foreign_errors = [
        dataerror.IntegrityError,
        dataerror.NotADirectory,
        dataerror.NoPermission,
    ]

    # these are the valid access levels and their translation from the
    # protocol message
    _valid_access_levels = {
        protocol_pb2.CreateShare.VIEW: "View",
        protocol_pb2.CreateShare.MODIFY: "Modify",
    }

    user_activity = 'create_share'

    @inlineCallbacks
    def _process(self):
        """Create the share."""
        mes = self.source_message.create_share
        access_level = self._valid_access_levels[mes.access_level]

        create_share = self.protocol.user.create_share
        share_id = yield create_share(mes.node, mes.share_to,
                                      mes.name, access_level)

        # send the ok to the requesting client
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.SHARE_CREATED
        response.share_created.share_id = str(share_id)
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s access_level=%s" % (
            share_id, access_level)


class DeleteShare(SimpleRequestResponse):
    """Deletes a share we had offered."""

    __slots__ = ()

    expected_foreign_errors = [
        dataerror.IntegrityError,
        dataerror.NoPermission,
    ]

    def _get_node_info(self):
        """Return node info from the message."""
        share_id = self.source_message.delete_share.share_id
        return "share: %r" % (share_id,)

    @inlineCallbacks
    def _process(self):
        """Delete the share."""
        share_id = self.convert_share_id(
            self.source_message.delete_share.share_id)
        yield self.protocol.user.delete_share(share_id)

        # send the ok to the requesting client
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.OK
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s" % (share_id,)


class CreateUDF(SimpleRequestResponse):
    """CREATE_UDF Request Response."""

    __slots__ = ()

    expected_foreign_errors = [dataerror.NoPermission]

    user_activity = 'sync_activity'

    @inlineCallbacks
    def _process(self):
        """Create the share."""
        mes = self.source_message.create_udf
        data = yield self.protocol.user.create_udf(
            mes.path, mes.name, self.protocol.session_id)
        udf_id, udf_rootid, udf_path = data

        # send the ok to the requesting client
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.VOLUME_CREATED
        response.volume_created.type = protocol_pb2.Volumes.UDF
        response.volume_created.udf.volume = str(udf_id)
        response.volume_created.udf.node = str(udf_rootid)
        response.volume_created.udf.suggested_path = udf_path
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s" % (udf_id,)


class DeleteVolume(SimpleRequestResponse):
    """DELETE_VOLUME Request Response."""

    __slots__ = ()

    expected_foreign_errors = [dataerror.NoPermission]

    user_activity = 'sync_activity'

    def _get_node_info(self):
        """Return node info from the message."""
        volume_id = self.source_message.delete_volume.volume
        return "volume: %r" % (volume_id,)

    @inlineCallbacks
    def _process(self):
        """Delete the volume."""
        volume_id = self.source_message.delete_volume.volume
        if volume_id == request.ROOT:
            raise dataerror.NoPermission("Root volume can't be deleted.")
        try:
            vol_id = uuid.UUID(volume_id)
        except ValueError:
            raise dataerror.DoesNotExist("No such volume %r" % volume_id)
        yield self.protocol.user.delete_volume(
            vol_id, self.protocol.session_id)

        # send the ok to the requesting client
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.OK
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s" % (vol_id,)


class ListVolumes(SimpleRequestResponse):
    """LIST_VOLUMES Request Response."""

    __slots__ = ()

    @inlineCallbacks
    def _process(self):
        """List volumes for the client."""
        user = self.protocol.user
        result = yield user.list_volumes()
        root_info, shares, udfs, free_bytes = result
        self.length = 1 + len(shares) + len(udfs)

        # send root
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.VOLUMES_INFO
        response.list_volumes.type = protocol_pb2.Volumes.ROOT
        response.list_volumes.root.node = str(root_info['root_id'])
        response.list_volumes.root.generation = root_info['generation']
        response.list_volumes.root.free_bytes = free_bytes
        self.sendMessage(response)

        # send shares
        for share in shares:
            direction = "to_me"
            share_resp = sharersp.ShareResponse.from_params(
                share['id'], direction, share['root_id'], share['name'],
                share['shared_by_username'] or u"",
                share['shared_by_visible_name'] or u"",
                share['accepted'], share['access'])
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.VOLUMES_INFO
            response.list_volumes.type = protocol_pb2.Volumes.SHARE
            share_resp.dump_to_msg(response.list_volumes.share)
            response.list_volumes.share.generation = share['generation']
            response.list_volumes.share.free_bytes = share['free_bytes']
            self.sendMessage(response)

        # send udfs
        for udf in udfs:
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.VOLUMES_INFO
            response.list_volumes.type = protocol_pb2.Volumes.UDF
            response.list_volumes.udf.volume = str(udf['id'])
            response.list_volumes.udf.node = str(udf['root_id'])
            response.list_volumes.udf.suggested_path = udf['path']
            response.list_volumes.udf.generation = udf['generation']
            response.list_volumes.udf.free_bytes = free_bytes
            self.sendMessage(response)

        # we're done!
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.VOLUMES_END
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "root=%s shares=%d udfs=%d" % (
            root_info['root_id'], len(shares), len(udfs))


class Unlink(SimpleRequestResponse):
    """UNLINK Request Response."""

    __slots__ = ()

    expected_foreign_errors = [dataerror.NotEmpty, dataerror.NoPermission]

    user_activity = 'sync_activity'

    def _get_node_info(self):
        """Return node info from the message."""
        node_id = self.source_message.unlink.node
        return "node: %r" % (node_id,)

    @inlineCallbacks
    def _process(self):
        """Unlink a node."""
        share_id = self.convert_share_id(self.source_message.unlink.share)
        node_id = self.source_message.unlink.node
        generation, kind, name, mime = yield self.protocol.user.unlink_node(
            share_id, node_id, session_id=self.protocol.session_id)

        # answer the ok to original user
        response = protocol_pb2.Message()
        response.new_generation = generation
        response.type = protocol_pb2.Message.OK
        self.sendMessage(response)

        # save data to be logged on operation end
        extension = self._get_extension(name)
        self.operation_data = "vol_id=%s node_id=%s type=%s mime=%r ext=%r" % (
            share_id, node_id, kind, mime, extension)


class BytesMessageProducer(object):
    """Adapt a bytes producer to produce BYTES messages."""

    payload_size = request.MAX_PAYLOAD_SIZE

    def __init__(self, bytes_producer, request):
        """Create a BytesMessageProducer."""
        self.producer = bytes_producer
        bytes_producer.consumer = self
        self.request = request
        self.logger = request.log

    def resumeProducing(self):
        """IPushProducer interface."""
        self.logger.trace("BytesMessageProducer resumed, http producer: %s",
                          self.producer)
        if self.producer:
            self.producer.resumeProducing()

    def stopProducing(self):
        """IPushProducer interface."""
        self.logger.trace("BytesMessageProducer stopped, http producer: %s",
                          self.producer)
        if self.producer:
            self.producer.stopProducing()

    def pauseProducing(self):
        """IPushProducer interface."""
        self.logger.trace("BytesMessageProducer paused, http producer: %s",
                          self.producer)
        if self.producer:
            self.producer.pauseProducing()

    def write(self, content):
        """Part of IConsumer."""
        p = 0
        part = content[p:p + self.payload_size]
        while part:
            if self.request.cancelled:
                # stop generating messages
                return
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.BYTES
            response.bytes.bytes = part
            self.request.transferred += len(part)
            self.request.sendMessage(response)
            p += self.payload_size
            part = content[p:p + self.payload_size]
        self.request.last_good_state_ts = time.time()


def cancel_filter(function):
    """Raises RequestCancelledError if the request is cancelled.

    This methods exists to be used in a addCallback sequence to assure
    that it does not continue if the request is cancelled, like:

    >>> d.addCallback(cancel_filter(foo))
    >>> d.addCallbacks(done_callback, error_errback)

    Note that you may receive RequestCancelledError in your
    'error_errback' func.
    """
    @wraps(function)
    def f(self, *args, **kwargs):
        '''Function to be called from twisted when its time arrives.'''
        if self.cancelled:
            raise request.RequestCancelledError(
                "The request id=%d "
                "is cancelled! (before calling %r)" % (self.id, function))
        return function(self, *args, **kwargs)
    return f


class GetContentResponse(SimpleRequestResponse):
    """GET_CONTENT Request Response."""

    __slots__ = ('cancel_message', 'message_producer',
                 'transferred', 'init_time')

    def __init__(self, protocol, message):
        super(GetContentResponse, self).__init__(protocol, message)
        self.cancel_message = None
        self.message_producer = None
        self.transferred = 0
        self.init_time = 0
        self.length = None  # to not inform automatically on done()

    def _get_node_info(self):
        """Return node info from the message."""
        node_id = self.source_message.get_content.node
        return "node: %r" % (node_id,)

    def _send_protocol_error(self, failure):
        """Convert the failure to a protocol error.

        Send it if possible; otherwise, continue propagating it.

        """
        is_cancelled = (failure.check(ProducerStopped) and self.cancelled)

        if failure.check(request.RequestCancelledError) or is_cancelled:
            # the normal sequence was interrupted by a cancel
            if self.cancel_message is not None:
                response = protocol_pb2.Message()
                response.id = self.cancel_message.id
                response.type = protocol_pb2.Message.CANCELLED
                self.sendMessage(response)
            else:
                msg = 'Got cancelling failure %s but cancel_message is None.'
                self.log.warning(msg, failure)
            return  # the error has been consumed

        else:
            # handle all the TRY_AGAIN cases
            if failure.check(ProducerStopped, error.ConnectionLost):
                failure = Failure(errors.TryAgain(failure.value))
            return SimpleRequestResponse._send_protocol_error(self, failure)

    def _start(self):
        """Get node content and send it."""
        self.init_time = time.time()
        self._log_start()
        share_id = self.convert_share_id(self.source_message.get_content.share)

        def done(result):
            """Send EOF message."""
            response = protocol_pb2.Message()
            response.type = protocol_pb2.Message.EOF
            self.sendMessage(response)

        def get_content(node):
            """Get the content for node."""
            d = node.get_content(
                user=self.protocol.user,
                previous_hash=self.source_message.get_content.hash,
                start=self.source_message.get_content.offset)
            return d

        def stop_if_cancelled(producer):
            """Stop the producer if cancelled."""
            # let's forget ProducerStopped if we're cancelled
            def ignore_producer_stopped(failure):
                """Like failure trap with extras."""
                if failure.check(ProducerStopped) and self.cancelled:
                    # I know, I know, I cancelled you!
                    return
                return failure

            producer.deferred.addErrback(ignore_producer_stopped)

            if self.cancelled:
                producer.stopProducing()
            return producer

        def get_node_attrs(node):
            """Get attributes for 'node' and send them."""
            r = protocol_pb2.Message()
            r.type = protocol_pb2.Message.NODE_ATTR
            r.node_attr.deflated_size = node.deflated_size
            r.node_attr.size = node.size
            r.node_attr.hash = node.content_hash
            r.node_attr.crc32 = node.crc32
            self.sendMessage(r)

            # save data to be logged on operation end
            self.operation_data = "vol_id=%s node_id=%s hash=%s size=%s" % (
                share_id, self.source_message.get_content.node,
                node.content_hash, node.size)
            return node

        if self.protocol.user is None:
            self.sendError(protocol_pb2.Error.AUTHENTICATION_REQUIRED,
                           comment=self.auth_required_error)
            self.done()
        else:
            # get node
            d = self.protocol.user.get_node(
                share_id,
                self.source_message.get_content.node,
                self.source_message.get_content.hash)
            d.addCallback(self.cancel_filter(get_node_attrs))
            # get content and validate hash
            d.addCallback(self.cancel_filter(get_content))
            # stop producer if cancelled
            d.addCallback(stop_if_cancelled)
            # send content
            d.addCallback(self.cancel_filter(self.send))
            # send eof
            d.addCallback(self.cancel_filter(done))
            d.addErrback(self._send_protocol_error)
            d.addCallbacks(self.done, self.internal_error)

    def send(self, producer):
        """Send node to the client."""
        delta = time.time() - self.init_time
        self.protocol.factory.sli_metrics.sli('GetContentResponseInit', delta)

        message_producer = BytesMessageProducer(producer, self)
        self.message_producer = message_producer
        self.registerProducer(message_producer, streaming=True)

        # release this request early
        self.protocol.release(self)

        return producer.deferred

    def _cancel(self):
        """Cancel the request.

        This method is called if the request was not cancelled before
        (check self.cancel).
        """
        producer = self.message_producer
        if producer is not None:
            self.unregisterProducer()
            producer.stopProducing()

    def _processMessage(self, message):
        """Process new messages from the client inside this request."""
        if message.type == protocol_pb2.Message.CANCEL_REQUEST:
            self.cancel_message = message
            self.cancel()
            self.protocol.release(self)
        else:
            self.error(request.StorageProtocolProtocolError(message))


class PutContentResponse(SimpleRequestResponse):
    """PUT_CONTENT Request Response."""

    __slots__ = ('cancel_message', 'upload_job', 'transferred',
                 'state', 'init_time')

    expected_foreign_errors = [dataerror.NoPermission, dataerror.QuotaExceeded]

    user_activity = 'sync_activity'

    # indicators for internal fsm
    states = "INIT UPLOADING COMMITING CANCELING ERROR DONE"
    states = collections.namedtuple("States", states.lower())(*states.split())

    # will send TRY_AGAIN on all these errors
    _try_again_errors = (
        ProducerStopped,
        dataerror.RetryLimitReached,
        errors.BufferLimit,
    )

    def __init__(self, protocol, message):
        super(PutContentResponse, self).__init__(protocol, message)
        self.cancel_message = None
        self.upload_job = None
        self.transferred = 0
        self.state = self.states.init
        self.init_time = 0
        self.length = None  # to not inform automatically on done()

    def done(self):
        """Override done to skip and log 'double calls'."""
        # Just a hack to avoid the KeyError flood until we find the
        # reason of the double done calls.
        if self.finished:
            # build a call chain of 3 levels
            curframe = inspect.currentframe()
            calframe1 = inspect.getouterframes(curframe, 2)
            callers = [calframe1[i][3] for i in range(1, 4)]
            call_chain = ' -> '.join(reversed(callers))
            self.log.warning("%s: called done() finished=%s",
                             call_chain, self.finished)
        else:
            SimpleRequestResponse.done(self)

    def _get_node_info(self):
        """Return node info from the message."""
        node_id = self.source_message.put_content.node
        return "node: %r" % (node_id,)

    def _start(self):
        """Create upload reservation and start receiving."""
        self.init_time = time.time()
        self._log_start()

        if self.protocol.user is None:
            self.sendError(protocol_pb2.Error.AUTHENTICATION_REQUIRED,
                           comment=self.auth_required_error)
            self.state = self.states.done
            self.done()
        else:
            d = self._start_upload()
            d.addErrback(self._generic_error)

    def _log_exception(self, exc):
        """Log an exception before it is handled by the parent request."""
        if self.upload_job is not None:
            size_hint = self.upload_job.inflated_size_hint
        else:
            size_hint = 0
        if isinstance(exc, errors.TryAgain):
            m = "Upload failed with TryAgain error: %s, size_hint: %s"
            self.log.debug(m, exc.orig_error.__class__.__name__, size_hint)
        else:
            m = "Upload failed with error: %s, size_hint: %s"
            self.log.debug(m, exc.__class__.__name__, size_hint)

    @defer.inlineCallbacks
    def _generic_error(self, failure):
        """Process all possible errors."""
        # it can be an exception, let's work with a failure from now on
        if not isinstance(failure, Failure):
            failure = Failure(failure)
        exc = failure.value

        if failure.check(request.RequestCancelledError):
            if self.state == self.states.canceling:
                # special error while canceling
                self.log.debug("Request cancelled: %s", exc)
                return

        self.log.warning("Error while in %s: %s (%s)", self.state,
                         exc.__class__.__name__, exc)
        if self.state in (self.states.error, self.states.done):
            # if already in error/done state, just ignore it (after logging)
            return
        self.state = self.states.error

        # on any error, we just stop the upload job
        if self.upload_job is not None:
            self.log.debug("Stoping the upload job after an error")
            yield self.upload_job.stop()
            # and unregister the transport from the upload_job
            self.upload_job.unregisterProducer()

        # handle all the TRY_AGAIN cases
        if failure.check(*self._try_again_errors):
            exc = errors.TryAgain(exc)
            failure = Failure(exc)

        # generic error handler, and done (and really fail if we get
        # a problem while doing that)
        try:
            self._log_exception(exc)
            yield self._send_protocol_error(failure)
            yield self.done()
        except:
            yield self.internal_error(Failure())

    @trace_message
    def _processMessage(self, message):
        """Receive the content for the upload."""
        if self.state == self.states.uploading:
            try:
                self._process_while_uploading(message)
            except Exception as e:
                self._generic_error(e)
            return

        if self.state == self.states.init:
            if message.type == protocol_pb2.Message.CANCEL_REQUEST:
                try:
                    self.state = self.states.canceling
                    self.cancel_message = message
                    self.cancel()
                except Exception as e:
                    self._generic_error(e)
                return

        if self.state == self.states.error:
            # just ignore the message
            return

        self.log.warning("Received out-of-order message: state=%s  message=%s",
                         self.state, message.type)

    def _process_while_uploading(self, message):
        """Receive any messages while in UPLOADING."""
        if message.type == protocol_pb2.Message.CANCEL_REQUEST:
            self.state = self.states.canceling
            self.cancel_message = message
            self.cancel()

        elif message.type == protocol_pb2.Message.EOF:
            self.state = self.states.commiting
            self.upload_job.deferred.addCallback(self._commit_uploadjob)
            self.upload_job.deferred.addErrback(self._generic_error)

        elif message.type == protocol_pb2.Message.BYTES:
            # process BYTES, stay here in UPLOADING
            received_bytes = message.bytes.bytes
            self.transferred += len(received_bytes)
            self.upload_job.add_data(received_bytes)
            self.last_good_state_ts = time.time()

        else:
            self.log.error("Received unknown message: %s", message.type)

    @defer.inlineCallbacks
    def _cancel(self):
        """Cancel put content.

        This is called from request.Request.cancel(), after a
        client's CANCEL_REQUEST or a request.stop().
        """
        cancelled_by_user = self.state == self.states.canceling
        self.log.debug("Request canceled (in %s)", self.state)
        self.state = self.states.canceling

        if cancelled_by_user:
            # cancel the upload job and answer back
            if self.upload_job is not None:
                self.log.debug("Canceling the upload job")
                yield self.upload_job.cancel()
            response = protocol_pb2.Message()
            response.id = self.cancel_message.id
            response.type = protocol_pb2.Message.CANCELLED
            self.sendMessage(response)
        else:
            # just stop the upload job
            if self.upload_job is not None:
                self.log.debug("Stoping the upload job after a cancel")
                yield self.upload_job.stop()

        # done
        self.state = self.states.done
        self.done()

    @defer.inlineCallbacks
    def _commit_uploadjob(self, result):
        """Callback for uploadjob when it's complete."""
        commit_time = time.time()
        if self.state != self.states.commiting:
            # if we are not in committing state, bail out
            return

        def commit():
            """The callable to queue."""
            if self.state != self.states.commiting:
                # Request has been canceled since we last checked
                return defer.succeed(None)
            return self.upload_job.commit(result)

        # queue the commit work
        new_generation = yield self.queue_action(commit)

        if self.state != self.states.commiting:
            # if we are not in committing state, bail out
            return

        # when commit's done, send the ok
        self.protocol.factory.sli_metrics.sli('PutContentResponseCommit',
                                              time.time() - commit_time)
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.OK
        response.new_generation = new_generation
        self.sendMessage(response)

        # done
        self.state = self.states.done
        self.done()

    @defer.inlineCallbacks
    def _start_upload(self):
        """Setup the upload and tell the client to start uploading."""
        self.upload_job = yield self._get_upload_job()
        self.upload_job.deferred.addErrback(self._generic_error)
        if self.state in (self.states.done, self.states.canceling):
            # Manually canceling the upload because we were canceled
            # while getting the upload
            self.log.debug("Manually canceling the upload job (in %s)",
                           self.state)
            yield self.upload_job.cancel()
            return
        yield self.upload_job.connect()
        # register the client transport as the producer
        self.upload_job.registerProducer(self.protocol.transport)
        self.protocol.release(self)
        yield self._send_begin()
        self.state = self.states.uploading

    @defer.inlineCallbacks
    @cancel_filter
    def _get_upload_job(self):
        """Get the uploadjob."""
        share_id = self.convert_share_id(self.source_message.put_content.share)
        if config.api_server.magic_upload_active:
            magic_hash = self.source_message.put_content.magic_hash or None
        else:
            magic_hash = None

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s node_id=%s hash=%s size=%s" % (
            share_id, self.source_message.put_content.node,
            self.source_message.put_content.hash,
            self.source_message.put_content.size)

        # create upload reservation
        uploadjob = yield self.protocol.user.get_upload_job(
            share_id,
            self.source_message.put_content.node,
            self.source_message.put_content.previous_hash,
            self.source_message.put_content.hash,
            self.source_message.put_content.crc32,
            self.source_message.put_content.size,
            self.source_message.put_content.deflated_size,
            session_id=self.protocol.session_id,
            magic_hash=magic_hash,
            upload_id=self.source_message.put_content.upload_id or None)
        defer.returnValue(uploadjob)

    @cancel_filter
    def _send_begin(self):
        """Notify the client that he can start sending data."""
        delta = time.time() - self.init_time
        self.protocol.factory.sli_metrics.sli('PutContentResponseInit', delta)
        upload_type = self.upload_job.__class__.__name__
        offset = self.upload_job.offset
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.BEGIN_CONTENT
        response.begin_content.offset = offset
        # only send the upload id for a new put content
        upload_id = self.upload_job.upload_id
        upload_id = '' if upload_id is None else str(upload_id)
        if self.source_message.put_content.upload_id != upload_id:
            response.begin_content.upload_id = upload_id
        self.sendMessage(response)

        factory = self.protocol.factory
        factory.metrics.meter("%s.upload.begin" % (upload_type,), 1)
        factory.metrics.gauge("%s.upload" % (upload_type,), offset)
        self.log.debug("%s begin content from offset %d", upload_type, offset)


class GetDeltaResponse(SimpleRequestResponse):
    """GetDelta Request Response."""

    __slots__ = ()

    def _get_node_info(self):
        """Return node info from the message."""
        volume_id = self.source_message.get_delta.share
        return "volume: %r" % (volume_id,)

    @inlineCallbacks
    def _process(self):
        """Get the deltas and send messages."""
        msg = self.source_message
        share_id = self.convert_share_id(msg.get_delta.share)
        from_generation = msg.get_delta.from_generation
        delta_max_size = config.api_server.delta_max_size
        delta_info = yield self.protocol.user.get_delta(
            share_id, from_generation, limit=delta_max_size)
        nodes, vol_generation, free_bytes = delta_info
        yield self.send_delta_info(nodes, msg.get_delta.share)
        self.length = len(nodes)

        # now send the DELTA_END
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.DELTA_END
        full = True
        if nodes:
            full = vol_generation == nodes[-1].generation
        response.delta_end.full = full
        if full:
            response.delta_end.generation = vol_generation
        else:
            response.delta_end.generation = nodes[-1].generation
        response.delta_end.free_bytes = free_bytes
        self.sendMessage(response)

        # save data to be logged on operation end
        t = "vol_id=%s from_gen=%s current_gen=%s nodes=%d free_bytes=%s" % (
            share_id, from_generation, vol_generation, self.length, free_bytes)
        self.operation_data = t

    def send_delta_info(self, nodes, share_id):
        """Build and send the DELTA_INFO for each node."""
        return task.cooperate(
            self._send_delta_info(nodes, share_id)).whenDone()

    def _send_delta_info(self, nodes, share_id):
        """Build and send the DELTA_INFO for each node."""
        count = 0
        for node in nodes:
            if count == config.api_server.max_delta_info:
                count = 0
                yield
            message = protocol_pb2.Message()
            message.type = protocol_pb2.Message.DELTA_INFO
            message.delta_info.type = protocol_pb2.DeltaInfo.FILE_INFO
            delta_info = message.delta_info
            delta_info.generation = node.generation
            delta_info.is_live = node.is_live
            if node.is_file:
                delta_info.file_info.type = protocol_pb2.FileInfo.FILE
            else:
                delta_info.file_info.type = protocol_pb2.FileInfo.DIRECTORY
            delta_info.file_info.name = node.name
            delta_info.file_info.share = share_id
            delta_info.file_info.node = str(node.id)
            if node.parent_id is None:
                delta_info.file_info.parent = ''
            else:
                delta_info.file_info.parent = str(node.parent_id)
            delta_info.file_info.is_public = node.is_public
            if node.content_hash is None:
                delta_info.file_info.content_hash = ''
            else:
                delta_info.file_info.content_hash = str(node.content_hash)
            delta_info.file_info.crc32 = node.crc32
            delta_info.file_info.size = node.size
            delta_info.file_info.last_modified = node.last_modified
            self.sendMessage(message)
            count += 1


class RescanFromScratchResponse(GetDeltaResponse):
    """RescanFromScratch Request Response."""

    __slots__ = ()

    @inlineCallbacks
    def _process(self):
        """Get all the live nodes and send DeltaInfos."""
        msg = self.source_message
        limit = config.api_server.get_from_scratch_limit
        share_id = self.convert_share_id(msg.get_delta.share)
        # get the first chunk
        delta_info = yield self.protocol.user.get_from_scratch(
            share_id, limit=limit)
        # keep vol_generation and free_bytes for later
        nodes, vol_generation, free_bytes = delta_info
        self.length = len(nodes)
        while nodes:
            # while it keep returning nodes, send the current nodes
            yield self.send_delta_info(nodes, msg.get_delta.share)
            # and request more
            last_path = (nodes[-1].path, nodes[-1].name)
            delta_info = yield self.protocol.user.get_from_scratch(
                share_id, start_from_path=last_path, limit=limit,
                max_generation=vol_generation)
            # but don't overwrite vol_generation or free_bytes in case
            # something changes while fetching nodes
            nodes, _, _ = delta_info
            self.length += len(nodes)

        # now send the DELTA_END
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.DELTA_END
        response.delta_end.full = True
        response.delta_end.generation = vol_generation
        response.delta_end.free_bytes = free_bytes
        self.sendMessage(response)

        # save data to be logged on operation end
        t = "vol_id=%s current_gen=%s nodes=%d free_bytes=%s" % (
            share_id, vol_generation, self.length, free_bytes)
        self.operation_data = t


class QuerySetCapsResponse(SimpleRequestResponse):
    """QUERY_CAPS and SET_CAPS Request Response."""

    __slots__ = ('set_mode',)

    authentication_required = False

    def __init__(self, protocol, message, set_mode=False):
        """Stores the mode."""
        self.set_mode = set_mode
        super(QuerySetCapsResponse, self).__init__(protocol, message)

    def _process(self):
        """Validate the list of capabilities passed to us."""
        if self.set_mode:
            msg_caps = self.source_message.set_caps
        else:
            msg_caps = self.source_message.query_caps
        caps = frozenset(q.capability for q in msg_caps)

        # after one succesful, no more allowed
        if self.protocol.working_caps == MIN_CAP:
            accepted = caps in SUPPORTED_CAPS
            if self.set_mode:
                if accepted:
                    self.protocol.working_caps = caps

            # get the redirecting info
            redirect = SUGGESTED_REDIRS.get(caps, {})
            red_host, red_port, red_srvr = [redirect.get(x, "")
                                            for x in "hostname", "port",
                                            "srvrecord"]
        else:
            accepted = False
            red_host = red_port = red_srvr = ""

        # log what we just decided
        action = "Set" if self.set_mode else "Query"
        result = "Accepted" if accepted else "Rejected"
        self.log.info("Capabilities %s %s: %s", action, result, caps)

        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.ACCEPT_CAPS
        response.accept_caps.accepted = accepted
        response.accept_caps.redirect_hostname = red_host
        response.accept_caps.redirect_port = red_port
        response.accept_caps.redirect_srvrecord = red_srvr
        self.sendMessage(response)


class MoveResponse(SimpleRequestResponse):
    """Move Request Response."""

    __slots__ = ()

    expected_foreign_errors = [
        dataerror.InvalidFilename,
        dataerror.NoPermission,
        dataerror.AlreadyExists,
        dataerror.NotADirectory,
    ]

    user_activity = 'sync_activity'

    def _get_node_info(self):
        """Return node info from the message."""
        node_id = self.source_message.move.node
        return "node: %r" % (node_id,)

    @inlineCallbacks
    def _process(self):
        """Move the node."""
        share_id = self.convert_share_id(self.source_message.move.share)
        node_id = self.source_message.move.node
        new_parent_id = self.source_message.move.new_parent_node
        new_name = self.source_message.move.new_name

        generation, mimetype = yield self.protocol.user.move(
            share_id, node_id, new_parent_id, new_name,
            session_id=self.protocol.session_id)

        response = protocol_pb2.Message()
        response.new_generation = generation
        response.type = protocol_pb2.Message.OK
        self.sendMessage(response)

        # save data to be logged on operation end
        extension = self._get_extension(new_name)
        self.operation_data = "vol_id=%s node_id=%s mime=%r ext=%r" % (
            share_id, node_id, mimetype, extension)


class MakeResponse(SimpleRequestResponse):
    """MAKE_[DIR|FILE] Request Response."""

    __slots__ = ()

    expected_foreign_errors = [
        dataerror.InvalidFilename,
        dataerror.NoPermission,
        dataerror.AlreadyExists,
        dataerror.NotADirectory,
    ]

    user_activity = 'sync_activity'

    def _get_node_info(self):
        """Return node info from the message."""
        parent_id = self.source_message.make.parent_node
        return "parent: %r" % (parent_id,)

    @inlineCallbacks
    def _process(self):
        """Create the file/directory."""
        if self.source_message.type == protocol_pb2.Message.MAKE_DIR:
            response_type = protocol_pb2.Message.NEW_DIR
            create_method_name = "make_dir"
            node_type = "Directory"
        elif self.source_message.type == protocol_pb2.Message.MAKE_FILE:
            response_type = protocol_pb2.Message.NEW_FILE
            create_method_name = "make_file"
            node_type = "File"
        else:
            raise request.StorageProtocolError(
                "Can not create from message "
                "(type %s)." % self.source_message.type)

        share_id = self.convert_share_id(self.source_message.make.share)
        parent_id = self.source_message.make.parent_node
        name = self.source_message.make.name
        create_method = getattr(self.protocol.user, create_method_name)
        d = create_method(share_id, parent_id, name,
                          session_id=self.protocol.session_id)
        node_id, generation, mimetype = yield d
        response = protocol_pb2.Message()
        response.type = response_type
        response.new_generation = generation
        response.new.node = str(node_id)
        response.new.parent_node = parent_id
        response.new.name = name
        self.sendMessage(response)

        # save data to be logged on operation end
        extension = self._get_extension(name)
        self.operation_data = "type=%s vol_id=%s node_id=%s mime=%r ext=%r" % (
            node_type, share_id, node_id, mimetype, extension)


class FreeSpaceResponse(SimpleRequestResponse):
    """Implements FREE_SPACE_INQUIRY."""

    __slots__ = ()

    expected_foreign_errors = [dataerror.NoPermission]

    @inlineCallbacks
    def _process(self):
        """Reports available space for the given share (or the user's own
        storage).

        """
        share_id = self.convert_share_id(
            self.source_message.free_space_inquiry.share_id)
        free_bytes = yield self.protocol.user.get_free_bytes(share_id)
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.FREE_SPACE_INFO
        response.free_space_info.free_bytes = free_bytes
        self.sendMessage(response)

        # save data to be logged on operation end
        self.operation_data = "vol_id=%s free_bytes=%s" % (
            share_id, free_bytes)


class AccountResponse(SimpleRequestResponse):
    """Implements ACCOUNT_INQUIRY."""

    __slots__ = ()

    @inlineCallbacks
    def _process(self):
        """Reports user account information."""
        (purchased, used) = yield self.protocol.user.get_storage_byte_quota()
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.ACCOUNT_INFO
        response.account_info.purchased_bytes = purchased
        self.sendMessage(response)


class AuthenticateResponse(SimpleRequestResponse):
    """AUTH_REQUEST Request Response."""

    __slots__ = ()

    authentication_required = False
    not_allowed_re = re.compile("[^\w_]")

    user_activity = 'connected'

    @inlineCallbacks
    def _process(self):
        """Authenticate the user."""
        # check that its not already logged in
        if self.protocol.user is not None:
            raise errors.AuthenticationError("User already logged in.")

        self.protocol.check_poison("authenticate_start")
        # get metadata and send stats
        metadata = dict(
            (m.key, m.value) for m in self.source_message.metadata)
        if metadata:
            self.log.info("Client metadata: %s", metadata)
            for key in ("platform", "version"):
                if key in metadata:
                    value = self.not_allowed_re.sub("_", metadata[key])
                    self.protocol.factory.metrics.meter("client.%s.%s" %
                                                        (key, value), 1)
        # do auth
        auth_parameters = dict(
            (param.name, param.value)
            for param in self.source_message.auth_parameters)
        authenticate = self.protocol.factory.auth_provider.authenticate
        user = yield authenticate(auth_parameters, self.protocol)
        self.protocol.check_poison("authenticate_continue")
        if user is None:
            raise errors.AuthenticationError("Authentication failed.")

        # confirm authentication
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.AUTH_AUTHENTICATED
        # include the session_id
        response.session_id = str(self.protocol.session_id)
        self.sendMessage(response)

        # set up user for this session
        self.protocol.set_user(user)

        # let the client know the root
        root_id, _ = yield user.get_root()
        response = protocol_pb2.Message()
        response.type = protocol_pb2.Message.ROOT
        response.root.node = str(root_id)
        self.sendMessage(response)


class Action(BaseRequestResponse):
    """This is an internal action, that behaves like a request.

    We can use this Action class to queue work in the pending_requests without
    need to worry of handling errors in the request itself.
    """

    __slots__ = ('_callable', 'log')

    def __init__(self, request, action):
        """Create the request response. Setup logger."""
        self._callable = action
        # keep the request logger around
        self.log = request.log
        super(Action, self).__init__(request.protocol, request.source_message)

    def start(self, head=True):
        """Schedule the action start for later.

        It will be executed when the server has no further pending requests.

        """
        def _scheduled_start():
            """The scheduled call."""
            super(Action, self).start()

        self.log.debug("Action being scheduled (%s)", self._callable)
        self.protocol.factory.metrics.increment("action_instances.%s" %
                                                (self.__class__.__name__,))
        self.protocol.schedule_request(self, _scheduled_start, head=head)

    def _start(self):
        """Kick off action processing."""
        self.log.debug("Action being started, working on: %s", self._callable)
        d = maybeDeferred(self._callable)
        d.addCallbacks(self.done, self.error)

    def cleanup(self):
        """Remove the reference to self from the request handler"""
        self.finished = True
        self.started = False
        self.protocol.factory.metrics.decrement("action_instances.%s" %
                                                (self.__class__.__name__,))
        self.protocol.release(self)

    def done(self, result):
        """Overrided done to add logging and to use the callable result.

        Added an ignored parameter so we can hook this to the defered chain.
        """
        try:
            self.log.debug("Action done (%s)", self._callable)
            self.cleanup()
            self.deferred.callback(result)
        except Exception as e:
            self.error(Failure(e))

    def error(self, failure):
        """Rall this to signal that the action finished with failure

        @param failure: the failure instance
        """
        self.cleanup()
        self.deferred.errback(failure)


class StorageServerFactory(Factory):
    """The Storage Server Factory.

    @cvar protocol: The class of the server.
    """
    protocol = StorageServer
    graceful_shutdown = config.api_server.graceful_shutdown

    def __init__(self, s3_host, s3_port, s3_ssl, s3_key, s3_secret,
                 s3_proxy_host=None, s3_proxy_port=None,
                 auth_provider_class=auth.DummyAuthProvider,
                 s3_class=S3, content_class=content.ContentManager,
                 reactor=reactor, oops_config=None,
                 servername=None, reactor_inspector=None):
        """Create a StorageServerFactory."""
        self.auth_provider = auth_provider_class(self)
        self.content = content_class(self)
        self.s3_class = s3_class
        self.s3_host = s3_host
        self.s3_port = s3_port
        self.s3_ssl = s3_ssl
        self.s3_key = s3_key
        self.s3_secret = s3_secret
        self.s3_proxy_host = s3_proxy_host
        self.s3_proxy_port = s3_proxy_port
        self.logger = logging.getLogger("storage.server")

        self.metrics = MetricsConnector.get_metrics("root")
        self.user_metrics = MetricsConnector.get_metrics("user")
        self.sli_metrics = MetricsConnector.get_metrics("sli")

        self.servername = servername
        self.reactor_inspector = reactor_inspector

        # note that this relies on the notifier calling the
        # callback from the reactor thread
        notif = notifier.get_notifier()
        notif.set_event_callback(
            notifier.UDFCreate,
            self.event_callback_handler(self.deliver_udf_create))
        notif.set_event_callback(
            notifier.UDFDelete,
            self.event_callback_handler(self.deliver_udf_delete))
        notif.set_event_callback(
            notifier.ShareCreated,
            self.event_callback_handler(self.deliver_share_created))
        notif.set_event_callback(
            notifier.ShareAccepted,
            self.event_callback_handler(self.deliver_share_accepted))
        notif.set_event_callback(
            notifier.ShareDeclined,
            self.event_callback_handler(self.deliver_share_declined))
        notif.set_event_callback(
            notifier.ShareDeleted,
            self.event_callback_handler(self.deliver_share_deleted))
        notif.set_event_callback(
            notifier.VolumeNewGeneration,
            self.event_callback_handler(self.deliver_volume_new_generation))

        self.protocols = []
        self.reactor = reactor
        self.trace_users = set(config.api_server.trace_users)

        # oops and log observer
        self.oops_config = oops_config
        twisted.python.log.addObserver(self._deferror_handler)

    def _deferror_handler(self, data):
        """Deferred error handler.

        We receive all stuff here, filter the errors and use correct info.
        """
        if not data.get('isError', None):
            return
        try:
            failure = data['failure']
        except KeyError:
            msg = data['message']
            failure = None
        else:
            msg = failure.getTraceback()

        # log
        self.logger.error("Unhandled error in deferred! %s", msg)

    def build_notification_oops(self, failure, notif, tl=None):
        """Create an oops entry to log the notification failure."""
        context = {"exc_info": (failure.type, failure.value, failure.tb)}
        del failure

        if tl is not None:
            context["timeline"] = tl

        report = self.oops_config.create(context)
        del context

        return report

    def event_callback_handler(self, func):
        """Wrap the event callback in an error handler."""
        @wraps(func)
        def wrapper(notif, **kwargs):
            """The wrapper."""
            tl = timeline.Timeline(format_stack=None)
            action = tl.start("EVENT-%s" % notif.event_type, "")

            def notification_error_handler(failure):
                """Handle error while processing a Notification."""
                action.detail = "NOTIFY WITH %r (%s)" % (notif, kwargs)
                action.finish()

                oops = self.build_notification_oops(failure, notif, tl)
                oops_id = self.oops_config.publish(oops)
                self.logger.error(" %s in notification %r logged in OOPS: %s",
                                  failure.value, notif, oops_id)

            d = defer.maybeDeferred(func, notif, **kwargs)
            d.addErrback(notification_error_handler)
            return d
        return wrapper

    def s3(self):
        """Get an s3lib instance to do s3 operations."""
        return self.s3_class(self.s3_host, self.s3_port, self.s3_key,
                             self.s3_secret, use_ssl=self.s3_ssl,
                             proxy_host=self.s3_proxy_host,
                             proxy_port=self.s3_proxy_port)

    @inlineCallbacks
    def deliver_udf_create(self, udf_create):
        """Handle UDF creation notification."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != udf_create.source_session

        user = yield self.content.get_user_by_id(udf_create.owner_id)
        if user:  # connected instances for this user?
            resp = protocol_pb2.Message()
            resp.type = protocol_pb2.Message.VOLUME_CREATED
            resp.volume_created.type = protocol_pb2.Volumes.UDF
            resp.volume_created.udf.volume = str(udf_create.udf_id)
            resp.volume_created.udf.node = str(udf_create.root_id)
            resp.volume_created.udf.suggested_path = udf_create.suggested_path
            user.broadcast(resp, filter=notif_filter)

    @inlineCallbacks
    def deliver_share_created(self, share_notif):
        """Handle Share creation notification."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != share_notif.source_session

        to_user = yield self.content.get_user_by_id(share_notif.shared_to_id)
        if to_user is None:
            return
        share_resp = sharersp.NotifyShareHolder.from_params(
            share_notif.share_id, share_notif.root_id, share_notif.name,
            to_user.username, to_user.visible_name, share_notif.access)
        proto_msg = protocol_pb2.Message()
        proto_msg.type = protocol_pb2.Message.NOTIFY_SHARE
        share_resp.dump_to_msg(proto_msg.notify_share)
        to_user.broadcast(proto_msg, filter=notif_filter)

    @inlineCallbacks
    def deliver_share_accepted(self, share_notif, recipient_id):
        """Handle Share accepted notification."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != share_notif.source_session

        def to_notif_filter(protocol):
            """Return True if the client should recieve the notification."""
            return protocol.session_id != share_notif.source_session

        by_user = yield self.content.get_user_by_id(share_notif.shared_by_id)
        to_user = yield self.content.get_user_by_id(share_notif.shared_to_id)

        if by_user and recipient_id == by_user.id:
            proto_msg = protocol_pb2.Message()
            proto_msg.type = protocol_pb2.Message.SHARE_ACCEPTED
            proto_msg.share_accepted.share_id = str(share_notif.share_id)
            proto_msg.share_accepted.answer = protocol_pb2.ShareAccepted.YES
            by_user.broadcast(proto_msg, filter=notif_filter)
        if to_user and recipient_id == to_user.id:
            if by_user is None:
                by_user = yield self.content.get_user_by_id(
                    share_notif.shared_by_id, required=True)
            share_resp = sharersp.ShareResponse.from_params(
                str(share_notif.share_id), "to_me", share_notif.root_id,
                share_notif.name, by_user.username or u"",
                by_user.visible_name or u"", protocol_pb2.ShareAccepted.YES,
                share_notif.access)
            resp = protocol_pb2.Message()
            resp.type = protocol_pb2.Message.VOLUME_CREATED
            resp.volume_created.type = protocol_pb2.Volumes.SHARE
            share_resp.dump_to_msg(resp.volume_created.share)
            to_user.broadcast(resp, filter=to_notif_filter)

    @inlineCallbacks
    def deliver_share_declined(self, share_notif):
        """Handle Share declined notification."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != share_notif.source_session

        by_user = yield self.content.get_user_by_id(share_notif.shared_by_id)
        if by_user:
            proto_msg = protocol_pb2.Message()
            proto_msg.type = protocol_pb2.Message.SHARE_ACCEPTED
            proto_msg.share_accepted.share_id = str(share_notif.share_id)
            proto_msg.share_accepted.answer = protocol_pb2.ShareAccepted.NO
            by_user.broadcast(proto_msg, filter=notif_filter)

    @inlineCallbacks
    def deliver_share_deleted(self, share_notif):
        """Handle Share deletion notification."""
        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != share_notif.source_session

        to_user = yield self.content.get_user_by_id(share_notif.shared_to_id)
        #by_user = yield self.content.get_user_by_id(share_notif.shared_by_id)
        if to_user is None:
            return
        proto_msg = protocol_pb2.Message()
        proto_msg.type = protocol_pb2.Message.SHARE_DELETED
        proto_msg.share_deleted.share_id = str(share_notif.share_id)
        if to_user:
            to_user.broadcast(proto_msg, filter=notif_filter)
        #if by_user:
        #    by_user.broadcast(proto_msg, filter=notif_filter)

    @inlineCallbacks
    def deliver_udf_delete(self, udf_delete):
        """Handle UDF deletion notification."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != udf_delete.source_session

        user = yield self.content.get_user_by_id(udf_delete.owner_id)
        if user:  # connected instances for this user?
            resp = protocol_pb2.Message()
            resp.type = protocol_pb2.Message.VOLUME_DELETED
            resp.volume_deleted.volume = str(udf_delete.udf_id)
            user.broadcast(resp, filter=notif_filter)

    @inlineCallbacks
    def deliver_volume_new_generation(self, notif):
        """Handle the notification of a new generation for a volume."""

        def notif_filter(protocol):
            """Return True if the client should receive the notification."""
            return protocol.session_id != notif.source_session

        user = yield self.content.get_user_by_id(notif.user_id)
        if user:  # connected instances for this user?
            resp = protocol_pb2.Message()
            resp.type = protocol_pb2.Message.VOLUME_NEW_GENERATION
            if notif.client_volume_id is None:
                resp.volume_new_generation.volume = ''
            else:
                resp.volume_new_generation.volume = str(notif.client_volume_id)
            resp.volume_new_generation.generation = notif.new_generation
            user.broadcast(resp, filter=notif_filter)

    def wait_for_shutdown(self):
        """Wait until all the current servers have finished serving."""
        if not self.protocols or not self.graceful_shutdown:
            return defer.succeed(None)

        wait_d = [protocol.wait_for_shutdown() for protocol in self.protocols]
        done_with_current = self.wait_for_current_shutdown()
        d = defer.DeferredList(wait_d + [done_with_current])
        return d

    def wait_for_current_shutdown(self):
        """wait for the current protocols to end."""
        d = defer.Deferred()

        def _wait():
            "do the waiting"
            if self.protocols:
                self.reactor.callLater(0.1, _wait)
            else:
                d.callback(True)
        _wait()
        return d


class OrderedMultiService(MultiService):
    """A container which starts and shuts down the services in strict order."""

    def startService(self):
        """Start the services sequentually.  Returns a deferred which
        signals completion.
        """
        # deliberately skip MultiService's implementation
        Service.startService(self)
        d = defer.succeed(None)
        for service in self:
            d.addCallback(lambda _, s: s.startService(), service)
        return d

    def stopService(self):
        """Stop the services sequentially (in the opposite order they
        are started).  Returns a deferred which signals completion.
        """
        # deliberately skip MultiService's implementation
        Service.stopService(self)
        d = defer.succeed(None)
        services = list(self)
        services.reverse()
        for service in services:
            d.addBoth(lambda _, s: s.stopService(), service)
        return d


def get_service_port(service):
    """Returns the actual port a simple service is bound to."""
    # we have to query this rather than simply using the port number the
    # service was passed at creation time -- if the given port was 0,
    # the real port number will have been dynamically assigned
    return service._port.getHost().port


class StorageServerService(OrderedMultiService):
    """Wrap the whole TCP StorageServer mess as a single twisted serv."""

    def __init__(self, port, s3_host, s3_port, s3_ssl, s3_key, s3_secret,
                 s3_proxy_host=None, s3_proxy_port=None,
                 auth_provider_class=None,
                 oops_config=None, status_port=0, heartbeat_interval=None):
        """Create a StorageServerService.

        @param port: the port to listen on without ssl.
        @param s3_host: the S3 server hostname.
        @param s3_port: the S3 server port to connect to.
        @param s3_key: the S3 key.
        @param s3_secret: the S3 secret.
        @param auth_provider_class: the authentication provider.
        """
        OrderedMultiService.__init__(self)
        self.heartbeat_writer = None
        if heartbeat_interval is None:
            heartbeat_interval = float(config.api_server.heartbeat_interval)
        self.heartbeat_interval = heartbeat_interval
        self.rpcdal_client = None
        self.rpcauth_client = None
        self.logger = logging.getLogger("storage.server")
        self.servername = config.api_server.servername
        self.logger.info("Starting %s", self.servername)
        self.logger.info(
            "protocol buffers implementation: %s",
            os.environ.get("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "None"))

        namespace = config.api_server.metrics_namespace
        # Register all server metrics components
        MetricsConnector.register_metrics("root", namespace)
        # Important:  User activity is in a global namespace!
        environment = config.general.environment_name
        user_namespace = environment + ".storage.user_activity"
        MetricsConnector.register_metrics("user", user_namespace)
        MetricsConnector.register_metrics("reactor_inspector",
                                          namespace + ".reactor_inspector")
        sli_metric_namespace = config.api_server.sli_metric_namespace
        MetricsConnector.register_metrics('sli', sli_metric_namespace)

        self.metrics = get_meter(scope='service')

        listeners = MultiService()
        listeners.setName("Listeners")
        listeners.setServiceParent(self)

        self._reactor_inspector = ReactorInspector(self.logger,
                                                   reactor.callFromThread)

        self.factory = StorageServerFactory(
            s3_host, s3_port, s3_ssl, s3_key, s3_secret,
            s3_proxy_host=s3_proxy_host, s3_proxy_port=s3_proxy_port,
            auth_provider_class=auth_provider_class,
            oops_config=oops_config, servername=self.servername,
            reactor_inspector=self._reactor_inspector)

        self.tcp_service = TCPServer(port, self.factory)
        self.tcp_service.setName("TCP")
        self.tcp_service.setServiceParent(listeners)

        # setup the status service
        self.status_service = stats.create_status_service(
            self, listeners, status_port)

        self.next_log_loop = None
        stats_log_interval = config.api_server.stats_log_interval
        self.stats_worker = stats.StatsWorker(self, stats_log_interval,
                                              self.servername)

    @property
    def port(self):
        """The port without ssl."""
        return get_service_port(self.tcp_service)

    @property
    def status_port(self):
        """The status service port."""
        return get_service_port(self.status_service)

    def start_rpc_client(self):
        """Setup the rpc client."""
        self.logger.info("Starting the RPC clients.")
        self.rpcdal_client = inthread.ThreadedNonRPC(inthread.DAL_BACKEND)
        self.rpcauth_client = inthread.ThreadedNonRPC(inthread.AUTH_BACKEND)

    @inlineCallbacks
    def startService(self):
        """Start listening on two ports."""
        self.logger.info("- - - - - SERVER STARTING")
        yield OrderedMultiService.startService(self)
        yield defer.maybeDeferred(self.start_rpc_client)
        self.factory.content.rpcdal_client = self.rpcdal_client
        self.factory.rpcauth_client = self.rpcauth_client
        self.stats_worker.start()
        self.metrics.meter('server_start')
        self.metrics.increment('services_active')
        metrics.services.revno()

        self._reactor_inspector.start()
        # only start the HeartbeatWriter if the interval is > 0
        if self.heartbeat_interval > 0:
            self.heartbeat_writer = stdio.StandardIO(
                supervisor_utils.HeartbeatWriter(self.heartbeat_interval,
                                                 self.logger))

    @inlineCallbacks
    def stopService(self):
        """Stop listening on both ports."""
        self.logger.info("- - - - - SERVER STOPPING")
        yield self.stats_worker.stop()
        yield OrderedMultiService.stopService(self)
        yield self.factory.wait_for_shutdown()
        self.metrics.meter('server_stop')
        self.metrics.decrement('services_active')
        if self.metrics.connection:
            self.metrics.connection.disconnect()
        if self.factory.metrics.connection:
            self.factory.metrics.connection.disconnect()
        if self.factory.user_metrics.connection:
            self.factory.user_metrics.connection.disconnect()
        self._reactor_inspector.stop()
        if self.heartbeat_writer:
            self.heartbeat_writer.loseConnection()
            self.heartbeat_writer = None
        for metrics_key in ["reactor_inspector", "sli"]:
            metrics = MetricsConnector.get_metrics(metrics_key)
            if metrics.connection:
                metrics.connection.disconnect()
        self.logger.info("- - - - - SERVER STOPPED")


def create_service(s3_host, s3_port, s3_ssl, s3_key, s3_secret,
                   s3_proxy_host=None, s3_proxy_port=None,
                   status_port=None,
                   auth_provider_class=auth.DummyAuthProvider,
                   oops_config=None):
    """Start the StorageServer service."""

    # configure logs
    logger = logging.getLogger(config.api_server.logger_name)
    handler = configure_logger(logger=logger, propagate=False,
                               filename=config.api_server.log_filename,
                               start_observer=True)

    # set up s3
    s3_logger = logging.getLogger('s3lib')
    s3_logger.setLevel(config.general.log_level)
    s3_logger.addHandler(handler)

    # set up the hacker's logger always in TRACE
    h_logger = logging.getLogger(config.api_server.logger_name + ".hackers")
    h_logger.setLevel(TRACE)
    h_logger.propagate = False
    h_logger.addHandler(handler)

    logger.debug('S3 host:%s port:%s', s3_host, s3_port)

    # turn on heapy if must to
    if os.getenv('USE_HEAPY'):
        logger.debug('importing heapy')
        try:
            import guppy.heapy.RM
        except ImportError:
            logger.warning('guppy-pe/heapy not available, remote monitor '
                           'thread not started')
        else:
            guppy.heapy.RM.on()
            logger.debug('activated heapy remote monitor')

    # set GC's debug
    if config.api_server.gc_debug:
        import gc
        gc.set_debug(gc.DEBUG_UNCOLLECTABLE)
        logger.debug("set gc debug on")

    if status_port is None:
        status_port = config.api_server.status_port

    # create the service
    service = StorageServerService(
        config.api_server.tcp_port, s3_host, s3_port, s3_ssl, s3_key,
        s3_secret, s3_proxy_host, s3_proxy_port, auth_provider_class,
        oops_config, status_port)
    return service
