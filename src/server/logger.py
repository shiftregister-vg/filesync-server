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

"""Logging utilities."""

from __future__ import absolute_import

import os
import platform
import sys
import time
import signal
import socket
import logging

from functools import partial
from logging.handlers import TimedRotatingFileHandler, WatchedFileHandler
from twisted.python import log

from config import config


# disable storm txn logger
txn_logger = logging.getLogger('txn')
txn_logger.propagate = False

# define the TRACE level
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


class SignalAwareFileHandler(WatchedFileHandler):
    """Close and reopen the stream when signal SIGUSR1 isreceived."""

    def __init__(self, *args, **kwargs):
        WatchedFileHandler.__init__(self, *args, **kwargs)

        def reopenLog(signal, frame):
            """the signal handler"""
            self.reopen_stream()
        signal.signal(signal.SIGUSR1, reopenLog)

    def reopen_stream(self):
        """close and open the stream"""
        if self.stream:
            self.stream.close()
        self.stream = self._open()


# rotate at midnight
MidnightRotatingFileHandler = partial(TimedRotatingFileHandler,
                                      when='midnight')


class SysLogHandler(logging.handlers.SysLogHandler):

    def emit(self, record):
        """
        Emit a record.

        The record is formatted, and then sent to the syslog server. If the
        message contains multiple lines, it's split over multiple records.
        """
        prio = '<%d>' % self.encodePriority(self.facility,
                                            self.mapPriority(record.levelname))
        msg = self.format(record)

        # Message is a string. Convert to bytes as required by RFC 5424
        if type(msg) is unicode:
            msg = msg.encode('utf-8')

        # Break the message up into lines and send them.
        lines = msg.strip().split('\n')

        firstLine = True
        fmt = self.formatter._fmt
        data = record.__dict__.copy()
        for line in lines:
            if firstLine:
                firstLine = False
            else:
                # Each additional line must be formatted again since only the
                # first line is formated by format() above.
                data["message"] = "\t" + line
                line = fmt % data

            line = prio + line + "\000"

            # Copied from logging/handlers.py in Python stdlib.
            try:
                if self.unixsocket:
                    try:
                        self.socket.send(line)
                    except socket.error:
                        self._connect_unixsocket(self.address)
                        self.socket.send(line)
                elif getattr(self, "socktype",
                             socket.SOCK_DGRAM) == socket.SOCK_DGRAM:
                    self.socket.sendto(line, self.address)
                else:
                    self.socket.sendall(line)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.handleError(record)


def configure_handler(folder=None, filename=None,
                      log_format=None, logger_class=None,
                      log_to_syslog=None, syslog_name=None):
    """Configure a logging handler."""
    if folder is None:
        folder = config.general.log_folder
    if log_format is None:
        log_format = config.general.log_format
    if logger_class is None:
        logger_class = SignalAwareFileHandler
    if log_to_syslog is None:
        log_to_syslog = config.general.log_to_syslog
    if syslog_name is None:
        syslog_name = config.general.app_name

    datefmt = None
    if log_to_syslog:
        datefmt = "%Y-%m-%dT%H:%M:%S"
        log_format = " ".join(["%(asctime)s.%(msecs)03dZ", platform.node(),
                               syslog_name, config.general.syslog_format])
        handler = SysLogHandler()
    else:
        if filename is not None:
            if not os.path.isabs(filename):
                filename = os.path.join(folder, os.path.basename(filename))
            else:
                folder = os.path.dirname(filename)
            if not os.access(folder, os.F_OK):
                os.makedirs(folder)
            handler = logger_class(filename)
        else:
            handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(TRACE)
    log_formatter = logging.Formatter(log_format, datefmt=datefmt)
    log_formatter.converter = time.gmtime
    handler.setFormatter(log_formatter)
    return handler


def configure_logger(name=None, logger=None, filename=None, folder=None,
                     level=None, log_format=None, start_observer=False,
                     propagate=True, logger_class=None, syslog_name=None,
                     log_to_syslog=None):
    """Configure the logger."""
    if level is None:
        level = config.general.log_level
    if syslog_name is None:
        syslog_name = name

    if logger is None and name is None:
        raise ValueError("Must provide one of 'name' or 'logger'")

    if logger is None:
        logger = logging.getLogger(name)
    elif name is None:
        name = logger.name

    # remove StreamHandler from the root logger.
    root_logger = logging.getLogger()
    for hdlr in root_logger.handlers:
        if isinstance(hdlr, logging.StreamHandler):
            root_logger.removeHandler(hdlr)

    logger.propagate = propagate
    logger.setLevel(level)
    handler = configure_handler(folder=folder, filename=filename,
                                log_format=log_format,
                                logger_class=logger_class,
                                syslog_name=syslog_name,
                                log_to_syslog=log_to_syslog)
    logger.addHandler(handler)

    if start_observer:
        log_observer = log.PythonLoggingObserver(
            name == 'root' and 'twisted' or name + '.twisted')
        log_observer.start()

    return handler


def setup_logging(name, logger=None, start_observer=False):
    """Setting up logging."""
    cfg = getattr(config, name)
    return configure_logger(name, logger=logger, filename=cfg.log_filename,
                            propagate=False, start_observer=start_observer)
