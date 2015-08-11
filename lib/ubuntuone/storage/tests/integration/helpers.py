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

"""Helpers for the integration tests."""

import os
import random
import time

import dbus

from twisted.internet import defer


LOREM_IPSUM = """Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Nam dapibus commodo magna, et dignissim eros aliquam nec. Vivamus
bibendum, mi quis porta volutpat, velit turpis volutpat urna, at tempor
ante massa at turpis. Nulla at felis ac dui accumsan fermentum. Mauris
accumsan convallis vehicula. Sed urna justo, mattis feugiat cursus a,
semper non augue. Ut et congue velit. Fusce non nunc erat. Donec id
sagittis elit. Curabitur bibendum vulputate accumsan. Nunc pharetra
molestie orci, a mattis nulla sodales in.
"""


def debug(prefix, msg, *args, **kwargs):
    """Print debug messages."""
    t = time.time()
    tstamp = time.strftime("%H:%M:%S", time.localtime(t)) + str(t % 1)[1:5]
    if 'previous_newline' in kwargs and kwargs['previous_newline']:
        print
    print tstamp, prefix, msg, ' '.join(map(str, args))


def walk_and_list_dir(directory, with_dirname=False):
    """List every directory and file under dirname.

    This filters out all support directories.
    """
    result = []
    dir_len = len(directory) + 1
    for (dirname, dirpath, files) in os.walk(directory):
        # remove special files from the system and from tests infrastructure
        if '.config' in dirname or '.local' in dirname or '.mark' in dirname:
            continue
        if not with_dirname:
            dirname = dirname[dir_len:]
        result.append(dirname)
        result.extend([os.path.join(dirname, f) for f in files])

    # remove bogus empty nothing
    if '' in result:
        result.remove('')

    result.sort()
    return result


def create_file_and_add_content(filepath, content=None):
    """Create a file under filepath and add random content to it."""
    if content is None:
        limit = random.randint(0, len(LOREM_IPSUM))
        content = LOREM_IPSUM[:limit]
    with open(filepath, 'w') as fd:
        fd.write(content)
    return content


def _is_retry_exception(err):
    """Check if the exception is a retry one."""
    if isinstance(err, dbus.exceptions.DBusException):
        if err.get_dbus_name() == 'org.freedesktop.DBus.Error.NoReply':
            return True
    return False


def retryable(func):
    """Call the function until its deferred not timeouts (max n times)."""

    @defer.inlineCallbacks
    def f(*a, **k):
        """Built func."""
        opportunities = 10
        while opportunities:
            try:
                res = yield func(*a, **k)
            except Exception, err:
                opportunities -= 1
                if opportunities == 0 or not _is_retry_exception(err):
                    raise
            else:
                break
        defer.returnValue(res)

    return f
