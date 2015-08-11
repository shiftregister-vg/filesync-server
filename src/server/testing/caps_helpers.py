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

"""helpers for capabilities related tests"""

import functools
from ubuntuone import syncdaemon
from ubuntuone.storage.server import server


def required_caps(*args, **kwargs):
    """A TestCase methods decorator to specify a set of capabilities required
    in test methods.

    @param supported_caps_set: (*args) a set of capabilities (each
    capability is a set)
    @param validate: (keyword arg) check the supported_caps_set against the
    server SUPPORTED_CAPS and fail the tests if don't match
    """
    # get the kewyord argument
    validate = kwargs.get('validate', True)
    # get the caps sets
    if not len(args):
        raise TypeError('required_caps takes exactly 1 argument (0 given)')
    supported_caps_set = set()
    for cap in args:
        supported_caps_set.add(frozenset(cap))

    def wrapper(func):
        """the wrapped function/method"""

        if validate and supported_caps_set.difference(server.SUPPORTED_CAPS):
            def fail_func(self, *args, **kwargs):
                """a wrapper that always fail"""
                self.fail("The specified supported capabilities don't match "
                          "the server.SUPPORTED_CAPS")
            functools.update_wrapper(fail_func, func)
            return fail_func
        else:
            # to support older client without explicit required capabilities
            client_required_caps = getattr(syncdaemon, 'REQUIRED_CAPS',
                                           frozenset())
            if client_required_caps not in supported_caps_set:
                if kwargs.get('message') is not None:
                    func.skip = kwargs.get('message')
                else:
                    func.skip = "client don't have the required capabilities" \
                        ": %r" % supported_caps_set
            return func
    return wrapper
