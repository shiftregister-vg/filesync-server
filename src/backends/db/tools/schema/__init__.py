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

"""Support for Database Schemas"""

import sys
import time

from functools import wraps


def print_elapsed_time(f):
    """Decorator to print elapsed time.

    To format the output nicely, do a:
      sys.stderr.write("This is what I am doing... ")

    Note the missing newline at the end.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        """Wrapper."""
        start = time.time()
        r = f(*args, **kwargs)
        end = time.time()
        sys.stderr.write(" %0.3f ms\n" % ((end - start) * 1000.0))
        return r
    return wrapper

from backends.db.tools.schema.patch import (  # NOQA
    PatchApplier, UnknownPatchError, BadPatchError)
from backends.db.tools.schema.schema import Schema  # NOQA
