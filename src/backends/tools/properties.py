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

"""Some helper functions for storm."""

import uuid

from psycopg2.extensions import register_adapter, AsIs

from storm.locals import Enum
from storm.properties import SimpleProperty
from storm.variables import Variable


def StormEnum(*args, **kwargs):
    """Make enumerations for storm and postgres"""
    return Enum(map=dict([(arg, unicode(arg)) for arg in args]), **kwargs)


class _UUIDVariable(Variable):
    """an UUID column kind"""
    __slots__ = ()

    def parse_set(self, value, from_db):
        """parse the data"""
        if isinstance(value, str):
            value = uuid.UUID(value)
        elif not isinstance(value, uuid.UUID):
            raise TypeError("Expected UUID, found %r: %r"
                            % (type(value), value))
        return value


class StormUUID(SimpleProperty):
    """A property type for handling UUIDs in Storm.

    >>> class Foo(object):
    >>>   id = StormUUID(primary=True)
    """
    variable_class = _UUIDVariable


def adapt_uuid(uu):
    """what to do when an uuid is found"""
    return AsIs("'%s'" % str(uu))
register_adapter(uuid.UUID, adapt_uuid)
