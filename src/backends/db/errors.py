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

"""Exceptions raised by database."""


class RetryLimitReached(Exception):
    """Raised when there have been to many retries."""

    def __init__(self, msg, extra_info=None):
        super(RetryLimitReached, self).__init__(msg)
        self.extra_info = extra_info


class IntegrityError(Exception):
    """Raised when there has been an integrity error."""


class NoTimeoutTracer(Exception):
    """No timeout tracer was registered"""
