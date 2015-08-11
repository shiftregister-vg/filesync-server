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


def log_dal_function(logger):
    """Returns a function that logs dal operations in a consistent manner."""

    def log_dal(operation, user=None, **kwargs):
        """Logs a dal operation, user_id, and optional other arguments."""
        params = sorted(["%s=%r" % (k, v) for k, v in kwargs.iteritems()])
        if user:
            params.insert(0, 'user_id=%r' % user.id)
        msg = "Performing dal operation: %s(%s)"
        logger.info(msg, operation, ", ".join(params))

    def log_nothing(operation, user=None, **kwargs):
        """Does nothing."""

    return log_dal if logger else log_nothing
