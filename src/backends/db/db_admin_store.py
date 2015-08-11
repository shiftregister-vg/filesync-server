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

"""Provides a test connection with admin access to the databases"""

from storm.zope.zstorm import global_zstorm as zstorm

from backends.db.dbconfig import get_connection_settings, get_postgres_uri


def _format_postgres_admin_uri(store_name):
    """Internal function that formats a URI for a local admin connection"""
    cfg = get_connection_settings()[store_name]
    #bypass username & password to get an admin config
    cfg['username'] = None
    cfg['password'] = None
    cfg['options'] = None
    return get_postgres_uri(cfg)


def get_admin_store(store_name):
    """ Return the Storm.Store With and admin connection"""
    uri = _format_postgres_admin_uri(store_name)
    return zstorm.get("%s_admin" % store_name, uri)
