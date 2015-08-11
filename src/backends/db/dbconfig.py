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

"""Database connection specific configuration settings."""

import os
import urllib

from config import config
from utilities.utils import get_tmpdir
from utilities.localendpoints import get_local_server

db_dir = os.path.abspath(os.path.join(get_tmpdir(), 'db1'))
conn_settings = None


def get_connection_settings():
    """Get a dict of connections settings."""
    global conn_settings
    if conn_settings is None:

        # try to get live config from db server, else defaults
        local_server = get_local_server("postgres")
        if local_server is not None:
            default_host, default_port = local_server.split(":")
            default_port = int(default_port)
        else:
            default_host = None
            default_port = 5432

        # build the connection settings
        base_cfg = config.database.defaults
        conn_settings = {}
        stores = config.database.stores
        for store_name in stores:
            store_data = getattr(stores, store_name)

            # optional settings
            host = store_data.get("host", base_cfg.get("host", default_host))
            port = store_data.get("port", base_cfg.get("port", default_port))
            pwkey = store_data.get('password_key',
                                   base_cfg.get("password_key"))
            password = config.secret[pwkey] if pwkey else ''
            ops = store_data.get("options", base_cfg.get("options", None))
            options = urllib.urlencode(ops) if ops else None
            username = store_data.get("user", config.database.user)
            conn_settings[store_name] = dict(
                host=host, port=port, username=username, password=password,
                database=store_data.database, options=options, db_dir=db_dir)
    return conn_settings.copy()


def get_postgres_uri(settings):
    """Return a postgres connection uri from settings."""
    db_name = settings["database"]
    #optional settings
    host = settings["host"]
    port = settings["port"]
    username = settings["username"]
    password = settings["password"]
    options = settings["options"]
    if not host:
        host = urllib.quote(settings["db_dir"], '')
    if password:
        password = ':' + password
    uri = 'postgres://'
    if username:
        uri += '%s%s@' % (username, password)
    uri += '%s:%s/%s' % (host, port, db_name)
    if options:
        uri += '?%s' % options
    return uri


def _format_postgres_uri(store_name):
    """Formats a STORM URI for local or remote postgres connetion."""
    all_settings = get_connection_settings()
    settings = all_settings[store_name]
    return get_postgres_uri(settings)
