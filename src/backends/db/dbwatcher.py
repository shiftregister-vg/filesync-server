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

"""Infrastructure for tracking activity on database connections."""

__metaclass__ = type

import re


class NoDatabaseName(Exception):
    """Could not detect database name in connect arguments."""


class DatabaseNotEnabled(Exception):
    """An attempt to use a disabled database was made."""

    def __init__(self, dbname):
        super(DatabaseNotEnabled, self).__init__(
            "An attempt was made to use %s without enabling it." % dbname)


class DatabaseWatcher:
    """Watch database connections for use and commits."""

    def __init__(self, module):
        self._module = module
        self._orig_connect = None
        self._enabled_databases = set()
        self._used_databases = set()
        self._dirty_databases = set()
        self._callbacks = {}

    def install(self):
        """Install the database watcher."""
        assert self._orig_connect is None, "Already installed"
        self._orig_connect = self._module.connect
        self._module.connect = self._connect

    def uninstall(self):
        """Uninstall the database watcher."""
        assert self._orig_connect is not None, "Watcher not installed"
        self._module.connect = self._orig_connect
        self._orig_connect = None

    def enable(self, dbname):
        """Enable use of a given database."""
        if dbname in self._enabled_databases:
            raise AssertionError("%s already enabled" % dbname)
        self._enabled_databases.add(dbname)

    def disable(self, dbname):
        """Disable use of a given database."""
        if dbname not in self._enabled_databases:
            raise AssertionError("%s not enabled" % dbname)
        self._enabled_databases.remove(dbname)

    def _check_enabled(self, dbname):
        """Raise an exception if access to a database has not been enabled."""
        if dbname not in self._enabled_databases:
            raise DatabaseNotEnabled(dbname)

    def _connect(self, *args, **kwargs):
        """Create a new connection object, noting the database name."""
        dbname = None
        if 'database' in kwargs:
            dbname = kwargs['database']
        elif len(args) > 0:
            match = re.search(r'dbname=(\w+)', args[0])
            if match:
                dbname = match.group(1)
        if dbname is None:
            raise NoDatabaseName("Could not determine database name.")
        self._check_enabled(dbname)
        return ConnectionWrapper(
            self, dbname, self._orig_connect(*args, **kwargs))

    def hook(self, dbname, callback):
        """Register a callback to be notified about a particular database.

        The callback will be called with arguments (dbname, commit).
        """
        self._callbacks.setdefault(dbname, set()).add(callback)

    def unhook(self, dbname, callback):
        """Deregister a callback that was registered with hook()."""
        try:
            self._callbacks[dbname].remove(callback)
        except KeyError:
            raise

    def reset(self, dbname):
        """Reset the used and dirty states for a database."""
        self._used_databases.discard(dbname)
        self._dirty_databases.discard(dbname)

    def _notify(self, dbname, commit=False):
        """Notify interested parties about activity on a database."""
        for callback in self._callbacks.get(dbname, set()):
            callback(dbname, commit)

    def _saw_execute(self, dbname):
        """Report statement execution for a database."""
        if dbname not in self._used_databases:
            self._used_databases.add(dbname)
            self._notify(dbname)

    def _saw_commit(self, dbname):
        """Report a commit on a database."""
        if dbname not in self._dirty_databases:
            self._dirty_databases.add(dbname)
            self._notify(dbname, True)
        # If we've committed to the DB, mark it as used too.
        self._used_databases.add(dbname)


class ConnectionWrapper:
    """A wrapper around a DB-API connection that reports commits."""

    def __init__(self, dbwatcher, dbname, real_connection):
        self.__dict__['_dbwatcher'] = dbwatcher
        self.__dict__['_dbname'] = dbname
        self.__dict__['_real_connection'] = real_connection

    def commit(self):
        """Commit the transaction and notify the watcher."""
        self._dbwatcher._check_enabled(self._dbname)
        try:
            self._real_connection.commit()
        finally:
            self._dbwatcher._saw_commit(self._dbname)

    def cursor(self):
        """Create a cursor that notifies the watcher of statement execution."""
        return CursorWrapper(self, self._real_connection.cursor())

    def __getattr__(self, attr):
        """Pass attribute access through to the real connection."""
        return getattr(self._real_connection, attr)

    def __setattr__(self, attr, value):
        """Pass attribute access through to the real connection."""
        setattr(self._real_connection, attr, value)


class CursorWrapper:
    """A wrapper around a DB-API cursor that reports executes."""

    def __init__(self, connection, real_cursor):
        self.__dict__['_connection'] = connection
        self.__dict__['_real_cursor'] = real_cursor

    def execute(self, *args, **kwargs):
        """Execute a statement and notify the watcher."""
        self._connection._dbwatcher._check_enabled(self._connection._dbname)
        try:
            return self._real_cursor.execute(*args, **kwargs)
        finally:
            self._connection._dbwatcher._saw_execute(self._connection._dbname)

    def executemany(self, *args, **kwargs):
        """Execute a statement and notify the watcher."""
        self._connection._dbwatcher._check_enabled(self._connection._dbname)
        try:
            return self._real_cursor.executemany(*args, **kwargs)
        finally:
            self._connection._dbwatcher._saw_execute(self._connection._dbname)

    def __getattr__(self, attr):
        """Pass attribute access through to the real cursor."""
        return getattr(self._real_cursor, attr)

    def __setattr__(self, attr, value):
        """Pass attribute access through to the real cursor."""
        setattr(self._real_cursor, attr, value)
