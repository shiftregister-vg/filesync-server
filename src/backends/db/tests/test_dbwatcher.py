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

"""Tests for the backends.tools.dbwatcher module."""

__metaclass__ = type

import mocker

from backends.db import dbwatcher


class StubModule:
    """A stub implementation of part of the DB-API module interface."""

    def __init__(self):
        pass

    def connect(self, *args, **kwargs):
        """Create a stub connection."""
        return StubConnection()


class StubConnection:
    """A stub implementation of the DB-API connection interface."""
    connection_attribute = 42

    def __init__(self):
        pass

    def cursor(self):
        """Create a stub cursor."""
        return StubCursor()

    def commit(self):
        """Commit the transaction."""
        pass

    def rollback(self):
        """Commit the transaction."""
        pass


class StubCursor:
    """A stub implementation of the DB-API cursor interface."""
    cursor_attribute = 42

    def __init__(self):
        pass

    def execute(self, statement, params=None):
        """Execute a statement."""
        pass

    def executemany(self, statement, params=None):
        """Execute a statement multiple times."""
        pass


class DbWatcherTests(mocker.MockerTestCase):
    """Tests for the DatabaseWatcher class."""

    def test_install_uninstall(self):
        """The install() and uninstall() methods correctly alter the module."""
        module = StubModule()
        orig_connect = module.connect
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        self.assertNotEqual(module.connect, orig_connect)
        self.assertEqual(module.connect, watcher._connect)
        self.assertEqual(watcher._orig_connect, orig_connect)
        watcher.uninstall()
        self.assertEqual(module.connect, orig_connect)

    def test_enable_disable(self):
        """The enable() and disable() methods restrict access to databases."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        self.assertRaises(
            dbwatcher.DatabaseNotEnabled, watcher._check_enabled, 'foo')
        watcher.enable('foo')
        watcher._check_enabled('foo')
        # Multiple invocations fail.
        self.assertRaises(AssertionError, watcher.enable, 'foo')

        watcher.disable('foo')
        self.assertRaises(
            dbwatcher.DatabaseNotEnabled, watcher._check_enabled, 'foo')
        self.assertRaises(AssertionError, watcher.disable, 'foo')

    def test_connect(self):
        """Connection wrappers know what database they are for."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        watcher.enable('foo')
        conn = module.connect('dbname=foo')
        self.assertTrue(isinstance(conn, dbwatcher.ConnectionWrapper))
        self.assertEqual(conn._dbname, 'foo')

    def test_connect_kwargs(self):
        """Connection wrappers know what database they are for."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        watcher.enable('foo')
        conn = module.connect(database='foo')
        self.assertTrue(isinstance(conn, dbwatcher.ConnectionWrapper))
        self.assertTrue(isinstance(conn._real_connection, StubConnection))
        self.assertEqual(conn._dbname, 'foo')

    def test_connection_cursor(self):
        """Wrapped connections create wrapped cursors."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        watcher.enable('foo')
        conn = module.connect(database='foo')
        cursor = conn.cursor()
        self.assertTrue(isinstance(cursor, dbwatcher.CursorWrapper))
        self.assertTrue(isinstance(cursor._real_cursor, StubCursor))
        self.assertTrue(isinstance(cursor._connection,
                                   dbwatcher.ConnectionWrapper))

    def test_connection_commit(self):
        """Commits on the connection are reported to the watcher."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        self.expect(MockDatabaseWatcher._check_enabled('foo')).count(2)
        MockDatabaseWatcher._saw_commit('foo')
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        conn = module.connect(database='foo')
        conn.commit()

    def test_connection_attr_access(self):
        """Attribute access is passed through to the underlying connection."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        watcher.enable('foo')
        conn = module.connect(database='foo')
        self.assertTrue(conn.connection_attribute, 42)
        conn.connection_attribute = 43
        self.assertTrue(conn.connection_attribute, 43)
        self.assertRaises(AttributeError, getattr, conn, 'no_such_attr')

    def test_cursor_execute(self):
        """execute() on cursors is reported to the watcher."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        self.expect(MockDatabaseWatcher._check_enabled('foo')).count(2)
        MockDatabaseWatcher._saw_execute('foo')
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        conn = module.connect(database='foo')
        cursor = conn.cursor()
        cursor.execute('dummy sql')

    def test_cursor_executemany(self):
        """executemany() on cursors is reported to the watcher."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        self.expect(MockDatabaseWatcher._check_enabled('foo')).count(2)
        MockDatabaseWatcher._saw_execute('foo')
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        conn = module.connect(database='foo')
        cursor = conn.cursor()
        cursor.executemany('dummy sql', [[1], [2]])

    def test_cursor_attr_access(self):
        """Attribute access is passed through to the underlying cursor."""
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        watcher.install()
        watcher.enable('foo')
        conn = module.connect(database='foo')
        cursor = conn.cursor()
        self.assertTrue(cursor.cursor_attribute, 42)
        cursor.cursor_attribute = 43
        self.assertTrue(cursor.cursor_attribute, 43)
        self.assertRaises(AttributeError, getattr, cursor, 'no_such_attr')

    def test_saw_execute(self):
        """The _saw_execute() method sends notifications when appropriate."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        MockDatabaseWatcher._notify('foo')
        MockDatabaseWatcher._notify('bar')
        self.expect(MockDatabaseWatcher._notify('foo', True)).count(0)
        self.expect(MockDatabaseWatcher._notify('bar', True)).count(0)
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        # Only a single notification is sent, even though two executes
        # are seen on 'foo'.
        watcher._saw_execute('foo')
        watcher._saw_execute('foo')
        self.assertTrue('foo' in watcher._used_databases)
        watcher._saw_execute('bar')

    def test_saw_commit(self):
        """The _saw_commit() method sends notifications when appropriate."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        MockDatabaseWatcher._notify('foo', True)
        MockDatabaseWatcher._notify('bar')
        MockDatabaseWatcher._notify('bar', True)
        self.expect(MockDatabaseWatcher._notify('foo')).count(0)
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        # Only a single notification is sent, even though two executes
        # are seen on 'foo'.
        watcher._saw_commit('foo')
        watcher._saw_commit('foo')
        # Executes after a commit do not trigger a notification.
        watcher._saw_execute('foo')

        # In the other order, both notifications are sent.
        watcher._saw_execute('bar')
        watcher._saw_commit('bar')

    def test_reset(self):
        """The reset() method allows notifications to begin again."""
        MockDatabaseWatcher = self.mocker.patch(dbwatcher.DatabaseWatcher)
        self.expect(MockDatabaseWatcher._notify('foo')).count(2)
        self.expect(MockDatabaseWatcher._notify('bar', True)).count(2)
        self.mocker.replay()

        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        # One notification:
        watcher._saw_execute('foo')
        watcher._saw_execute('foo')
        # And a second after reset:
        watcher.reset('foo')
        watcher._saw_execute('foo')

        # Same for commits:
        watcher._saw_commit('bar')
        watcher._saw_commit('bar')
        watcher.reset('bar')
        watcher._saw_commit('bar')

    def test_notify(self):
        """The _notify() method calls """
        module = StubModule()
        watcher = dbwatcher.DatabaseWatcher(module)
        calls = []

        def callback(dbname, commit):
            """Log arguments passed to the callback."""
            calls.append((dbname, commit))

        watcher.hook('foo', callback)
        watcher._notify('foo')
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[-1], ('foo', False))
        watcher._notify('foo', commit=True)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[-1], ('foo', True))
        # No notification sent for other databases.
        watcher._notify('bar')
        self.assertEqual(len(calls), 2)

        # Unhooking the callback stops notification too.
        watcher.unhook('foo', callback)
        watcher._notify('foo')
        self.assertEqual(len(calls), 2)
