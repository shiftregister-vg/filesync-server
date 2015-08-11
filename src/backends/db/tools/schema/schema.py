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

"""A system for managing a database schema."""

import sys
import transaction

from storm.locals import StormError

from backends.db.tools.schema import PatchApplier, print_elapsed_time


__all__ = ["Schema"]


class Schema(object):
    """The schema that we are managing."""

    def __init__(self, creates, drops, deletes, patch_package,
                 patch_table='patch'):
        """
        @param creates: A list of C{CREATE TABLE} statements.
        @param drops: A list of C{DROP TABLE} statements.
        @param deletes: A list of C{DELETE FROM} statements.
        @param patch_package: The Python package containing patch modules.
        """
        self._creates = creates
        self._drops = drops
        self._deletes = deletes
        self._patch_package = patch_package
        self._patch_table = patch_table

    def _execute_statements(self, store, statements):
        """Run a list of SQL statements and commit."""
        for statement in statements:
            store.execute(statement)
        store.commit()

    def preview(self):
        """Preview C{CREATE TABLE} SQL statements so they can be inspected"""
        return ''.join(self._creates)

    @print_elapsed_time
    def create(self, store):
        """Run C{CREATE TABLE} SQL statements with C{store}."""
        sys.stderr.write('Creating %s schema in %s database' % (
            self._patch_package.__name__,
            getattr(store._database, "name", str(store))))
        self._execute_statements(store, self._creates)
        self._execute_statements(store, [CREATE % self._patch_table])

    def drop(self, store):
        """Run C{DROP TABLE} SQL statements with C{store}."""
        self._execute_statements(store, self._drops)
        self._execute_statements(store, [DROP % self._patch_table])

    def delete(self, store):
        """Run C{DELETE FROM} SQL statements with C{store}."""
        self._execute_statements(store, self._deletes)

    def upgrade(self, store):
        """Upgrade C{store} to have the latest schema.

        If a schema isn't present a new one will be created.  Unapplied
        patches will be applied to an existing schema.
        """
        patch_applier = PatchApplier(store, self._patch_package,
                                     self._patch_table)
        try:
            store.execute("SELECT * FROM %s WHERE 1=2" % self._patch_table)
        except StormError:
            # No schema at all. Create it from the ground.
            transaction.abort()
            self.create(store)
            patch_applier.apply_all()
            transaction.commit()
        else:
            patch_applier.apply_all()

# Required for versioning
CREATE = "CREATE TABLE %s (version int NOT NULL PRIMARY KEY)"
DROP = "DROP TABLE IF EXISTS %s"
