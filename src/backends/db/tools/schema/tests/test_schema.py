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

"""Tests for the schema module"""

from storm.locals import StormError, Store, create_database

from backends.db.tools.schema import Schema
from backends.db.tools.schema.tests import MakePath, MakePackage


class SchemaTest(MakePackage, MakePath):
    """Test the Storm Schema Create, Delete, and Drop and upgrade"""
    def setUp(self):
        super(SchemaTest, self).setUp()
        sqlite_path = self.make_path("")
        self.database = create_database("sqlite:///%s" % sqlite_path)
        self.store = Store(self.database)
        self.patch_table = "my_patch_table"

        self.package = self.create_package(self.make_path(), "patch_package")
        # patch_package is created during the tests and is not around during
        # lint checks, so we'll diable the error
        import patch_package

        creates = ["CREATE TABLE person (id INTEGER, name TEXT)"]
        drops = ["DROP TABLE person"]
        deletes = ["DELETE FROM person"]

        self.schema = Schema(creates, drops, deletes, patch_package,
                             self.patch_table)

    def test_create(self):
        """Create a Schema"""
        self.assertRaises(StormError,
                          self.store.execute, "SELECT * FROM person")
        self.schema.create(self.store)
        self.assertEquals(list(self.store.execute("SELECT * FROM person")), [])

    def test_drop(self):
        """Drop a Schema"""
        self.schema.create(self.store)
        self.assertEquals(list(self.store.execute("SELECT * FROM person")), [])
        self.schema.drop(self.store)
        self.assertRaises(StormError,
                          self.store.execute, "SELECT * FROM person")

    def test_delete(self):
        """Delete a Schema"""
        self.schema.create(self.store)
        self.store.execute("INSERT INTO person (id, name) VALUES (1, 'Jane')")
        self.assertEquals(list(self.store.execute("SELECT * FROM person")),
                          [(1, u"Jane")])
        self.schema.delete(self.store)
        self.assertEquals(list(self.store.execute("SELECT * FROM person")), [])

    def test_upgrade_creates_schema(self):
        """Upgrade a Schema, aka apply all patches"""
        self.assertRaises(StormError,
                          self.store.execute, "SELECT * FROM person")
        self.schema.upgrade(self.store)
        self.assertEquals(list(self.store.execute("SELECT * FROM person")), [])

    def test_upgrade_marks_patches_applied(self):
        """Test that an upgrade updates the patch table"""
        contents = """
def apply(store):
    store.execute('ALTER TABLE person ADD COLUMN phone TEXT')
"""
        self.package.create_module("patch_1.py", contents)
        self.assertRaises(StormError, self.store.execute,
                          "SELECT * FROM %s" % self.patch_table)
        self.schema.upgrade(self.store)
        self.assertEquals(
            list(self.store.execute("SELECT * FROM %s" % self.patch_table)),
            [(1,)])

    def test_upgrade_applies_patches(self):
        """Test that an upgrade actually applies the patches"""
        self.schema.create(self.store)
        contents = """
def apply(store):
    store.execute('ALTER TABLE person ADD COLUMN phone TEXT')
"""
        self.package.create_module("patch_1.py", contents)
        self.schema.upgrade(self.store)
        self.store.execute(
            "INSERT INTO person (id, name, phone) VALUES (1, 'Jane', '123')")
        self.assertEquals(list(self.store.execute("SELECT * FROM person")),
                          [(1, u"Jane", u"123")])
