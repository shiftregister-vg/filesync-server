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

"""Add ObjectsToDelete table for the dead nodes cleanup scripts.

This table will be used until we can use temporary tables via pgbouncer or
directly talking to the shards.
"""

SQL = ["CREATE TABLE ObjectsToDelete (id uuid, content_hash BYTEA)",
       "CREATE INDEX objectstodelete_idx ON ObjectsToDelete(id)",
       "GRANT SELECT,INSERT,DELETE,UPDATE,TRUNCATE ON TABLE ObjectsToDelete TO"
       " storage, webapp;"]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
