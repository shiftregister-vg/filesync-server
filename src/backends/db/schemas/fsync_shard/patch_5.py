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

"""
Add a function to help on the copy of contentblobs between shards,
by handling duplicate keys.
"""

SQL = [
    """
    CREATE FUNCTION user_migration_contentblob_insert() RETURNS VOID AS
    $$
    DECLARE
        new_cb RECORD;
    BEGIN
        FOR new_cb IN SELECT * FROM user_migration_contentblob_temp LOOP
            -- Now "new_cb" has one record from temp contentblob
            BEGIN
               INSERT INTO contentblob ("hash", "crc32", "size", "storage_key",
                   "deflated_size", "content", "status", "magic_hash") values
                      (new_cb.hash, new_cb.crc32, new_cb.size,
                      new_cb.storage_key, new_cb.deflated_size, new_cb.content,
                      new_cb.status, new_cb.magic_hash);
            EXCEPTION WHEN unique_violation THEN
                -- Do nothing, and loop to the next record
            END;
        END LOOP;
    END;
    $$
    LANGUAGE plpgsql
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
