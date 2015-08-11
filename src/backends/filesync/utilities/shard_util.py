#!/usr/bin/env python

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

"""Utility for moving users between shards."""

import _pythonpath  # NOQA
import warnings
warnings.simplefilter("ignore")

from optparse import OptionParser

from backends.db.tools.schema import print_elapsed_time
from backends.filesync.data.dbmanager import (
    get_user_store, get_shard_store, storage_tm)

DUMP = 'DUMP'
LOAD = 'LOAD'
DELETE = 'DELETE'

INITUSER = """
update storageuser set shard_id='%(shard_id)s'
where id in (%(userids)s)
"""

GET_COLUMNS_SQL = """
SELECT attname FROM pg_class, pg_attribute WHERE
pg_class.relname = '%s' AND pg_class.oid = pg_attribute.attrelid AND
pg_attribute.attnum > 0 and attname not like '%%pg.dropped%%'
ORDER BY attname;
"""

# because of the circular dependency, we need to drop and recreate a constraint
DROP_INDEXES_SQL = [
    """ALTER TABLE userdefinedfolder
        DROP CONSTRAINT userdefinedfolder_root_id_fkey""",
]

CREATE_INDEXES_SQL = [
    """ALTER TABLE userdefinedfolder
        ADD CONSTRAINT userdefinedfolder_root_id_fkey
            FOREIGN KEY (root_id) REFERENCES object (id)""",
]


# COPYINFO has the folloing structure:
#  ('table', info)
#  info['columns'], info['query']
COPYINFO = [
    ("StorageUserInfo", {
        'columns': None,
        'query':
        "select %(columns)s from StorageUserInfo where id in (%(userids)s)"}),
    ("UserDefinedFolder", {
        'columns': None,
        'query':
        """
        select %(columns)s from UserDefinedFolder
            where owner_id in (%(userids)s)
        """}),
    ("Download", {
        'columns': None,
        'query':
        "select %(columns)s from Download where owner_id in (%(userids)s)"}),
    ("ContentBlob", {
        'columns': None,
        'query':
        """
        select %(columns)s
        from contentblob, object
        where contentblob.hash=object.content_hash and
        object.owner_id in (%(userids)s)
        """}),
    ("hash_to_id3", {
        'columns': None,
        'query':
        """
        select %(columns)s
        from hash_to_id3, object
        where hash_to_id3.hash=object.content_hash and
        object.owner_id in (%(userids)s)
        """}),
    ("Object", {
        'columns': None,
        'query':
        """
        select %(columns)s from object where owner_id in (%(userids)s)
        """}),
    ("MoveFromShare", {
        'columns': None,
        'query':
        """
        select %(columns)s from MoveFromShare where owner_id in (%(userids)s)
        """}),
]

DELETE_SQL = [
    "DELETE from object where owner_id in (%(userids)s);",
    "DELETE from movefromshare where owner_id in (%(userids)s);"
    "DELETE from download where owner_id in (%(userids)s);"
    "DELETE from storageuserinfo where id in (%(userids)s);"
]

CREATE_NO_DUPLICATE_SQL = [
    """
    CREATE OR REPLACE  FUNCTION no_dupe_content() RETURNS trigger AS
    $no_dupe_content$
        BEGIN
            IF EXISTS(SELECT 1 from ContentBlob where hash=New.hash) THEN
               RETURN NULL;
            ELSE
               RETURN NEW;
            END IF;
        END;
    $no_dupe_content$
    LANGUAGE plpgsql;
    CREATE TRIGGER no_dupe_content_T BEFORE INSERT OR UPDATE ON ContentBlob
        FOR EACH ROW EXECUTE PROCEDURE no_dupe_content();
    """,
    """
    CREATE OR REPLACE  FUNCTION no_dupe_id3() RETURNS trigger AS
    $no_dupe_id3$
        BEGIN
            IF EXISTS(SELECT 1 from hash_to_id3 where hash=New.hash) THEN
               RETURN NULL;
            ELSE
               RETURN NEW;
            END IF;
        END;
    $no_dupe_id3$
    LANGUAGE plpgsql;
    CREATE TRIGGER no_dupe_id3_T BEFORE INSERT OR UPDATE ON hash_to_id3
        FOR EACH ROW EXECUTE PROCEDURE no_dupe_id3();
    """,
]

DROP_NO_DUPLICATE_SQL = [
    """
    DROP function if exists no_dupe_content() CASCADE;
    """,
    """
    DROP function if exists no_dupe_id3() CASCADE;
    """,
]


@print_elapsed_time
def execute_sql(store, sql):
    """Excute sql for the store."""
    try:
        store.execute(sql)
    except:
        print "Error Executing Query: %s" % sql
        raise


def get_column_names(store, prefixed=False):
    """Get the colum names for tables to be copied."""
    for table_name, table_info in COPYINFO:
        tab = table_name.lower()
        sql = GET_COLUMNS_SQL % tab
        result = store.execute(sql)
        columns = [r[0] for r in result if r[0]]
        if prefixed:
            table_info['columns'] = ",".join([tab + "." + c for c in columns])
        else:
            table_info['columns'] = ",".join(columns)


def main(options):
    """The main function."""
    store = get_shard_store(options.from_shard_id)

    user_ids = options.user_ids or ""
    if options.action == DUMP:
        get_column_names(store, True)
        for c in COPYINFO:
            table = c[0]
            columns = c[1]['columns']
            select = c[1]['query']
            replacements = dict(columns=columns,
                                userids=user_ids,
                                shard_id=options.to_shard_id)
            sql = "COPY (%s) TO '%s/%s.dat';" % (select % replacements,
                                                 options.path, table)
            print "Dumping %s TO %s/%s.dat" % (table, options.path, table),
            storage_tm.begin()
            execute_sql(store, sql)
            storage_tm.abort()
        return

    if options.action == LOAD:
        get_column_names(store, False)
        shard_store = get_shard_store(options.to_shard_id)
        storage_tm.begin()
        print "Dropping Indexes..."
        for sql in DROP_NO_DUPLICATE_SQL:
            shard_store.execute(sql)
        for sql in CREATE_NO_DUPLICATE_SQL:
            shard_store.execute(sql)
        for sql in DROP_INDEXES_SQL:
            shard_store.execute(sql)
        for c in COPYINFO:
            table = c[0]
            columns = c[1]['columns']
            sql = "COPY %s (%s) FROM '%s/%s.dat';" % (table, columns,
                                                      options.path, table)
            print "Loading %s From %s/%s.dat" % (table, options.path,
                                                 table),
            execute_sql(shard_store, sql)
        user_store = get_user_store()
        user_store.execute(INITUSER % dict(shard_id=options.to_shard_id,
                                           userids=user_ids))
        print "Rebuilding Indexes..."
        for sql in DROP_NO_DUPLICATE_SQL:
            execute_sql(shard_store, sql)
        for sql in CREATE_INDEXES_SQL:
            execute_sql(shard_store, sql)
        storage_tm.commit()
        return

    if options.action == DELETE:
        shard_store = get_shard_store(options.from_shard_id)
        storage_tm.begin()
        print "Deleting user data..."
        for sql in DELETE_SQL:
            execute_sql(shard_store, sql % dict(userids=user_ids))
        storage_tm.commit()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--action", dest="action", help="The action to take")
    parser.add_option("--user_ids", dest="user_ids",
                      help="A list of comma separated userids to shard.")
    parser.add_option("-p", "--path", dest="path",
                      help="The path to dump the data files to.")
    parser.add_option("--from_shard_id", dest="from_shard_id",
                      default='storage',
                      help="The Shard to copy the data from.")
    parser.add_option("--to_shard_id", dest="to_shard_id",
                      help="The Shard to copy the data to.")

    (options, args) = parser.parse_args()

    #verify the options
    if options.from_shard_id is None:
        raise Exception("Error: --from_shard_id is required")
    if options.to_shard_id is None:
        raise Exception("Error: --to_shard_id is required")
    if options.user_ids is None:
        raise Exception("Error: --user_ids is required")
    actions = [DUMP, LOAD, DELETE]
    if options.action not in actions:
        raise Exception("--action must be on of %s" % ",".join(actions))

    main(options)
