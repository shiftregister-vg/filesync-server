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

"""Create/delete/drop the transaction log database schema."""

from backends.db.tools.schema import Schema

__all__ = ['create_schema']


def create_schema():
    """Return a Schema"""
    from backends.db.schemas import txlog as patch_package
    return Schema(CREATE, DROP, DELETE, patch_package, 'txlog.patch')


CREATE = [
    """
    CREATE SCHEMA txlog;
    """,
    """
    GRANT USAGE on SCHEMA txlog to webapp, storage;
    """,
    """
    CREATE TABLE txlog.transaction_log (
        id BIGSERIAL PRIMARY KEY,
        owner_id INTEGER NOT NULL,
        node_id UUID,
        volume_id UUID,
        op_type INTEGER NOT NULL,
        path TEXT,
        old_path TEXT,
        generation BIGINT,
        timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT \
                timezone('UTC'::text, now()),
        mimetype TEXT,
        extra_data TEXT
    )
    """,
    # TODO: Write comments for the non-obvious columns
    """
    GRANT SELECT, INSERT, UPDATE
        ON TABLE txlog.transaction_log
        TO storage, webapp
    """,
    """
    GRANT SELECT, UPDATE
        ON SEQUENCE txlog.transaction_log_id_seq
        TO storage, webapp
    """,
    #
    """
    CREATE TABLE txlog.db_worker_last_row (
        id SERIAL PRIMARY KEY,
        worker_id text NOT NULL,
        row_id bigint REFERENCES
            txlog.transaction_log(id)
    )
    """,
    """
    COMMENT ON COLUMN
        txlog.db_worker_last_row.row_id IS
        'Store the row currently reached by this worker'
    """,
    """
    GRANT SELECT, INSERT, UPDATE
        ON TABLE txlog.db_worker_last_row
        TO storage, webapp
    """,
    """
    GRANT SELECT, UPDATE
        ON SEQUENCE txlog.db_worker_last_row_id_seq
        TO storage, webapp
    """,
    """
    CREATE PROCEDURAL LANGUAGE plpythonu;
    """,
    """
    CREATE OR REPLACE FUNCTION txlog.get_extra_data_to_recreate_file(
            kind object_kind, size bigint, storage_key uuid, publicfile_id
            integer, public_uuid uuid, content_hash bytea,
            when_created double precision,
            last_modified double precision) RETURNS TEXT
        LANGUAGE plpythonu IMMUTABLE
        AS $_$
        import json
        return json.dumps(
            dict(size=size, storage_key=storage_key,
                 publicfile_id=publicfile_id, public_uuid=public_uuid,
                 content_hash=content_hash, when_created=int(when_created),
                 last_modified=int(last_modified), kind=kind))
    $_$;
    """,
    """
    CREATE OR REPLACE FUNCTION txlog.path_join(
            elem1 text, elem2 text) RETURNS TEXT
        LANGUAGE plpythonu IMMUTABLE
        AS $_$
        import os
        return os.path.join(elem1, elem2)
    $_$;
    """,
]

DROP = [
    """
    DROP TABLE txlog.db_worker_last_row
    """,
    #
    """
    DROP TABLE txlog.transaction_log
    """,
]

DELETE = [
    """
    DELETE FROM txlog.db_worker_last_row
    """,
    """
    ALTER SEQUENCE txlog.db_worker_last_row_id_seq
        RESTART WITH 1
    """,
    #
    """
    DELETE FROM txlog.transaction_log
    """,
    """
    ALTER SEQUENCE txlog.transaction_log_id_seq RESTART WITH 1
    """,
]
