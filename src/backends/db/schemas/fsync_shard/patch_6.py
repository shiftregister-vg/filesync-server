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

"""Support for REST Resumable Uploads."""

SQL = [
    """
    CREATE TABLE resumable_upload (
        upload_id uuid PRIMARY KEY NOT NULL,
        owner_id integer NOT NULL REFERENCES StorageUserInfo ON DELETE CASCADE,
        volume_path text NOT NULL,
        size bigint NOT NULL,
        mimetype text,
        when_started timestamp without time zone
            default timezone('UTC'::text, now()) not null,
        when_last_active timestamp without time zone
            default timezone('UTC'::text, now()) not null,
        status lifecycle_status default 'Live'::lifecycle_status not null,
        multipart_id bytea NOT NULL,
        storage_key uuid NOT NULL,
        part_count bigint NOT NULL,
        uploaded_bytes bigint NOT NULL,
        hash_context bytea,
        magic_hash_context bytea,
        crc_context int
    );
    """,
    """
    COMMENT ON TABLE resumable_upload IS
        'Tracks resumable upload that will result in a \
         ContentBlob being created'
    """,
    """
    COMMENT ON COLUMN resumable_upload.upload_id IS
        'Unique identifier for the upload attempt';
    """,
    """
    COMMENT ON COLUMN resumable_upload.owner_id IS
        'The id of the owner of the file';
    """,
    """
    COMMENT ON COLUMN resumable_upload.volume_path IS
        'The volume path for the file which will be created';
    """,
    """
    COMMENT ON COLUMN resumable_upload.size IS
        'The size in bytes of the file being uploaded';
    """,
    """
    COMMENT ON COLUMN resumable_upload.when_started IS
        'Timestamp when this upload started';
    """,
    """
    COMMENT ON COLUMN resumable_upload.when_last_active IS
        'Timestamp for when the last chunk was created for this upload action';
    """,
    """
    COMMENT ON COLUMN resumable_upload.multipart_id IS
        'The upload_id returned by S3';
    """,
    """
    COMMENT ON COLUMN resumable_upload.storage_key IS
        'The S3 objectName which will be created by this upload';
    """,
    """
    COMMENT ON COLUMN resumable_upload.part_count IS
        'The number of parts uploaded so far';
    """,
    """
    COMMENT ON COLUMN resumable_upload.uploaded_bytes IS
        'The number of bytes uploaded so far';
    """,
    """
    COMMENT ON COLUMN resumable_upload.hash_context IS
        'The state of the resumable hasher';
    """,
    """
    COMMENT ON COLUMN resumable_upload.magic_hash_context IS
        'The state of the resumable magic hasher';
    """,
    """
    COMMENT ON COLUMN resumable_upload.crc_context IS
        'The state of the resumable compressor';
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE
        ON TABLE resumable_upload TO storage, webapp;
    """,
    """
    CREATE INDEX resumable_upload_owner_idx ON resumable_upload(owner_id);
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
