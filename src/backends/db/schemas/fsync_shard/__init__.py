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

"""Initializing schema.

It has all the create, delete and drop instructions to build the
database.
"""

from backends.db.tools.schema import Schema

__all__ = ["create_schema"]

OBJECT_NAME_LIMIT = 256
EMPTY_CONTENT_HASH = 'sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709'


def create_schema():
    '''Creates the Schema with all the instructions.'''
    # We pass this very package to schema, so we need to reimport ourselves
    from backends.db.schemas import fsync_shard as patch_package
    return Schema(CREATE, DROP, DELETE, patch_package, 'patch_shard')


CREATE = [
    # this avoids problems if the shard is created in the same database
    # as the main schema.
    """
    create or replace function create_common() returns void as
    $$
    begin
    if not exists(select * from pg_type where typname='lifecycle_status') then
       CREATE TYPE lifecycle_status as enum('Live', 'Dead');
    end if;
    end;
    $$
    language plpgsql;
    select create_common();
    drop function create_common();
    """,
    """
    CREATE TYPE object_kind as enum('File', 'Directory', 'Symlink');
    """,
    """
    CREATE FUNCTION uuid_generate_v4() RETURNS uuid
        AS '$libdir/uuid-ossp', 'uuid_generate_v4'
        LANGUAGE c STRICT;
    """,
    """
    CREATE TABLE StorageUserInfo (
        id integer NOT NULL PRIMARY KEY,
        max_storage_bytes bigint NOT NULL,
        used_storage_bytes bigint DEFAULT 0
        )
    """,
    """
    COMMENT ON TABLE StorageUserInfo IS
        'Information for storage_user which will go on each shard'
    """,
    """
    COMMENT ON COLUMN StorageUserInfo.max_storage_bytes IS
        'The StorageUser''s storage quota.'
    """,
    """
    COMMENT ON COLUMN StorageUserInfo.used_storage_bytes IS
        'How much of the storage quota is currently used'
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE
        ON TABLE StorageUserInfo TO storage, webapp
    """,
    """
    CREATE TABLE ContentBlob (
        hash BYTEA PRIMARY KEY NOT NULL,
        crc32 BIGINT NOT NULL,
        size BIGINT NOT NULL,
        storage_key UUID,
        deflated_size BIGINT,
        content BYTEA,
        status lifecycle_status NOT NULL DEFAULT 'Live'::lifecycle_status,
        magic_hash BYTEA
    ) WITH (
        autovacuum_vacuum_scale_factor = 0.02
    );
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE ContentBlob TO storage, webapp
    """,
    """
    COMMENT ON TABLE ContentBlob IS 'Associates a hash with a specific \
    storage key in S3, or in the case of symlinks and sufficiently small \
    files, the file contents directly.  In S3, file contents are deflated, \
    but this is not reflected anywhere except in ContentBlob.deflated_size.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.hash IS 'The hash for the raw file content \
    represented by this record.  It consists of an identifier for the hash \
    algorithm ("sha1" for SHA-1), followed by a colon, followed by the hash \
    value in (lower-case) hexadecimal.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.crc32 IS 'The crc32 for the raw file \
    content represented by this record.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.size IS 'The size of the raw file content \
    represented by this record, in bytes.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.storage_key IS 'The S3 content key which \
    references the deflated file contents, or NULL if the content is stored \
    locally in the table or if it has been garbage collected.  This column is \
    set after inserting an object into S3, and NULLed just before deleting an \
    object from S3.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.deflated_size IS 'The deflated size of the \
    file content as stored in S3, or NULL if inapplicable.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.content IS 'The file content as a raw byte \
    string, or else NULL.  Used for symlinks and potentially sufficiently \
    small files.';
    """,
    """
    COMMENT ON COLUMN ContentBlob.status IS 'Whether this content entry is \
    live, or else a candidate for garbage collection.';
    """,
    """
    CREATE TABLE Object (
        id uuid PRIMARY KEY NOT NULL,
        owner_id integer NOT NULL REFERENCES StorageUserInfo,
        volume_id UUID,
        parent_id uuid REFERENCES Object (id) ON DELETE CASCADE,
        name character varying(%d) NOT NULL,
        content_hash BYTEA REFERENCES ContentBlob (hash),
        directory_hash BYTEA,
        kind object_kind,
        when_created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT \
                timezone('UTC'::text, now()),
        when_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT \
                timezone('UTC'::text, now()),
        status lifecycle_status DEFAULT 'Live'::lifecycle_status NOT NULL,
        path character varying(4096) NOT NULL,
        is_public BOOLEAN DEFAULT FALSE NOT NULL,
        publicfile_id integer,
        generation bigint,
        generation_created bigint,
        mimetype text,
        public_uuid uuid
    ) WITH (
        autovacuum_vacuum_scale_factor = 0.02
    );
    """ % (OBJECT_NAME_LIMIT,),
    """
    COMMENT ON TABLE Object IS
        'A file, directory, or symbolic link.';
    """,
    """
    COMMENT ON COLUMN Object.id IS
        'A unique identifier for the node.';
    """,
    """
    COMMENT ON COLUMN object.volume_id IS
        'If not null, the uuid of the UDF this object belongs to.';
    """,
    """
    COMMENT ON COLUMN Object.owner_id IS
        'The object''s owner, for access control and accounting purposes.';
    """,
    """
    COMMENT ON COLUMN Object.parent_id IS 'The directory containing the \
    object, or NULL if the object is a volume root (only directories should \
    be volume roots).'
    """,
    """
    COMMENT ON COLUMN Object.name IS 'The object''s name within its \
    containing directory.  Ignored for volume roots.';
    """,
    """
    COMMENT ON COLUMN object.directory_hash IS 'The hash of a directory'
    """,
    """
    COMMENT ON COLUMN Object.kind IS 'The kind of object:  directory, file, \
    or symbolic link.';
    """,
    """
    COMMENT ON COLUMN Object.when_created IS 'Timestamp at which the object \
    was first created.';
    """,
    """
    COMMENT ON COLUMN Object.when_last_modified IS 'Timestamp at which the \
    objects content was last modified.';
    """,
    """
    COMMENT ON COLUMN Object.status IS 'Whether this object is alive or dead.';
    """,
    """
    COMMENT ON COLUMN object.path IS 'The path of the object from its root.';
    """,
    """
    COMMENT ON COLUMN Object.is_public IS
        'Whether this object is publicly available.';
    """,
    """
    COMMENT ON COLUMN object.generation
        IS 'The current Generation of this object.';
    """,
    """
    COMMENT ON COLUMN object.generation_created
        IS 'The Generation this object was created.';
    """,
    """
    CREATE INDEX object__parent_id__idx ON object(parent_id);
    """,
    """
    CREATE INDEX object_content_hash_fkey ON Object(content_hash)
    """,
    """
    CREATE INDEX object_owner_id_fkey ON Object(owner_id)
    """,
    """
    CREATE INDEX object_kind ON Object (kind)
    """,
    """
    CREATE INDEX object_delta_idx
        ON object(owner_id, volume_id, generation, path);
    """,
    """
    CREATE INDEX object_generation_created_idx ON object(generation_created);
    """,
    """
    CREATE INDEX object_generation_idx ON object(generation);
    """,
    """
    CREATE INDEX object_mimtype_idx ON object(mimetype);
    """,
    """
    CREATE UNIQUE INDEX object_parent_name_uk ON object(parent_id, name)
        WHERE (status = 'Live'::lifecycle_status);
    """,
    """
    CREATE INDEX object_parent_name_volume
        ON object(parent_id, name, volume_id)
        WHERE ((status = 'Live'::lifecycle_status)
            AND (parent_id IS NOT NULL));
    """,
    """
    CREATE INDEX object_publicfile_idx ON object(volume_id, publicfile_id)
        WHERE ((kind = 'File'::object_kind)
            AND (status = 'Live'::lifecycle_status));
    """,
    """
    CREATE INDEX object_roots ON object(owner_id)
        WHERE ((parent_id IS NULL) AND ((name)::text = ''::text));
    """,
    """
    CREATE INDEX dead_object_last_modified_idx ON object (when_last_modified)
        WHERE (status = 'Dead'::lifecycle_status);
    """,
    """
    ALTER TABLE ONLY object
        ADD CONSTRAINT volume_generation_uk UNIQUE (volume_id, generation);
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE object TO storage, webapp;
    """,
    """
    CREATE TABLE uploadjob (
        uploadjob_id SERIAL PRIMARY KEY NOT NULL,
        storage_object_id uuid NOT NULL  REFERENCES object(id)
            ON DELETE CASCADE,
        chunk_count integer DEFAULT 0 NOT NULL,
        hash_hint bytea,
        crc32_hint bigint,
        inflated_size_hint bigint,
        deflated_size_hint bigint,
        when_started timestamp without time zone
            default timezone('UTC'::text, now()) not null,
        when_last_active timestamp without time zone
            default timezone('UTC'::text, now()) not null,
        status lifecycle_status default 'Live'::lifecycle_status not null,
        multipart_id bytea,
        multipart_key uuid,
        uploaded_bytes bigint,
        inflated_size bigint,
        crc32 bigint,
        hash_context bytea,
        decompress_context bytea
    ) WITH (
        autovacuum_vacuum_scale_factor = 0.02
    );
    """,
    """
    COMMENT ON TABLE UploadJob IS
        'Tracks blob upload that will result in a ContentBlob being created'
    """,
    """
    COMMENT ON COLUMN UploadJob.uploadjob_id IS
        'Unique identifier for the upload attempt';
    """,
    """
    COMMENT ON COLUMN UploadJob.storage_object_id IS
        'Links a UploadJob to the storage object for which it is \
         being created';
    """,
    """
    COMMENT ON COLUMN UploadJob.chunk_count IS
        'Integrity check count for the number of chunks created so far';
    """,
    """
    COMMENT ON COLUMN UploadJob.hash_hint IS
        'The hash the client claims that the uploaded file should have.';
    """,
    """
    COMMENT ON COLUMN UploadJob.crc32_hint IS
        'The crc32 the client claims that the uploaded file should have.';
    """,
    """
    COMMENT ON COLUMN UploadJob.inflated_size_hint IS
        'The size which the client claims that the uploaded file should have.';
    """,
    """
    COMMENT ON COLUMN UploadJob.deflated_size_hint IS
        'The size of the deflated content';
    """,
    """
    COMMENT ON COLUMN UploadJob.when_started IS
        'Timestamp when this upload started';
    """,
    """
    COMMENT ON COLUMN UploadJob.when_last_active IS
        'Timestamp for when the last chunk was created for this upload action';
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE uploadjob TO storage, webapp;
    """,
    """
    GRANT ALL ON SEQUENCE uploadjob_uploadjob_id_seq TO storage, webapp;
    """,
    """
    CREATE INDEX uploadjob_object_idx ON uploadjob(storage_object_id);
    """,
    """
    CREATE TABLE UserDefinedFolder (
        id uuid NOT NULL PRIMARY KEY,
        owner_id INT NOT NULL REFERENCES StorageUserInfo,
        root_id UUID NOT NULL REFERENCES Object(id) ON DELETE CASCADE,
        path text NOT NULL,
        when_created timestamp without time zone
            DEFAULT timezone('UTC'::text, now()) NOT NULL,
        status lifecycle_status DEFAULT 'Live'::lifecycle_status NOT NULL,
        generation BIGINT
    );
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE
        ON TABLE UserDefinedFolder TO storage, webapp;
    """,
    """
    COMMENT ON TABLE UserDefinedFolder IS
        'Folders that the user chooses to sync with the service.';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.id IS
        'A unique identifier for the UDF';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.owner_id IS
        'The StorageUser who created the UDF.';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.root_id IS
        'The node that is root of all the tree of the UDF.';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.path IS 'The path in the local \
    machine where the UDF was created (this is suggested to other clients, \
    but it''s not enforced; the different local clients may put the UDF \
    in other path, but this attribute will remain as the originally one).';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.when_created IS
        'Timestamp when the UDF was originally created.';
    """,
    """
    COMMENT ON COLUMN UserDefinedFolder.status IS
        'Whether this UDF is active/alive.';
    """,
    """
    COMMENT ON COLUMN userdefinedfolder.generation
        IS 'The Generation of this volume.';
    """,
    """
    CREATE INDEX udf_owner_fkey ON UserDefinedFolder(owner_id)
    """,
    """
    CREATE INDEX udf_root_fkey ON UserDefinedFolder(root_id)
    """,
    """
    CREATE UNIQUE INDEX udf_owner_path_uk
        ON userdefinedfolder(owner_id, path)
        WHERE (status = 'Live'::lifecycle_status);
    """,
    """
    CREATE TYPE download_status as enum(
        'Queued', 'Downloading', 'Complete', 'Error');
    """,
    """
    CREATE TABLE Download (
        id uuid PRIMARY KEY NOT NULL,
        owner_id integer NOT NULL,
        volume_id uuid,
        file_path text NOT NULL,
        download_url text NOT NULL,
        status download_status NOT NULL,
        status_change_date timestamp without time zone NOT NULL,
        node_id uuid,
        error_message text,
        download_key text,
        UNIQUE (owner_id, volume_id, file_path, download_url)
    );
    """,
    """
    COMMENT ON TABLE Download IS 'A queued download.';
    """,
    """
    COMMENT ON COLUMN Download.id IS 'Primary key for download.';
    """,
    """
    COMMENT ON COLUMN Download.owner_id IS
        'The user associated with this download.'
    """,
    """
    COMMENT ON COLUMN Download.volume_id IS
        'The UDF volume ID for the download.';
    """,
    """
    COMMENT ON COLUMN Download.file_path IS
        'The path within the UDF where the file will be stored.';
    """,
    """
    COMMENT ON COLUMN Download.download_url IS 'The URL to download.';
    """,
    """
    COMMENT ON COLUMN Download.status IS 'The status of this download.';
    """,
    """
    COMMENT ON COLUMN Download.node_id IS
        'The node ID of the download after it has completed.';
    """,
    """
    COMMENT ON COLUMN Download.error_message IS
        'An error message, if the download failed.';
    """,
    """
    COMMENT ON COLUMN download.download_key IS
        'A unique key for this download/user/udf combination.'
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE Download to storage, webapp;
    """,
    """
    CREATE TABLE movefromshare (
        share_id uuid NOT NULL,
        id uuid NOT NULL,
        owner_id integer,
        volume_id uuid,
        parent_id uuid,
        name text,
        content_hash bytea,
        directory_hash bytea,
        kind object_kind,
        mime_type_id integer,
        total_download_bytes bigint,
        when_created timestamp without time zone,
        when_last_modified timestamp without time zone,
        status lifecycle_status,
        path text,
        is_public boolean,
        publicfile_id integer,
        generation bigint,
        generation_created bigint,
        mimetype text,
        public_uuid uuid,
        PRIMARY KEY (share_id, id)
    );
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE
        ON TABLE movefromshare TO storage, webapp;
    """,
    """
    CREATE INDEX move_from_share_delta_idx
        ON movefromshare (share_id, owner_id, volume_id, generation, path);
    """,
    """
    CREATE VIEW share_delta_view AS
        SELECT NULL::unknown AS share_id, o.id, o.owner_id,
        o.volume_id, o.parent_id, o.name, o.content_hash,
        o.directory_hash, o.kind,
        o.mimetype, o.when_created, o.when_last_modified, o.status, o.path,
        o.is_public, o.publicfile_id, o.public_uuid, o.generation,
        o.generation_created FROM object o
    UNION
        SELECT movefromshare.share_id, movefromshare.id,
        movefromshare.owner_id, movefromshare.volume_id,
        movefromshare.parent_id, movefromshare.name,
        movefromshare.content_hash, movefromshare.directory_hash,
        movefromshare.kind, movefromshare.mimetype, movefromshare.when_created,
        movefromshare.when_last_modified, movefromshare.status,
        movefromshare.path,
        movefromshare.is_public, movefromshare.publicfile_id,
        movefromshare.public_uuid, movefromshare.generation,
        movefromshare.generation_created FROM movefromshare;
    """,
    """
    GRANT SELECT ON TABLE share_delta_view TO storage, webapp;
    """,
]

INITIALIZE = [
    """
    insert into contentblob (hash, size, crc32) values ('%s', 0, 0);
    """ % EMPTY_CONTENT_HASH,
]
CREATE.extend(INITIALIZE)

DROP = []
DELETE = [
    "DELETE FROM MoveFromShare",
    "DELETE FROM Download",
    "DELETE FROM UserDefinedFolder",
    "DELETE FROM UploadJob",
    "DELETE FROM Object",
    "DELETE FROM ContentBlob where hash != '%s'" % EMPTY_CONTENT_HASH,
    "DELETE FROM StorageUserInfo",
    "ALTER SEQUENCE UploadJob_uploadjob_id_seq RESTART WITH 1",
]
