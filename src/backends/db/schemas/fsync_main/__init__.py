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


def create_schema():
    '''Creates the Schema with all the instructions.'''
    # We pass this very package to schema, so we need to reimport ourselves
    from backends.db.schemas import fsync_main as patch_package
    return Schema(CREATE, DROP, DELETE, patch_package, 'patch_main')


CREATE = [
    """
    CREATE TYPE lifecycle_status as enum('Live', 'Dead');
    """,
    """
    CREATE TABLE StorageUser (
        id integer NOT NULL PRIMARY KEY,
        max_average_download_bytes_per_day bigint,
        status lifecycle_status
            DEFAULT 'Live'::lifecycle_status NOT NULL,
        visible_name character varying(256),
        username character varying(256) UNIQUE,
        subscription_status lifecycle_status
            DEFAULT 'Live'::lifecycle_status NOT NULL,
        shard_id Text DEFAULT 'storage',
        root_volume_id UUID
    );
    """,
    """
    COMMENT ON TABLE StorageUser IS 'StorageUsers that the \
    storage system is aware of.';
    """,
    """
    COMMENT ON COLUMN StorageUser.id IS 'A unique identifier for this record.';
    """,
    """
    COMMENT ON COLUMN StorageUser.status IS
        'Whether the record represents a live, valid StorageUser.';
    """,
    """
    COMMENT ON COLUMN storageuser.visible_name IS
    'The visible name of the user, the more human friendly of his/her \
    denominations.';
    """,
    """
    COMMENT ON COLUMN storageuser.username IS
        'The username of the user, its human friendly unique id.';
    """,
    """
    COMMENT ON COLUMN storageuser.subscription_status IS
        'The status of the user subscription.';
    """,
    """
    COMMENT ON COLUMN StorageUser.shard_id IS
        'The internal identifier for the database shard the user''s data is on'
    """,
    """
    COMMENT ON COLUMN StorageUser.root_volume_id IS
        'The root UserVolume id for this user';
    """,
    """
    CREATE TYPE access_level AS ENUM('View', 'Modify');
    """,
    """
    CREATE TABLE Share (
        shared_by INT NOT NULL REFERENCES StorageUser (id),
        subtree UUID NOT NULL,
        shared_to INT REFERENCES StorageUser (id),
        name text NOT NULL,
        accepted boolean NOT NULL,
        access access_level NOT NULL,
        when_shared timestamp without time zone
            DEFAULT timezone('UTC'::text, now()) NOT NULL,
        when_last_changed timestamp without time zone
            DEFAULT timezone('UTC'::text, now()) NOT NULL,
        status lifecycle_status
            DEFAULT 'Live'::lifecycle_status NOT NULL,
        id uuid NOT NULL PRIMARY KEY,
        email text
    );
    """,
    """
    COMMENT ON TABLE share IS 'Shares encompass both access control and \
    inter-user_id visbility of objects.  An object from a StorageUser''s \
    volume is visible to another StorageUser iff there is a Share entry \
    for it or for its ancestors.  Access granted is in terms of the highest p\
    ermission granted the StorageUser on the object or any of its \
    ancestors.  Owners always have full access to their own objects.  Note \
    that, given visibility, people can copy objects and do whatever they \
    like with the copies.'
    """,
    """
    COMMENT ON COLUMN share.id IS 'A unique identifier for the share';
    """,
    """
    COMMENT ON COLUMN share.shared_by IS 'The StorageUser who initiated the \
    share (can be different from the owner, though the owner must explicitly \
    approve all shares of subtrees she owns).';
    """,
    """
    COMMENT ON COLUMN Share.subtree IS
        'The root of the subtree being offered.';
    """,
    """
    COMMENT ON COLUMN Share.shared_to IS
        'The StorageUser to whom the share is being offered.';
    """,
    """
    COMMENT ON COLUMN share.name IS 'The name the StorageUser sees the share \
    as in his list of shares, once it has been accepted.';
    """,
    """
    COMMENT ON COLUMN Share.accepted IS
        'Whether the offered share has been accepted yet or not.';
    """,
    """
    COMMENT ON COLUMN Share.access IS
        'The access level granted by this share.';
    """,
    """
    COMMENT ON COLUMN Share.when_shared IS
        'Timestamp when the share was originally offered.';
    """,
    """
    COMMENT ON COLUMN share.when_last_changed IS 'Timestamp when details \
    of the share were changed (e.g. if it was accepted, or the granted \
    access was changed).';
    """,
    """
    COMMENT ON COLUMN Share.status IS
        'Whether this share is active/alive.';
    """,
    """
    COMMENT ON COLUMN share.email IS 'Email address that \
             this share was offered to.';
    """,
    """
    CREATE INDEX share_subtree_idx ON share USING btree (subtree);
    """,
    """
    CREATE INDEX share_shared_by_fkey ON Share(shared_by)
    """,
    """
    CREATE UNIQUE INDEX share_shared_to_key
        ON Share(shared_to, name) WHERE status='Live'
    """,
    """
    CREATE UNIQUE INDEX share_shared_to_shared_by_subtree
        ON Share(shared_to, shared_by, subtree)
        WHERE status='Live' AND shared_to IS NOT NULL
    """,
    """
    CREATE TABLE PublicFile (
        id SERIAL PRIMARY KEY,
        owner_id integer NOT NULL REFERENCES StorageUser (id),
        node_id uuid NOT NULL UNIQUE,
        public_uuid UUID UNIQUE);
    """,
    """
    COMMENT ON TABLE PublicFile IS
        'A mapping of public file IDs to storage objects.';
    """,
    """
    COMMENT ON COLUMN PublicFile.id IS
        'Unique identifier for this public file.';
    """,
    """
    COMMENT ON COLUMN PublicFile.owner_id IS 'The owner of this file.';
    """,
    """
    COMMENT ON COLUMN PublicFile.node_id IS 'The file to be made public.';
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE share TO storage, webapp;
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE storageuser TO storage, webapp;
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE PublicFile to storage, webapp;
    """,
    """
    GRANT SELECT,USAGE ON TABLE publicfile_id_seq to storage, webapp;
    """,
]


DROP = []
DELETE = [
    "DELETE FROM Share",
    "DELETE FROM PublicFile",
    "DELETE FROM StorageUser",
    "ALTER SEQUENCE PublicFile_id_seq RESTART WITH 1",
]
