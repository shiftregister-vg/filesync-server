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

"""Cleanup indexes on Object table.

Based on feedback from Serius, this patch drops indexes no longer used.
"""

SQL = [
    # this is used when doing magic uploads
    # CREATE INDEX object_content_hash_fkey ON Object(content_hash)
    # Since all queries are done using volume_id, owner_id, there is
    # no need for this index
    # CREATE INDEX object_owner_id_fkey ON Object(owner_id)
    """
    DROP INDEX object_owner_id_fkey;
    """,
    # we don't look up nodes only by kind.
    # CREATE INDEX object_kind ON Object (kind)
    """
    DROP INDEX object_kind;
    """,
    # This index is not needed
    # CREATE INDEX object_generation_created_idx ON object(generation_created);
    """
    DROP INDEX object_generation_created_idx;
    """,
    # This index is not needed
    # CREATE INDEX object_generation_idx ON object(generation);
    """
    DROP INDEX object_generation_idx;
    """,
    # Since we are looking up files by owner_id, volume_id, status=Live,
    # This index never gets used.
    # CREATE INDEX object_mimtype_idx ON object(mimetype);
    """
    DROP INDEX object_mimtype_idx;
    """,
    # This index is still important
    # CREATE UNIQUE INDEX object_parent_name_uk ON object(parent_id, name)
    #    WHERE (status = 'Live'::lifecycle_status);
    #
    # This index is redundant based on the index above
    #CREATE INDEX object_parent_name_volume
    #    ON object(parent_id, name, volume_id)
    #    WHERE ((status = 'Live'::lifecycle_status)
    #        AND (parent_id IS NOT NULL));
    """
    DROP INDEX object_parent_name_volume;
    """,
    # This is used to get user's public files for a volume
    # CREATE INDEX object_publicfile_idx ON object(volume_id, publicfile_id)
    #    WHERE ((kind = 'File'::object_kind)
    #           AND (status = 'Live'::lifecycle_status));
    #
    # This is no longer used.
    # CREATE INDEX object_roots ON object(owner_id)
    #    WHERE ((parent_id IS NULL) AND ((name)::text = ''::text));
    """
    DROP INDEX object_roots;
    """,
    # This was used when doing mass deletes of old files. we cant do this now.
    # CREATE INDEX dead_object_last_modified_idx ON object (when_last_modified)
    #    WHERE (status = 'Dead'::lifecycle_status);
    """
    DROP INDEX dead_object_last_modified_idx;
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
