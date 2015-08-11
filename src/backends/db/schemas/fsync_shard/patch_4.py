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

"""Drop a couple columns that are no longer used for a long time."""

SQL = [
    """
    ALTER TABLE storageuserinfo DROP COLUMN pre_gen_client;
    """,
    """
    DROP VIEW share_delta_view;
    """,
    """
    CREATE VIEW share_delta_view AS
        SELECT NULL::unknown AS share_id, o.id, o.owner_id,
        o.volume_id, o.parent_id, o.name, o.content_hash, o.directory_hash,
        o.kind, o.mimetype, o.when_created, o.when_last_modified, o.status,
        o.path, o.publicfile_id, o.public_uuid, o.generation,
        o.generation_created
        FROM object o
    UNION
        SELECT movefromshare.share_id, movefromshare.id,
        movefromshare.owner_id, movefromshare.volume_id,
        movefromshare.parent_id, movefromshare.name,
        movefromshare.content_hash, movefromshare.directory_hash,
        movefromshare.kind, movefromshare.mimetype, movefromshare.when_created,
        movefromshare.when_last_modified, movefromshare.status,
        movefromshare.path,
        movefromshare.publicfile_id, movefromshare.public_uuid,
        movefromshare.generation, movefromshare.generation_created
        FROM movefromshare;
    """,
    """
    GRANT SELECT ON TABLE share_delta_view TO storage, webapp;
    """,
    """
    ALTER TABLE object DROP COLUMN is_public;
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
