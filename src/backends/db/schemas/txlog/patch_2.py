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

"""New function to add volume information to the extra data."""

SQL = """
    CREATE OR REPLACE FUNCTION txlog.get_extra_data_to_recreate_file_1(
            kind object_kind,
            size bigint,
            storage_key uuid,
            publicfile_id integer,
            public_uuid uuid,
            content_hash bytea,
            when_created double precision,
            last_modified double precision,
            volume_path TEXT) RETURNS TEXT
        LANGUAGE plpythonu IMMUTABLE
        AS $_$
        import json
        return json.dumps(dict(
            size=size,
            storage_key=storage_key,
            publicfile_id=publicfile_id,
            public_uuid=public_uuid,
            content_hash=content_hash,
            when_created=int(when_created),
            last_modified=int(last_modified),
            kind=kind,
            volume_path=volume_path))
    $_$;
    """


def apply(store):
    """Apply the patch"""
    store.execute(SQL)
