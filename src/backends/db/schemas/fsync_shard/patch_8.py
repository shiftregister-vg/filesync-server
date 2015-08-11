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
Add a function to help on getting formatted stats on running transactions.
"""

SQL = [
    """
    SET search_path = public, pg_catalog;

    CREATE FUNCTION activity() RETURNS SETOF pg_stat_activity
        LANGUAGE sql SECURITY DEFINER
        SET search_path TO public
        AS $$
        SELECT
            datid, datname, procpid, usesysid, usename,
            application_name, client_addr, client_hostname, client_port,
            backend_start, xact_start, query_start, waiting,
            CASE
                WHEN current_query LIKE '<IDLE>%'
                    OR current_query LIKE 'autovacuum:%'
                    THEN current_query
                ELSE
                    '<HIDDEN>'
            END AS current_query
        FROM pg_catalog.pg_stat_activity;
    $$;

    COMMENT ON FUNCTION activity() IS
        'SECURITY DEFINER wrapper around pg_stat_activity allowing \
         unprivileged users to access most of its information.';
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
