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

"""Add db_worker_unseen table to keep track of unseen items on the database
side.
"""

SQL = [
    """
    CREATE TABLE txlog.db_worker_unseen (
       id INTEGER NOT NULL,
       worker_id TEXT NOT NULL,
       created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT \
           timezone('UTC'::text, now())
    )
    """,
    """
    GRANT SELECT, INSERT, UPDATE, DELETE
        ON TABLE txlog.db_worker_unseen
        TO storage, webapp
    """,
    """
    CREATE INDEX db_worker_unseen_idx
        ON txlog.db_worker_unseen(worker_id, created, id)
    """
]


def apply(store):
    """Apply the patch"""
    for statement in SQL:
        store.execute(statement)
