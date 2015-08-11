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

"""Add account2.referral_capability table and update capability summ view."""

SQL = [
    """
    CREATE TABLE account2.referral_capability (
        id SERIAL PRIMARY KEY,
        referral_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        capability_id INTEGER NOT NULL REFERENCES account2.capability(id)
            ON DELETE CASCADE,
        amount BIGINT,
        active_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        active_until TIMESTAMP WITHOUT TIME ZONE
    )
    """,
    """
    GRANT USAGE, SELECT, UPDATE
        ON SEQUENCE account2.referral_capability_id_seq TO webapp;
    """,
    """
    CREATE INDEX referral_capability_user_idx ON
        account2.referral_capability (user_id)
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.referral_capability TO webapp
    """,
    """DROP VIEW account2.user_capability_summary""",
    """
    CREATE VIEW account2.user_capability_summary as
        select up.user_id, up.active_from, up.active_until,
            c.code, pc.base_amount as amount
        from account2.user_plan up,
             account2.plan p,
             account2.plan_capability pc,
             account2.capability c
        where up.plan_id = p.id and p.id = pc.plan_id and
            pc.capability_id = c.id and p.is_base_plan is false
        UNION ALL
        select uc.user_id, uc.active_from, uc.active_until,
            c.code, uc.units * c.unit_amount as amount
        from account2.user_capability uc,
             account2.capability c
        where uc.capability_id=c.id
        UNION ALL
        select u.id as user_id, u.accepted_tos_on as active_from,
            null as active_until, c.code, pc.base_amount as amount
        from account2.user_profile u,
             account2.plan p,
             account2.plan_capability pc,
             account2.capability c
        where u.accepted_tos_on is not null and p.id = pc.plan_id and
            pc.capability_id = c.id and p.is_base_plan is true
        UNION ALL
        select rc.user_id, rc.active_from, rc.active_until,
            c.code, rc.amount as amount
        from account2.referral_capability rc,
             account2.capability c
        where rc.capability_id=c.id
    """,
    """
    GRANT SELECT ON TABLE account2.user_capability_summary TO webapp
    """,
]


def apply(store):
    """Apply the patch."""
    for sql in SQL:
        store.execute(sql)
