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

"""This is used to create/delete/drop the account database schema."""

from backends.db.tools.schema import Schema

__all__ = ["create_schema"]


def create_schema():
    """Return a Schema"""
    from backends.db.schemas import account as patch_package
    return Schema(CREATE, DROP, DELETE, patch_package, 'account2.patch')


CREATE = [
    """
    CREATE SCHEMA account2
    """,
    """
    GRANT USAGE on SCHEMA account2 TO webapp
    """,
    """
    CREATE TABLE account2.user_profile (
        id INTEGER NOT NULL PRIMARY KEY,
        accepted_tos_on TIMESTAMP WITHOUT TIME ZONE,
        email_notification boolean default false
    )
    """,
    """
    GRANT SELECT, UPDATE, INSERT, DELETE
        ON TABLE account2.user_profile TO webapp;
    """,
    """
    CREATE TABLE account2.plan (
        id SERIAL PRIMARY KEY,
        name TEXT,
        description TEXT,
        is_base_plan boolean default false,
        available_from TIMESTAMP WITHOUT TIME ZONE,
        available_until TIMESTAMP WITHOUT TIME ZONE,
        price_table BYTEA,
        promotional_days INTEGER
    );
    """,
    """
    GRANT USAGE, SELECT, UPDATE ON SEQUENCE account2.plan_id_seq TO webapp;
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.plan TO webapp
    """,
    """
    CREATE TABLE account2.capability (
        id SERIAL PRIMARY KEY,
        description TEXT NOT NULL,
        code TEXT NOT NULL UNIQUE,
        allow_amount boolean,
        unit_amount BIGINT,
        unit_price_table BYTEA
    );
    """,
    """
    GRANT USAGE, SELECT, UPDATE
        ON SEQUENCE account2.capability_id_seq TO webapp;
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.capability TO webapp
    """,
    """
    CREATE TABLE account2.plan_capability (
        id SERIAL PRIMARY KEY,
        plan_id INTEGER NOT NULL REFERENCES account2.plan(id)
            ON DELETE CASCADE,
        capability_id INTEGER NOT NULL REFERENCES account2.capability(id)
            ON DELETE CASCADE,
        base_amount BIGINT,
        UNIQUE (plan_id, capability_id)
    );
    """,
    """
    GRANT USAGE, SELECT, UPDATE
        ON SEQUENCE account2.plan_capability_id_seq TO webapp;
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.plan_capability TO webapp
    """,
    """
    CREATE TABLE account2.user_plan (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        plan_id INTEGER NOT NULL REFERENCES account2.plan(id)
            ON DELETE CASCADE,
        active_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        active_until TIMESTAMP WITHOUT TIME ZONE,
        cancel_date TIMESTAMP WITHOUT TIME ZONE,
        sub_id INTEGER
    )
    """,
    """
    GRANT USAGE, SELECT, UPDATE
        ON SEQUENCE account2.user_plan_id_seq TO webapp;
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.user_plan TO webapp
    """,
    """
    CREATE INDEX user_plan_user__plan_idx ON
        account2.user_plan (user_id, plan_id)
    """,
    """
    CREATE TABLE account2.user_capability (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        capability_id INTEGER NOT NULL REFERENCES account2.capability(id)
            ON DELETE CASCADE,
        units BIGINT NOT NULL,
        active_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        active_until TIMESTAMP WITHOUT TIME ZONE,
        sub_id INTEGER
    )
    """,
    """
    GRANT USAGE, SELECT, UPDATE
        ON SEQUENCE account2.user_capability_id_seq TO webapp;
    """,
    """
    CREATE INDEX user_capability_user_idx ON
        account2.user_capability (user_id)
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE account2.user_capability TO webapp
    """,
    """
    CREATE VIEW account2.user_capability_summary as
        select up.user_id, up.active_from, up.active_until,
        c.code, pc.base_amount as amount
        from account2.user_plan up,
             account2.plan p,
             account2.plan_capability pc,
             account2.capability c
        where up.plan_id = p.id and p.id = pc.plan_id
            and pc.capability_id = c.id and p.is_base_plan is false
        UNION ALL
        select uc.user_id, uc.active_from, uc.active_until,
            c.code, uc.units * c.unit_amount as amount
        from account2.user_capability uc,
             account2.capability c
        where uc.capability_id=c.id
        UNION ALL
        select u.id as user_id, u.accepted_tos_on as active_from, null
            as active_until, c.code, pc.base_amount as amount
        from account2.user_profile u,
             account2.plan p,
             account2.plan_capability pc,
             account2.capability c
        where u.accepted_tos_on is not null and p.id = pc.plan_id and
            pc.capability_id = c.id and p.is_base_plan is true
    """,
    """
    GRANT SELECT ON TABLE account2.user_capability_summary TO webapp
    """,
]

DROP = []

DELETE = [
    "DELETE FROM account2.user_plan",
    "DELETE FROM account2.plan_capability",
    "DELETE FROM account2.user_capability",
    "DELETE FROM account2.capability",
    "DELETE FROM account2.plan",
    "DELETE FROM account2.user_profile",
    "ALTER SEQUENCE account2.plan_id_seq RESTART WITH 1",
    "ALTER SEQUENCE account2.capability_id_seq RESTART WITH 1",
    "ALTER SEQUENCE account2.plan_capability_id_seq RESTART WITH 1",
    "ALTER SEQUENCE account2.user_plan_id_seq RESTART WITH 1",
    "ALTER SEQUENCE account2.user_capability_id_seq RESTART WITH 1",
]
