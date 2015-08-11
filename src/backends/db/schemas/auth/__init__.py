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

"""This is used to create/delete/drop the main database schema."""

from backends.db.tools.schema import Schema

__all__ = ["create_schema"]


def create_schema():
    """Returns a Schema"""
    from backends.db.schemas import auth as patch_package
    return Schema(CREATE, DROP, DELETE, patch_package, 'public.patch')

DJANGO_CREATE = [
    """
    CREATE TABLE django_content_type (
        id SERIAL PRIMARY KEY,
        name character varying(100) NOT NULL,
        app_label character varying(100) NOT NULL,
        model character varying(100) NOT NULL,
        CONSTRAINT django_content_type_app_label_key UNIQUE (app_label, model)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_content_type TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE django_content_type_id_seq TO webapp
    """,
    """
    CREATE TABLE django_session (
        session_key character varying(40) PRIMARY KEY,
        session_data text NOT NULL,
        expire_date timestamp with time zone NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_session TO webapp
    """,
    """
    CREATE TABLE django_site (
        id SERIAL PRIMARY KEY,
        domain character varying(100) NOT NULL,
        name character varying(50) NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_site TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE django_site_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_user (
        id SERIAL PRIMARY KEY,
        username character varying(30) NOT NULL,
        first_name character varying(30) NOT NULL,
        last_name character varying(30) NOT NULL,
        email character varying(75) NOT NULL,
        password character varying(128) NOT NULL,
        is_staff boolean NOT NULL,
        is_active boolean NOT NULL,
        is_superuser boolean NOT NULL,
        last_login timestamp with time zone NOT NULL,
        date_joined timestamp with time zone NOT NULL,
        CONSTRAINT auth_user_username_key UNIQUE (username)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_user TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_user_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_group (
        id SERIAL PRIMARY KEY,
        name character varying(80) NOT NULL,
        CONSTRAINT auth_group_name_key UNIQUE (name)
    )
    """,
    """
    GRANT SELECT, INSERT, UPDATE
        ON TABLE auth_group TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_group_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_user_groups (
        id SERIAL PRIMARY KEY,
        user_id integer NOT NULL
          REFERENCES auth_user(id) DEFERRABLE INITIALLY DEFERRED,
        group_id integer NOT NULL
          REFERENCES auth_group(id) DEFERRABLE INITIALLY DEFERRED,
        CONSTRAINT auth_user_groups_user_id_key UNIQUE (user_id, group_id)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_user_groups TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_user_groups_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_permission (
        id SERIAL PRIMARY KEY,
        name character varying(50) NOT NULL,
        content_type_id integer NOT NULL
          REFERENCES django_content_type(id) DEFERRABLE INITIALLY DEFERRED,
        codename character varying(100) NOT NULL,
        CONSTRAINT auth_permission_content_type_id_key
          UNIQUE (content_type_id, codename)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_permission TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_permission_id_seq TO webapp
    """,
    """
    CREATE INDEX auth_permission_content_type_id
        ON auth_permission (content_type_id)
    """,
    """
    CREATE TABLE auth_user_user_permissions (
        id SERIAL PRIMARY KEY,
        user_id integer NOT NULL
          REFERENCES auth_user(id) DEFERRABLE INITIALLY DEFERRED,
        permission_id integer NOT NULL
          REFERENCES auth_permission(id) DEFERRABLE INITIALLY DEFERRED,
        CONSTRAINT auth_user_user_permissions_user_id_key
          UNIQUE (user_id, permission_id)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_user_user_permissions TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_user_user_permissions_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_group_permissions (
        id SERIAL PRIMARY KEY,
        group_id integer NOT NULL
          REFERENCES auth_group(id) DEFERRABLE INITIALLY DEFERRED,
        permission_id integer NOT NULL
          REFERENCES auth_permission(id) DEFERRABLE INITIALLY DEFERRED,
        CONSTRAINT auth_group_permissions_group_id_key
          UNIQUE (group_id, permission_id)
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_group_permissions TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_group_permissions_id_seq TO webapp
    """,
    """
    CREATE TABLE auth_message (
        id SERIAL PRIMARY KEY,
        user_id integer NOT NULL
          REFERENCES auth_user(id) DEFERRABLE INITIALLY DEFERRED,
        message text NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE auth_message TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE auth_message_id_seq TO webapp
    """,
    """
    CREATE INDEX auth_message_user_id ON auth_message (user_id)
    """,
    """
    CREATE TABLE django_openid_auth_association (
        id SERIAL PRIMARY KEY,
        server_url text NOT NULL,
        handle character varying(255) NOT NULL,
        secret text NOT NULL,
        issued integer NOT NULL,
        lifetime integer NOT NULL,
        assoc_type text NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_openid_auth_association TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE django_openid_auth_association_id_seq TO webapp
    """,
    """
    CREATE TABLE django_openid_auth_nonce (
        id SERIAL PRIMARY KEY,
        server_url character varying(2047) NOT NULL,
        "timestamp" integer NOT NULL,
        salt character varying(40) NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_openid_auth_nonce TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE django_openid_auth_nonce_id_seq TO webapp
    """,
    """
    CREATE TABLE django_openid_auth_useropenid (
        id SERIAL PRIMARY KEY,
        user_id integer NOT NULL
          REFERENCES auth_user(id) DEFERRABLE INITIALLY DEFERRED,
        claimed_id text NOT NULL,
        display_id text NOT NULL
    )
    """,
    """
    GRANT SELECT, INSERT, DELETE, UPDATE
        ON TABLE django_openid_auth_useropenid TO webapp
    """,
    """
    GRANT SELECT, UPDATE, USAGE
        ON TABLE django_openid_auth_useropenid_id_seq TO webapp
    """,
    """
    CREATE INDEX django_openid_auth_useropenid_user_id
        ON django_openid_auth_useropenid (user_id)
    """,
]


OAUTH_CREATE = [
    """
    CREATE TABLE oauth_consumer (
        id SERIAL NOT NULL PRIMARY KEY,
        key text NOT NULL,
        secret text NOT NULL
    );
    """,
    """
    CREATE TABLE oauth_access_token (
        id SERIAL NOT NULL PRIMARY KEY,
        key text NOT NULL,
        secret text NOT NULL,
        consumer_id integer NOT NULL REFERENCES oauth_consumer(id),
        user_id integer REFERENCES public.auth_user(id),
        description text,
        date_created timestamp without time zone
            DEFAULT timezone('UTC'::text, now()) NOT NULL,
        platform text,
        platform_version text,
        platform_arch text,
        client_version text
    );
    """,
    """
    CREATE TABLE oauth_request_token (
        id SERIAL NOT NULL PRIMARY KEY,
        key text NOT NULL,
        secret text NOT NULL,
        consumer_id integer NOT NULL REFERENCES oauth_consumer(id),
        user_id integer REFERENCES public.auth_user(id),
        description text,
        date_created timestamp without time zone
            DEFAULT timezone('UTC'::text, now()) NOT NULL,
        date_reviewed timestamp without time zone,
        verifier text,
        callback text,
        scope text[]
    );
    """,
    """
    ALTER TABLE ONLY oauth_access_token
        ADD CONSTRAINT oauth_access_token__key__unique UNIQUE (key);
    """,
    """
    ALTER TABLE ONLY oauth_consumer
        ADD CONSTRAINT oauth_consumer__key__unique UNIQUE (key);
    """,
    """
    ALTER TABLE ONLY oauth_request_token
        ADD CONSTRAINT oauth_request_token__key__unique UNIQUE (key);
    """,
    """
    CREATE INDEX oauth_access_token_consumer_id_fkey
            ON oauth_access_token (consumer_id);
    """,
    """
    CREATE INDEX oauth_access_token_user_id_fkey
            ON oauth_access_token (user_id);
    """,
    """
    CREATE INDEX oauth_request_token_consumer_id_fkey
            ON oauth_request_token (consumer_id);
    """,
    """
    CREATE INDEX oauth_request_token_user_id_fkey
            ON oauth_request_token (user_id);
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE oauth_access_token TO webapp;
    """,
    """
    GRANT SELECT ON TABLE oauth_access_token TO storage;
    """,
    """
    GRANT ALL ON SEQUENCE oauth_access_token_id_seq TO webapp;
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE oauth_consumer TO webapp;
    """,
    """
    GRANT SELECT ON TABLE oauth_consumer TO storage;
    """,
    """
    GRANT ALL ON SEQUENCE oauth_consumer_id_seq TO webapp;
    """,
    """
    GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE oauth_request_token TO webapp;
    """,
    """
    GRANT ALL ON SEQUENCE oauth_request_token_id_seq TO webapp;
    """,
]


#need to find a better way to do this other than here.
INITIALIZE_DATA = [
    """
    INSERT INTO public.oauth_consumer (key, secret)
        VALUES ('sledgetime', 'hammertime')
    """,
    """
    INSERT INTO public.django_content_type (id, name, app_label, model) VALUES
        (1, 'admin', 'admin', '')
    """,
    """
    INSERT INTO public.auth_permission (id, name, content_type_id, codename)
        VALUES
            (1, 'crazyhacker',   1, 'crazyhacker'),
            (2, 'crazyadmin',    1, 'crazyadmin'),
            (3, 'employee', 1, 'employee');
    """
    """
    INSERT INTO public.auth_group (id, name) VALUES
        (1, 'crazyhackers'),
        (2, 'crazyadmins'),
        (3, 'employees');
    """,
    """
    INSERT INTO public.auth_group_permissions (group_id, permission_id) VALUES
        (1, 1), (2, 2), (3, 3);
    """,
    # This is the consumer key for tomboy 0.15.3
    """
    INSERT INTO public.oauth_consumer (key, secret)
        VALUES ('anyone', 'anyone');
    """,
]

CREATE = []
CREATE.extend(DJANGO_CREATE)
CREATE.extend(OAUTH_CREATE)
CREATE.extend(INITIALIZE_DATA)


DROP = []

DELETE = [
    "DELETE FROM oauth_access_token",
    "DELETE FROM oauth_request_token",
    "DELETE FROM oauth_consumer",
    "DELETE FROM auth_user",
    "DELETE FROM django_session",
    "DELETE FROM django_site",
    "DELETE FROM auth_user",
    "DELETE FROM auth_user_groups",
    "DELETE FROM auth_user_user_permissions",
    "DELETE FROM auth_message",
    "DELETE FROM django_openid_auth_association",
    "DELETE FROM django_openid_auth_nonce",
    "DELETE FROM django_openid_auth_useropenid",
    "ALTER SEQUENCE auth_user_id_seq RESTART WITH 1",
]
