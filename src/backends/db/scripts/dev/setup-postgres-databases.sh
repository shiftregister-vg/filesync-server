#! /bin/bash

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

ROOTDIR=${ROOTDIR:-`bzr root`}
if [ ! -d "$ROOTDIR"  ]; then
    echo "ROOTDIR '$ROOTDIR' doesn't exist" >&2
    exit 1
fi

DATABASES="
storage
shard0
shard1
shard2
graphite
"

function setup_databases() {
    local HOST=$1 PORT=$2
    echo "## Setting up Postgres databases in $TESTDIR ##"

    if [ -d /usr/lib/postgresql/9.1 ]; then
        export PGBINDIR=/usr/lib/postgresql/9.1/bin
    elif [ -d /usr/lib/postgresql/8.4 ]; then
        export PGBINDIR=/usr/lib/postgresql/8.4/bin
    else
        echo "Cannot find valid parent for PGBINDIR"
    fi

    for db in $DATABASES; do
        $PGBINDIR/createdb -h $HOST -p $PORT --encoding UNICODE "$db" &>/dev/null
        $PGBINDIR/createlang -h $HOST -p $PORT plpgsql "$db"
    done

    $PGBINDIR/createuser -h $HOST -p $PORT --superuser --createdb "postgres" &>/dev/null
    # create the additional users we need via a psql script
    $PGBINDIR/psql -h $HOST -p $PORT -U postgres template1 <<EOF
CREATE ROLE webapp INHERIT;
CREATE ROLE storage INHERIT;

CREATE USER fuj IN ROLE webapp;
CREATE USER exchangeserv IN ROLE webapp;
CREATE USER appserv      IN ROLE webapp, storage;
CREATE USER apiserv      IN ROLE webapp, storage;
CREATE USER updownserv   IN ROLE webapp, storage;
CREATE USER graphite     IN ROLE webapp, storage;
CREATE USER syncuser     IN ROLE webapp, storage;
CREATE USER dbserv       IN ROLE webapp;

ALTER ROLE webapp NOLOGIN;
ALTER ROLE storage NOLOGIN;
EOF
    echo "To set your environment so psql will connect to this DB instance type:"
    echo "    export PGHOST=$HOST PGPORT=$PORT"
    echo "## Done. ##"
}

setup_databases 127.0.0.1 $(cat $ROOTDIR/tmp/postgres.port)
