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

function setup_database() {
    local TESTDIR=$1

    echo "## Setting up Postgres in $TESTDIR ##"
    mkdir -p "$TESTDIR"
    chmod 700 "$TESTDIR"

    if [ -d /usr/lib/postgresql/9.1 ]; then
        export PGBINDIR=/usr/lib/postgresql/9.1/bin
    elif [ -d /usr/lib/postgresql/8.4 ]; then
        export PGBINDIR=/usr/lib/postgresql/8.4/bin
    else
        echo "Cannot find valid parent for PGBINDIR"
    fi

    $PGBINDIR/initdb -E UNICODE -D $TESTDIR &>/dev/null

    # set up the database options file
    if [ ! -e $TESTDIR/postgresql.conf ]; then
        echo "PostgreSQL data directory apparently didn't init"
    else
    (
        cat <<EOF
search_path='\$user,public,ts2'
log_statement='all'
log_line_prefix='[%m] %q%u@%d %c '
fsync = off
EOF
    ) > $TESTDIR/postgresql.conf
    fi

    if [ -x /usr/bin/pgtune ]; then
        pgtune -c 50 -M 67108864 -i $TESTDIR/postgresql.conf -o $TESTDIR/postgresql.conf
    fi
}

setup_database $ROOTDIR/tmp/db1/data
