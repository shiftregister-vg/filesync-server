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

if [ -d /usr/lib/postgresql/9.1 ]; then
    export PGBINDIR=/usr/lib/postgresql/9.1/bin
elif [ -d /usr/lib/postgresql/8.4 ]; then
    export PGBINDIR=/usr/lib/postgresql/8.4/bin
else
    echo "Cannot find valid parent for PGBINDIR"
fi

# setting PGDATA tells pg_ctl which DB to talk to
export PGDATA=$ROOTDIR/tmp/db1/data/
$PGBINDIR/pg_ctl status > /dev/null
if [ $? = 0 ]; then
    $PGBINDIR/pg_ctl stop -t 60 -w -m fast
fi
