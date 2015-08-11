#!/usr/bin/env sh

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
PID_FILE="${ROOTDIR}/tmp/dbus.pid"
ADDRESS_FILE="${ROOTDIR}/tmp/dbus.address"

/sbin/start-stop-daemon -v --stop --oknodo --pidfile $PID_FILE
kill `cat $PID_FILE`
# cleanup the files created on start
rm -f $PID_FILE $ADDRESS_FILE

