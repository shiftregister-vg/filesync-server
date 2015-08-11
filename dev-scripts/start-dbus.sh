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
CONFIG_FILE="${ROOTDIR}/configs/dbus-session.conf"
COMMAND=`which dbus-daemon`

# Redirect FD 3 and 4 to files.
exec 3>${ADDRESS_FILE} 
exec 4>${PID_FILE} 

/sbin/start-stop-daemon --start --chdir $ROOTDIR --pidfile $PID_FILE --exec $COMMAND -- --fork --config-file=${CONFIG_FILE} --print-address=3 --print-pid=4

# Wait for dbus-daemon to start.
for i in `seq 1 10`; do
    grep -q ^unix $ADDRESS_FILE && break
    sleep .1
done

# Close FDs 3 and 4 so they are not leaked.
exec 3>&-
exec 4>&-

echo "DBUS_SESSION_BUS_ADDRESS=$(cat $ADDRESS_FILE)"

