#!/bin/bash

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

# Argument = -d data_seg_size, -m max_memory_size, -v virtual_memory, -n open_files

usage()
{
cat << EOF

usage: $0 [options] <command>

This script will change the ulimit and execute the given command.

OPTIONS:
   -h      Show this message
   -d      Data seg size
   -m      max memory size
   -v      virtual memory
   -n      open files
EOF
}

DATA_SEG_SIZE=
MAX_MEM_SIZE=
VIRT_MEM_SIZE=
OPEN_FD=
COMMAND=


while getopts “hd:m:v:n:” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         d)
             DATA_SEG_SIZE="-d $OPTARG"
             ;;
         m)
             MAX_MEM_SIZE="-m $OPTARG"
             ;;
         v)
             VIRT_MEM_SIZE="-v $OPTARG"
             ;;
         n)
             OPEN_FD="-n $OPTARG"
             ;;
     esac
done

shift "$((OPTIND-1))" # shift all the already parsed args
COMMAND=$@

if [[ -z $COMMAND ]]
then
    echo "no command"
     usage
     exit 1
fi

ulimit $OPEN_FD $VIRT_MEM_SIZE $MAX_MEM_SIZE $DATA_SEG_SIZE;
exec $COMMAND
