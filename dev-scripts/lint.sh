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

# Runs flake8 on files changed in this branch.


EXIT=0

if [ "$1" == "-a" ]; then
    shift
    files=`find src -name '*.py'`
elif [ -z "$1" ]; then
    # No command line argument provided, use the default logic.
    bzr diff > /dev/null
    diff_status=$?
    if [ $diff_status -eq 0 ] ; then
        # No uncommitted changes in the tree, lint changes relative to the
        # parent.
        rev=`bzr info | sed '/parent branch:/!d; s/ *parent branch: /ancestor:/'`
        rev_option="-r $rev"
    elif [ $diff_status -eq 1 ] ; then
        # Uncommitted changes in the tree, lint those changes.
        rev_option=""
    else
        # bzr diff failed
        exit 1
    fi
    files=`bzr st --short $rev_option | sed '/^.[MN]/!d; s/.* //'`
else
    # Add newlines so grep filters out pyfiles correctly later.
    files=`echo $* | tr " " "\n"`
fi

echo "= Backends Lint Check ="
echo ""
echo "Checking for conflicts. and issues in doctests and templates."
echo "Running flake8."

if [ -z "$files" ]; then
    echo "No changed files detected."
    exit 0
else
    echo
    echo "Linting changed files:"
    for file in $files; do
        echo "  $file"
    done
fi


group_lines_by_file() {
    # Format file:line:message output as lines grouped by file.
    file_name=""
    echo "$1" | sed 's,\(^[^ :<>=+]*:\),~~\1\n,' | while read line; do
        current=`echo $line | sed '/^~~/!d; s/^~~\(.*\):$/\1/;'`
        if [ -z "$current" ]; then
            echo "    $line"
        elif [ "$file_name" != "$current" ]; then
            file_name="$current"
            echo ""
            echo "$file_name"
        fi
    done
}


conflicts=""
for file in $files; do
    # NB. Odd syntax on following line to stop lint.sh detecting conflict
    # markers in itself.
    if [ ! -f "$file" ]; then
        continue
    fi
    if grep -q -e '<<<''<<<<' -e '>>>''>>>>' $file; then
        conflicts="$conflicts $file"
    fi
done

if [ "$conflicts" ]; then
    echo ""
    echo ""
    echo "== Conflicts =="
    echo ""
    for conflict in $conflicts; do
        echo "$conflict"
    done
    EXIT=1
fi

pyfiles=`echo "$files" | egrep '.py$'`

if [ -z "$pyfiles" ]; then
    exit $EXIT
fi

# Filtering out false-positives
sed_deletes="/^*/d; /'_pythonpath' imported but unused/d; "

flake8_notices=`flake8 $pyfiles | sed "$sed_deletes"`

if [ ! -z "$flake8_notices" ]; then
    echo ""
    echo ""
    echo "== Flake8 notices =="
    group_lines_by_file "$flake8_notices"
    EXIT=1
fi

exit $EXIT
