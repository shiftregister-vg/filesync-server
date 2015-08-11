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

"""Utility functions to create users for testing."""

import json
import os

import warnings
#enjoy the silence
warnings.simplefilter("ignore")

from backends.filesync.data.dbmanager import fsync_commit, get_shard_ids
from backends.db.db_admin_store import get_admin_store
from backends.db.schemas.fsync_main import create_schema as fsync_main
from backends.db.schemas.fsync_shard import create_schema as fsync_shard

from utilities import utils

TMP_DIR = os.path.join(utils.get_rootdir(), 'tmp')
AUTH_TOKENFILE = os.path.join(TMP_DIR, 'authkeys.json')


@fsync_commit
def delete_all_data():
    """Delete all data from the database."""
    # they must be reversed to avoid dependency issues
    fsync_main().delete(get_admin_store('storage'))
    for shard_id in get_shard_ids():
        fsync_shard().delete(get_admin_store(shard_id))


def add_auth_info_to_keyfile(username, auth_info, filename=AUTH_TOKENFILE):
    """Add auth info to the key file."""
    token_dict = {}
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                token_dict = json.load(f)
            except ValueError:
                pass
    with open(filename, 'w') as f:
        token_dict[username] = auth_info
        json.dump(token_dict, f)


def get_auth_info_from_keyfile(username, filename=AUTH_TOKENFILE):
    """Get auth info from a file."""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                auth_info = json.load(f)
            except ValueError:
                print "auth token file (%s) is empty" % filename
        try:
            return auth_info[username]
        except KeyError:
            print "User not found in token file (%s)." % filename
    else:
        print "auth token file (%s) does not exist:" % filename
