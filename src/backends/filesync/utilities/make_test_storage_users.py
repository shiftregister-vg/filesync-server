#!/usr/bin/python

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

"""Utility used for creating test Storage Users with an oauth token."""

import os
import json

import _pythonpath  # NOQA

from optparse import OptionParser

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--count", dest="count", default="100",
                      help="number of users to create")

    (options, args) = parser.parse_args()

    from utilities import userutils
    import uuid
    from backends.filesync.data.services import make_storage_user
    from backends.filesync.data.dbmanager import get_new_user_shard_id

    token_data = {}
    for i in range(int(options.count)):
        username = u"testuser%s" % i
        userinfo = {
            'username': unicode(uuid.uuid4()),
            'full_name': u"name %s" % i,
            'active': True,
            'email': u"user@somemail.com",
        }
        # create the user account
        user = userutils.create_user(userinfo)
        # create the storage account
        make_storage_user(
            user.id, user.username,
            "%s %s" % (user.first_name, user.last_name), 2 * (2 ** 30),
            shard_id=get_new_user_shard_id(user.id))
        # get an oauth token
        token = userutils.make_oauth_token(user)
        token_data[username] = (user.id, str(token))
    token_file = os.path.join('testoauthkeys.json')
    with open(token_file, 'w') as f:
        json.dump(token_data, f)
