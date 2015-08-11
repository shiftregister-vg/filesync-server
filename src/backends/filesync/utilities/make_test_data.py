#!/usr/bin/env python

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

"""Create test data for shard data migration testing."""

import warnings
warnings.simplefilter("ignore")

import random
import uuid

import _pythonpath  # NOQA

from backends.filesync.data import services, downloadservices
from backends.filesync.data.testing.testdata import get_fake_hash
from backends.filesync.data.dbmanager import get_storage_store
from backends.filesync.data.tests.test_music_metadata import TEST_MUSIC_DATA

user_names = [u'hola', u'crazyhacker', u'chico', u'active.user']


def make_user_data(user):
    """Make some user data."""
    u = services.get_storage_user(username=user_names[random.randint(0, 4)])
    s = user.root.share(u.id, u"%s to %s" % (user.username, u.username))
    u.get_share(s.id).accept()
    for i in range(10):
        d = user.root.make_subdirectory(u"%s dir%s" % (user.username, i))
        for f in range(10):
            f = d.make_file(u"%s file%s.txt" % (user.username, f))
            uj = f.make_uploadjob(
                f.content_hash, get_fake_hash(str(random.random())),
                random.randint(1, 100), random.randint(100, 1000000),
                random.randint(100, 1000000))
            uj.commit_content(f.content_hash, uuid.uuid4(), None)
            services.add_music_metadata(
                uj.file.content_hash, TEST_MUSIC_DATA, user.shard_id)
    for i in range(10):
        user.make_musicstore_download(
            random.randint(100, 1000000),
            random.randint(1, 100), u"https://fake.com/%s/%s" % (user.id, i),
            u"%s" % user.username, u"%s Greatest Hits" % user.username,
            u"Track %s" % i, "US")
    for i in range(10):
        downloadservices.get_or_make_download(
            user.id, user.root_volume_id,
            u"https://fake.com/%s/%s" % (user.id, i),
            u"/a/b", u"%s%s" % (user.id, i))


for username in user_names:
    user = services.get_storage_user(username=username)
    make_user_data(user)
    store = get_storage_store()
    store.commit()
