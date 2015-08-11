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

"""Benchmark script for a user."""

import uuid
import os

from datetime import datetime

from ubuntuone.storageprotocol import content_hash
from optparse import OptionParser

from backends.filesync.notifier.notifier import register_notifier_for_bus
from backends.filesync.notifier.testing.testcase import AccumulatingNotifyBus
from backends.filesync.data.services import (
    get_storage_user, make_storage_user)


MIME_TYPES = [
    u'audio/mp3',
    u'audio/mpg',
    u'audio/mpeg',
    u'audio/mpeg3',
    u'audio/x-mp3',
    u'audio/x-mpeg',
    u'application/mp3',
    # OGG
    u'audio/ogg',
    u'audio/x-vorbis+ogg',
    u'application/ogg',
    # itunes
    u'audio/mp4a-latm',
    u'audio/mp4',
    u'audio/x-m4a',
]


def make_file_with_content(root, name, mimetype=None):
    """Make a file with content."""
    hash = content_hash.content_hash_factory()
    hash.update(str(uuid.uuid4()))
    hashstr = hash.content_hash()
    root.make_file_with_content(name, hashstr, 10, 100, 100, uuid.uuid4(),
                                mimetype=mimetype)


def make_files(root):
    """Make a bunch of files in a subdir"""
    for mime in MIME_TYPES:
        make_file_with_content(root, u"%s.mp3" % unicode(uuid.uuid4), mime)


def make_deep_tree_with_files(root):
    """Make a deeply nested tree with files."""
    for i in range(100):
        root = root.make_subdirectory(u"sub%s" % i)
        make_files(root)


def benchmark_one(bench, user_id):
    """Run a benchmark on a bunch of things."""

    user = get_storage_user(int(user_id), active_only=False)
    if user is None:
        user = make_storage_user(int(user_id), u"TestUser",
                                 u"", 100 * (2 ** 30))

    # setup a test directory with loads of files.
    testroot = u"stormbench-%s" % datetime.now().strftime('%Y%m%d%H%M%S')
    root = user.volume().root.make_subdirectory(testroot)
    for i in range(10):
        d = root.make_subdirectory(unicode(i))
        make_deep_tree_with_files(d)

    #move the children on the test root
    moved_child = user.volume().get_node_by_path(u"%s/1" % testroot)
    dest_dir = root.make_subdirectory(u"test-move")
    with bench("big_move"):
        moved_child.move(dest_dir.id, moved_child.name)

    with bench("deltafull"):
        generation, _, _ = user.volume().get_delta(0)

    with bench("deltaempty"):
        user.volume().get_delta(generation - 1000)

    with bench("getmusic"):
        user.volume().get_all_nodes(mimetypes=MIME_TYPES)

    with bench("get_shared_volumes"):
        user.get_share_volumes()

    with bench("get_udfs"):
        user.get_udfs()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--user_id", dest="user_id", default=None,
                      help="The user_id to run the test against.")
    options, args = parser.parse_args()

    from stormbench.benchmark import run_benchmark
    from stormbench.reporter import generate_report

    nb = AccumulatingNotifyBus()
    register_notifier_for_bus(nb)
    timestampdir = datetime.now().strftime('%Y%m%d%H%M%S')
    root_dir = 'tmp/stormbench'
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    datafile = os.path.join(root_dir, "%s.dat" % timestampdir)
    reports_dir = os.path.join(root_dir, 'reports', timestampdir)

    outfile = run_benchmark(benchmark_one, datafile, user_id=options.user_id)
    generate_report(outfile, reports_dir)
