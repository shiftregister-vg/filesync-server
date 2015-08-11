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

"""Benchmark script for the DAL."""

import uuid
from datetime import datetime

from ubuntuone.storageprotocol import content_hash
from optparse import OptionParser

from backends.filesync.notifier.notifier import register_notifier_for_bus
from backends.filesync.notifier.testing.testcase import AccumulatingNotifyBus
from backends.filesync.data.services import make_storage_user

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


def create_user(shard_id):
    """Create a new random user."""
    id = datetime.utcnow().toordinal()
    username = unicode(uuid.uuid4())
    return make_storage_user(id, username, u"", 2 * (2 ** 30),
                             shard_id=shard_id)


def do_fileops(bench, base, sufix):
    """Do a bunch of file operations for a directory."""
    with bench("makefile_" + sufix):
        file = base.make_file(unicode(uuid.uuid4()))
    with bench("makedir_" + sufix):
        dir = base.make_subdirectory(unicode(uuid.uuid4()))

    with bench("delfile_" + sufix):
        file.delete()

    with bench("deldir_" + sufix):
        dir.delete()

    with bench("5050_flat_" + sufix):
        for i in range(50):
            base.make_file(unicode(uuid.uuid4()))
            base.make_subdirectory(unicode(uuid.uuid4()))

    with bench("getchildren_" + sufix):
        base.get_children()

    first = dir = base.make_subdirectory(unicode(uuid.uuid4()))
    with bench("5050_nested_" + sufix):
        for i in range(50):
            dir.make_file(unicode(uuid.uuid4()))
            dir = dir.make_subdirectory(unicode(uuid.uuid4()))

    first.make_file(unicode(uuid.uuid4()))
    with bench("getchildren_sparse_" + sufix):
        first.get_children()

    hash = content_hash.content_hash_factory()
    hash.update(str(uuid.uuid4()))
    hashstr = hash.content_hash()
    storage_key = uuid.uuid4()

    with bench("make_with_content_" + sufix):
        base.make_file_with_content(unicode(uuid.uuid4()), hashstr, 100, 100,
                                    12, storage_key)
    with bench("make_with_content_repeated_" + sufix):
        base.make_file_with_content(unicode(uuid.uuid4()), hashstr, 100, 100,
                                    12, storage_key)


def benchmark_one(bench, shard_id=None):
    """Run a benchmark on a bunch of things."""
    user = create_user(shard_id)
    with bench("getroot"):
        user.root._load()

    volume = user.volume()

    with bench("make_random_dir"):
        root_base = user.root.make_subdirectory(unicode(uuid.uuid4()))

    udf = user.make_udf("~/" + unicode(uuid.uuid4()))
    udf_dir = user.volume(udf.id).get_root().make_subdirectory(
        unicode(uuid.uuid4()))
    for base, sufix in [(root_base, "root"),
                        (udf_dir, "udf")]:
        do_fileops(bench, base, sufix)

    with bench("getvolume"):
        generation = volume.get_volume().generation

    with bench("deltafull"):
        volume.get_delta(0)

    with bench("deltaempty"):
        volume.get_delta(generation)

    with bench("getmusic"):
        volume.get_all_nodes(mimetypes=MIME_TYPES)

    with bench("get_shared_volumes"):
        user.get_share_volumes()

    with bench("get_udfs"):
        user.get_udfs()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--shard_id", dest="shard_id", default=None,
                      help="The shard to run the test against.")
    options, args = parser.parse_args()

    import os
    from stormbench.benchmark import run_benchmark
    from stormbench.reporter import generate_report

    nb = AccumulatingNotifyBus()
    register_notifier_for_bus(nb)
    shard_id = options.shard_id
    timestampdir = datetime.now().strftime('%Y%m%d%H%M%S')
    root_dir = 'tmp/stormbench'
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    datafile = os.path.join(root_dir, "%s.dat" % timestampdir)
    reports_dir = os.path.join(root_dir, 'reports', timestampdir)
    outfile = run_benchmark(benchmark_one, datafile, shard_id=shard_id)
    generate_report(outfile, reports_dir)
