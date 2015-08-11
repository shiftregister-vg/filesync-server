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

"""Tools used to initialize Test Data"""

import sha
import datetime
import uuid

from backends.filesync.data import model


default_shard_id = u'shard0'

# XXX: salgado: The functionality here should be moved into methods of
# ObjectFactory.


def get_fake_hash(key=None):
    """Return a hashkey."""
    return "sha1:" + sha.sha(key or str(uuid.uuid4())).hexdigest()


def get_test_contentblob(content=None):
    """Get a content blob"""
    cb = model.ContentBlob()
    cb.hash = get_fake_hash(content)
    cb.crc32 = 1023
    cb.size = 1024
    cb.deflated_size = 10000
    cb.storage_key = uuid.uuid4()
    cb.content = content
    cb.status = model.STATUS_LIVE
    return cb


def content_blob_args():
    """Returns example blob arguments."""
    return dict(hash=get_fake_hash(), crc32=1023, size=1024,
                storage_key=uuid.uuid4(), deflated_size=10000,
                magic_hash="magic!", content="hola", status=model.STATUS_LIVE)


def uploadjob_args(key="hola"):
    """Returns example upload job arguments."""
    return dict(crc32_hint=1024, hash_hint="sha1:" + sha.sha(key).hexdigest(),
                inflated_size_hint=200, deflated_size_hint=100,
                when_started=datetime.datetime.now(),
                when_last_active=datetime.datetime.now(),
                status=model.STATUS_DEAD)
