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

"""This class supports older testcases which don't use the DAL"""

import uuid

from testresources import ResourcedTestCase
from backends.filesync.data import model
from backends.filesync.data.dbmanager import (
    storage_tm, get_user_store, get_shard_store)
from backends.filesync.data.testing.testcase import DAOObjectFactory
from backends.filesync.data.testing.testdata import (
    get_fake_hash,
    default_shard_id,
)

from backends.txlog.model import TransactionLog
from backends.testing.resources import StorageDatabaseResource, shard_resources


class ORMTestCase(ResourcedTestCase):
    """Base class for test cases which use storm to connect to Storage DB."""

    resources = [('storage_db', StorageDatabaseResource)]
    resources.extend(shard_resources)

    def setUp(self):
        super(ORMTestCase, self).setUp()
        self.obj_factory = ORMObjectFactory()

    def create(self, klass, *args, **kwargs):
        """Create and verify all attributes on the object.

        will create the object, set the attributes and then
        commit it to the database. then check to see that the
        attributes match.
        """
        try:
            obj = klass()
            for k, v in kwargs.items():
                setattr(obj, k, v)
            self.sstore.add(obj)
        except Exception, e:
            storage_tm.abort()
            raise e
        storage_tm.commit()
        for k, v in kwargs.items():
            self.assertEqual(getattr(obj, k), v)

        return obj

    @property
    def ustore(self):
        """Get the user store."""
        return self.obj_factory.ustore

    @property
    def sstore(self):
        """Get the shard store."""
        return self.obj_factory.sstore

    def make_user(self, id, username):
        """Deprecated! Tests should use self.obj_factory.make_user() directly
        instead."""
        return self.obj_factory.make_user(id, username)


class ORMObjectFactory(DAOObjectFactory):
    """A factory used to build model fixtures."""

    def __init__(self, sstore_name=None):
        self.sstore_name = sstore_name or u'shard0'
        self.users = {}

    def make_user(self, user_id=None, username=None, visible_name=None,
                  max_storage_bytes=2 ** 20, shard_id=default_shard_id):
        try:
            return self.users[user_id]
        except KeyError:
            pass

        user = super(ORMObjectFactory, self).make_user(
            user_id, username, visible_name, max_storage_bytes, shard_id)
        suser = self.ustore.get(model.StorageUser, user.id)
        self.users[user_id] = suser
        return suser

    def make_content(self, hash=None, crc32=None, size=None,
                     deflated_size=None, storage_key=None, magic_hash=None):
        """Create content for a file node."""
        content = model.ContentBlob()
        content.hash = hash or get_fake_hash()
        content.magic_hash = magic_hash or get_fake_hash()
        content.crc32 = crc32 or self.get_unique_integer()
        content.size = size or self.get_unique_integer()
        content.deflated_size = deflated_size or self.get_unique_integer()
        content.status = model.STATUS_LIVE
        content.storage_key = storage_key or uuid.uuid4()
        self.sstore.add(content)
        return content

    def make_file(self, user=None, parent=None, name=None,
                  mimetype=u'text/plain', public=False):
        """Create a file node."""
        if user is None:
            user = self.make_user()
        if name is None:
            name = self.get_unique_unicode()
        if parent is None:
            parent = model.UserVolume.get_root(self.sstore, user.id).root_node
        f = model.StorageObject(
            user.id, name, 'File', provided_mimetype=mimetype, parent=parent)
        f.content = self.make_content()
        if public:
            publicfile = self.ustore.add(model.PublicNode(f.id, f.owner_id))
            self.ustore.flush()
            f.publicfile_id = publicfile.id
        self.sstore.add(f)
        return f

    def make_transaction_log(self, tx_id=None, timestamp=None, owner_id=1,
                             op_type=TransactionLog.OP_DELETE):
        """Create a transaction log."""
        txlog = TransactionLog(
            uuid.uuid4(), owner_id, uuid.uuid4(), op_type, u"",
            u"text/plain", generation=1, old_path=u"",
            extra_data=u"")
        if timestamp:
            txlog.timestamp = timestamp
        if tx_id:
            txlog.id = tx_id
        self.sstore.add(txlog)
        self.sstore.flush()
        return txlog

    def make_directory(self, user=None, parent=None, name=None, public=False):
        """Create a folder node."""
        if user is None:
            user = self.make_user()
        if name is None:
            name = self.get_unique_unicode()
        if parent is None:
            parent = model.UserVolume.get_root(self.sstore, user.id).root_node
        subdir = parent.make_subdirectory(name)
        if public:
            publicfile = self.ustore.add(
                model.PublicNode(subdir.id, subdir.owner_id))
            self.ustore.flush()
            subdir.publicfile_id = publicfile.id
        return subdir

    def make_udf(self, user=None, path=None, status=model.STATUS_LIVE):
        """Create a UDF node."""
        if user is None:
            user = self.make_user()
        if path is None:
            path = '~/' + self.get_unique_unicode()
        udf = model.UserVolume.create(self.sstore, user.id, path)
        udf.status = status
        return udf

    def make_share(self, node=None, name=None, recipient=None,
                   access_level='View', accepted=True):
        """Create a share node."""
        if recipient is None:
            recipient = self.make_user()
        if node is None:
            node = self.make_directory()
        if name is None:
            name = self.get_unique_unicode()
        share = model.Share(
            node.owner_id, node.id, recipient.id, name, access_level)
        share.accepted = accepted
        self.ustore.add(share)
        return share

    @property
    def ustore(self):
        """ gets the store, dont cache, threading issues may arise
        """
        return get_user_store()

    @property
    def sstore(self):
        """ gets the store, dont cache, threading issues may arise
        """
        return get_shard_store(self.sstore_name)
