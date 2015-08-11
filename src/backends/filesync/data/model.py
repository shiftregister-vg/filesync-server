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

"""The Storage database model."""

import datetime
import os
import re
import uuid

from types import NoneType

from storm.locals import (Int, DateTime, Unicode, RawStr, Reference, SQL,
                          Storm, Store, Bool, ReferenceSet)
from storm.expr import Or, Sum, Desc
from storm.store import AutoReload, EmptyResultSet

import posixpath as pypath

from backends.db.store import implicit_flushes_blocked_on
from backends.filesync.data import EMPTY_CONTENT_HASH
from backends.filesync.data.errors import (
    DirectoriesHaveNoContent,
    InvalidFilename,
    InvalidVolumePath,
    NoPermission,
    NotADirectory,
    NotEmpty,
)
from backends.filesync.data.utils import encode_base62
from backends.tools.properties import StormEnum, StormUUID


# lifecycle constants
STATUS_LIVE = 'Live'
STATUS_DEAD = 'Dead'

__all__ = ["StorageUser", "ContentBlob", "StorageObject", "UploadJob"]


#this is used by the like statements in storm
# XXX("jdobien", "until storm has this, we need it this for escaping")
like_escape = {ord(u"!"): u"!!", ord(u"_"): u"!_", ord(u"%"): u"!%"}
sql_escape = {ord(u"'"): u"''", ord(u'"'): u'""'}

ROOT_NAME = u''
ROOT_PATH = u'/'
ROOT_PARENTID = None
ROOT_VOLUME = None
ROOT_USERVOLUME_PATH = u"~/Ubuntu One"

# info for the name validation
ILLEGAL_FILENAMES = [u".", u".."]
ILLEGAL_FILENAME_CHARS_RE = re.compile(r'[\000/]')


def validate_name(obj, attr, value):
    """Validate the object Name."""
    if value:
        if type(value) != unicode:
            raise InvalidFilename("Filename is not unicode")
        if value in ILLEGAL_FILENAMES:
            raise InvalidFilename(u"%s is a reserved filename" % (value,))
        if ILLEGAL_FILENAME_CHARS_RE.search(value) is not None:
            raise InvalidFilename(u"%s contains illegal characters" % (value,))
    return value


def lifecycle_status(**kwargs):
    """lifecycle status field creator"""
    return StormEnum(STATUS_LIVE, STATUS_DEAD, **kwargs)


def object_kind(**kwargs):
    """object kind field creator"""
    return StormEnum('File', 'Directory', 'Symlink', **kwargs)


def access_level(**kwargs):
    """access level of the share"""
    return StormEnum('View', 'Modify', **kwargs)


def delete_user_main_data(store, id):
    """Delete all the main data for a user."""
    store.find(PublicNode, PublicNode.owner_id == id).remove()
    store.find(Share, Or(Share.shared_by == id,
                         Share.shared_to == id)).remove()
    store.find(StorageUser, StorageUser.id == id).remove()


def delete_user_shard_data(store, id):
    """Delete all the shard data for a user."""
    store.find(MoveFromShare, MoveFromShare.owner_id == id).remove()
    store.find(Download, Download.owner_id == id).remove()
    # Remove all the root nodes for the user.
    # With the cascade, this will also delete children, UserVolumes, uploadjobs
    store.find(StorageObject, StorageObject.owner_id == id).remove()
    store.find(StorageUserInfo, StorageUserInfo.id == id).remove()


def undelete_volume(store, owner_id, volume_id, restore_parent, limit=100):
    """Undelete all the files the user ever deleted on this volume.

    In this case, everything will be restored to a directory structure under
    restore_parent.
    """
    if volume_id is None:
        root = StorageObject.get_root(store, owner_id)
    else:
        vol = store.get(UserVolume, volume_id)
        if vol:
            root = vol.root_node
        else:
            return
    path = u""
    # find deleted order by path to make sure directories are created in the
    # correct order
    deleted = store.find(
        StorageObject,
        StorageObject.volume_id == root.volume_id,
        StorageObject.owner_id == root.owner_id,
        StorageObject.status == STATUS_DEAD,
        StorageObject.kind == 'File').order_by(
            Desc(StorageObject.when_last_modified))
    if not deleted.is_empty():
        parent = restore_parent.build_tree_from_path(path)
        for d in deleted[:limit]:
            leaf = parent.build_tree_from_path(d.path)
            d.undelete(leaf)
        root.when_last_modified = datetime.datetime.utcnow()
        return parent


class StorageUserInfo(object):
    """The information for each Storage User which is stored on the shard"""

    __storm_table__ = "StorageUserInfo"

    # A unique identifier for this record.
    id = Int(primary=True)

    # The StorageUser's storage quota.
    max_storage_bytes = Int(allow_none=False)
    # used storage bytes
    used_storage_bytes = Int(allow_none=False)

    def __init__(self, id, max_storage_bytes=0):
        """Initialize a StorageUserInfo."""
        self.id = id
        self.max_storage_bytes = max_storage_bytes
        self.used_storage_bytes = 0

    @property
    def free_bytes(self):
        """Return the free bytes."""
        return max(0, self.max_storage_bytes - self.used_storage_bytes)

    @staticmethod
    def lock_info(store, id):
        """Lock a specific userinfo."""
        store.execute("SELECT * from storageuserinfo where id = %s "
                      "FOR UPDATE NOWAIT" % id)

    def lock_for_update(self):
        """Lock the user record for update"""
        store = Store.of(self)
        StorageUserInfo.lock_info(store, self.id)

    @staticmethod
    def get_storage_stats(store, id):
        """Return a storage_stats for the user matching the id"""
        user = store.get(StorageUserInfo, id)
        if user:
            return user.max_storage_bytes, user.used_storage_bytes
        return None, None

    def update_used_bytes(self, difference, enforce_quota=True):
        """Adjusts used bytes based on the difference.

        @param difference: change in size

        A negative difference is passed when files are deleted or reduced
        in size. A possitive difference passed when a file is added or
        increased in size.
        """
        if difference == 0:
            return

        self.used_storage_bytes += difference
        if self.used_storage_bytes < 0:
            self.used_storage_bytes = 0

    def recalculate_used_bytes(self):
        """Recaclulate the used bytes for this user."""
        #this will look at all Live UserVolumes to get the sum.
        self.lock_for_update()
        store = Store.of(self)
        old_root_size = store.find(
            Sum(ContentBlob.size),
            StorageObject.owner_id == self.id,
            StorageObject.kind == 'File',
            StorageObject.status == STATUS_LIVE,
            StorageObject.volume_id == None, # flake8: NOQA
            StorageObject._content_hash == ContentBlob.hash).one()
        vol_size = store.find(
            Sum(ContentBlob.size),
            UserVolume.owner_id == self.id,
            UserVolume.status == STATUS_LIVE,
            UserVolume.id == StorageObject.volume_id,
            StorageObject.kind == 'File',
            StorageObject.status == STATUS_LIVE,
            StorageObject._content_hash == ContentBlob.hash).one()
        self.used_storage_bytes = (old_root_size or 0) + (vol_size or 0)
        return self.used_storage_bytes


class StorageUser(object):
    """StorageUsers that the storage system is aware of."""

    __storm_table__ = "StorageUser"

    # A unique identifier for this record.
    id = Int(primary=True)

    # The following is information that comes from the Auth Service. It may be
    # duplicated from that system (it's its responsibility to update it), but
    # it's necessary to have it also here to be able to use this information
    # without changing of database.
    visible_name = Unicode()
    username = Unicode()
    subscription_status = lifecycle_status(allow_none=False)

    # Whether the record represents a live, valid StorageUser.
    status = lifecycle_status(allow_none=False)

    # an internal identifier for the shard the user's data is stored on
    shard_id = Unicode()

    #the UserVolume for this user's root volume
    root_volume_id = StormUUID()

    # locked flag
    locked = Bool(default=False)

    def __init__(self, id, username, visible_name, shard_id,
                 status=STATUS_LIVE, subscription_status=STATUS_LIVE):
        self.id = id
        self.username = username
        self.visible_name = visible_name
        self.status = status
        self.subscription_status = subscription_status
        self.shard_id = shard_id

    @classmethod
    def new(cls, store, user_id, username, visible_name, shard_id):
        """Create a new StorageUser, add it to the given store and return it.

        This is the preferred way of creating new StorageUsers.
        """
        user = store.add(cls(user_id, username, visible_name, shard_id))
        from backends.txlog.model import TransactionLog
        TransactionLog.record_user_created(user)
        return user

    def lock_for_update(self):
        """Lock the user record for update"""
        store = Store.of(self)
        store.execute("SELECT * from storageuser where id = %s "
                      "FOR UPDATE NOWAIT" % self.id)


class PublicNode(object):
    """A mapping of a public file URL to the underlying node."""
    # Used to be just for files, which is why the table is called PublicFile
    __storm_table__ = "PublicFile"
    id = Int(primary=True)
    owner_id = Int()
    owner = Reference(owner_id, StorageUser.id)
    node_id = StormUUID()
    public_uuid = StormUUID()

    def __init__(self, node_id, owner_id, public_uuid=None):
        """Create a new PublicNode."""
        self.node_id = node_id
        self.owner_id = owner_id
        self.public_uuid = public_uuid


class ContentBlob(object):
    """ Associates a hash with a specific storage key in S3
    or in the case of symlinks and sufficiently small files,
    the file contents directly.
    In S3, file contents are deflated, but this is not reflected
    anywhere except in ContentBlob.deflated_size.
    """
    __storm_table__ = "ContentBlob"

    # The hash for the raw file content represented by this record.
    hash = RawStr(primary=True, allow_none=False)

    # The crc32 for the raw file content represented by this record.
    crc32 = Int(allow_none=False)

    # The size of the raw file content represented by this record, in bytes.
    size = Int(allow_none=False)

    # The S3 content key which references the deflated file contents,
    # or NULL if the content is stored locally in the table or if it
    # has been garbage collected.  This column is set after inserting
    # an object into S3, and NULLed just before deleting an object from S3.
    storage_key = StormUUID()

    # The deflated size of the file content as stored in S3,
    # or NULL if inapplicable.
    deflated_size = Int()

    # The file content as a raw byte string, or else NULL.
    # Used for symlinks and potentially sufficiently small files.
    content = RawStr()

    # Whether this content entry is live, or else a candidate for
    # garbage collection.
    status = lifecycle_status(allow_none=False)

    # The magic hash of the content
    magic_hash = RawStr()

    # timestamp at which the blob was first created
    when_created = DateTime(allow_none=True)

    def __init__(self):
        self.when_created = datetime.datetime.utcnow()

    @classmethod
    def make_empty(cls, store):
        """Create the empty content blob."""
        o = cls()
        o.hash = EMPTY_CONTENT_HASH
        o.crc32 = 0
        o.size = 0
        o.when_created = datetime.datetime.utcnow()
        store.add(o)
        return o


def get_path_startswith(node):
    """Get a path comparison for children of the node."""
    return pypath.join('/', node.path, node.name, '')


class StorageObject(Storm):
    """ A file, directory, or symbolic link.

    Files or symbolic links refer to ContentBlob for their contents.
    """
    __storm_table__ = "Object"

    # A unique identifier for the file, corresponding roughly in
    # function to an inode number.
    id = StormUUID(primary=True, allow_none=False)

    # The object's owner, for access control and accounting purposes.
    owner_id = Int(allow_none=False)

    # The directory containing the object, or NULL if the object is
    # a volume root (only directories should be volume roots).
    parent_id = StormUUID()
    parent = Reference(parent_id, "StorageObject.id")

    # The UserVolume containing this object
    volume_id = StormUUID(allow_none=True)
    volume = Reference(volume_id, "UserVolume.id")

    # The object's name within its containing directory.
    # Ignored for volume roots.
    name = Unicode(allow_none=False, validator=validate_name)

    # Used by the content_hash property for the hash of this object
    _content_hash = RawStr(name="content_hash")
    _content = Reference(_content_hash, ContentBlob.hash)

    # The kind of object:  directory, file, or symbolic link.
    kind = object_kind()

    # Timestamp at which the object was first created.
    when_created = DateTime(allow_none=False, default=AutoReload)

    # Timestamp at which the objects content was last modified.
    when_last_modified = DateTime(allow_none=False, default=AutoReload)

    # Whether this object is alive or dead.
    status = lifecycle_status(allow_none=False)

    # The path of the object from its root
    path = Unicode(allow_none=False)

    # the mimetype of the file
    mimetype = Unicode()

    # If the file is public, this will be its ID in the PublicNode
    # table.  If it is private, then it will be None.
    _publicfile_id = Int(name="publicfile_id")
    public_uuid = StormUUID()

    # The current generation of this object
    generation = Int(allow_none=True)

    # The first generation of this object
    generation_created = Int(allow_none=True)

    def __init__(self, user_id, name, kind,
                 provided_mimetype=None, parent=None, status=STATUS_LIVE):
        """Create a Node."""
        super(StorageObject, self).__init__()
        self.id = uuid.uuid4()
        self.owner_id = user_id
        self.name = name
        self.kind = kind
        if parent is not None:
            self.path = pypath.join(parent.path, parent.name)
            self.parent = parent
            self.volume = parent.volume
            self.generation = self.volume.increment_generation()
            self.generation_created = self.generation
        else:
            # the only node with parent == None, and no name is Root; other
            # special nodes (as the root) have no parent, and no path,
            # but different names
            if name == ROOT_NAME:
                self.path = ROOT_PATH
            else:
                self.path = u""

            # both have volume and parent ids as None
            self.parent_id = ROOT_PARENTID
            self.volume_id = ROOT_VOLUME
            self.generation = 0
            self.generation_created = 0
        self.status = status
        self.mimetype = provided_mimetype

    def __repr__(self):
        """Representation with info."""
        return "<StorageObject node_id: %s  vol: %s>" % (
            self.id, self.volume_id)

    def lock_for_update(self):
        """Lock the storageobject record for update"""
        store = Store.of(self)
        store.execute("SELECT * from object where id = '%s'::UUID "
                      "FOR UPDATE NOWAIT" % self.id)

    def lock_tree_for_update(self):
        """Lock the storageobject record for update"""
        store = Store.of(self)
        path = get_path_startswith(self) + '%'
        path = path.translate(sql_escape)
        if self.volume_id is None:
            volume_check = "volume_id is Null"
        else:
            volume_check = "volume_id = '%s'::UUID" % self.volume_id
        store.execute("SELECT * from object where "
                      "owner_id=%s and status='%s' AND %s and "
                      "(parent_id='%s'::UUID or path like '%s') "
                      "FOR UPDATE NOWAIT" % (
                      self.owner_id, STATUS_LIVE, volume_check, self.id, path))

    @property
    def full_path(self):
        """The full path of this node"""
        return pypath.join(self.path, self.name)

    @property
    def is_public(self):
        """True if the file is public."""
        return self.publicfile_id is not None

    @property
    def base62_publicfile_id(self):
        """The base-62 version of the public file ID, or None."""
        if self.publicfile_id is None:
            return None
        return encode_base62(self.publicfile_id)

    @property
    def children(self):
        """The LIVE children of this node."""
        store = Store.of(self)
        if store:
            return store.find(StorageObject,
                              StorageObject.parent_id == self.id,
                              StorageObject.status == STATUS_LIVE)
        return EmptyResultSet()

    def get_descendants(self, live_only=True, kind=None):
        """Return all the descendants of this node."""
        store = Store.of(self)
        if store:
            path_sw = get_path_startswith(self)
            # Notice that in order to get indirect children we need to filter
            # by path. Also, we need to add a trailing slash to the path so
            # that our startswith query doesn't match, say, /a/b/code when
            # we're searching for the descendants of /a/b/c. Finally, because
            # of the trailing slash in our startswith query, it won't find
            # direct descendants (e.g. /a/b/c/d will have /a/b/c as path and
            # that won't match /a/b/c/, which is what we're searching for), so
            # we need the extra clause (parent_id == self.id) to get them *and*
            # we need to filter out self.
            conditions = [StorageObject.id != self.id,
                          StorageObject.volume_id == self.volume_id,
                          StorageObject.owner_id == self.owner_id,
                          Or(StorageObject.parent_id == self.id,
                             StorageObject.path.startswith(path_sw))]
            if live_only:
                conditions.append(StorageObject.status == STATUS_LIVE)
            if kind in ('File', 'Directory'):
                conditions.append(StorageObject.kind == kind)
            elif kind is not None:
                # isn't one of File, Directory or None
                raise ValueError('Invalid kind value, must be File, '
                                 'Directory or None')
            return store.find(StorageObject, *conditions)
        return EmptyResultSet()

    @property
    def descendants(self):
        """The LIVE descendants of this node."""
        return self.get_descendants()

    def get_publicfile_id(self):
        """Get the publicfile_id."""
        return self._publicfile_id

    def set_publicfile_id(self, value):
        """Setter for publicfile_id."""
        self.update_generation()
        self._publicfile_id = value
        from backends.txlog.model import TransactionLog
        TransactionLog.record_public_access_change(self)

    publicfile_id = property(get_publicfile_id, set_publicfile_id)

    @property
    def content_hash(self):
        """Get the associated hash value."""
        if self.kind == "Directory":
            raise DirectoriesHaveNoContent("Directory has no content.")
        return self._content_hash

    def get_content(self):
        """Return this object ContentBlob"""
        if self.kind == "Directory":
            raise DirectoriesHaveNoContent("Directory has no content.")
        return self._content

    def set_content(self, new_content):
        """Set this object ContentBlob and updates
        self.owner.used_storage_bytes
        """
        if self.kind == "Directory":
            raise DirectoriesHaveNoContent("Directory has no content.")
        curr_size = getattr(self.content, 'size', 0)
        self._update_used_bytes(new_content.size - curr_size)
        self._content = new_content
        self.when_last_modified = datetime.datetime.utcnow()
        self.update_generation()

        from backends.txlog.model import TransactionLog
        TransactionLog.record_put_content(self)

    content = property(get_content, set_content)

    def update_generation(self):
        """Update the generation of this object to match it's volume."""
        self.generation = self.volume.increment_generation()

    def get_child_by_name(self, name):
        """Get the child named name."""
        store = Store.of(self)
        if store:
            return store.find(StorageObject,
                              StorageObject.parent_id == self.id,
                              StorageObject.name == name,
                              StorageObject.status == STATUS_LIVE).one()

    def move(self, new_parent_id, new_name):
        """Move the node to another parent and/or to a different name."""
        if not isinstance(new_parent_id, (uuid.UUID, NoneType)):
            # It feels weird to accept None for new_parent_id, but there's an
            # explicit check for that (new_parent_id == ROOT_PARENTID) below,
            # so we can leave None through as well.
            raise TypeError(
                "new_parent_id must be a UUID or None, got: %s"
                % type(new_parent_id))

        if not new_name:
            raise InvalidFilename("Invalid name.")
        if self.parent_id == new_parent_id and self.name == new_name:
            # no changes, then do nothing
            return
        if new_parent_id == self.id:
            raise NoPermission("Can't move a node to itself.")
        if new_parent_id == ROOT_PARENTID:
            raise NoPermission("Can't move a node to a root level.")

        store = Store.of(self)
        new_parent = store.get(StorageObject, new_parent_id)
        if new_parent.kind == 'File':
            raise NotADirectory("New parent (%r) isn't a directory"
                                % new_parent_id)
        if new_parent.volume_id != self.volume_id:
            raise NoPermission("Can't move a node between volumes.")

        old_parent = self.parent
        old_name = self.name
        new_parent_path = new_parent.path
        new_parent_name = new_parent.name
        if self.kind == 'Directory':
            if self.parent_id != new_parent_id:
                # it was actually moved to other place, not just renamed
                full_path_with_sep = self.full_path + '/'
                if new_parent.full_path.startswith(full_path_with_sep):
                    raise NoPermission("Can't move a node to a child.")

            # need to update all the paths that are under the current directory
            # this will be the new path to all children
            new_path = pypath.join(new_parent_path, new_parent_name, new_name)
            new_path = new_path.replace('\\', '\\\\')
            # this will be the size of the path parts to replace
            replace_size = len(self.full_path) + 1
            #update the path of all descendants
            sql = "? || substring(path from %d)" % replace_size
            self.get_descendants(live_only=True).set(path=SQL(sql, [new_path]))
        self.update_generation()
        #update this node
        if self.parent_id != new_parent_id:
            self.parent_id = new_parent_id
            self.path = pypath.join(new_parent_path, new_parent_name)
        self.name = new_name
        now = datetime.datetime.utcnow()
        if old_parent.id != new_parent_id:
            old_parent.when_last_modified = now
        new_parent.when_last_modified = now

        from backends.txlog.model import TransactionLog
        TransactionLog.record_move(self, old_name, old_parent)

    def get_unique_childname(self, name):
        """Find a unique child name in this directory."""
        store = Store.of(self)
        if store:
            if self.kind != 'Directory':
                raise NotADirectory("%s is not a directory." % self.full_path)
            basename, extension = os.path.splitext(name)
            idx = 0
            while self.get_child_by_name(name):
                idx += 1
                name = "%s~%s%s" % (basename, idx, extension)
                if idx > 5:
                    name = "%s~%s%s" % (basename, uuid.uuid4(), extension)
                    break
        return name

    def build_tree_from_path(self, path):
        """Build subdirectories from a path.

        This will return the last directory created from the path.
        """
        store = Store.of(self)
        if self.kind != 'Directory':
            raise NotADirectory("%s is not a directory." % self.full_path)

        def getleaf(start, path_parts):
            """Get the leaf directory"""
            if not path_parts:
                return start

            rest = path_parts[1:]
            head = path_parts[0]
            d = store.find(StorageObject,
                           StorageObject.parent_id == start.id,
                           StorageObject.status == 'Live',
                           StorageObject.name == head).one()
            if d is None:
                d = start.make_subdirectory(head)
            else:
                if d.kind == 'File':
                    #if a file with the same name exists, find a
                    #unique name for the directory
                    name = start.get_unique_childname(d.name)
                    d = start.make_subdirectory(name)
            return getleaf(d, rest)
        return getleaf(self, [x for x in path.split("/") if x])

    def undelete(self, new_parent=None):
        """Undelete file or directory.

        If a new_parent is passed in, the file's parent node and path
        will be updated as well.
        """
        # no need to do anything with a live node
        if self.status == STATUS_LIVE:
            return
        store = Store.of(self)
        if new_parent and new_parent.kind != 'Directory':
            raise NotADirectory("Must reparent to a Directory on Undelete.")
        parent = new_parent or self.parent
        self.name = parent.get_unique_childname(self.name)
        if self.kind == 'File':
            self._update_used_bytes(getattr(self.content, 'size', 0))

        self.parent = parent
        self.volume = parent.volume
        self.path = parent.full_path
        self.status = STATUS_LIVE
        self.update_generation()
        # update the parent
        if self.parent.status == STATUS_DEAD:
            # if the parent directory dead, we need to check to see if there
            # is a live directory with the same path to put it in.
            path, name = os.path.split(self.path)
            store = Store.of(self)
            new_parent = store.find(StorageObject,
                                    StorageObject.owner_id == self.owner_id,
                                    StorageObject.volume_id == self.volume_id,
                                    StorageObject.path == path,
                                    StorageObject.name == name,
                                    StorageObject.status == STATUS_LIVE).one()
            if new_parent:
                # if we have a suitable parent, update the parent
                self.parent = new_parent
                self.parent.when_last_modified = datetime.datetime.utcnow()
            else:
                # if we can't find a suitable parent, we need to restore the
                # old one.
                self.parent.undelete()
        else:
            # if the parent was live, we just need to update the timestamp
            self.parent.when_last_modified = datetime.datetime.utcnow()

    def unlink(self):
        """Mark the node as Dead."""
        # we don't modify the 'path' when unlinking the file, to preserve
        # its location when unlinked
        if self.kind == "Directory":
            if self._has_children():
                raise NotEmpty("Can't unlink a non empty directory.")

        if self.parent == ROOT_PARENTID:
            raise NoPermission("Can't unlink special files.")

        # Block implicit flushes so that the status/timestamp update is
        # performed together with the generation update, as a single SQL
        # statement. Without this we'd issue two updates because
        # update_generation() updates self.volume, which triggers an (in this
        # case unnecessary) implicit flush.
        with implicit_flushes_blocked_on(Store.of(self)):
            self.status = STATUS_DEAD
            self.when_last_modified = datetime.datetime.utcnow()
            self.update_generation()

        from backends.txlog.model import TransactionLog
        TransactionLog.record_unlink(self)

        if self.kind == "File":
            self._update_used_bytes(0 - getattr(self.content, 'size', 0))
        if self.parent_id != ROOT_PARENTID:
            self.parent.when_last_modified = datetime.datetime.utcnow()

    def unlink_tree(self):
        """Unlink and entire directory and it's subdirectories"""
        if self.kind != 'Directory':
            raise NotADirectory("%s is not a directory." % self.id)

        if self.parent is None:
            raise NoPermission("Can't unlink special files.")
        if self.status == STATUS_DEAD:
            return

        # First update the generation so that we can use it in the new TXLog
        # entries.
        self.update_generation()

        if self._has_children():
            size_to_remove = self.tree_size
            self._update_used_bytes(0 - size_to_remove)

            from backends.txlog.model import TransactionLog
            TransactionLog.record_unlink_tree(self)

            self.descendants.set(status=STATUS_DEAD,
                                 when_last_modified=datetime.datetime.utcnow())

        self.status = STATUS_DEAD
        self.when_last_modified = datetime.datetime.utcnow()

        if self.parent_id != ROOT_PARENTID:
            self.parent.when_last_modified = datetime.datetime.utcnow()

    @property
    def tree_size(self):
        """Get the size of the entire tree"""
        store = Store.of(self)
        if store is None:
            return 0
        if self.kind != 'Directory':
            raise NotADirectory("%s is not a directory." % self.id)
        if self.status == STATUS_DEAD:
            return 0
        path_sw = get_path_startswith(self)

        size = store.find(Sum(ContentBlob.size),
                          StorageObject.owner_id == self.owner_id,
                          StorageObject.kind == 'File',
                          StorageObject.status == STATUS_LIVE,
                          StorageObject.volume_id == self.volume_id,
                          StorageObject._content_hash == ContentBlob.hash,
                          Or(StorageObject.parent_id == self.id,
                             StorageObject.path.startswith(path_sw))).one()
        return size or 0

    def _has_children(self):
        """Return True if this node has children."""
        store = Store.of(self)
        if store is None:
            return False
        any_child = store.find(StorageObject.owner_id == self.owner_id,
                               StorageObject.status == STATUS_LIVE,
                               StorageObject.volume_id == self.volume_id,
                               StorageObject.parent_id == self.id).any()
        return bool(any_child)

    def make_subdirectory(self, name):
        """Create a subdirectory named name."""
        if not name:
            raise InvalidFilename("Invalid directory Name")
        store = Store.of(self)
        # parent must be directory
        if self.kind != 'Directory':
            raise NotADirectory("%s is not a directory." % self.id)
        node = self.__class__(user_id=self.owner_id, name=name,
                              kind='Directory',
                              parent=self)
        store.add(node)
        self.when_last_modified = datetime.datetime.utcnow()
        return node

    def make_file(self, name):
        """Create a file named name.

        If the "no-content" capability is present, this operation does not put
        any content in the file, and that's why its content_hash remains in
        the default value (Null)
        """
        if not name:
            raise InvalidFilename("Invalid File Name")
        store = Store.of(self)
        if self.kind != 'Directory':
            raise NotADirectory("%s is not a directory." % self.id)

        node = self.__class__(user_id=self.owner_id, name=name,
                              kind='File',
                              parent=self)
        store.add(node)
        self.when_last_modified = datetime.datetime.utcnow()
        return node

    @staticmethod
    def make_root(store, user_id):
        """Create the root node for user_id."""
        vol = UserVolume.get_root(store, user_id)
        if vol is None:
            vol = UserVolume.make_root(store, user_id)
        return vol.root_node

    @staticmethod
    def get_root(store, user_id):
        """Get the root node for this user."""
        vol = UserVolume.get_root(store, user_id)
        return vol.root_node

    def _update_used_bytes(self, difference):
        """Helper function to update a used storage bytes"""
        store = Store.of(self)
        info = store.get(StorageUserInfo, self.owner_id)
        info.update_used_bytes(difference)

    @property
    def parent_paths(self):
        """Return a list of paths for parents of this node.

        For example, if the nodes path is /a/b/c/d, the parent paths
        would be ['/', '/a', '/a/b', '/a/b/c', '/a/b/c/d']
        """
        if self.parent_id is None:
            return []
        if self.path == '/':
            return [u'/']
        pp = self.path.split('/')
        pp[0] = u'/'
        return [os.path.join(*pp[0:i]) for i in range(1, len(pp) + 1)]

    def get_parent_ids(self):
        """Get the parent ids of this node id."""
        if not self.parent_id:
            return []
        sql = """
        with recursive parents(id,parent_id, path, name, status) AS
        (
            SELECT id, parent_id, path, name, status
            from object where id = '%s'
            UNION ALL
            SELECT p.id, p.parent_id, p.path, p.name, p.status
            from parents c, object p
            where c.parent_id = p.id
        )
        select id from parents where status='Live';
        """ % str(self.parent_id)
        return [uuid.UUID(r[0]) for r in Store.of(self).execute(sql)]


class MoveFromShare(StorageObject):
    """A record of a node which was moved outside of it's volume.

    This is to support generation deltas so nodes will show up
    in a delta for a share even though a node has been moved outside of it.

    See StorageObject for details about this model.
    """

    __storm_table__ = "MoveFromShare"
    __storm_primary__ = "share_id", "id"

    share_id = StormUUID()

    def __init__(self):
        """Override the base __init__."""

    @classmethod
    def from_move(cls, node, share_id):
        """Create an instance from a Storage Node.

        This is really the only way one of these should be created.
        """
        store = Store.of(node)
        mnode = cls()
        mnode.share_id = share_id
        mnode.id = node.id
        mnode.owner_id = node.owner_id
        mnode.name = node.name
        mnode.parent_id = node.parent_id
        mnode.volume_id = node.volume_id
        mnode._content_hash = node._content_hash
        mnode.mimetype = node.mimetype
        mnode.kind = node.kind
        mnode.when_created = node.when_created
        mnode.when_last_modified = node.when_last_modified
        mnode.status = 'Dead'
        mnode.path = node.path
        mnode._publicfile_id = node.publicfile_id
        mnode.public_uuid = node.public_uuid
        mnode.generation = node.generation
        mnode.generation_created = node.generation_created
        store.add(mnode)
        return mnode


class ShareVolumeDelta(StorageObject):
    """A special case used only for getting Deltas for shares using a view."""
    __storm_table__ = "share_delta_view"
    __storm_primary__ = "share_id", "id"

    share_id = StormUUID()


class Share(Storm):
    """ A Share.

    Shares are subtrees that a user shares to another.
    """
    __storm_table__ = "Share"
    __storm_primary__ = "id"

    id = StormUUID(primary=True, allow_none=False)

    # user who shares
    shared_by = Int(allow_none=False)
    sharedbyuser = Reference(shared_by, StorageUser.id)

    # node that is root of the shared subtree
    subtree = StormUUID(allow_none=False)

    # user to whom the subtree is shared
    shared_to = Int(allow_none=True)
    sharedtouser = Reference(shared_to, StorageUser.id)
    email = Unicode(allow_none=True)

    # name of the sharing. The pair (shared_to, name) is unique.
    name = Unicode(allow_none=False, validator=validate_name)

    # if the share was accepted or not
    accepted = Bool(allow_none=False, default=False)

    # access level of the share
    access = access_level(allow_none=False)

    # timestamp at which the share was first created.
    when_shared = DateTime(allow_none=False, default=AutoReload)

    # timestamp at which the share was last modified.
    when_last_changed = DateTime(allow_none=False, default=AutoReload)

    # whether this object is alive or dead
    status = lifecycle_status(allow_none=False, default=STATUS_LIVE)

    def __init__(self, user_id, node_id, share_to, name, access_level,
                 email=None):
        """Create a Share."""
        super(Share, self).__init__()
        self.id = uuid.uuid4()
        self.shared_by = user_id
        self.subtree = node_id
        self.shared_to = share_to
        self.name = name
        self.access = access_level
        self.email = unicode(email) if email else None

    def delete(self):
        """Marks itself as dead."""
        self.status = STATUS_DEAD
        from backends.txlog.model import TransactionLog
        TransactionLog.record_share_deleted(self)

    def accept(self):
        """Marks itself as accepted.

        Must not be called if self.status is not STATUS_LIVE or if
        self.shared_to is None.
        """
        assert self.status == STATUS_LIVE
        assert self.shared_to is not None
        self.accepted = True
        from backends.txlog.model import TransactionLog
        TransactionLog.record_share_accepted(self)

    @staticmethod
    def get_unique_name(store, user_id, name):
        """Find a unique child name in this directory."""
        basename = name
        idx = 0
        while store.find(Share, Share.status == STATUS_LIVE,
                         Share.shared_to == user_id, Share.name == name).one():
            idx += 1
            name = "%s~%s" % (basename, idx)
            if idx > 50:
                name = "%s~%s" % (basename, uuid.uuid4())
                break
        return name

    def claim_share(self, store, user_id):
        """Claim a share offer."""
        #Check and make sure this folder isn't shared to them already
        matching_share = store.find(Share,
                                    Share.shared_by == self.shared_by,
                                    Share.shared_to == user_id,
                                    Share.status == STATUS_LIVE,
                                    Share.subtree == self.subtree).one()
        #if there was already a share matching the share_offer, return it
        if matching_share:
            matching_share.accept()
            return matching_share
        # make sure the share_name isn't taken for this user
        self.name = Share.get_unique_name(store, user_id, self.name)
        self.shared_to = user_id
        self.accept()


# Add StoragaUser references to Shared folders
StorageUser.sharedto_folders = ReferenceSet(StorageUser.id, Share.shared_to)
StorageUser.sharedby_folders = ReferenceSet(StorageUser.id, Share.shared_by)


class UploadJob(object):
    """ Table used to track pending blob Uploads """
    __storm_table__ = "UploadJob"

    # A unique serial-type identifier for the blob upload
    uploadjob_id = Int(primary=True, allow_none=False)

    # The storage object this blob upload is linked to
    storage_object_id = StormUUID(allow_none=False)
    storage_object = Reference(storage_object_id, StorageObject.id)

    # A simple sanity check count for how many chunks have been
    # recorded for this blob upload
    chunk_count = Int(allow_none=False)

    # The hash the client claims that the completely uploaded file should have.
    hash_hint = RawStr(allow_none=False)
    # The crc32 the client claims that the completely uploaded
    # file should have.
    crc32_hint = Int(allow_none=False)
    # The total size which the client claims that the completely uploaded
    # file should have.
    inflated_size_hint = Int(allow_none=False)
    # The total size which the client claims that the completely uploaded
    # deflated file should have.
    deflated_size_hint = Int(allow_none=False)

    # When the upload was started.
    when_started = DateTime(allow_none=False, default=AutoReload)
    # When the upload was last active.
    when_last_active = DateTime(allow_none=False, default=AutoReload)

    # Whether this is a live upload or a done-with one
    status = lifecycle_status(allow_none=False)

    # the multipart upload id
    multipart_id = RawStr(allow_none=True)
    # the key name for this multipart upload
    multipart_key = StormUUID(allow_none=True)
    # the number of the uploaded bytes so far.
    uploaded_bytes = Int(allow_none=True)
    # the inflated size
    inflated_size = Int(allow_none=True)
    # the crc32
    crc32 = Int(allow_none=True)
    # the hash context of this resumable upload
    hash_context = RawStr(allow_none=True)
    # the magic hash context of this resumable upload
    magic_hash_context = RawStr(allow_none=True)
    # the decompressobj context of this resumable upload
    decompress_context = RawStr(allow_none=True)

    def __init__(self, storage_object_id, multipart_id=None,
                 multipart_key=None):
        """ Create a blob upload """
        self.storage_object_id = storage_object_id
        self.chunk_count = 0
        self.uploaded_bytes = 0
        self.multipart_id = multipart_id
        self.multipart_key = multipart_key
        self.hash_context = None
        self.magic_hash_context = None
        self.decompress_context = None
        self.when_last_active = datetime.datetime.utcnow()
        self.status = STATUS_LIVE

    @classmethod
    def new_uploadjob(cls, store, storage_object_id):
        """ creates a new uploadjob object """
        obj = cls(storage_object_id)
        store.add(obj)
        return obj

    @classmethod
    def new_multipart_uploadjob(cls, store, storage_object_id,
                                multipart_id, multipart_key):
        """ creates a new uploadjob object """
        obj = cls(storage_object_id, multipart_id, multipart_key)
        store.add(obj)
        return obj

    def add_part(self, size, inflated_size, crc32,
                 hash_context, magic_hash_context, decompress_context):
        """Add a part of size: 'size' and increment the chunk count."""
        self.uploaded_bytes += size
        self.inflated_size = inflated_size
        self.crc32 = crc32
        self.chunk_count += 1
        self.hash_context = hash_context
        self.magic_hash_context = magic_hash_context
        self.decompress_context = decompress_context


def validate_volume_path(obj, attr, value):
    """Validate the UserVolume path."""
    if not value.startswith("~/"):
        raise InvalidVolumePath
    return value


class UserVolume(Storm):
    """A User Volume.

    These represent's the user's roots or volumes.
    """

    __storm_table__ = "UserDefinedFolder"
    __storm_primary__ = "id"

    id = StormUUID(primary=True, allow_none=False)

    owner_id = Int(allow_none=False)

    # node that is root of the UserVolume
    root_id = StormUUID(allow_none=False)
    root_node = Reference(root_id, StorageObject.id)

    # suggested path (it's not enforced to the client)
    path = Unicode(allow_none=False, validator=validate_volume_path)

    # timestamp at which the udf was first created.
    when_created = DateTime(allow_none=False)

    # whether this object is alive or dead
    status = lifecycle_status(allow_none=False)

    # the generation of this volume
    generation = Int()

    def __init__(self, user_id, node_id, path):
        """Create an User Defined Folder."""
        super(UserVolume, self).__init__()
        self.id = uuid.uuid4()
        self.owner_id = user_id
        self.root_id = node_id
        self.path = path.strip(u'/')
        self.status = STATUS_LIVE
        self.generation = 0
        self.when_created = datetime.datetime.utcnow()

    def delete(self):
        """Delete the UDF."""
        size_to_remove = self.volume_size()
        self.root_node._update_used_bytes(0 - size_to_remove)
        self.status = STATUS_DEAD
        self.increment_generation()
        from backends.txlog.model import TransactionLog
        TransactionLog.record_udf_deleted(self)

    def volume_size(self):
        """Get the size of the entire volume"""
        store = Store.of(self)
        if self.status == STATUS_DEAD:
            return 0
        size = store.find(
            Sum(ContentBlob.size),
            StorageObject.kind == 'File',
            StorageObject.owner_id == self.owner_id,
            StorageObject.status == STATUS_LIVE,
            StorageObject.volume_id == self.id,
            StorageObject._content_hash == ContentBlob.hash).one()
        return size or 0

    @property
    def is_root(self):
        """Return true if this is the root volume."""
        return self.path == ROOT_USERVOLUME_PATH

    @staticmethod
    def lock_volume(store, id):
        """Lock a specific userinfo."""
        store.execute("SELECT * from userdefinedfolder where id = '%s'::UUID "
                      "FOR UPDATE NOWAIT" % id)

    def lock_for_update(self):
        """Lock the user record for update"""
        store = Store.of(self)
        UserVolume.lock_volume(store, self.id)

    @classmethod
    def _create(cls, store, user_id, path):
        """Create a new UserVolume (with its root node) on the given path."""
        node = StorageObject(user_id, name=ROOT_NAME, kind='Directory')
        # Create the udf and fix the volume_id in the node
        vol = cls(user_id, node.id, path)
        node.volume_id = vol.id
        # Add both records to DB (node first, because of foreign key)
        store.add(node)
        store.add(vol)
        from backends.txlog.model import TransactionLog
        TransactionLog.record_udf_created(vol)
        return vol

    @classmethod
    def create(cls, store, user_id, path):
        """Create the UserVolume and its root node."""
        if path == ROOT_USERVOLUME_PATH:
            raise NoPermission("Invalid Path Volume.")
        return cls._create(store, user_id, path)

    @staticmethod
    def make_root(store, user_id):
        """Create the root UserVolume and its root node, if they don't already
        exist.
        """
        # create the node
        vol = UserVolume.get_root(store, user_id)
        if vol is None:
            vol = UserVolume._create(store, user_id, ROOT_USERVOLUME_PATH)
        return vol

    @staticmethod
    def get_root(store, user_id):
        """Get the root UserVolume."""
        return store.find(UserVolume,
                          UserVolume.owner_id == user_id,
                          UserVolume.path == ROOT_USERVOLUME_PATH,
                          UserVolume.status == STATUS_LIVE).one()

    def increment_generation(self):
        """Update the generation number."""
        # max is to avoid issues when it is None
        self.generation = max(0, self.generation) + 1
        return self.generation

    @staticmethod
    def get_new_generation(store, id):
        """Get a new generation number."""
        vol = store.get(UserVolume, id)
        return vol.increment_generation()


DOWNLOAD_STATUS_QUEUED = 'Queued'
DOWNLOAD_STATUS_DOWNLOADING = 'Downloading'
DOWNLOAD_STATUS_COMPLETE = 'Complete'
DOWNLOAD_STATUS_ERROR = 'Error'


def download_status(**kwargs):
    """Current status of a download"""
    return StormEnum(DOWNLOAD_STATUS_QUEUED,
                     DOWNLOAD_STATUS_DOWNLOADING,
                     DOWNLOAD_STATUS_COMPLETE,
                     DOWNLOAD_STATUS_ERROR,
                     **kwargs)


class Download(object):
    """A download to be performed by the download daemon."""

    __storm_table__ = "Download"
    id = StormUUID(primary=True)
    owner_id = Int(allow_none=False)
    volume_id = StormUUID()
    file_path = Unicode(allow_none=False)
    download_url = Unicode(allow_none=False)
    download_key = Unicode(allow_none=True)
    _status = download_status(name="status", allow_none=False)
    status_change_date = DateTime(allow_none=False, default=AutoReload)
    node_id = StormUUID()
    error_message = Unicode()

    def __init__(self, owner_id, volume_id, file_path, download_url,
                 download_key=None):
        dl_key = download_url
        if download_key:
            dl_key = unicode(repr(download_key))
        self.id = uuid.uuid4()
        self.owner_id = owner_id
        self.volume_id = volume_id
        self.file_path = file_path
        self.download_url = download_url
        self.download_key = dl_key
        self.status = DOWNLOAD_STATUS_QUEUED
        super(Download, self).__init__()

    def get_status(self):
        """Get the status of the download."""
        return self._status

    def set_status(self, status):
        """Set the status of the download, and update the change date."""
        self._status = status
        self.status_change_date = datetime.datetime.utcnow()
    status = property(get_status, set_status)


class ResumableUpload(object):
    """An Upload created through the REST API."""
    __storm_table__ = "resumable_upload"

    upload_id = StormUUID(primary=True)
    # the owner of this upload
    owner_id = Int(allow_none=False)
    # the volume path of the file when this upload completes
    volume_path = Unicode(allow_none=False)
    # the final size of this file
    size = Int(allow_none=False)
    # When the upload was started.
    when_started = DateTime(allow_none=False, default=AutoReload)
    # When the upload was last active.
    when_last_active = DateTime(allow_none=False, default=AutoReload)
    # Whether this is a live upload or a done-with one
    status = lifecycle_status(allow_none=False)
    # the multipart upload id
    multipart_id = RawStr(allow_none=False)
    # the s3 key for this multipart upload
    storage_key = StormUUID(allow_none=False)
    # the number of parts currently created
    part_count = Int(allow_none=False)
    # the number of the uploaded bytes so far.
    uploaded_bytes = Int(allow_none=False)
    # the hash context of this resumable upload
    hash_context = RawStr(allow_none=True)
    # the magic hash context of this resumable upload
    magic_hash_context = RawStr(allow_none=True)
    # the crc context from compressing content
    crc_context = Int(allow_none=True)

    def __init__(self, owner_id, volume_path, size, multipart_id, storage_key):
        """ Create a blob upload """
        self.upload_id = uuid.uuid4()
        self.owner_id = owner_id
        self.volume_path = volume_path
        self.size = size
        self.multipart_id = multipart_id
        self.storage_key = storage_key
        self.part_count = 0
        self.uploaded_bytes = 0
        self.hash_context = None
        self.magic_hash_context = None
        self.crc_context = None
        self.status = STATUS_LIVE

    def add_part(self, size, hash_context, magic_hash_context, crc_context):
        """Updated when a part is added."""
        self.uploaded_bytes += size
        self.part_count += 1
        self.hash_context = hash_context
        self.magic_hash_context = magic_hash_context
        self.crc_context = crc_context
        self.when_last_active = datetime.datetime.utcnow()
