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

"""The Data Access Objects (DAO) for Storage Database.

These are not storm models and do not contain storm stores.  The embeded
gateways are thread safe in that they only represent the basic information to
produce a connection or get an existing connection off the thread so they can
be passed around.

They database access will be disabled if the underlying gateway is None or not
initialized.
"""

import datetime
import os
import posixpath as pypath

from weakref import WeakValueDictionary

from backends.filesync.data import errors, utils, model
from backends.filesync.data.dbmanager import (
    fsync_readonly,
    fsync_readonly_slave,
    fsync_commit,
    retryable_transaction,
)


class DAOBase(object):
    """Base class for all object that provide database access via a gateway."""

    readonly_error = 'This object is readonly.'

    def __init__(self, gateway=None):
        self.__gateway = gateway

    def _get_gateway(self):
        """Return the inner gateway for db access."""
        if self.__gateway is None:
            raise errors.StorageError(self.readonly_error)
        return self.__gateway

    def _set_gateway(self, gateway):
        """Set the gateway for db access."""
        self.__gateway = gateway

    _gateway = property(_get_gateway, _set_gateway)

    def _copy(self, obj):
        """Copy the data from obj into this object."""
        self.__dict__.clear()
        self.__dict__.update(obj.__dict__)

    def _load(self, *args, **kwargs):
        """Overriden by subclasses."""
        raise NotImplementedError("Need to override _load in subclasses.")

    @fsync_readonly
    def load(self, *args, **kwargs):
        """Reload the object."""
        self._load(*args, **kwargs)
        return self


class UserInfo(DAOBase):
    """A wrapper class to hold sharded user info."""

    def __init__(self, user_info, gateway=None):
        super(UserInfo, self).__init__(gateway)
        self.id = user_info.id
        self.max_storage_bytes = user_info.max_storage_bytes
        self.used_storage_bytes = user_info.used_storage_bytes

    @property
    def free_bytes(self):
        """Return the free bytes."""
        return max(0, self.max_storage_bytes - self.used_storage_bytes)

    def _load(self):
        """Load this storage user from the database."""
        i = self._gateway.get_quota()
        self._copy(i)
        return self


class StorageUser(DAOBase):
    """A Storage User DAO.

    This will be the main DAO for accessing all storage data on behalf of a
    user. All the data access performed from this class will return DAOs and
    not storm abjects.
    """

    def __init__(self, user):
        super(StorageUser, self).__init__()
        self.id = user.id
        self.username = user.username
        self.visible_name = user.visible_name
        self._status = user.status
        self._subscription_status = user.subscription_status
        self.shard_id = user.shard_id
        self.root_volume_id = user.root_volume_id
        self._volumes = WeakValueDictionary()

    @property
    def is_active(self):
        """True if the user is active."""
        return self._status == 'Live' and self._subscription_status == 'Live'

    def _load(self):
        """Load this storage user from the database."""
        u = self._gateway.get_user(self.id)
        self._copy(u)
        return self

    @fsync_readonly
    def get_quota(self):
        """Get the user's quota information."""
        return self._gateway.get_quota()

    @retryable_transaction()
    @fsync_commit
    def update(self, max_storage_bytes=None, subscription=None):
        """Update the storage user information."""
        u = self._gateway.update(max_storage_bytes=max_storage_bytes,
                                 subscription=subscription)
        self._copy(u)

    @retryable_transaction()
    @fsync_commit
    def recalculate_quota(self):
        """Recalculate the user's quota."""
        return self._gateway.recalculate_quota()

    @retryable_transaction()
    @fsync_commit
    def make_udf(self, path):
        """Create a UDF for this user."""
        return self._gateway.make_udf(path)

    @fsync_readonly
    def get_udf(self, udf_id):
        """Get a UDF owned by this user.

        Note that this is not a udf volume, it is a readonly object.
        """
        return self._gateway.get_udf(udf_id)

    @retryable_transaction()
    @fsync_commit
    def delete_udf(self, udf_id):
        """Delete this UDF."""
        return self._gateway.delete_udf(udf_id)

    @fsync_readonly
    def get_share(self, share_id):
        """Get a share offer."""
        return self._gateway.get_share(share_id, accepted_only=False)

    def get_node_with_key(self, key):
        """Using a nodekey, get the node from the appropriate volume."""
        vol_id, node_id = utils.split_nodekey(key)
        return self.volume(vol_id).get_node(node_id)

    def get_node(self, id, **kwargs):
        """Get a Node owned by this user."""
        return self.volume().get_node(id, **kwargs)

    @fsync_readonly
    def get_share_volumes(self):
        """Get volume gateways for all accepted shares."""
        gws = []
        for share in self._gateway.get_shared_to(accepted=True):
            gw = self._gateway.get_volume_gateway(share=share)
            vp = VolumeProxy.from_gateway(gw)
            self._volumes[share.id] = vp
            gws.append(vp)
        return gws

    @fsync_readonly
    def get_shared_by(self, node_id=None, accepted=None):
        """Get SharedDirectory volumes for this user."""
        return list(self._gateway.get_shared_by(node_id=node_id,
                                                accepted=accepted))

    @fsync_readonly
    def get_shared_to(self, accepted=None):
        """Get the SharedDirectories to this user."""
        return list(self._gateway.get_shared_to(accepted=accepted))

    @fsync_readonly
    def get_node_shares(self, node_id):
        """Return all shares this node is involved with.

        This will get the also nodes parents and look for shares of them
        """
        gw = self._gateway.get_root_gateway()
        nodeids = gw.get_node_parent_ids(node_id)
        return list(self._gateway.get_shares_of_nodes([node_id] + nodeids))

    @fsync_readonly
    def get_udf_by_path(self, path, from_full_path=False):
        """Get a UDF owned by this user.

        Note that this is not a udf volume, it is a readonly object.
        """
        return self._gateway.get_udf_by_path(path,
                                             from_full_path=from_full_path)

    @fsync_readonly
    def get_udfs(self):
        """Get the user's UDFs."""
        return list(self._gateway.get_udfs())

    @fsync_readonly
    def get_udf_volumes(self):
        """Get volume gatways for all UDFs."""
        gws = []
        for udf in self._gateway.get_udfs():
            gw = self._gateway.get_volume_gateway(udf=udf)
            vp = VolumeProxy.from_gateway(gw)
            self._volumes[udf.id] = vp
            gws.append(vp)
        return gws

    @fsync_readonly
    def get_uploadjob(self, uploadjob_id):
        """Get the user's UploadJobs for this user.

        @param node_id: optionally only gets jobs for a specific file
        """
        gw = self._gateway.get_root_gateway()
        return gw.get_uploadjob(uploadjob_id)

    @fsync_readonly
    def get_uploadjobs(self, node_id=None):
        """Get the user's UploadJobs for this user.

        @param node_id: optionally only gets jobs for a specific file
        """
        gw = self._gateway.get_root_gateway()
        return list(gw.get_user_uploadjobs(node_id=node_id))

    @fsync_readonly
    def get_downloads(self):
        """Get all downloads for this user."""
        return self._gateway.get_downloads()

    def volume(self, id=None):
        """Return an uninitialized Volume."""
        key = id or 'root'
        try:
            vol = self._volumes[key]
        except KeyError:
            vol = VolumeProxy(id, self)
            self._volumes[key] = vol
        return vol

    @property
    def root(self):
        """A shortcut for getting user's default root volume."""
        return self.volume().root

    @fsync_readonly
    def get_public_files(self):
        """Get the nodes list of public files for this user."""
        return list(self._gateway.get_public_files())

    @fsync_readonly
    def get_public_folders(self):
        """Get the nodes list of public folders for this user."""
        return list(self._gateway.get_public_folders())

    @fsync_readonly_slave
    def get_photo_directories(self):
        """Get all the directories the user has containing photos."""
        return list(self._gateway.get_photo_directories())

    def _path_helper(self, vol_full_path):
        """Using the path, return the remaining path and the udf."""
        if (vol_full_path == model.ROOT_USERVOLUME_PATH or
                vol_full_path.startswith(model.ROOT_USERVOLUME_PATH + "/")):
            udf = self._gateway.get_udf(self.root_volume_id)
        else:
            udf = self._gateway.get_udf_by_path(vol_full_path,
                                                from_full_path=True)
        remaining_path = vol_full_path.split(udf.path)[1] or '/'
        vol_id = udf.id if udf.id != self.root_volume_id else None
        return vol_id, udf, remaining_path

    @fsync_readonly
    def get_node_by_path(self, path, **kwargs):
        """Get a node using only a path."""
        vol_id, udf, remaining_path = self._path_helper(path)
        return self.volume(vol_id).gateway.get_node_by_path(
            remaining_path, **kwargs)

    @retryable_transaction()
    @fsync_commit
    def make_tree_by_path(self, path):
        """Create a subdirectory using a path."""
        vol_id, udf, remaining_path = self._path_helper(path)
        return self.volume(vol_id).gateway.make_tree(
            udf.root_id, remaining_path)

    @retryable_transaction()
    @fsync_commit
    def make_file_by_path(self, path, hash=None, magic_hash=None):
        """Create a file using a path."""
        vol_id, udf, remaining_path = self._path_helper(path)
        dir_path, filename = os.path.split(remaining_path)
        if dir_path == "/":
            d = self.volume(udf.id).gateway.get_root()
        else:
            d = self.volume(udf.id).gateway.make_tree(udf.root_id, dir_path)
        return self.volume(vol_id).gateway.make_file(
            d.id, filename, hash=hash, magic_hash=magic_hash)

    @retryable_transaction()
    @fsync_commit
    def make_filepath_with_content(self, path, hash, crc32, size,
                                   deflated_size, storage_key, mimetype=None,
                                   enforce_quota=True, is_public=False,
                                   previous_hash=None, magic_hash=None):
        """Create a file using a path."""
        vol_id, udf, remaining_path = self._path_helper(path)
        dir_path, filename = os.path.split(remaining_path)
        if dir_path == "/":
            d = self.volume(udf.id).gateway.get_root()
        else:
            d = self.volume(udf.id).gateway.make_tree(udf.root_id, dir_path)
        return self.volume(vol_id).gateway.make_file_with_content(
            d.id, filename, hash, crc32, size, deflated_size, storage_key,
            mimetype=mimetype, enforce_quota=enforce_quota,
            is_public=is_public, previous_hash=previous_hash,
            magic_hash=magic_hash)

    @fsync_readonly
    def is_reusable_content(self, hash_value, magic_hash):
        """Return if the user can reuse the content."""
        return self._gateway.is_reusable_content(hash_value, magic_hash)


class VolumeObjectBase(DAOBase):
    """Base class for Data Access Objects that exist on a volume."""

    def __init__(self, volume, gateway):
        super(VolumeObjectBase, self).__init__(gateway)
        self.__volume = volume
        self.__gateway = gateway

    def _get_gateway(self):
        """Override base class so a gateway can be retrieved from the volume
        proxy."""
        if self.__gateway:
            return self.__gateway
        if self.__volume:
            self.__gateway = self.__volume.gateway
            return self.__gateway
        raise errors.StorageError(self.readonly_error)

    def _set_gateway(self, gateway):
        """Set the gateway for db access."""
        self.__gateway = gateway
    _gateway = property(_get_gateway, _set_gateway)

    @property
    def vol_type(self):
        """Return the volume type (root, udf, share)."""
        if self._gateway.udf:
            return 'udf'
        elif self._gateway.share:
            return 'share'
        return 'root'

    @property
    def vol_share(self):
        """Return the share if this is a share volume."""
        return self._gateway.share

    @property
    def vol_udf(self):
        """Return the udf if this is a share udf."""
        return self._gateway.udf

    @property
    def vol_id(self):
        """The virtual volume id of this object.

        It is either a None or a share or udf id.
        """
        if self.__volume:
            return self.__volume.id
        if self.__gateway:
            return self.__gateway.vol_id

    def _load(self, *args, **kwargs):
        """Overriden by subclasses."""
        raise NotImplementedError("Need to override _load in subclasses.")


class StorageNode(VolumeObjectBase):
    """A base class for File and Directory Nodes.

    This object can be initialized with only an ID for lazy loading. In the
    case that it has been initialized with an id of 'root' This will result
    in a DirectoryNode for the root of the volume.
    """

    def __init__(self, id, gateway=None, volume=None):
        super(StorageNode, self).__init__(volume, gateway)
        self.id = id
        self.kind = None
        self.parent_id = None
        self.owner_id = None
        self.path = None
        self.full_path = None
        self.name = None
        self.content_hash = None
        self.volume_id = None
        self.public_id = None
        self.public_uuid = None
        self.can_delete = None
        self.can_write = None
        self.can_read = None
        self.status = None
        self.when_created = None
        self.when_last_modified = None
        self.generation = None
        self.generation_created = None
        self.mimetype = None
        self._udf = None
        self._owner = None
        self._content = None

    def __eq__(self, other):
        """Return true if objects are the same ID."""
        return (other is not None and
                self.id == other.id and self.vol_id == other.vol_id)

    @staticmethod
    def factory(gateway, object, permissions=None, content=None, udf=None,
                owner=None):
        """Create the appropriate DAO from storm model."""
        if object.kind == 'File':
            klass = FileNode
        elif object.kind == 'Directory':
            klass = DirectoryNode
        o = klass(object.id, gateway=gateway)
        o.kind = object.kind
        o.parent_id = object.parent_id
        o.owner_id = object.owner_id
        o.path = object.path
        o.full_path = object.full_path
        o.name = object.name
        # if this has a gateway, and it was a share, we want the root
        # to appear as a root, and the paths to start at the root
        if gateway and gateway.share:
            if object.id == gateway.root_id:
                o.parent_id = None
                o.path = u'/'
                o.name = u''
                o.full_path = u'/'
            else:
                # mask the root path
                o.path = object.path[len(gateway.root_path_mask)::]
                o.path = o.path if o.path else u'/'
                o.full_path = pypath.join(o.path, o.name)
        o.volume_id = object.volume_id
        o.public_id = object.publicfile_id
        o.public_uuid = object.public_uuid
        o.status = object.status
        o.when_created = object.when_created
        o.when_last_modified = object.when_last_modified
        o.generation = object.generation or 0
        o.generation_created = object.generation_created or 0
        o._owner = owner
        o._udf = udf
        o.mimetype = object.mimetype
        if object.kind == 'File':
            # only files have content
            o.content_hash = object.content_hash
            o._content = content
        # just a sanity check
        if owner:
            assert owner.id == o.owner_id
        o.set_permissions(permissions)
        return o

    def set_permissions(self, permissions):
        """Set the permissions for this node."""
        self.can_write = permissions["can_write"] if permissions else False
        self.can_delete = permissions["can_delete"] if permissions else False
        self.can_read = permissions["can_read"] if permissions else False

    @property
    def publicfile_id(self):
        """An alias to .public_id.

        This is to keep compatibility with model.StorageObject so that we can
        pass instances of either class to functions like get_public_file_url().
        """
        return self.public_id

    @property
    def public_url(self):
        """Return the public URL of the file."""
        if self.public_id is not None and self.kind == 'File':
            return utils.get_public_file_url(self)

    @property
    def public_key(self):
        """Return the public key for this node."""
        if self.public_uuid:
            return utils.get_node_public_key(self, True)
        if self.public_id:
            return utils.get_node_public_key(self)

    @property
    def owner(self):
        """The owner (StorageUser) of the node."""
        return self._owner

    @property
    def udf(self):
        """The UserVolume of the node."""
        return self._udf

    @property
    def is_public(self):
        """True if the file has a public id."""
        return self.public_id is not None

    @property
    def nodekey(self):
        """Get the encoded key for this node."""
        if self.id:
            if self.owner and self.vol_id == self.owner.root_volume_id:
                return utils.make_nodekey(None, self.id)
            return utils.make_nodekey(self.vol_id, self.id)

    def _load(self, with_content=False):
        """Load the object from the database base on the id."""
        ob = self._gateway.get_node(self.id, with_content=with_content)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def delete(self, cascade=False):
        """Delete this node."""
        ob = self._gateway.delete_node(self.id, cascade=cascade)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def restore(self):
        """Restore this node."""
        ob = self._gateway.restore_node(self.id)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def move(self, new_parent_id, new_name):
        """Move this node."""
        ob = self._gateway.move_node(self.id, new_parent_id, new_name)
        self._copy(ob)
        return self

    @property
    def content(self):
        """The FileNodeContent for this FileNode."""
        return self._content

    def has_children(self, kind=None):
        """Return True or False if the node directory has Children.

        Optionally kind can be provided and only children matching the kind
        will be checked.
        """
        if self.kind == 'Directory':
            f = fsync_readonly(self._gateway.check_has_children)
            return f(self.id, kind=kind)
        return False

    @retryable_transaction()
    @fsync_commit
    def change_public_access(self, is_public, allow_directory=False):
        """Set the node public based on the value passed in."""
        self._load()
        self._copy(self._gateway.change_public_access(
            self.id, is_public, allow_directory))
        return self


class DirectoryNode(StorageNode):
    """DAO for a Directory."""

    @retryable_transaction()
    @fsync_commit
    def make_file(self, name):
        """Create a file in this directory."""
        self._load()
        return self._gateway.make_file(self.id, name)

    @retryable_transaction()
    @fsync_commit
    def make_subdirectory(self, name):
        """Create a subsdirectory in this directory."""
        self._load()
        return self._gateway.make_subdirectory(self.id, name)

    @retryable_transaction()
    @fsync_commit
    def make_tree(self, path):
        """Create directory structure from a path in this directory."""
        self._load()
        return self._gateway.make_tree(self.id, path)

    @retryable_transaction()
    @fsync_commit
    def share(self, user_id, share_name, readonly=False):
        """Share this directory."""
        self._load()
        return self._gateway.make_share(self.id, share_name, user_id=user_id,
                                        readonly=readonly)

    @retryable_transaction()
    @fsync_commit
    def make_shareoffer(self, email, share_name, readonly=False):
        """Share this directory."""
        self._load()
        return self._gateway.make_share(self.id, share_name, email=email,
                                        readonly=readonly)

    @fsync_readonly
    def get_children(self, **kwargs):
        """Get Children of this directory."""
        self._load()
        if self.has_children():
            return list(self._gateway.get_children(self.id, **kwargs))
        return []

    @fsync_readonly
    def get_child_by_name(self, name, with_content=False):
        """Get a Node in this directory based on name."""
        self._load()
        return self._gateway.get_child_by_name(self.id, name, with_content)

    @retryable_transaction()
    @fsync_commit
    def make_file_with_content(self, file_name, hash, crc32, size,
                               deflated_size, storage_key, mimetype=None,
                               enforce_quota=True, is_public=False,
                               previous_hash=None, magic_hash=None):
        """Make a File and content in one transaction."""
        self._load()
        return self._gateway.make_file_with_content(
            self.id, file_name, hash, crc32, size, deflated_size, storage_key,
            mimetype=mimetype, enforce_quota=enforce_quota,
            is_public=is_public, previous_hash=previous_hash,
            magic_hash=magic_hash)


class FileNode(StorageNode):
    """DAO for an StorageObject kind='File'."""

    @retryable_transaction()
    @fsync_commit
    def make_uploadjob(self, verify_hash, new_hash, crc32, size, deflated_size,
                       multipart_id=None, multipart_key=None):
        """Create an UploadJob for this file."""
        self._load()
        return self._gateway.make_uploadjob(
            self.id, verify_hash, new_hash, crc32, size, deflated_size,
            multipart_id=multipart_id, multipart_key=multipart_key)

    @fsync_readonly
    def get_multipart_uploadjob(self, upload_id, hash_hint=None,
                                crc32_hint=None, inflated_size_hint=None,
                                deflated_size_hint=None):
        """Get the multipart UploadJob with upload_id for this file."""
        self._load()
        return self._gateway.get_user_multipart_uploadjob(
            self.id, upload_id, hash_hint=hash_hint, crc32_hint=crc32_hint,
            inflated_size_hint=inflated_size_hint,
            deflated_size_hint=deflated_size_hint)

    @fsync_readonly
    def get_content(self):
        """Return the FileNodeContent for this file."""
        self._load()
        return self._gateway.get_content(self.content_hash)

    @retryable_transaction()
    @fsync_commit
    def make_content(self, original_hash, hash_hint, crc32_hint,
                     inflated_size_hint, deflated_size_hint, storage_key,
                     magic_hash=None):
        """Make content or reuse it for this file."""
        self._load()
        ob = self._gateway.make_content(self.id, original_hash, hash_hint,
                                        crc32_hint, inflated_size_hint,
                                        deflated_size_hint, storage_key,
                                        magic_hash)
        self._copy(ob)
        return self


class FileNodeContent(object):
    """A ContentBlob for a File."""

    def __init__(self, contentblob):
        self.hash = contentblob.hash
        self.crc32 = contentblob.crc32
        self.size = contentblob.size
        self.status = contentblob.status
        self.magic_hash = contentblob.magic_hash
        if contentblob.size == 0:
            self.deflated_size = 0
            self.storage_key = None
        else:
            self.deflated_size = contentblob.deflated_size or 0
            self.storage_key = contentblob.storage_key
        self.when_created = contentblob.when_created


class UploadJob(VolumeObjectBase):
    """DAO for an Upload Job"""

    def __init__(self, upload, file=None,
                 gateway=None, volume=None):
        """Create DAO from storm model"""
        super(UploadJob, self).__init__(volume, gateway)
        self.id = upload.uploadjob_id
        self.storage_object_id = upload.storage_object_id
        self.chunk_count = upload.chunk_count
        self.hash_hint = upload.hash_hint
        self.crc32_hint = upload.crc32_hint
        self.inflated_size_hint = upload.inflated_size_hint
        self.deflated_size_hint = upload.deflated_size_hint
        self.when_started = upload.when_started
        self.when_last_active = upload.when_last_active
        self.status = upload.status
        self.multipart_id = upload.multipart_id
        self.multipart_key = upload.multipart_key
        self.uploaded_bytes = upload.uploaded_bytes
        self.inflated_size = upload.inflated_size
        self.crc32 = upload.crc32
        self.hash_context = upload.hash_context
        self.magic_hash_context = upload.magic_hash_context
        self.decompress_context = upload.decompress_context
        self._file = file

    @property
    def file(self):
        """The FileNode this upload is for."""
        return self._file

    @property
    def content_exists(self):
        """True if there is content for this upload job."""
        return not (self._file is None or self._file.content is None)

    @retryable_transaction()
    @fsync_commit
    def delete(self):
        """Delete this uploadjob."""
        self._gateway.delete_uploadjob(self.id)

    def _load(self):
        "Load the object from the database base on the id"
        ob = self._gateway.get_uploadjob(self.id)
        # the returned object doesn't have the node
        node = self._file
        self.__dict__.update(ob.__dict__)
        self._file = node
        return self

    @retryable_transaction()
    @fsync_commit
    def add_part(self, size, inflated_size, crc32, hash_context,
                 magic_hash_context, decompress_context):
        """Add part info to this uploadjob."""
        self._gateway.add_uploadjob_part(self.id, size, inflated_size, crc32,
                                         hash_context, magic_hash_context,
                                         decompress_context)
        # also update the when_last_active value.
        self._gateway.set_uploadjob_when_last_active(
            self.id, datetime.datetime.utcnow())
        self._load()

    @retryable_transaction()
    @fsync_commit
    def set_multipart_id(self, multipart_id):
        """Set the multipart_id on this job."""
        self._gateway.set_uploadjob_multpart_id(self.id, multipart_id)
        self._load()

    @retryable_transaction()
    @fsync_commit
    def touch(self):
        """Update the when_last_active attribute."""
        self._gateway.set_uploadjob_when_last_active(
            self.id, datetime.datetime.utcnow())
        self._load()


class SharedDirectory(DAOBase):
    """Represents a Share."""

    def __init__(self, share, by_user=None, to_user=None):
        super(SharedDirectory, self).__init__()
        self.id = share.id
        self.name = share.name
        self.root_id = share.subtree
        self.accepted = share.accepted
        self.access = share.access
        self.read_only = share.access == 'View'
        self.when_shared = share.when_shared
        self.when_last_changed = share.when_last_changed
        self.status = share.status
        self.offered_to_email = share.email
        self.shared_by_id = share.shared_by
        self.shared_to_id = share.shared_to
        self._shared_by = by_user
        self._shared_to = to_user

    def _load(self, **kwargs):
        """Reload the SharedDirectory from the db."""
        s = self._gateway.get_share(self.id, **kwargs)
        self._copy(s)
        return self

    @property
    def shared_by(self):
        """The StorageUser sharing this."""
        return self._shared_by

    @property
    def shared_to(self):
        """The StorageUser this is shared to."""
        return self._shared_to

    @retryable_transaction()
    @fsync_commit
    def delete(self):
        """Delete this share."""
        ob = self._gateway.delete_share(self.id)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def accept(self):
        """Accept this share."""
        ob = self._gateway.accept_share(self.id)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def decline(self):
        """Decline this share."""
        ob = self._gateway.decline_share(self.id)
        self._copy(ob)
        return self

    @retryable_transaction()
    @fsync_commit
    def set_access(self, readonly):
        """Change readonly access this share."""
        ob = self._gateway.set_share_access(self.id, readonly)
        self._copy(ob)
        return self

    @fsync_readonly
    def get_generation(self):
        """Return the generation for the volume of this share."""
        return self._gateway.get_share_generation(self)


class UserVolume(object):
    """User Defined Folder."""

    def __init__(self, vol, owner):
        super(UserVolume, self).__init__()
        self.id = vol.id
        self.root_id = vol.root_id
        self.owner_id = vol.owner_id
        self.status = vol.status
        self.path = vol.path
        self.when_created = vol.when_created
        self.generation = vol.generation or 0
        self.owner = owner

    @property
    def is_root(self):
        """Return true if this is a root volume."""
        return self.id == self.owner.root_volume_id


class VolumeProxy(object):
    """Provide lazy access to gateways via their volume (root, udf or share).

    In the case that id is None, this is always the user's root volume.
    The key 'root' is also used to represent the root directory of a volume.
    """

    def __init__(self, id, user):
        self.id = id
        self.user = user
        self._gateway = None
        self._root = None

    @classmethod
    def from_gateway(cls, gw):
        """Get a Volume Proxy from a gateway."""
        if gw.share:
            id = gw.share.id
        elif gw.udf:
            id = gw.udf.id
        else:
            id = None
        p = cls(id, gw.user)
        p._gateway = gw
        return p

    # the following node references created without created a db query.
    # It will later be dereferenced when used. For example:
    #     volumeproxy.node(id) will not execute a query.
    #     volumeproxy.node(id).delete() will execute a query.
    # This is done to isolate transactions without haveing a purely functional
    # design.
    @property
    def root(self):
        """Return an uninitialized DirectoryNode for the root volume.

        Since root nodes do not change, it is cached.
        """
        if self._root is None:
            self._root = DirectoryNode('root', volume=self)
        return self._root

    def node(self, id):
        """Return an uninitialized StorageNode.

        This is used in the case of moves and deletes only.
        """
        return StorageNode(id, volume=self)

    def dir(self, id):
        """Return an uninitialized DirectoryNode"""
        return DirectoryNode(id, volume=self)

    def file(self, id):
        """Return an uninitialized FileNode"""
        return FileNode(id, volume=self)

    @property
    def gateway(self):
        """Get the gateway for this volume."""
        #if id is None, we must be looking for the root volume
        if self._gateway is None:
            if self.id is None:
                self._gateway = self.user._gateway.get_root_gateway()
            else:
                self._gateway = self.user._gateway.get_share_gateway(self.id)
                if self._gateway is None:
                    self._gateway = self.user._gateway.get_udf_gateway(self.id)
                if self._gateway is None:
                    raise errors.DoesNotExist("Invalid Volume")
        return self._gateway

    @fsync_readonly
    def get_quota(self):
        """Get the UserInfo (quota) for this volume."""
        return self.gateway.get_quota()

    @fsync_readonly
    def get_volume(self):
        """Get the UserVolume for this volume."""
        return self.gateway.get_user_volume()

    @fsync_readonly
    def get_root(self):
        """Get the root directory for this volume."""
        return self.gateway.get_root()

    @fsync_readonly
    def get_node(self, id, **kwargs):
        """Get a Node off this volume."""
        return self.gateway.get_node(id, **kwargs)

    @fsync_readonly
    def get_node_by_path(self, path, **kwargs):
        """Get a Node off this volume using the path."""
        return self.gateway.get_node_by_path(path, **kwargs)

    @fsync_readonly
    def get_nodes(self, ids, with_content=False):
        """Get Nodes off this volume."""
        return self.gateway.get_nodes(ids, with_content=with_content)

    @fsync_readonly_slave
    def get_all_nodes(self, **kwargs):
        """Get a all nodes on this volume.

        This should be limited by mimetype and kind using named arguments.
        """
        return self.gateway.get_all_nodes(**kwargs)

    @fsync_readonly
    def get_deleted_files(self, start=0, limit=100):
        """Get a dead nodes on this."""
        return self.gateway.get_deleted_files(start=start, limit=limit)

    @fsync_readonly
    def get_content(self, content_hash):
        """Get a FileNodeContent from this volume's shard."""
        return self.gateway.get_content(content_hash)

    @fsync_readonly
    def get_delta(self, generation, limit=None):
        """Get this volumes generational delta.

        The return value is a tuple of (generation, free_bytes, [nodes])
        """
        quota = self.gateway.get_quota()
        volume = self.gateway.get_user_volume()
        if volume.generation <= generation:
            return (volume.generation, quota.free_bytes, [])
        delta_nodes = list(self.gateway.get_generation_delta(generation,
                                                             limit))
        return (volume.generation, quota.free_bytes, delta_nodes)

    @fsync_readonly
    def get_from_scratch(self, start_from_path=None, limit=None,
                         max_generation=None):
        """Get all of this volumes live nodes.

        The return value is a tuple of (generation, free_bytes, [nodes])
        """
        quota = self.gateway.get_quota()
        volume = self.gateway.get_user_volume()
        nodes = self.gateway.get_all_nodes(start_from_path=start_from_path,
                                           limit=limit,
                                           max_generation=max_generation)
        return (volume.generation, quota.free_bytes, nodes)

    @retryable_transaction()
    @fsync_commit
    def undelete_all(self, prefix, limit=100):
        """Undelete all the deleted files on this volume."""
        return self.gateway.undelete_volume(prefix, limit=limit)

    @fsync_readonly_slave
    def get_directories_with_mimetypes(self, mimetypes):
        """Get a list of {DirectoryNode}s that have files with mimetypes."""
        return self.gateway.get_directories_with_mimetypes(mimetypes)


class Download(object):
    """Pending download."""

    def __init__(self, download):
        super(Download, self).__init__()
        self.id = download.id
        self.owner_id = download.owner_id
        self.volume_id = download.volume_id
        self.file_path = download.file_path
        self.download_url = download.download_url
        self.download_key = download.download_key
        self.status = download.status
        self.status_change_date = download.status_change_date
        self.node_id = download.node_id
        self.error_message = download.error_message


class UserDAO(object):
    """DAO to retrieve data not associated to a specific user in context."""

    def __init__(self):
        from backends.filesync.data.gateway import SystemGateway
        self._gateway = SystemGateway()

    def get_random_user_id(self):
        """Retrieves a random user id from the gateway."""
        return self._gateway.get_random_user_id()
