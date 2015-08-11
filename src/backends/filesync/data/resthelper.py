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
#
"""A set of utilites to build a REST API."""

from os import path

from backends.filesync.data import errors
from backends.filesync.data.dao import VolumeProxy
from backends.filesync.data.logging import log_dal_function


def date_formatter(dt):
    """Format dates."""
    return dt.isoformat().replace("T ", "T").split(".")[0] + "Z"


class ForbiddenRestOperation(Exception):
    """Base exception for most rest errors."""


class CannotPublishDirectory(ForbiddenRestOperation):
    """Raised if a directory is published"""


class FileNodeHasNoChildren(ForbiddenRestOperation):
    """Raised trying to access the children of a FileNode"""


class InvalidKind(ForbiddenRestOperation):
    """Raised when the kind is not directory or file."""


# resource mapper is used to generate URLs in the resources
class ResourceMapper(object):
    """A Resource Mapper that is passed into RestHelper.

    This is used so an external application can overried
    """

    def __init__(self):
        # mapping is a dictionary of URL maps keyed by resources,
        self.mapping = {
            'USER_INFO': '',
            'VOLUME_INFO': '/volumes/%(volume_path)s',
            'NODE_INFO': '/%(volume_path)s%(node_path)s',
            'FILE_CONTENT': '/content/%(volume_path)s%(node_path)s',
        }

    def user(self):
        """map a user to a user info resource."""
        # although the current design doesn't user the id
        # a mapper that overrides it may
        return self.mapping["USER_INFO"]

    def volume(self, volume_path):
        """Map the volume resource."""
        return self.mapping["VOLUME_INFO"] % dict(volume_path=volume_path)

    def node(self, volume_path, node_path=''):
        """Map the node resource."""
        node_path = "" if node_path == "/" else node_path
        return self.mapping["NODE_INFO"] % dict(volume_path=volume_path,
                                                node_path=node_path)

    def content(self, volume_path, node_path):
        """Map the file content resource."""
        node_path = "" if node_path == "/" else node_path
        return self.mapping["FILE_CONTENT"] % dict(volume_path=volume_path,
                                                   node_path=node_path)

    def user_repr(self, user, quota, udfs=None):
        """Return a serializable representation of a user."""
        udfs = [] if udfs is None else udfs
        return dict(
            resource_path=self.user(),
            user_id=user.id,
            visible_name=user.visible_name,
            used_bytes=quota.used_storage_bytes,
            max_bytes=quota.max_storage_bytes,
            root_node_path=self.node('~/Ubuntu One', ''),
            user_node_paths=[self.node(u.path) for u in udfs])

    def volume_repr(self, volume,
                    from_generation=None, nodes=None):
        """Return a serializable representation of a volume."""
        repr_ = dict(
            resource_path=self.volume(volume.path),
            type='root' if volume.is_root else 'udf',
            path=volume.path,
            generation=volume.generation,
            node_path=self.node(volume.path),
            content_path=self.content(volume.path, ""),
            when_created=date_formatter(volume.when_created))
        if from_generation is not None and nodes is not None:
            delta = dict(
                from_generation=from_generation,
                nodes=[self.node_repr(node) for node in nodes])
            repr_['delta'] = delta
        return repr_

    def node_repr(self, node):
        """Return a serializable representation of a node."""
        if node.vol_type == 'root':
            volume_path = u"~/Ubuntu One"
        else:
            volume_path = node.vol_udf.path
        if node.full_path == '/':
            parent_resource_path = None
        else:
            parent_resource_path = self.node(volume_path, node.path)
        info = dict(
            resource_path=self.node(volume_path, node.full_path),
            key=node.nodekey,
            kind=node.kind.lower(),
            path=node.full_path,
            parent_path=parent_resource_path,
            volume_path=self.volume(volume_path),
            when_created=date_formatter(node.when_created),
            when_changed=date_formatter(node.when_last_modified),
            generation=node.generation,
            generation_created=node.generation_created,
            is_live=node.status == 'Live',
            content_path=self.content(volume_path, node.full_path),
        )
        if node.kind == 'File':
            info.update(dict(
                hash=node.content_hash,
                public_url=node.public_url,
                is_public=node.is_public,
                size=node.content.size if node.content else None))
        else:
            info['has_children'] = node.has_children()
        return info


resourcemapper = ResourceMapper()


class RestHelper(object):
    """A class to be used by a REST server."""

    def __init__(self, mapper=resourcemapper, metrics=None, logger=None):
        self.map = mapper
        self.metrics = metrics
        self.log_dal = log_dal_function(logger)

    def get_user(self, user):
        """GET User Representation"""
        self.log_dal("get_quota", user)
        quota = user.get_quota()
        self.log_dal("get_udfs", user)
        udfs = user.get_udfs()
        return self.map.user_repr(user, quota, udfs)

    def get_volumes(self, user):
        """GET Volume Representation for all volumes."""
        self.log_dal("get_volume", user)
        volumes = [self.map.volume_repr(user.volume().get_volume())]
        self.log_dal("get_udfs", user)
        volumes.extend((self.map.volume_repr(u) for u in user.get_udfs()))
        return volumes

    def get_volume(self, user, volume_path, from_generation=None):
        """GET a volume representation."""
        self.log_dal("get_udf_by_path", user, volume_path=unicode(volume_path))
        volume = user.get_udf_by_path(unicode(volume_path))
        if from_generation is not None:
            volume_proxy = VolumeProxy(volume.id, user)
            self.log_dal("get_delta", user, volume_id=volume.id,
                         from_generation=from_generation)
            generation, _, delta_nodes = volume_proxy.get_delta(
                from_generation)
            return self.map.volume_repr(
                volume, from_generation, delta_nodes)
        else:
            return self.map.volume_repr(volume)

    def put_volume(self, user, path):
        """PUT a new volume."""
        self.log_dal("make_udf", user, path=unicode(path))
        udf = user.make_udf(unicode(path))
        return self.map.volume_repr(udf)

    def delete_volume(self, user, volume_path):
        """DELETE a volume."""
        self.log_dal("get_udf_by_path", user, volume_path=volume_path)
        volume = user.get_udf_by_path(volume_path)
        self.log_dal("delete_udf", user, volume_id=volume.id)
        user.delete_udf(volume.id)

    def get_node(self, user, node_path, include_children=False):
        """GET a Node Representation."""
        self.log_dal("get_node_by_path", user, node_path=node_path,
                     with_content=True)
        node = user.get_node_by_path(node_path, with_content=True)
        node_info = self.map.node_repr(node)
        if include_children:
            if node.kind == 'File':
                raise FileNodeHasNoChildren("Files have no Children")
            self.log_dal("get_children", user, node_id=node.id,
                         with_content=True)
            children = node.get_children(with_content=True)
            node_info['children'] = [self.map.node_repr(n) for n in children]
        return node_info

    def get_public_files(self, user):
        """GET a list of Node Representations for the user's public files."""
        self.log_dal("get_public_files", user)
        return map(self.map.node_repr, user.get_public_files())

    def delete_node(self, user, node_path):
        """DELETE a node."""
        self.log_dal("get_node_by_path", user, node_path=node_path)
        node = user.get_node_by_path(node_path)
        self.log_dal("delete", user, node_id=node.id, cascade=True)
        node.delete(cascade=True)

    def put_node(self, user, node_path, node_repr):
        """PUT a node.

        This method will either create or update an existing node.

        To create a node, the node must not exist at that path and the
        node_repr must contain the kind.

        To update a node, the node must exist and any of the following will
        result in an update:
        node_path: Change the path and rename a node (aka move)
        is_public: Make the node public.

        If any of these are not provided, nothing will happen. Also othe
        attributes sent will be ignored.
        """
        hash = node_repr.get('hash')
        magic_hash = node_repr.get('magic_hash')
        hash = str(hash) if hash else None
        magic_hash = str(magic_hash) if magic_hash else None
        try:
            self.log_dal("get_node_by_path", user, node_path=node_path)
            node = user.get_node_by_path(node_path)
        except errors.DoesNotExist:
            node = None
        if node and node.kind == 'File' and hash and magic_hash:
            # if the file already exists and we have a hash and magic hash
            # update the file
            self.log_dal("make_file_by_path", user, node_path=node_path)
            node = user.make_file_by_path(
                node_path, hash=hash, magic_hash=magic_hash)
        elif node is None:
            if node_repr.get('kind'):
                # we must be creating a node if only kind is passed
                if node_repr['kind'].lower() == 'file':
                    self.log_dal("make_file_by_path",
                                 user, node_path=node_path)
                    node = user.make_file_by_path(
                        node_path, hash=hash, magic_hash=magic_hash)
                elif node_repr['kind'].lower() == 'directory':
                    self.log_dal("make_tree_by_path",
                                 user, node_path=node_path)
                    node = user.make_tree_by_path(node_path)
                else:
                    raise InvalidKind("Invalid node Kind")
                return self.map.node_repr(node)
            else:
                raise errors.DoesNotExist("The node does not exist.")
        # we must be changing a node
        # are we moving?
        new_path = node_repr.get('path')
        if new_path:
            new_path, new_name = path.split(new_path)
            if node.name != new_name or node.path != new_path:
                self.log_dal("get_node_by_path", user, volume_id=node.vol_id,
                             path=unicode(new_path))
                parent = user.volume(node.vol_id).get_node_by_path(
                    unicode(new_path))
                self.log_dal("move", user, node_id=node.id,
                             parent_id=parent.id,
                             new_name=unicode(new_name))
                node.move(parent.id, unicode(new_name))
        # are we changing it's public status?
        is_public = node_repr.get('is_public')
        if is_public is not None:
            if node.kind == 'Directory':
                raise CannotPublishDirectory("Directories cant be public.")
            self.log_dal("change_public_access", user, node_id=node.id,
                         is_public=is_public)
            node.change_public_access(is_public)
            if self.metrics:
                self.metrics.report(
                    'resthelper.put_node.change_public',
                    str(user.id), "d")
        node.load(with_content=True)
        return self.map.node_repr(node)
