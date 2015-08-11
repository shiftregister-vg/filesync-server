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

"""The DAL entry point as a service."""

import uuid

from backends.filesync.data import services, errors, model


class DAL(object):
    """The entry point for the DAL."""

    def _get_user(self, user_id, session_id=None):
        """Return a storage user for the given id."""
        return services.get_storage_user(user_id, session_id=session_id)

    def ping(self):
        """Used for a simple liveness check."""
        return dict(response="pong")

    def unlink_node(self, user_id, volume_id, node_id, session_id=None):
        """Unlink a node."""
        user = self._get_user(user_id, session_id)
        node = user.volume(volume_id).node(node_id).delete()
        return dict(generation=node.generation, kind=node.kind,
                    name=node.name, mimetype=node.mimetype)

    def list_volumes(self, user_id):
        """List all the volumes the user is involved.

        This includes the real Root, the UDFs, and the shares that were shared
        to the user already accepted.
        """
        user = self._get_user(user_id)

        # root info
        root = user.volume().get_volume()
        root_info = dict(root_id=root.root_id, generation=root.generation)

        # quota
        free_bytes = user.get_quota().free_bytes

        # shares
        shares = []
        for share in user.get_shared_to(accepted=True):
            suser = share.shared_by
            resp = dict(id=share.id, root_id=share.root_id, name=share.name,
                        shared_by_username=suser.username,
                        shared_by_visible_name=suser.visible_name,
                        accepted=share.accepted, access=share.access)

            info = services.get_user_info_for_shard(suser.id, suser.shard_id)
            resp['free_bytes'] = info.free_bytes

            resp['generation'] = share.get_generation()
            shares.append(resp)

        # udfs
        udfs = []
        for udf in user.get_udfs():
            resp = dict(id=udf.id, root_id=udf.root_id, path=udf.path,
                        generation=udf.generation)
            udfs.append(resp)

        result = dict(root=root_info, free_bytes=free_bytes,
                      shares=shares, udfs=udfs)
        return result

    def move(self, user_id, volume_id, node_id,
             new_parent_id, new_name, session_id=None):
        """Move a node and/or rename it."""
        user = self._get_user(user_id, session_id)
        # If we get new_parent_id as str/unicode, generate a UUID using it
        # because StorageObject.move() expects it to be either a UUID or None.
        if isinstance(new_parent_id, basestring):
            new_parent_id = uuid.UUID(new_parent_id)
        node = user.volume(volume_id).node(node_id).move(new_parent_id,
                                                         new_name)
        return dict(generation=node.generation, mimetype=node.mimetype)

    def make_dir(self, user_id, volume_id, parent_id, name, session_id=None):
        """Make a subdirectory."""
        user = self._get_user(user_id, session_id)
        node = user.volume(volume_id).dir(parent_id).make_subdirectory(name)
        return dict(generation=node.generation, node_id=node.id,
                    mimetype=node.mimetype)

    def make_file(self, user_id, volume_id, parent_id, name, session_id=None):
        """Make a no-content file."""
        user = self._get_user(user_id, session_id)
        node = user.volume(volume_id).dir(parent_id).make_file(name)
        return dict(generation=node.generation, node_id=node.id,
                    mimetype=node.mimetype)

    def make_file_with_content(self, user_id, volume_id, parent_id, name,
                               node_hash, crc32, size, deflated_size,
                               storage_key, session_id=None):
        """Make a file with associated content."""
        user = self._get_user(user_id, session_id)
        func = user.volume(volume_id).dir(parent_id).make_file_with_content
        node = func(name, node_hash, crc32, size, deflated_size, storage_key)
        return dict(generation=node.generation, node_id=node.id)

    def create_share(self, user_id, node_id,
                     to_username, share_name, readonly):
        """Create a share."""
        user = self._get_user(user_id)
        to_user = services.get_storage_user(username=to_username)
        share = user.volume().dir(node_id).share(to_user.id,
                                                 share_name, readonly)
        return dict(share_id=share.id)

    def delete_share(self, user_id, share_id):
        """Delete a share."""
        user = self._get_user(user_id)
        share = user.get_share(share_id)
        share.delete()
        return {}

    def accept_share(self, user_id, share_id):
        """Accept a share."""
        user = self._get_user(user_id)
        share = user.get_share(share_id)
        share.accept()
        return {}

    def decline_share(self, user_id, share_id):
        """Decline a share."""
        user = self._get_user(user_id)
        share = user.get_share(share_id)
        share.decline()
        return {}

    def list_shares(self, user_id, accepted):
        """List all the shares the user is involved.

        This includes the 'shared_by' (shares from the user to somebody else)
        and the 'shared_to' (shares from somebody else to the user). In the
        later, a filter is done regarding if the shares were accepted by the
        user or not.
        """
        user = self._get_user(user_id)

        # shared_by
        shared_by = []
        for share in user.get_shared_by():
            # get info from the shared_to user (if any)
            other_user = share.shared_to
            to_username = other_user.username if other_user else None
            to_visible_name = other_user.visible_name if other_user else None

            resp = dict(id=share.id, root_id=share.root_id, name=share.name,
                        shared_to_username=to_username,
                        shared_to_visible_name=to_visible_name,
                        accepted=share.accepted, access=share.access)
            shared_by.append(resp)

        # shared_to
        shared_to = []
        for share in user.get_shared_to(accepted=accepted):
            # get info from the shared_to user (if any)
            other_user = share.shared_by
            by_username = other_user.username if other_user else None
            by_visible_name = other_user.visible_name if other_user else None

            resp = dict(id=share.id, root_id=share.root_id, name=share.name,
                        shared_by_username=by_username,
                        shared_by_visible_name=by_visible_name,
                        accepted=share.accepted, access=share.access)
            shared_to.append(resp)

        return dict(shared_by=shared_by, shared_to=shared_to)

    def create_udf(self, user_id, path, session_id):
        """Create an UDF."""
        user = self._get_user(user_id, session_id)
        udf = user.make_udf(path)
        return dict(udf_id=udf.id, udf_root_id=udf.root_id, udf_path=udf.path)

    def delete_volume(self, user_id, volume_id, session_id):
        """Delete a volume, being it a share or an udf."""
        user = self._get_user(user_id, session_id)

        # we could ask permission instead of forgiveness here, but is
        # cheaper this than to access database just to see if exists
        try:
            share = user.get_share(volume_id)
            share.delete()
        except errors.DoesNotExist:
            # not a share, try it with a UDF
            try:
                user.delete_udf(volume_id)
            except errors.DoesNotExist:
                msg = "Volume %r does not exist" % (volume_id,)
                raise errors.DoesNotExist(msg)
        return {}

    def get_user_quota(self, user_id):
        """Get the quota info for an user."""
        user = self._get_user(user_id)
        quota = user.get_quota()
        d = dict(max_storage_bytes=quota.max_storage_bytes,
                 used_storage_bytes=quota.used_storage_bytes,
                 free_bytes=quota.free_bytes)
        return d

    def get_share(self, user_id, share_id):
        """Get the share information for a given id."""
        user = self._get_user(user_id)
        share = user.get_share(share_id)
        d = dict(share_id=share.id, share_root_id=share.root_id,
                 name=share.name, shared_by_id=share.shared_by_id,
                 shared_to_id=share.shared_to_id, accepted=share.accepted,
                 access=share.access)
        return d

    def get_root(self, user_id):
        """Get the root id."""
        user = self._get_user(user_id)
        node = user.root.load()
        return dict(root_id=node.id, generation=node.generation)

    def get_volume_id(self, user_id, node_id):
        """Get the volume_id of a node.

        Note that this method returns not the same parameter that is
        returned in get_node (that is vol_id).
        """
        user = self._get_user(user_id)
        node = user.volume().get_node(node_id)
        volume_id = node.volume_id
        if volume_id == user.root_volume_id:
            volume_id = None
        return dict(volume_id=volume_id)

    def get_node_from_user(self, user_id, node_id):
        """Get node info from its id and the user, no matter the volume.

        Note that in this case the content for the node is not returned.
        """
        user = self._get_user(user_id)
        node = services.get_node_for_shard(node_id, user.shard_id)
        return self._process_node(node)

    def get_node(self, user_id, volume_id, node_id):
        """Get node info from its id, volume and user.

        The node is returned with its content.
        """
        user = self._get_user(user_id)
        node = user.volume(volume_id).get_node(node_id, with_content=True)
        return self._process_node(node)

    def _process_node(self, node):
        """Get info from a node."""
        is_live = node.status == model.STATUS_LIVE
        is_file = node.kind == 'File'
        content = node.content
        if content is not None:
            crc32 = content.crc32
            size = content.size
            deflated_size = content.deflated_size
            storage_key = content.storage_key
            has_content = True
        else:
            crc32 = None
            size = None
            deflated_size = None
            storage_key = None
            has_content = False

        d = dict(id=node.id, name=node.name, generation=node.generation,
                 is_public=node.is_public, deflated_size=deflated_size,
                 last_modified=node.when_last_modified, crc32=crc32,
                 storage_key=storage_key, is_live=is_live,
                 size=size, is_file=is_file, volume_id=node.vol_id,
                 parent_id=node.parent_id, content_hash=node.content_hash,
                 path=node.path, has_content=has_content)
        return d

    def get_delta(self, user_id, volume_id, from_generation, limit):
        """Get a delta from a given generation."""
        user = self._get_user(user_id)
        get_delta = user.volume(volume_id).get_delta
        vol_gen, free_bytes, delta = get_delta(from_generation, limit=limit)
        nodes = [self._process_node(n) for n in delta]
        return dict(vol_generation=vol_gen, free_bytes=free_bytes, nodes=nodes)

    def get_from_scratch(self, user_id, volume_id, start_from_path=None,
                         limit=None, max_generation=None):
        """Get all nodes from scratch."""
        user = self._get_user(user_id)
        vol_gen, free_bytes, nodes = user.volume(volume_id).get_from_scratch(
            start_from_path=start_from_path, limit=limit,
            max_generation=max_generation)
        nodes = [self._process_node(n) for n in nodes]
        return dict(vol_generation=vol_gen, free_bytes=free_bytes, nodes=nodes)

    def get_user_data(self, user_id, session_id):
        """Get data from the user."""
        user = self._get_user(user_id, session_id)
        return dict(root_volume_id=user.root_volume_id, username=user.username,
                    visible_name=user.visible_name)

    def make_content(self, user_id, volume_id, node_id, original_hash,
                     hash_hint, crc32_hint, inflated_size_hint,
                     deflated_size_hint, storage_key, magic_hash, session_id):
        """Get node and make content for it."""
        user = self._get_user(user_id, session_id)
        node = user.volume(volume_id).get_node(node_id)
        node.make_content(original_hash, hash_hint, crc32_hint,
                          inflated_size_hint, deflated_size_hint,
                          storage_key, magic_hash)
        return dict(generation=node.generation)

    def _process_uploadjob(self, uj):
        """Get the info of an upload job to return as dict."""
        d = dict(
            uploadjob_id=uj.id,
            uploaded_bytes=uj.uploaded_bytes,
            multipart_id=uj.multipart_id,
            multipart_key=uj.multipart_key,
            chunk_count=uj.chunk_count,
            hash_context=uj.hash_context,
            magic_hash_context=uj.magic_hash_context,
            decompress_context=uj.decompress_context,
            inflated_size=uj.inflated_size,
            crc32=uj.crc32,
            when_last_active=uj.when_last_active,
        )
        return d

    def get_uploadjob(self, user_id, volume_id, node_id, uploadjob_id,
                      hash_value, crc32, inflated_size,
                      deflated_size):
        """Make an upload job for a node."""
        user = self._get_user(user_id)
        node = user.volume(volume_id).get_node(node_id)
        uj = node.get_multipart_uploadjob(uploadjob_id, hash_value, crc32,
                                          inflated_size, deflated_size)
        return self._process_uploadjob(uj)

    def make_uploadjob(self, user_id, volume_id, node_id, previous_hash,
                       hash_value, crc32, inflated_size,
                       deflated_size, multipart_key):
        """Make an upload job for a node."""
        user = self._get_user(user_id)
        node = user.volume(volume_id).get_node(node_id)
        uj = node.make_uploadjob(previous_hash, hash_value, crc32,
                                 inflated_size, deflated_size,
                                 multipart_key=multipart_key)
        return self._process_uploadjob(uj)

    def set_uploadjob_multipart_id(self, user_id, uploadjob_id, multipart_id):
        """Set the multipart id for an upload job."""
        user = self._get_user(user_id)
        uj = user.get_uploadjob(uploadjob_id)
        uj.set_multipart_id(multipart_id)
        return {}

    def delete_uploadjob(self, user_id, uploadjob_id):
        """Delete an upload job."""
        user = self._get_user(user_id)
        uj = user.get_uploadjob(uploadjob_id)
        uj.delete()
        return {}

    def add_part_to_uploadjob(self, user_id, uploadjob_id, chunk_size,
                              inflated_size, crc32, hash_context,
                              magic_hash_context, decompress_context):
        """Add a part to an upload job."""
        user = self._get_user(user_id)
        uj = user.get_uploadjob(uploadjob_id)
        uj.add_part(chunk_size, inflated_size, crc32,
                    hash_context, magic_hash_context, decompress_context)
        return {}

    def touch_uploadjob(self, user_id, uploadjob_id):
        """Touch an upload job."""
        user = self._get_user(user_id)
        uj = user.get_uploadjob(uploadjob_id)
        uj.touch()
        return dict(when_last_active=uj.when_last_active)

    def get_reusable_content(self, user_id, hash_value, magic_hash):
        """Return if the blob exists and its storage_key."""
        user = self._get_user(user_id)
        be, sk = user.is_reusable_content(hash_value, magic_hash)
        return dict(blob_exists=be, storage_key=sk)
