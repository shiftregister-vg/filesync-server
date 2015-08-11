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

"""Model code for the Transaction Log."""

import calendar
import json
import os

from functools import wraps

from storm.expr import Join, LeftJoin
from storm.locals import Int, DateTime, Enum, Store, Unicode
from storm.store import AutoReload

from config import config
from backends.filesync.data.dbmanager import get_shard_store, get_user_store
from backends.filesync.data.model import (
    STATUS_LIVE,
    Share,
    StorageObject,
    StorageUser,
    UserVolume,
    get_path_startswith,
)
from backends.tools.properties import StormUUID


def get_epoch_secs(dt):
    """Get the seconds since epoch"""
    return calendar.timegm(dt.timetuple())


# XXX: The 3 methods below should die once we enable txlog for all users in
# production.
def _get_user_id(obj):
    """Return the DB ID of the given object's owner.

    If the given object is a StorageUser, then its ID is returned.
    """
    if isinstance(obj, (StorageObject, UserVolume)):
        user_id = obj.owner_id
    elif isinstance(obj, Share):
        user_id = obj.shared_by
    elif isinstance(obj, StorageUser):
        user_id = obj.id
    else:
        raise AssertionError("Unknown object type: %s" % obj)
    return user_id


def _does_user_id_end_in(user_id, ending_range):
    """Return True if the user ID ends in the given range."""
    low, high = ending_range.split('-')
    if (int(str(user_id)[-len(low):]) >= int(low) and
            int(str(user_id)[-len(high):]) <= int(high)):
        return True
    return False


def skip_if_txlog_not_enabled(func):
    """A function decorator that does not call the function if txlog is not
    enabled to the owner of the object passed as the first argument.

    We get the owner ID of the first argument passed when the method is
    called (args[1], because args[0] is the instance to which the method is
    bound) and check the configs to see if txlog is enabled for that user. If
    it is, we just call the wrapped function with the given arguments,
    otherwise we return None.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        user_id = _get_user_id(args[1])
        enabled_to_users = config.txlog.enabled_to_user_ids
        enabled_to_ending_range = config.txlog.enabled_to_user_ids_ending_in
        run = False
        if enabled_to_users:
            enabled_to_users = map(int, enabled_to_users.split(','))
            if user_id in enabled_to_users:
                run = True
        if enabled_to_ending_range:
            if _does_user_id_end_in(user_id, enabled_to_ending_range):
                run = True
        if run:
            return func(*args, **kwargs)
        return None
    return wrapper


class TransactionLog(object):
    """The log of an operation performed on a node."""

    # Constants; may want to move somewhere else.
    OP_DELETE = u'delete'
    OP_MOVE = u'move'
    OP_PUT_CONTENT = u'put_content'
    OP_SHARE_ACCEPTED = u'share_accepted'
    OP_SHARE_DELETED = u'share_deleted'
    OP_PUBLIC_ACCESS_CHANGED = u'public_access_changed'
    OP_USER_CREATED = u'user_created'
    OP_UDF_CREATED = 'udf_created'
    OP_UDF_DELETED = 'udf_deleted'
    OPERATIONS_MAP = {
        OP_DELETE: 0,
        OP_MOVE: 1,
        OP_PUT_CONTENT: 2,
        OP_SHARE_ACCEPTED: 3,
        OP_SHARE_DELETED: 4,
        OP_PUBLIC_ACCESS_CHANGED: 5,
        OP_USER_CREATED: 6,
        OP_UDF_CREATED: 7,
        OP_UDF_DELETED: 8,
    }
    OPERATIONS_REVERSED_MAP = dict(
        [tuple(reversed(item)) for item in OPERATIONS_MAP.items()])

    __storm_table__ = "txlog.transaction_log"

    id = Int(primary=True)
    # Most operations we care about are on nodes, but this can be None for
    # things like OP_USER_CREATED.
    node_id = StormUUID()
    # The volume where the node is; can also be None in some cases.
    volume_id = StormUUID()
    # The ID of the node's owner if this is an operation on a Node, or the ID
    # of the newly created user if it's a OP_USER_CREATED.
    owner_id = Int(allow_none=False)
    op_type = Enum(map=OPERATIONS_MAP, allow_none=False)
    path = Unicode()
    generation = Int()
    timestamp = DateTime(allow_none=False, default=AutoReload)
    mimetype = Unicode()
    extra_data = Unicode()
    # Only used when representing a move.
    old_path = Unicode()

    def __init__(self, node_id, owner_id, volume_id, op_type, path, mimetype,
                 generation=None, old_path=None, extra_data=None):
        self.node_id = node_id
        self.owner_id = owner_id
        self.volume_id = volume_id
        self.op_type = op_type
        self.path = path
        self.generation = generation
        self.mimetype = mimetype
        self.old_path = old_path
        self.extra_data = extra_data

    @property
    def extra_data_dict(self):
        """A dictionary obtained by json.load()ing self.extra_data, or None if
        self.extra_data is None.
        """
        if self.extra_data is None:
            return self.extra_data
        return json.loads(self.extra_data)

    @classmethod
    def bootstrap(cls, user):
        store = get_shard_store(user.shard_id)
        cls.record_user_created(user)
        # Number of TransactionLog rows we inserted.
        rows = 1

        for udf in store.find(UserVolume, owner_id=user.id,
                              status=STATUS_LIVE):
            cls.record_udf_created(udf)
            rows += 1

        # If this becomes a problem it can be done as a single INSERT, but
        # we'd need to duplicate the get_public_file_url() in plpython.
        udf_join = Join(
            StorageObject,
            UserVolume, StorageObject.volume_id == UserVolume.id)
        conditions = [StorageObject.kind == 'Directory',
                      StorageObject.owner_id == user.id,
                      StorageObject.status == STATUS_LIVE,
                      StorageObject._publicfile_id != None,  # NOQA
                      UserVolume.status == STATUS_LIVE]
        dirs = store.using(udf_join).find(StorageObject, *conditions)
        for directory in dirs:
            cls.record_public_access_change(directory)
            rows += 1

        # XXX: If this takes too long it will get killed by the transaction
        # watcher. Need to check what's the limit we could have here.
        # Things to check:
        #  * If it still takes too long, we could find out the IDs of the
        #    people who have a lot of music/photos, run it just for them with
        #    the transaction watcher disabled and then run it for everybody
        #    else afterwards.
        query = """
            INSERT INTO txlog.transaction_log (
                node_id, owner_id, volume_id, op_type, path, generation,
                mimetype, extra_data)
            SELECT O.id, O.owner_id, O.volume_id, ?,
                   txlog.path_join(O.path, O.name), O.generation, O.mimetype,
                   txlog.get_extra_data_to_recreate_file_1(
                        kind, size, storage_key, publicfile_id,
                        public_uuid, content_hash,
                        extract(epoch from O.when_created at time zone 'UTC'),
                        extract(epoch
                                from O.when_last_modified at time zone 'UTC'),
                        UserDefinedFolder.path
                    ) as extra_data
            FROM Object as O
            JOIN UserDefinedFolder on UserDefinedFolder.id = O.volume_id
            LEFT JOIN ContentBlob on ContentBlob.hash = O.content_hash
            WHERE
                O.kind != 'Directory'
                AND O.owner_id = ?
                AND O.status = 'Live'
                AND UserDefinedFolder.status = 'Live'
            """
        params = (cls.OPERATIONS_MAP[cls.OP_PUT_CONTENT], user.id)
        rows += store.execute(query, params=params).rowcount

        # Cannot create TransactionLogs for Shares in a single INSERT like
        # above because TransactionLogs and Shares live in separate databases.
        share_join = LeftJoin(
            Share, StorageUser, Share.shared_to == StorageUser.id)
        conditions = [Share.shared_by == user.id,
                      Share.status == STATUS_LIVE,
                      Share.accepted == True]  # NOQA
        shares = get_user_store().using(share_join).find(Share, *conditions)
        for share in shares:
            cls.record_share_accepted(share)
            rows += 1

        return rows

    @classmethod
    @skip_if_txlog_not_enabled
    def record_udf_created(cls, udf):
        """Create a TransactionLog representing a new UserVolume."""
        when_created = get_epoch_secs(udf.when_created)
        extra_data = json.dumps(dict(when_created=when_created))
        txlog = cls(
            None, udf.owner_id, udf.id, cls.OP_UDF_CREATED,
            udf.path, mimetype=None, generation=udf.generation,
            extra_data=extra_data.decode('ascii'))
        return Store.of(udf).add(txlog)

    @classmethod
    @skip_if_txlog_not_enabled
    def record_udf_deleted(cls, udf):
        """Create TransactionLogs representing a UserVolume deleted.

        This will create one TransactionLog for the deletion of the UserVolume
        itself (op_type=OP_UDF_DELETED) and then create TransactionLogs for
        the removal of every descendant of it. The latter part is similar to
        unlinking a directory tree where the top of the tree is the
        UserVolume's root node.

        Note that when a UserVolume is deleted its generation is increased but
        the generation of its children are not, so we use the UserVolume's
        generation in all TransactionLogs created.
        """
        rows = 1
        Store.of(udf).add(cls(
            None, udf.owner_id, udf.id, cls.OP_UDF_DELETED,
            udf.path, mimetype=None, generation=udf.generation))
        rows += cls._record_unlink_tree(udf.root_node, udf.generation)
        return rows

    @classmethod
    @skip_if_txlog_not_enabled
    def record_user_created(cls, user):
        """Create a TransactionLog entry representing a new user.

        We abuse the TransactionLog table to store the details of newly
        created users because our derived services need information about
        users as well as their files.

        A TransactionLog representing a newly created user will have
        no node_id, volume_id, generation or path. And its owner_id will be
        the ID of the newly created user.
        """
        extra_data = json.dumps(dict(
            name=user.username, visible_name=user.visible_name))
        txlog = cls(
            None, user.id, None, cls.OP_USER_CREATED, None, None,
            extra_data=extra_data.decode('ascii'))
        store = get_shard_store(user.shard_id)
        return store.add(txlog)

    @classmethod
    @skip_if_txlog_not_enabled
    def record_public_access_change(cls, node):
        """Create a TransactionLog entry representing a change in a
        node's public accessibility.

        Currently we only record TransactionLogs for directories that are made
        public/private, so if the given node is not a directory we'll return
        None without storing a TransactionLog.

        @param node: The StorageObject that was made public/private.
        @return: The newly created TransactionLog.
        """
        extra_data = json.dumps(
            cls._get_extra_data_for_new_node(node, node.volume.path))
        txlog = cls(
            node.id, node.owner_id, node.volume_id,
            cls.OP_PUBLIC_ACCESS_CHANGED, node.full_path, node.mimetype,
            generation=node.generation, extra_data=extra_data.decode('ascii'))
        return Store.of(node).add(txlog)

    @classmethod
    @skip_if_txlog_not_enabled
    def record_put_content(cls, node):
        """Create a TransactionLog entry representing a PUT_CONTENT operation.

        @param node: The StorageObject which points to the content uploaded.
        @return: The newly created TransactionLog.
        """
        extra_data = json.dumps(
            cls._get_extra_data_for_new_node(node, node.volume.path))
        txlog = cls(
            node.id, node.owner_id, node.volume_id, cls.OP_PUT_CONTENT,
            node.full_path, node.mimetype, generation=node.generation,
            extra_data=extra_data.decode('ascii'))
        return Store.of(node).add(txlog)

    @classmethod
    def _get_extra_data_for_new_node(cls, node, volume_path):
        """A dict containing the extra data needed to re-create this node.

        @param node: Could be a StorageObject or a StorageNode(DAO)
        @param volume_path: the path of the node's volume

        This includes the kind, size, storage_key, publicfile_id, public_uuid,
        content_hash and creation date of the given node.

        It is supposed to be included in the extra_data of all TransactionLogs
        representing operations on nodes so that the node can be created even
        if messages arrive out of order on the service workers (e.g. a move
        txlog being processed before the txlog representing the file
        creation).

        The volume_path is passed in separately since getting it now would
        require another db transaction. The transaction management for this
        method is unclear.
        """
        public_uuid = node.public_uuid
        if public_uuid is not None:
            public_uuid = unicode(public_uuid)
        when_created = get_epoch_secs(node.when_created)
        last_modified = get_epoch_secs(node.when_last_modified)
        d = dict(publicfile_id=node.publicfile_id, public_uuid=public_uuid,
                 when_created=when_created,
                 last_modified=last_modified, kind=node.kind,
                 volume_path=volume_path)
        if node.kind == 'File':
            d['content_hash'] = node.content_hash
            d['size'] = getattr(node.content, 'size', None)
            storage_key = getattr(node.content, 'storage_key', None)
            if storage_key is not None:
                storage_key = unicode(storage_key)
            d['storage_key'] = storage_key
        return d

    @classmethod
    def record_share_accepted(cls, share):
        """Create a TransactionLog entry representing a share being accepted.

        @param share: The Share which was accepted.
        @return: The newly created TransactionLog.
        """
        cls._record_share_accepted_or_deleted(share, cls.OP_SHARE_ACCEPTED)

    @classmethod
    def record_share_deleted(cls, share):
        """Create a TransactionLog entry representing a share being deleted.

        @param share: The Share which was deleted.
        @return: The newly created TransactionLog.
        """
        cls._record_share_accepted_or_deleted(share, cls.OP_SHARE_DELETED)

    @classmethod
    @skip_if_txlog_not_enabled
    def _record_share_accepted_or_deleted(cls, share, op_type):
        store = get_shard_store(share.sharedbyuser.shard_id)
        node = store.get(StorageObject, share.subtree)
        when_last_changed = share.when_last_changed
        extra_data = dict(
            shared_to=share.shared_to, share_id=str(share.id),
            share_name=share.name, access_level=share.access,
            when_shared=get_epoch_secs(share.when_shared),
            when_last_changed=get_epoch_secs(when_last_changed))
        txlog = cls(
            node.id, node.owner_id, node.volume_id, op_type, node.full_path,
            node.mimetype, generation=None,
            extra_data=json.dumps(extra_data).decode('ascii'))
        return Store.of(node).add(txlog)

    @classmethod
    @skip_if_txlog_not_enabled
    def record_unlink(cls, node):
        """See _record_unlink."""
        cls._record_unlink(node, node.generation)

    @classmethod
    def _record_unlink(cls, node, generation):
        """Create a TransactionLog entry representing an unlink operation.

        If the given node is a file and its mimetype is not in
        INTERESTING_MIMETYPES, we do nothing.

        @param node: The StorageObject which was unlinked.
        @param generation: The generation to use in the newly created
            TransactionLog.
        @return: The newly created TransactionLog or None.
        """
        extra_data = json.dumps({
            'kind': node.kind,
            'volume_path': node.volume.path}).decode('ascii')
        txlog = cls(
            node.id, node.owner_id, node.volume_id, cls.OP_DELETE,
            node.full_path, node.mimetype, generation=generation,
            extra_data=extra_data)
        return Store.of(node).add(txlog)

    @classmethod
    @skip_if_txlog_not_enabled
    def record_unlink_tree(cls, directory):
        """See _record_unlink_tree."""
        cls._record_unlink_tree(directory, directory.generation)

    @classmethod
    def _record_unlink_tree(cls, directory, generation):
        """Create TransactionLog entries representing an unlink_tree operation.

        We create one TransactionLog entry for the given directory and each of
        its descendants that is either a directory or a file with a mimetype
        in INTERESTING_MIMETYPES.

        @param directory: The StorageObject representing the directory that
            was unlinked.
        @param generation: The generation to use in all TransactionLogs
            created by this method.
        @return: The number of created TransactionLog entries.
        """
        assert directory.kind == 'Directory', (
            "The given node is not a directory.")
        cls._record_unlink(directory, generation)
        where_clause, extra_params = (
            cls._get_interesting_descendants_where_clause(directory))
        # Here we construct the extra_data json manually because it's trivial
        # enough and the alternative would be to use a stored procedure, which
        # requires a DB patch.
        sql = """
            INSERT INTO txlog.transaction_log (
                node_id, owner_id, volume_id, op_type, path, generation,
                mimetype, extra_data)
            SELECT Object.id, Object.owner_id, Object.volume_id, ?,
                   txlog.path_join(Object.path, Object.name),
                   ?, Object.mimetype,
                  '{"kind": "' || Object.kind || '",
                  "volume_path": "' || UserDefinedFolder.path || '"}'
            FROM Object, UserDefinedFolder
            WHERE Object.volume_id = UserDefinedFolder.id AND
            """ + where_clause
        params = (cls.OPERATIONS_MAP[cls.OP_DELETE], generation)
        result = Store.of(directory).execute(
            sql, params=params + extra_params)
        # TODO: Store the rowcount in our metrics.
        return result.rowcount

    @classmethod
    @skip_if_txlog_not_enabled
    def record_move(cls, node, old_name, old_parent):
        """Create TransactionLog entries representing a move operation.

        This must be called after the actual move is performed because we
        assume the attributes of the given node and its descendants have
        already been updated.
        """
        if node.parent == old_parent and node.name == old_name:
            raise ValueError(
                "The old name and parent are the same as the current ones.")

        old_path = os.path.join(old_parent.full_path, old_name)
        rowcount = 0

        # First, create a TransactionLog for the actual file/directory
        # being moved.
        extra_data = json.dumps(cls._get_extra_data_for_new_node(
            node, node.volume.path))
        txlog = cls(
            node.id, node.owner_id, node.volume_id, cls.OP_MOVE,
            node.full_path, node.mimetype, generation=node.generation,
            old_path=old_path, extra_data=extra_data.decode('ascii'))
        Store.of(node).add(txlog)
        rowcount += 1

        if node.kind == 'Directory':
            # Now we generate a TransactionLog for every interesting
            # descendant of the directory that is being moved.
            old_path_base = os.path.join(old_parent.full_path, old_name)
            where_clause, extra_params = (
                cls._get_interesting_descendants_where_clause(node))
            sql = """
                INSERT INTO txlog.transaction_log (
                    node_id, owner_id, volume_id, op_type, path, generation,
                    mimetype, old_path, extra_data)
                SELECT Object.id, Object.owner_id, Object.volume_id, ?,
                   Object.path || '/' || Object.name, ?, Object.mimetype,
                   ? || substring(Object.path from ?) || '/' || Object.name,
                   txlog.get_extra_data_to_recreate_file_1(
                       Object.kind,
                       ContentBlob.size,
                       ContentBlob.storage_key,
                       Object.publicfile_id,
                       Object.public_uuid,
                       Object.content_hash,
                       extract(epoch from Object.when_created
                               at time zone 'UTC'),
                       extract(epoch from Object.when_last_modified
                               at time zone 'UTC'),
                       UserDefinedFolder.path
                    ) as extra_data
                FROM Object
                JOIN UserDefinedFolder
                    on UserDefinedFolder.id = Object.volume_id
                LEFT JOIN ContentBlob on ContentBlob.hash = Object.content_hash
                WHERE Object.volume_id = UserDefinedFolder.id AND
                """ + where_clause
            params = (
                cls.OPERATIONS_MAP[cls.OP_MOVE], node.generation,
                old_path_base, len(node.full_path) + 1)
            result = Store.of(node).execute(sql, params=params + extra_params)
            rowcount += result.rowcount
        # TODO: Store the rowcount in our metrics.
        return rowcount

    @classmethod
    def _get_interesting_descendants_where_clause(cls, node):
        """Return the WHERE clause to get the interesting descendants of node.

        @return: A two-tuple containing the SQL clauses and the params to be
            interpolated. They are suitable to be used with Storm's
            store.execute() API.
        """
        sql = """
            Object.volume_id = ?
            -- See comment on StorageObject.get_descendants() as to why we
            -- need to OR the two clauses below.
            AND (Object.parent_id = ?
                 OR Object.path LIKE ? || '%%')
            AND Object.status = 'Live'
            """
        base_path = get_path_startswith(node)
        params = (node.volume_id, node.id, base_path)
        # We use this code to explode UDF operations and in those cases we
        # will delete the root of a UDF, so we add this extra clause to avoid
        # the query above picking up the root folder as a descendant of
        # itself.
        if node.path == '/':
            sql += " AND Object.id != ?"
            params = params + (node.id,)
        return sql, params
