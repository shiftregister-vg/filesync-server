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

"""Gateway objects for accessing Data Access Objects (DAO) from the database.

Each Gateway performs actions based on a security principal and limits the
actions based on the pricipal. In the case of a ReadWriteVolumeGateway,
security is imposed based on the user's access to the storage objects.
"""

import mimetypes
import os
import posixpath as pypath
import time
import uuid

from functools import wraps

from storm.locals import SQL
from storm.expr import Or, LeftJoin, Desc, And
from storm.info import ClassAlias

from metrics.metricsconnector import MetricsConnector
from backends.db.dbtransaction import db_timeout, TRANSACTION_MAX_TIME
from backends.filesync.data import model, errors, dao, utils
from backends.filesync.notifier.notifier import get_notifier
from backends.filesync.data.dbmanager import (
    get_new_user_shard_id,
    get_shard_store,
    get_user_store,
)
from config import config


class TimingMetrics(object):
    """Class to hold everything related to the timing metrics of DB calls."""

    def __init__(self):
        namespace = config.general.environment_name + ".filesync.DAL"
        self.reporter = MetricsConnector.new_metrics(namespace=namespace)

    def __call__(self, orig_func):
        """Decorator to issue metrics with the timing of the executed method.

        Warning: only apply this decorator to a method that will receive
        as first argument ('self') an object that has a DAO's user.
        """
        @wraps(orig_func)
        def wrapper(inner_self, *args, **kwargs):
            """Wrapper method."""
            # grab info for the metric
            shard_id = inner_self.owner.shard_id
            func_name = orig_func.func_name

            tini = time.time()
            try:
                result = orig_func(inner_self, *args, **kwargs)
            finally:
                delta_t = time.time() - tini
                called = "%s.%s" % (shard_id, func_name)
                self.reporter.timing(called, delta_t)
            return result
        return wrapper

timing_metric = TimingMetrics()


class GatewayBase(object):
    """The base for gateway classes"""

    user_dne_error = "The provided User id does not exist."
    node_dne_error = "The provided Node id does not exist."
    contentblob_dne_error = "The provided Content Blob hash does not exist."
    share_dne_error = "The provided Share id does not exist."
    udf_dne_error = "The provided UDF id does not exist."
    shareoffer_dne_error = "The provided Share Offer id does not exist."
    publicfile_dne_error = "The Public File id does not exist."
    uploadjob_dne_error = "The Upload Job id does not exist."
    download_dne_error = "The Download does not exist."
    inactive_user_error = "Inactive user can not access volumes."
    cannot_write_error = "User can not write in the node."
    cannot_delete_error = "User can not delete in the node."
    hash_mismatch_error = "The given hash does not match the node's."
    not_a_directory_error = "The provided Node is not a Directory."
    readonly_error = "The provided Node id is readonly."

    def __init__(self, session_id=None, notifier=None):
        """Initializes a gateway"""
        if notifier is None:
            self._notifier = get_notifier()
        else:
            self._notifier = notifier
        self.session_id = session_id

    def queue_share_created(self, share):
        """When a share is changed."""
        self._notifier.queue_share_created(share,
                                           source_session=self.session_id)

    def queue_share_deleted(self, share):
        """When a share is changed."""
        self._notifier.queue_share_deleted(share,
                                           source_session=self.session_id)

    def queue_share_accepted(self, share):
        """When a share is changed."""
        self._notifier.queue_share_accepted(share,
                                            source_session=self.session_id)

    def queue_share_declined(self, share):
        """When a share is declined."""
        self._notifier.queue_share_declined(share,
                                            source_session=self.session_id)

    def queue_udf_create(self, udf):
        """A UDF has been created."""
        self._notifier.queue_udf_create(udf.owner_id, udf.id, udf.root_id,
                                        udf.path, self.session_id)

    def queue_udf_delete(self, udf):
        """When a udf is deleted."""
        self._notifier.queue_udf_delete(udf.owner_id, udf.id,
                                        self.session_id)

    def queue_new_generation(self, user_id, client_volume_id, generation):
        """Queue a new generation change for a volume."""
        self._notifier.queue_volume_new_generation(
            user_id, client_volume_id, generation or 0,
            source_session=self.session_id)

    def get_user(self, user_id=None, username=None, session_id=None,
                 ignore_lock=False):
        """All gateways are going to need to get a user."""
        self.session_id = session_id
        if user_id is not None:
            user = self.user_store.get(model.StorageUser, user_id)
        elif username is not None:
            user = self.user_store.find(model.StorageUser,
                                        model.StorageUser.username == username
                                        ).one()
        else:
            raise errors.StorageError("Invalid call to get_user,"
                                      " user_id or username must be provided.")

        if user:
            if user.locked and not ignore_lock:
                raise errors.LockedUserError()
            user_dao = dao.StorageUser(user)
            user_dao._gateway = StorageUserGateway(user_dao,
                                                   session_id=session_id)
            return user_dao

    @property
    def user_store(self):
        """The main storage store."""
        return get_user_store()


class SystemGateway(GatewayBase):
    """Used when there is no authenticated user or used by external systems."""

    def create_or_update_user(self, user_id, username, visible_name,
                              max_storage_bytes, shard_id):
        """Create or update a StorageUser and related data.

        This happens when a user subscribes to the service. If they upgrade or
        reactivate a subscription, this would get called again.
        """
        user = self.user_store.get(model.StorageUser, user_id)
        if user is None:
            user = model.StorageUser.new(
                self.user_store, user_id, username, visible_name, shard_id)
        else:
            user.username = username
            user.visible_name = visible_name
            user.status = 'Live'
            user.subscription_status = 'Live'
        #initialize the user's shard data
        shard_store = get_shard_store(user.shard_id)
        #create or update the user info table on the shard
        user_info = shard_store.get(model.StorageUserInfo, user_id)
        if user_info is None:
            user_info = model.StorageUserInfo(user_id, max_storage_bytes)
            shard_store.add(user_info)
        else:
            user_info.max_storage_bytes = max_storage_bytes

        #create the user's root volume if necessary
        vol = model.UserVolume.get_root(shard_store, user_id)
        if vol is None:
            vol = model.UserVolume.make_root(shard_store, user_id)
        user.root_volume_id = vol.id
        user_dao = dao.StorageUser(user)
        user_dao._gateway = StorageUserGateway(user_dao, self.session_id)
        return user_dao

    def _get_shareoffer(self, shareoffer_id):
        """Get a shareoffer and who shared it."""
        result = self.user_store.find(
            (model.Share, model.StorageUser),
            model.Share.shared_by == model.StorageUser.id,
            model.Share.id == shareoffer_id,
            model.Share.status == model.STATUS_LIVE).one()
        share = None
        user = None
        if result:
            share, user = result
            if share.accepted or share.shared_to is not None:
                raise errors.ShareAlreadyAccepted(
                    "This share offer has been accepted.")
        return (share, user)

    def get_shareoffer(self, shareoffer_id):
        """Get a Share Offer sent to an email."""
        share, user = self._get_shareoffer(shareoffer_id)
        if share is None:
            raise errors.DoesNotExist(self.shareoffer_dne_error)
        return dao.SharedDirectory(share, by_user=user)

    def claim_shareoffer(self, user_id, username, visible_name,
                         shareoffer_id):
        """Claim a share offer sent to an email.

        This is a strange function in that the user may not have subscribed yet
        and may not have a storage user record.
        """
        # A anonymous share offer is a share sent to an email address but not
        # to a specific user. We also don't let user's claim their own share
        share, byuser = self._get_shareoffer(shareoffer_id)
        if share is None or byuser.id == user_id:
            raise errors.DoesNotExist(self.shareoffer_dne_error)

        user = self.get_user(user_id)
        if user is None:
            gw = SystemGateway()
            shard = get_new_user_shard_id(user_id)
            user = gw.create_or_update_user(user_id, username, visible_name, 0,
                                            shard_id=shard)
            self.user_store.commit()
            #they are not subscribed!
            user._gateway.update(subscription=False)

        share.claim_share(self.user_store, user.id)
        share_dao = dao.SharedDirectory(share, by_user=byuser, to_user=user)
        self.queue_share_accepted(share_dao)
        return share_dao

    def _get_public_node(self, public_key, use_uuid=False):
        """Get a node from a public key."""
        if public_key is None:
            raise errors.DoesNotExist(self.publicfile_dne_error)
        try:
            public_id = utils.decode_base62(public_key, allow_padding=True)
        except utils.Base62Error:
            raise errors.DoesNotExist(self.publicfile_dne_error)

        if use_uuid:
            public_id = uuid.UUID(int=public_id)
            publicnode = self.user_store.find(
                model.PublicNode,
                model.PublicNode.public_uuid == public_id).one()
        else:
            publicnode = self.user_store.find(
                model.PublicNode,
                model.PublicNode.id == public_id,
                model.PublicNode.public_uuid == None).one()  # NOQA
        if publicnode is None:
            raise errors.DoesNotExist(self.publicfile_dne_error)

        user = self.get_user(publicnode.owner_id, ignore_lock=True)
        gw = ReadWriteVolumeGateway(user)
        node = gw.get_node(publicnode.node_id, with_content=True)
        if not node.is_public:
            # We raise DoesNotExist instead of NoPermission here,
            # since it reveals information to the user that we don't
            # have to (e.g. they might try to look in various caches
            # for files that have been withdrawn).
            raise errors.DoesNotExist(self.publicfile_dne_error)
        return node

    def get_public_directory(self, public_key, mimetypes=None):
        """Get a public directory."""
        # Use UUIDs instead of the old method
        node = self._get_public_node(public_key, use_uuid=True)
        if node.kind != 'Directory':
            raise errors.DoesNotExist(self.publicfile_dne_error)
        return node

    def get_public_file(self, public_key, use_uuid=False):
        """Get a public file."""
        node = self._get_public_node(public_key, use_uuid)
        if (node.content is None or node.content.storage_key is None
                or node.kind != 'File'):
            # if the file has no content, we should not be able to get it
            raise errors.DoesNotExist(self.publicfile_dne_error)
        return node

    def make_download(self, user_id, volume_id, file_path, download_url,
                      download_key=None):
        """Make a new download object."""
        user = self.get_user(user_id)
        shard_store = get_shard_store(user.shard_id)
        download = model.Download(
            user_id, volume_id, file_path, download_url, download_key)
        shard_store.add(download)
        return dao.Download(download)

    def _get_download(self, user_id, download_id):
        """Internal function to get the download and owner."""
        user = self.get_user(user_id)
        shard_store = get_shard_store(user.shard_id)
        download = shard_store.get(model.Download, download_id)
        return user, download

    def get_download(self, user_id, udf_id, file_path, download_url,
                     download_key=None):
        """Get a download by its UDF, file path and download key."""
        user = self.get_user(user_id)
        shard_store = get_shard_store(user.shard_id)
        download = shard_store.find(
            model.Download,
            model.Download.owner_id == user_id,
            model.Download.volume_id == udf_id,
            model.Download.file_path == file_path,
            Or(
                model.Download.download_key == unicode(repr(download_key)),
                model.Download.download_url == download_url
            )
        ).order_by(model.Download.status_change_date).last()
        if download is None:
            raise errors.DoesNotExist(self.download_dne_error)
        return dao.Download(download)

    def get_download_by_id(self, user_id, download_id):
        """Get a download by its ID."""
        user, download = self._get_download(user_id, download_id)
        if download is None:
            raise errors.DoesNotExist(self.download_dne_error)
        return dao.Download(download)

    def update_download(self, user_id, download_id, status=None,
                        node_id=None, error_message=None):
        """Updoate the download properties."""
        user, download = self._get_download(user_id, download_id)
        if download is None:
            raise errors.DoesNotExist(self.download_dne_error)
        if status is not None:
            download.status = status
        if node_id is not None:
            download.node_id = node_id
        if error_message is not None:
            download.error_message = error_message
        return dao.Download(download)

    def download_complete(self, user_id, download_id, hash, crc32, size,
                          deflated_size, mimetype, storage_key):
        """Complete the download."""
        user, download = self._get_download(user_id, download_id)
        if download is None:
            raise errors.DoesNotExist(self.download_dne_error)
        #get the proper gateway for creating the file
        ugw = StorageUserGateway(user)
        if download.volume_id == user.root_volume_id:
            vgw = ugw.get_root_gateway()
        else:
            vgw = ugw.get_udf_gateway(download.volume_id)
        path, filename = os.path.split(download.file_path)
        folder = vgw.make_tree(vgw.get_root().id, path)
        fnode = vgw.make_file_with_content(
            folder.id, filename, hash, crc32,
            size, deflated_size, storage_key, mimetype, enforce_quota=False)
        download.node_id = fnode.id
        download.status = model.DOWNLOAD_STATUS_COMPLETE
        return dao.Download(download)

    def get_failed_downloads(self, shard_id, start_date, end_date):
        """Get failed downloads."""
        shard_store = get_shard_store(shard_id)
        result = shard_store.find(
            model.Download,
            model.Download._status == model.DOWNLOAD_STATUS_ERROR,
            model.Download.status_change_date >= start_date,
            model.Download.status_change_date <= end_date)
        for dl in result:
            yield dao.Download(dl)

    def get_node(self, node_id, shard_id):
        """Get a node for the specified node_id, shard_id."""
        shard_store = get_shard_store(shard_id)
        node = shard_store.find(
            model.StorageObject,
            model.StorageObject.status == model.STATUS_LIVE,
            model.StorageObject.id == node_id).one()
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        return dao.StorageNode.factory(None, node, permissions={})

    def get_user_info(self, user_id, shard_id):
        """Get the UserInfo DAO for user_id, shard_id"""
        shard_store = get_shard_store(shard_id)
        user_info = shard_store.get(model.StorageUserInfo, user_id)
        if user_info is None:
            raise errors.DoesNotExist(self.user_dne_error)
        return dao.UserInfo(user_info)

    def cleanup_uploadjobs(self, shard_id, uploadjobs):
        """Delete uploadjobs."""
        uploadjob_ids = [job.id for job in uploadjobs]
        store = get_shard_store(shard_id)
        store.find(model.UploadJob,
                   model.UploadJob.uploadjob_id.is_in(uploadjob_ids)).remove()

    def get_abandoned_uploadjobs(self, shard_id, last_active, limit=1000):
        """Get uploadjobs that are older than last_active from shard_id."""
        store = get_shard_store(shard_id)
        jobs = store.find(
            model.UploadJob,
            model.UploadJob.when_last_active < last_active)[:limit]
        return [dao.UploadJob(job) for job in jobs]

    def get_random_user_id(self):
        """Retrieves a random user id from the user store."""

        query = """SELECT id FROM StorageUser
        ORDER BY RANDOM()
        LIMIT 1"""
        store = get_user_store()
        result = store.execute(SQL(query)).get_one()
        return result[0]


class StorageUserGateway(GatewayBase):
    """Access point for accessing storage users and shares.

    For a specific user's security context.
    """

    def __init__(self, user, session_id=None):
        super(StorageUserGateway, self).__init__(session_id)
        # also set the 'owner' for being explicit to whom shard the
        # timing metrics should be reported against
        self.owner = self.user = user

    @timing_metric
    def update(self, max_storage_bytes=None, subscription=None):
        """Update a user's max_storage_bytes or subscription_status.

        This typically only happens when a user's subscription changes.
        """
        user = self.user_store.get(model.StorageUser, self.user.id)
        shard_store = get_shard_store(user.shard_id)

        # update the subscription in the user
        if subscription is not None:
            if subscription:
                user.subscription_status = model.STATUS_LIVE
            else:
                user.subscription_status = model.STATUS_DEAD

        # update the user info
        if max_storage_bytes is not None:
            user_info = shard_store.get(model.StorageUserInfo, self.user.id)
            if user_info is None:
                user_info = model.StorageUserInfo(self.user.id,
                                                  max_storage_bytes)
                shard_store.add(user_info)
            else:
                user_info.max_storage_bytes = max_storage_bytes

        # save all back
        user_dao = dao.StorageUser(user)
        user_dao._gateway = StorageUserGateway(user_dao, self.session_id)
        return user_dao

    @timing_metric
    def get_quota(self):
        """Get the user's quota information."""
        store = get_shard_store(self.user.shard_id)
        info = store.get(model.StorageUserInfo, self.user.id)
        return dao.UserInfo(info, gateway=self)

    @timing_metric
    def recalculate_quota(self):
        """Recalculate a user's quota."""
        store = get_shard_store(self.user.shard_id)
        info = store.get(model.StorageUserInfo, self.user.id)
        info.recalculate_used_bytes()
        return dao.UserInfo(info, gateway=self)

    def get_root_gateway(self):
        """Get the volume gateway for the user's root folder."""
        return self.get_volume_gateway()

    @timing_metric
    def get_udf_gateway(self, udf_id):
        """Get the volume gateway for a user's udf."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        #sanity check
        store = get_shard_store(self.user.shard_id)
        udf = store.find(
            model.UserVolume,
            model.UserVolume.owner_id == self.user.id,
            model.UserVolume.id == udf_id,
            model.UserVolume.status == model.STATUS_LIVE).one()
        if udf:
            return self.get_volume_gateway(
                udf=dao.UserVolume(udf, self.user))

    @timing_metric
    def get_share_gateway(self, share_id):
        """Get the volume gateway for a folder shared to this user."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        share = self.user_store.find(
            model.Share,
            model.Share.shared_to == self.user.id,
            model.Share.shared_by == model.StorageUser.id,
            model.StorageUser.subscription_status == model.STATUS_LIVE,
            model.Share.id == share_id,
            model.Share.status == 'Live',
            model.Share.accepted == True).one()  # NOQA
        if share:
            by_user = self.get_user(share.shared_by)
            return self.get_volume_gateway(
                share=dao.SharedDirectory(share, by_user=by_user))

    @timing_metric
    def get_volume_gateway(self, udf=None, share=None):
        """Get a volume, this may be a user's root, udf, or a share."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        return ReadWriteVolumeGateway(
            self.user, share=share, udf=udf,
            session_id=self.session_id, notifier=self._notifier)

    @timing_metric
    def get_share(self, share_id, accepted_only=True, live_only=True):
        """Get a specific share shared by or shared_to this user."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        ToUser = ClassAlias(model.StorageUser)
        FromUser = ClassAlias(model.StorageUser)
        join1 = LeftJoin(model.Share, FromUser,
                         model.Share.shared_by == FromUser.id)
        joins = LeftJoin(join1, ToUser,
                         model.Share.shared_to == ToUser.id)
        conditions = [model.Share.id == share_id,
                      Or(model.Share.shared_to == self.user.id,
                         model.Share.shared_by == self.user.id)]
        if accepted_only:
            conditions.append(model.Share.accepted == True)  # NOQA
        if live_only:
            conditions.append(model.Share.status == model.STATUS_LIVE)
        result = self.user_store.using(joins).find(
            (model.Share, FromUser, ToUser),
            *conditions).one()
        if result is None:
            raise errors.DoesNotExist(self.share_dne_error)
        share, byuser, touser = result
        if touser:
            touser = dao.StorageUser(touser)
        share_dao = dao.SharedDirectory(
            share, to_user=touser, by_user=dao.StorageUser(byuser))
        share_dao._gateway = self
        return share_dao

    @timing_metric
    def get_shared_by(self, accepted=None, node_id=None):
        """Get shared folders shared by this user.

        Passing in a node_id will get the shares for that node only
        """
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        joins = LeftJoin(model.Share, model.StorageUser,
                         model.Share.shared_to == model.StorageUser.id)
        conditions = [model.Share.shared_by == self.user.id,
                      model.Share.status == 'Live']
        if accepted is not None:
            conditions.append(model.Share.accepted == accepted)
        if node_id:
            conditions.append(model.Share.subtree == node_id)

        shares = self.user_store.using(joins).find(
            (model.Share, model.StorageUser), *conditions)
        for share, user in shares:
            if user:
                user = dao.StorageUser(user)
            share_dao = dao.SharedDirectory(share,
                                            to_user=user,
                                            by_user=self.user)
            share_dao._gateway = self
            yield share_dao

    @timing_metric
    def get_shares_of_nodes(self, node_ids, accepted_only=True,
                            live_only=True):
        """Get accepted shares for nodes in node_ids."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        joins = LeftJoin(model.Share, model.StorageUser,
                         model.Share.shared_to == model.StorageUser.id)
        conditions = [model.Share.shared_by == self.user.id,
                      model.Share.subtree.is_in(node_ids)]
        if accepted_only:
            conditions.append(model.Share.accepted == True)  # NOQA
        if live_only:
            conditions.append(model.Share.status == model.STATUS_LIVE)

        shares = self.user_store.using(joins).find(
            (model.Share, model.StorageUser), *conditions)
        for share, user in shares:
            if user:
                user = dao.StorageUser(user)
            share_dao = dao.SharedDirectory(share,
                                            to_user=user,
                                            by_user=self.user)
            share_dao._gateway = self
            yield share_dao

    @timing_metric
    def get_shared_to(self, accepted=None):
        """Get shares shared to this user.

        accepted can be True, False, or None (to get all)
        """
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        conditions = [model.Share.shared_by == model.StorageUser.id,
                      model.Share.shared_to == self.user.id,
                      model.StorageUser.subscription_status == 'Live',
                      model.Share.status == 'Live']
        if accepted is not None:
            conditions.append(model.Share.accepted == accepted)

        shares = self.user_store.find(
            (model.Share, model.StorageUser), *conditions)
        for share, user in shares:
            share_dao = dao.SharedDirectory(share,
                                            by_user=dao.StorageUser(user),
                                            to_user=self.user)
            share_dao._gateway = self
            yield share_dao

    @timing_metric
    def accept_share(self, share_id):
        """Accept a share offer"""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        share = self.user_store.find(model.Share,
                                     model.Share.id == share_id,
                                     model.Share.shared_to == self.user.id,
                                     model.Share.status == model.STATUS_LIVE,
                                     model.Share.accepted == False).one()  # NOQA
        if share is None:
            raise errors.DoesNotExist(self.share_dne_error)
        share.accept()
        share_dao = self.get_share(share.id)
        share_dao._gateway = self
        self.queue_share_accepted(share_dao)
        return share_dao

    @timing_metric
    def decline_share(self, share_id):
        """Decline a share offer"""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        share = self.user_store.find(model.Share,
                                     model.Share.id == share_id,
                                     model.Share.shared_to == self.user.id,
                                     model.Share.status == model.STATUS_LIVE
                                     ).one()
        if share is None:
            raise errors.DoesNotExist(self.share_dne_error)
        share.accepted = False
        share.delete()
        share_dao = dao.SharedDirectory(share)
        self.queue_share_declined(share_dao)
        self.queue_share_deleted(share_dao)
        return share_dao

    @timing_metric
    def delete_share(self, share_id):
        """Delete a share shared by or to this user."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        share = self.user_store.find(model.Share,
                                     model.Share.id == share_id,
                                     Or(model.Share.shared_by == self.user.id,
                                        model.Share.shared_to == self.user.id),
                                     model.Share.status == model.STATUS_LIVE
                                     ).one()
        if share is None:
            raise errors.DoesNotExist(self.share_dne_error)
        share.delete()
        share_dao = dao.SharedDirectory(share)
        self.queue_share_deleted(share_dao)
        return share_dao

    @timing_metric
    def set_share_access(self, share_id, readonly):
        """Change the readonly access of this share."""
        share = self.user_store.find(
            model.Share,
            model.Share.id == share_id,
            model.Share.status == model.STATUS_LIVE,
            model.Share.shared_by == self.user.id).one()
        if share is None:
            raise errors.DoesNotExist(self.share_dne_error)
        share.access = "View" if readonly else "Modify"
        share_dao = dao.SharedDirectory(share)
        share_dao._gateway = self
        return share_dao

    @timing_metric
    def delete_related_shares(self, node):
        """Delete all related shares under the node.

        @param node: A StorageNode this user owns
        """
        if node.owner_id != self.user.id:
            msg = "User does not own the node, shares can not be deleted."
            raise errors.NoPermission(msg)
        #since we're using only the ids, use the inner function of the vgw
        nodeids = [n.id for n in node.get_descendants(kind='Directory')]
        nodeids.append(node.id)
        shares = []
        sublist = utils.split_in_list(nodeids)
        for l in sublist:
            s = self.get_shares_of_nodes(l, accepted_only=False)
            shares.extend(list(s))
        for share in shares:
            self.delete_share(share.id)

    @timing_metric
    def make_udf(self, path):
        """Create a UDF."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        shard_store = get_shard_store(self.user.shard_id)
        # need a lock here.
        info = shard_store.get(model.StorageUserInfo, self.user.id)
        info.lock_for_update()
        path_like = path + u'/'
        #make sure this UDF wont be the existing UDF of be the parent of
        #and existing UDF
        prev_udfs = shard_store.find(
            model.UserVolume,
            model.UserVolume.owner_id == self.user.id,
            model.UserVolume.status == model.STATUS_LIVE)
        for prev_udf in prev_udfs:
            if prev_udf.path == path:
                return dao.UserVolume(prev_udf, self.user)
            prvpath = prev_udf.path + u"/"
            if prvpath.startswith(path_like) or path_like.startswith(prvpath):
                raise errors.NoPermission("UDFs can not be nested.")
        udf = model.UserVolume.create(shard_store, self.user.id, path)
        udf_dao = dao.UserVolume(udf, self.user)
        self.queue_udf_create(udf_dao)
        return udf_dao

    @timing_metric
    def get_udf_by_path(self, path, from_full_path=False):
        """Get a UDF by the path parts."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        shard_store = get_shard_store(self.user.shard_id)
        path = path.rstrip('/')
        if from_full_path:
            udfs = shard_store.find(
                model.UserVolume,
                model.UserVolume.owner_id == self.user.id,
                model.UserVolume.status == model.STATUS_LIVE)
            udfs = [u for u in udfs
                    if u.path == path or path.startswith(u.path + '/')]
            udf = udfs[0] if len(udfs) == 1 else None
        else:
            udf = shard_store.find(
                model.UserVolume,
                model.UserVolume.path == path,
                model.UserVolume.owner_id == self.user.id,
                model.UserVolume.status == model.STATUS_LIVE).one()
        if udf is None:
            raise errors.DoesNotExist(self.udf_dne_error)
        udf_dao = dao.UserVolume(udf, self.user)
        return udf_dao

    @timing_metric
    def delete_udf(self, udf_id):
        """Delete a UDF."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        shard_store = get_shard_store(self.user.shard_id)
        udf = shard_store.find(
            model.UserVolume,
            model.UserVolume.id == udf_id,
            model.UserVolume.owner_id == self.user.id,
            model.UserVolume.status == model.STATUS_LIVE).one()
        if udf is None:
            raise errors.DoesNotExist(self.udf_dne_error)
        info = shard_store.get(model.StorageUserInfo, self.user.id)
        info.lock_for_update()
        node = shard_store.get(model.StorageObject, udf.root_id)
        self.delete_related_shares(node)
        udf.delete()
        udf.status = model.STATUS_DEAD
        udf_dao = dao.UserVolume(udf, self.user)
        self.queue_udf_delete(udf_dao)
        return udf_dao

    @timing_metric
    def get_udf(self, udf_id):
        """Get a UDF."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        shard_store = get_shard_store(self.user.shard_id)
        udf = shard_store.find(
            model.UserVolume,
            model.UserVolume.id == udf_id,
            model.UserVolume.owner_id == self.user.id,
            model.UserVolume.status == model.STATUS_LIVE).one()
        if udf is None:
            raise errors.DoesNotExist(self.udf_dne_error)
        udf_dao = dao.UserVolume(udf, self.user)
        return udf_dao

    @timing_metric
    def get_udfs(self):
        """Return Live UDFs."""
        if not self.user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        shard_store = get_shard_store(self.user.shard_id)
        udfs = shard_store.find(
            model.UserVolume,
            model.UserVolume.owner_id == self.user.id,
            model.UserVolume.path != model.ROOT_USERVOLUME_PATH,
            model.UserVolume.status == model.STATUS_LIVE)
        for udf in udfs:
            udf_dao = dao.UserVolume(udf, self.user)
            yield udf_dao

    @timing_metric
    def get_downloads(self):
        """Get all downloads for a user."""
        store = get_shard_store(self.user.shard_id)
        return [dao.Download(download)
                for download in store.find(
                    model.Download,
                    model.Download.owner_id == self.user.id)]

    @timing_metric
    def get_public_files(self):
        """Get all public files for a user."""
        store = get_shard_store(self.user.shard_id)
        nodes = store.find(
            model.StorageObject,
            model.StorageObject.status == model.STATUS_LIVE,
            model.StorageObject.kind == 'File',
            model.StorageObject.volume_id == model.UserVolume.id,
            model.UserVolume.status == model.STATUS_LIVE,
            model.StorageObject._publicfile_id != None,  # NOQA
            model.UserVolume.owner_id == self.user.id)
        return self._get_dao_nodes(nodes)

    @timing_metric
    def get_public_folders(self):
        """Get all public folders for a user."""
        store = get_shard_store(self.user.shard_id)
        nodes = store.find(
            model.StorageObject,
            model.StorageObject.status == model.STATUS_LIVE,
            model.StorageObject.kind == 'Directory',
            model.StorageObject.volume_id == model.UserVolume.id,
            model.UserVolume.status == model.STATUS_LIVE,
            model.StorageObject._publicfile_id != None,  # NOQA
            model.UserVolume.owner_id == self.user.id)
        return self._get_dao_nodes(nodes)

    def _get_dao_nodes(self, nodes):
        """Return dao.StorageNode for each node in nodes."""
        gws = {}
        perms = {}
        for node in nodes:
            vgw = gws.get(node.volume_id)
            if not vgw:
                if node.volume_id == self.user.root_volume_id:
                    gws[node.volume_id] = self.get_root_gateway()
                else:
                    gws[node.volume_id] = self.get_udf_gateway(node.volume_id)
            yield dao.StorageNode.factory(gws[node.volume_id], node, perms,
                                          owner=self.user)

    @timing_metric
    def get_share_generation(self, share):
        """Get the generation of the speficied share."""
        shard_store = get_shard_store(share.shared_by.shard_id)
        vol = shard_store.find(
            model.UserVolume,
            model.UserVolume.id == model.StorageObject.volume_id,
            model.StorageObject.id == share.root_id).one()
        return vol.generation or 0

    @timing_metric
    def get_photo_directories(self):
        """Get all the directories with photos in them.

        This is written specifically for the photo gallery.
        """

        sql = """
            WITH RECURSIVE t AS (
            SELECT min(CAST(o.parent_id As varchar(50))) AS parent_id
            FROM object o
            WHERE o.owner_id = %(owner_id)s AND
            o.status = E'Live'  AND
            o.content_hash != E'sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709'
            AND o.mimetype IN (E'image/jpeg', E'image/jpg')
            UNION ALL
            SELECT (
                SELECT min(CAST(o.parent_id As varchar(50)))
                FROM object o
                WHERE CAST(o.parent_id As varchar(50)) > t.parent_id AND
                o.owner_id = %(owner_id)s AND
                o.status = E'Live'  AND
                o.content_hash !=
                    E'sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709' AND
                o.mimetype IN (E'image/jpeg', E'image/jpg')
            ) FROM t WHERE t.parent_id IS NOT NULL)
            SELECT o.id, o.volume_id, o.generation, o.generation_created,
                   o.kind, o.name, o.owner_id, o.parent_id, o.path,
                   o.public_uuid, o.status, o.when_created,
                   o.when_last_modified FROM Object o, t, userdefinedfolder u
            WHERE o.id = t.parent_id::UUID AND
                  o.volume_id=u.id AND u.status = E'Live' ;
            """ % dict(owner_id=self.user.id)
        store = get_shard_store(self.user.shard_id)
        nodes = store.execute(SQL(sql))
        gws = {}
        for n in nodes:
            (node_id, volume_id, generation, generation_created, kind, name,
                owner_id, parent_id, path, public_uuid, status, when_created,
                when_last_modified) = n
            node_id = uuid.UUID(node_id)
            volume_id = uuid.UUID(volume_id)
            public_uuid = uuid.UUID(public_uuid) if public_uuid else None
            vgw = gws.get(volume_id)
            if not vgw:
                if volume_id == self.user.root_volume_id:
                    vgw = self.get_root_gateway()
                else:
                    vgw = self.get_udf_gateway(volume_id)
                gws[volume_id] = vgw
            d = dao.DirectoryNode(node_id, vgw)
            d.generation = generation
            d.generation_created = generation_created,
            d.kind = kind
            d.name = name
            d.owner_id = owner_id
            d.parent_id = parent_id
            d.public_uuid = public_uuid
            d.path = path
            d.status = status
            d.volume_id = volume_id
            d.when_created = when_created
            d.when_last_modified = when_last_modified
            d.full_path = pypath.join(d.path, d.name)
            d.can_delete = True
            d.can_write = True
            d.can_read = True
            d._owner = self.user
            yield d

    def _get_reusable_content(self, hash_value, magic_hash):
        """Get a contentblob for reusable content."""
        shard_store = get_shard_store(self.user.shard_id)

        # check to see if we have the content blob for that hash
        contentblob = shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == hash_value).one()

        # if content is not there, is not reusable
        if not contentblob:
            return False, None

        # if we have content have the same magic hash is reusable!
        if magic_hash is not None and contentblob.magic_hash == magic_hash:
            return True, contentblob

        # if not, but the user owns the blob, still can be reusable
        empty = shard_store.find(
            model.StorageObject,
            model.StorageObject._content_hash == hash_value,
            model.StorageObject.owner_id == self.user.id).is_empty()
        if not empty:
            return True, contentblob

        # exists, but it's not reusable
        return True, None

    @timing_metric
    def is_reusable_content(self, hash_value, magic_hash):
        """Check content blob existence and reusability.

        Return a pair of bools: if the blob exists, and if it's reusable.
        """
        reusable, cb = self._get_reusable_content(hash_value, magic_hash)
        if reusable:
            return True, cb.storage_key if cb else None
        return False, None


def with_notifications(f):
    """Decorator for ReadWriteVolumeGateway to send notifications."""
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        """Wrapper method."""
        # quota notification handling
        model.UserVolume.lock_volume(self.shard_store, self.volume_id)
        quota = self._get_quota()
        quota.lock_for_update()
        # call the wrapped method
        result = f(self, *args, **kwargs)
        return result
    return wrapper


class ReadOnlyVolumeGateway(GatewayBase):
    """Data access point for accessing storage data on a volume.

    This includes objects, contentblobs, upload jobs, etc.

    This always accesses Nodes based on the context of a StorageUser in
    that it enforces security from the context of a storage user. If
    this volume is associated with a share, the permissions of the
    share are applied.

    When using the root node, the user will have access to all of
    their objects, even if it's on a UDF
    """

    def __init__(self, user, udf=None, share=None, session_id=None,
                 notifier=None):
        super(ReadOnlyVolumeGateway, self).__init__(session_id=session_id,
                                                    notifier=notifier)
        self.user = user
        self.udf = None
        self.root_id = None
        self.read_only = False
        self.share = None
        self.owner = self.user
        self._volume_id = None
        #root path, used for shares only
        self.root_path_mask = None
        if not user.is_active:
            raise errors.NoPermission(self.inactive_user_error)
        if udf:
            if self.user.id != udf.owner_id or udf.status == model.STATUS_DEAD:
                raise errors.NoPermission("UDF access denied.")
            self.udf = udf
            self._volume_id = udf.id
            self.root_id = udf.root_id
        elif share:
            if (self.user.id != share.shared_to_id or
                    share.status == model.STATUS_DEAD or not share.accepted):
                raise errors.NoPermission("Share access denied.")
            self.owner = share.shared_by
            self.owner._gateway = StorageUserGateway(
                self.owner, session_id=self.session_id)
            self.root_id = share.root_id
            self.read_only = share.read_only
            self.share = share

    @property
    def owner_gateway(self):
        """return the StorageUserGateway for the owner of this volume."""
        return self.owner._gateway

    @property
    def vol_id(self):
        """This is the client volume id of this volume."""
        if self.udf:
            return self.udf.id
        if self.share:
            return self.share.id
        return None

    @property
    def volume_id(self):
        """The id of theUserVolume for this node."""
        if self._volume_id is None:
            if self.udf:
                self._volume_id = self.udf.id
            else:
                self._volume_id = self._get_user_volume().id
        return self._volume_id

    @property
    def shard_store(self):
        """The storm store to use."""
        return get_shard_store(self.owner.shard_id)

    def _get_root_node(self):
        """Get the root node for this volume."""
        if self.share:
            # if this is a share, we just want to check if it's valid.
            # since all share access has to get the root node, this will work.
            self._check_share()

        cond = [model.StorageObject.volume_id == model.UserVolume.id,
                model.StorageObject.status == model.STATUS_LIVE,
                model.UserVolume.owner_id == self.owner.id,
                model.UserVolume.status == model.STATUS_LIVE]
        if self.root_id:
            cond.append(model.StorageObject.id == self.root_id)
            #if this is a UDF, we can make sure it's a valid UDF by joining it
            if self.udf:
                cond.extend([
                    model.UserVolume.id == self.udf.id,
                    model.StorageObject.id == model.UserVolume.root_id])
        else:
            cond.extend([
                model.UserVolume.id == self.owner.root_volume_id,
                model.StorageObject.id == model.UserVolume.root_id])

        node = self.shard_store.find(model.StorageObject, *cond).one()
        if node is None:
            raise errors.DoesNotExist("Could not locate root for the volume.")
        if self.share:
            self.root_path_mask = node.full_path
        self.root_id = node.id
        self._volume_id = node.volume_id
        return node

    def _check_share(self):
        """Make sure the share is still good."""
        if self.share:
            #if this is a share, make sure it's still valid
            user_store = get_user_store()
            share = user_store.find(
                model.Share,
                model.Share.id == self.share.id,
                model.Share.subtree == self.share.root_id,
                model.Share.shared_to == self.user.id,
                model.Share.shared_by == self.owner.id,
                model.Share.shared_by == model.StorageUser.id,
                model.StorageUser.subscription_status == model.STATUS_LIVE,
                model.Share.accepted == True,  # NOQA
                model.Share.status == model.STATUS_LIVE).one()
            if not share:
                raise errors.DoesNotExist(self.share_dne_error)

    @timing_metric
    def get_root(self):
        """Get the root node and return the doa.StorageObject."""
        node = self._get_root_node()
        return dao.StorageNode.factory(self, node, owner=self.owner,
                                       permissions=self._get_node_perms(node))

    def _get_user_volume(self):
        """Return the UserVolume for this ReadWriteVolumeGateway."""
        if self.share:
            vol = self.shard_store.find(
                model.UserVolume,
                model.UserVolume.id == model.StorageObject.volume_id,
                model.StorageObject.status == model.STATUS_LIVE,
                model.StorageObject.id == self.share.root_id,
                model.UserVolume.status == model.STATUS_LIVE).one()

        elif self.udf:
            vol = self.shard_store.find(
                model.UserVolume,
                model.UserVolume.id == self.udf.id,
                model.UserVolume.status == model.STATUS_LIVE).one()
        else:
            vol = self.shard_store.find(
                model.UserVolume,
                model.UserVolume.id == self.owner.root_volume_id,
                model.UserVolume.status == model.STATUS_LIVE).one()
        if vol is None:
            raise errors.DoesNotExist(self.udf_dne_error)
        return vol

    @timing_metric
    def get_user_volume(self):
        """Return the doa.UserVolume for this ReadWriteVolumeGateway."""
        vol = self._get_user_volume()
        return dao.UserVolume(vol, self.owner)

    def _get_root_path(self):
        """Get the path of the root of this volume.

        This only changes for shares as shares may not start at the root.
        """
        if self.share:
            root = self._get_root_node()
            return root.full_path
        else:
            return u'/'

    @property
    def is_root_volume(self):
        """True if this volume is the user's root volume."""
        return (self.user.id == self.owner.id and
                self.share is None and
                self.udf is None)

    def _get_node_perms(self, node):
        """Get the permissions for the node."""
        permissions = {}
        permissions['can_read'] = True
        permissions['can_write'] = not self.read_only
        permissions['can_delete'] = not (
            self.read_only or node.parent_id == model.ROOT_PARENTID or
            node.id == self.root_id)
        return permissions

    def _check_can_write_node(self, node):
        """Raise error if user can't write to this node."""
        perms = self._get_node_perms(node)
        if not perms["can_write"]:
            raise errors.NoPermission(self.cannot_write_error)

    def _is_on_volume_conditions(self):
        """Conditions to only get children of nodes on this volume."""
        conditions = []
        if self.share:
            root = self._get_root_node()
            path_like = model.get_path_startswith(root)
            conditions.extend([
                model.StorageObject.volume_id == self.volume_id,
                Or(model.StorageObject.id == root.id,
                   model.StorageObject.parent_id == root.id,
                   model.StorageObject.path.startswith(path_like))])
        elif self.udf:
            conditions.append(
                model.StorageObject.volume_id == self.udf.id)
        return conditions

    def _get_left_joins(self, with_content, with_parent, with_volume=False):
        """Get the joins for storm queries."""
        included_tables = [model.StorageObject]
        origin = [model.StorageObject]
        if with_content:
            origin.append(LeftJoin(
                model.ContentBlob,
                model.ContentBlob.hash == model.StorageObject._content_hash))
            included_tables.append(model.ContentBlob)
        if with_parent:
            Parent = ClassAlias(model.StorageObject)
            origin.append(LeftJoin(
                Parent,
                Parent.id == model.StorageObject.parent_id))
            included_tables.append(Parent)
        if with_volume:
            origin.append(LeftJoin(
                model.UserVolume,
                model.UserVolume.id == model.StorageObject.volume_id))
            included_tables.append(model.UserVolume)
        return tuple(included_tables), origin

    def _get_node_simple(self, id, live_only=True, with_content=False,
                         with_parent=False, with_volume=False):
        """Just get a StorageObject on this volume."""

        tables, origin = self._get_left_joins(
            with_content, with_parent, with_volume)
        # XXX: Do we need the filter by owner_id here even though
        # StorageObject.id is unique?
        conditions = [model.StorageObject.id == id,
                      model.StorageObject.owner_id == self.owner.id]
        if live_only:
            conditions.append(model.StorageObject.status == model.STATUS_LIVE)
        conditions.extend(self._is_on_volume_conditions())
        result = self.shard_store.using(*origin).find(
            tables, *conditions).one()
        if result is not None:
            result = result[0]
        return result

    def _split_result(self, result):
        """When a result typle comes back with content.

        Results will always return in the order of: StorageObject, ContentBlob
        """
        object = None
        content = None
        parent = None
        if result:
            object = result[0]
            for r in result[1:]:
                if isinstance(r, model.ContentBlob):
                    content = r
                elif isinstance(r, model.StorageObject):
                    parent = r
        return object, content, parent

    def _get_node_from_result(self, result):
        """Get a dao.StorageNode from the result."""
        node, content, parent = self._split_result(result)
        if node:
            if content:
                content = dao.FileNodeContent(content)
            return dao.StorageNode.factory(
                self, node, owner=self.owner,
                permissions=self._get_node_perms(node),
                content=content)

    def _node_finder(self, extra_conditions, with_content, with_parent=False):
        """Find nodes based on joins and conditions all in one query."""
        conditions = [model.StorageObject.owner_id == self.owner.id,
                      model.StorageObject.status == 'Live']
        conditions.extend(self._is_on_volume_conditions())
        conditions.extend(extra_conditions)
        tables, origin = self._get_left_joins(
            with_content, with_parent=with_parent)
        store = self.shard_store.using(*origin)
        return store.find(tables, *conditions)

    def _get_kind_conditions(self, kind):
        """Get conditions for finding files by kind."""
        if kind is None:
            return []
        if kind in ['Directory', 'File']:
            return [model.StorageObject.kind == kind]
        raise errors.StorageError("Invalid Kind specified")

    def _get_node(self, id, kind=None, with_content=False, with_parent=False):
        """A common function used to get a node.

        Can optionally get related objects at the same time.
        """
        conditions = [model.StorageObject.id == id]
        conditions.extend(self._get_kind_conditions(kind))
        result = self._node_finder(conditions, with_content, with_parent)
        return result.one()

    def _get_children(self, id, kind=None, with_content=False, mimetypes=None):
        """A common function used to get the children of a node."""
        conditions = [model.StorageObject.parent_id == id]
        conditions.extend(self._get_kind_conditions(kind))
        if mimetypes:
            conditions.append(model.StorageObject.mimetype.is_in(mimetypes))
        return self._node_finder(conditions, with_content)

    def _get_quota(self):
        """Return the storm model quota info object for this volume"""
        return self.shard_store.get(model.StorageUserInfo, self.owner.id)

    def get_quota(self):
        """Return the dao.UserInfo for this volume."""
        return dao.UserInfo(self._get_quota())

    @timing_metric
    def get_generation_delta(self, generation, limit=None):
        """Get nodes since a generation."""
        if self.share:
            root = self._get_root_node()
            # if this is a share, get the delta from ShareVolumeDelta
            # ShareVolumeDelta is a Union of Objects and MoveFromShares

            # first get_left_joins for ShareVolumeDelta
            tables = (model.ShareVolumeDelta, model.ContentBlob)
            joins = LeftJoin(model.ShareVolumeDelta, *(
                model.ContentBlob,
                model.ShareVolumeDelta._content_hash == model.ContentBlob.hash
            ))
            store = self.shard_store.using(joins)
            # Must have the same owner id
            conditions = [
                model.ShareVolumeDelta.owner_id == self.owner.id,
                # Must not return the root node
                model.ShareVolumeDelta.id != root.id,
                model.ShareVolumeDelta.generation > generation,
                # The union includes a share_id but only
                # MovesFromShare have a share_id this will include the rows
                # from both that meet all other criteria
                Or(model.ShareVolumeDelta.share_id == None,  # NOQA
                   model.ShareVolumeDelta.share_id == self.share.id)]
            # we only want to get nodes within the path of the shared node
            path_like = model.get_path_startswith(root)
            conditions.extend([
                model.ShareVolumeDelta.volume_id == self.volume_id,
                # The path_like will be path.like('/path/with/closing/slash/%')
                # which not match the path of children directly in the root
                # so parent_id == root.id must be also be included.
                Or(model.ShareVolumeDelta.parent_id == root.id,
                   model.ShareVolumeDelta.path.startswith(path_like))])
            children = store.find(tables, *conditions).order_by(
                model.ShareVolumeDelta.generation)
        else:
            tables, origin = self._get_left_joins(
                with_content=True, with_parent=False)
            store = self.shard_store.using(*origin)
            conditions = [
                model.StorageObject.owner_id == self.owner.id,
                model.StorageObject.parent_id != None,  # NOQA
                model.StorageObject.volume_id == self.volume_id,
                model.StorageObject.generation > generation]
            conditions.extend(self._is_on_volume_conditions())
            children = store.find(tables, *conditions).order_by(
                model.StorageObject.generation)
        for node, content in children[:limit]:
            if content:
                content = dao.FileNodeContent(content)
            yield dao.StorageNode.factory(
                self, node, content=content,
                owner=self.owner, permissions=self._get_node_perms(node))

    @timing_metric
    def get_node(self, id, verify_hash=None, with_content=False):
        """Get one of the user's nodes."""
        if id == 'root':
            id = self._get_root_node().id
        result = self._get_node(id, with_content=with_content)
        node = self._get_node_from_result(result)
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        if verify_hash and node.content_hash != verify_hash:
            raise errors.HashMismatch(self.hash_mismatch_error)
        return node

    @timing_metric
    def get_node_by_path(self, full_path, kind=None, with_content=None):
        """Get a node based on the path.

        path is a path relative to the volume's root. So a shared
        directory will start relative to the path of the directory.
        """
        if full_path == '/':
            return self.get_root()
        if len(full_path) == 0:
            raise errors.StorageError("Invalid path provided %s" % full_path)
        #join it together with the path for the root of this volume,
        #this is necessary mostly for shares
        root_path = self._get_root_path()
        full_path = os.path.join(root_path, full_path.strip('/'))
        path, name = os.path.split(full_path)
        conditions = [model.StorageObject.path == path,
                      model.StorageObject.name == name]
        conditions.extend(self._get_kind_conditions(kind))
        #this is a little different that typical finds. Since paths can be
        #duplicated across udfs and root, we need to make sure that if
        #this is a root volume, it doesnt' collide with udfs.
        conditions.append(model.StorageObject.volume_id == self.volume_id)
        result = self._node_finder(conditions, with_content)
        node = self._get_node_from_result(result.one())
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        return node

    @timing_metric
    def get_all_nodes(self, mimetypes=None, kind=None, with_content=False,
                      start_from_path=None, limit=None, max_generation=None):
        """Get all nodes from this volume."""
        conditions = self._get_kind_conditions(kind)
        if mimetypes:
            conditions.append(model.StorageObject.mimetype.is_in(mimetypes))
        # A temporary hack, because we don't want this crossing volumes
        if self.is_root_volume:
            conditions.append(
                model.StorageObject.volume_id == self.owner.root_volume_id)
        if max_generation:
            conditions.append(model.StorageObject.generation <= max_generation)
        if start_from_path:
            # special case for shares, as the "root" isn't "/" and we need to
            # get the root path+name
            if self.share:
                root = self._get_root_node()
                real_path = model.get_path_startswith(root)
                # only strip the rightmost "/" if isn't the root of the sharer
                # volume.
                if real_path != '/':
                    real_path = model.get_path_startswith(root).rstrip("/")
                if start_from_path[0] != '/':
                    real_path = pypath.join(real_path,
                                            start_from_path[0].lstrip("/"))
                start_from_path = (real_path, start_from_path[1])
            # same path AND greater name OR greater path
            same_path = And(model.StorageObject.path == start_from_path[0],
                            model.StorageObject.name > start_from_path[1])
            conditions.append(Or(same_path,
                              model.StorageObject.path > start_from_path[0]))
        results = self._node_finder(conditions, with_content).order_by(
            model.StorageObject.path, model.StorageObject.name)
        if limit:
            results = results[:limit]
        with db_timeout(TRANSACTION_MAX_TIME, force=True):
            return list(self._get_node_from_result(n) for n in results)

    @timing_metric
    def get_deleted_files(self, start=0, limit=100):
        """Get Dead files on this volume.

        Files will be returned in descending order by date, path, name
        """
        conditions = self._is_on_volume_conditions()
        conditions.extend([model.StorageObject.status == model.STATUS_DEAD,
                           model.StorageObject.kind == 'File',
                           model.StorageObject.owner_id == self.owner.id])
        nodes = self.shard_store.find(
            model.StorageObject, *conditions
        ).order_by(
            Desc(model.StorageObject.when_last_modified),
            model.StorageObject.path,
            model.StorageObject.name)
        nodes.config(offset=start, limit=limit)
        return [dao.StorageNode.factory(self, n, owner=self.owner,
                                        permissions=self._get_node_perms(n))
                for n in nodes]

    @timing_metric
    def get_children(self, id, kind=None, with_content=False, mimetypes=None):
        """Get all the nodes children."""
        children = self._get_children(
            id, kind=kind, with_content=with_content,
            mimetypes=mimetypes)
        for child in children.order_by(model.StorageObject.name):
            yield self._get_node_from_result(child)

    @timing_metric
    def get_child_by_name(self, id, name, with_content=False):
        """Get a Child by Name returning a StorageNode."""
        children = self._get_children(id, with_content=with_content)
        node = children.find(model.StorageObject.name == name).one()
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        return self._get_node_from_result(node)

    @timing_metric
    def get_content(self, content_hash):
        """Get the ContentBlob."""
        content = self.shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == content_hash,
            model.ContentBlob.status == model.STATUS_LIVE).one()
        if content is None:
            raise errors.DoesNotExist(self.contentblob_dne_error)
        return dao.FileNodeContent(content)

    def _get_uploadjob(self, id):
        """Get a model.UploadJob belonging to this owner."""
        job = self.shard_store.find(
            model.UploadJob,
            model.UploadJob.uploadjob_id == id,
            model.UploadJob.status == model.STATUS_LIVE,
            model.UploadJob.storage_object_id == model.StorageObject.id,
            model.StorageObject.owner_id == self.owner.id).one()
        if job is None:
            raise errors.DoesNotExist(self.uploadjob_dne_error)
        return job

    @timing_metric
    def get_uploadjob(self, id):
        """Get an uploadjob."""
        job = self._get_uploadjob(id)
        return dao.UploadJob(job, gateway=self)

    @timing_metric
    def get_user_uploadjobs(self, node_id=None):
        """Get an uploadjob."""
        conditions = [
            model.UploadJob.status == model.STATUS_LIVE,
            model.UploadJob.storage_object_id == model.StorageObject.id,
            model.StorageObject.owner_id == self.owner.id]
        if node_id is not None:
            conditions.append(model.UploadJob.storage_object_id == node_id)
        for job in self.shard_store.find(model.UploadJob, *conditions):
            yield dao.UploadJob(job, gateway=self)

    @timing_metric
    def get_user_multipart_uploadjob(self, node_id, upload_id, hash_hint=None,
                                     crc32_hint=None,
                                     inflated_size_hint=None,
                                     deflated_size_hint=None):
        """Get multipart uploadjob."""
        conditions = [
            model.UploadJob.status == model.STATUS_LIVE,
            model.UploadJob.multipart_key == upload_id,
            model.UploadJob.storage_object_id == model.StorageObject.id,
            model.StorageObject.owner_id == self.owner.id,
            model.StorageObject.id == node_id,
            model.UploadJob.multipart_id != None]  # NOQA
        node = self._get_node_simple(node_id)
        self._check_can_write_node(node)
        if hash_hint is not None:
            conditions.append(model.UploadJob.hash_hint == hash_hint)
        if crc32_hint is not None:
            conditions.append(model.UploadJob.crc32_hint == crc32_hint)
        if inflated_size_hint is not None:
            conditions.append(
                model.UploadJob.inflated_size_hint == inflated_size_hint)
        if deflated_size_hint is not None:
            conditions.append(
                model.UploadJob.deflated_size_hint == deflated_size_hint)
        job = self.shard_store.find(model.UploadJob, *conditions).one()
        if job is None:
            raise errors.DoesNotExist(self.uploadjob_dne_error)
        # load the requested content using the hash_hint
        new_content = self.shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == hash_hint,
            model.ContentBlob.status == model.STATUS_LIVE).one()
        if new_content:
            new_content = dao.FileNodeContent(new_content)
        fnode = dao.StorageNode.factory(self, node, content=new_content,
                                        permissions={})
        return dao.UploadJob(job, file=fnode, gateway=self)

    @timing_metric
    def get_directories_with_mimetypes(self, mimetypes):
        """Get directories that have files with mimetype in mimetypes."""
        ParentObject = ClassAlias(model.StorageObject)
        conditions = [
            ParentObject.id == model.StorageObject.parent_id,
            model.StorageObject.owner_id == self.owner.id,
            model.StorageObject.status == 'Live',
            model.StorageObject._content_hash != model.EMPTY_CONTENT_HASH,
            model.StorageObject.mimetype.is_in(mimetypes)]
        if self.is_root_volume:
            conditions.append(
                model.StorageObject.volume_id == self.owner.root_volume_id)
        conditions.extend(self._is_on_volume_conditions())
        result = self.shard_store.find(
            ParentObject, *conditions).config(distinct=True)
        return [dao.StorageNode.factory(
                self, d, owner=self.owner) for d in result]

    @timing_metric
    def check_has_children(self, id, kind):
        """Find out if the node has children with kind == kind."""
        return not self._get_children(id, kind=kind).is_empty()


class ReadWriteVolumeGateway(ReadOnlyVolumeGateway):
    """Provide Write access to the Volume."""

    def handle_node_change(self, node):
        """Send new generation notifs."""
        # send updates to the owner
        if node.volume_id == self.owner.root_volume_id:
            volume_id = None
        else:
            volume_id = node.volume.id
        self.queue_new_generation(self.owner.id, volume_id,
                                  node.volume.generation)

        # send node updates to all shares of this node.
        nodeids = node.get_parent_ids()
        if node.kind == 'Directory':
            nodeids.append(node.id)
        # need to use the owner's user gateway.
        for s in self.owner_gateway.get_shares_of_nodes(nodeids):
            self.queue_new_generation(s.shared_to.id, s.id,
                                      node.volume.generation)

    def _make_content(self, hash, crc32, size, deflated_size,
                      storage_key, magic_hash):
        """Make a content blob."""
        content = model.ContentBlob()
        content.hash = hash
        content.magic_hash = magic_hash
        content.crc32 = crc32
        content.size = size
        content.deflated_size = deflated_size
        content.status = model.STATUS_LIVE
        if storage_key:
            content.storage_key = storage_key
        self.shard_store.add(content)
        self.shard_store.flush()
        return content

    def _get_directory_node(self, id, for_write=True):
        """Get a directory node so it can be modified."""
        if self.read_only and for_write:
            raise errors.NoPermission(self.cannot_write_error)

        conditions = [model.StorageObject.owner_id == self.owner.id,
                      model.StorageObject.status == 'Live',
                      model.StorageObject.id == id]
        conditions.extend(self._is_on_volume_conditions())
        node = self.shard_store.find(model.StorageObject, *conditions).one()
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        if node.kind != 'Directory':
            raise errors.NotADirectory(self.not_a_directory_error)
        if for_write:
            self._check_can_write_node(node)
        return node

    @with_notifications
    @timing_metric
    def make_file(self, parent_id, name, hash=None, magic_hash=None):
        """Make a file."""
        reusable = None
        blob = None
        make_new = False
        if hash:
            reusable, blob = self.owner_gateway._get_reusable_content(
                hash, magic_hash)
            if not reusable or not blob:
                raise errors.HashMismatch("The content could not be reused.")

        parent = self._get_directory_node(parent_id)
        newfile = parent.get_child_by_name(name)
        if newfile is None:
            make_new = True
            newfile = parent.make_file(name)
            mime = mimetypes.guess_type(name)
            if mime[0] is not None:
                mime = unicode(mime[0])
                newfile.mimetype = mime
        elif newfile.kind != 'File':
            raise errors.AlreadyExists(
                "Node already exists but is not a File.")
        if blob:
            # if there's content we'll update the content. This will also
            # trigger the notifications
            self._update_node_content(newfile, blob, make_new, True)
        elif make_new:
            # if we make a new file, we need to queue a node change
            self.handle_node_change(parent)
        return dao.StorageNode.factory(
            self, newfile, owner=self.owner,
            permissions=self._get_node_perms(newfile))

    @with_notifications
    @timing_metric
    def make_subdirectory(self, parent_id, name):
        """Make a subdirectory."""
        parent = self._get_directory_node(parent_id)
        newdir = parent.get_child_by_name(name)
        if newdir is None:
            newdir = parent.make_subdirectory(name)
            self.handle_node_change(parent)
        elif newdir.kind != 'Directory':
            raise errors.AlreadyExists("Node already exists"
                                       " but is not a Directory.")
        return dao.StorageNode.factory(
            self, newdir, owner=self.owner,
            permissions=self._get_node_perms(newdir))

    @with_notifications
    @timing_metric
    def make_tree(self, parent_id, path):
        """Create a directory structure from the path passed in."""
        parent = self._get_directory_node(parent_id)
        if path not in ("", "/"):
            newdir = parent.build_tree_from_path(path)
            self.handle_node_change(newdir.parent)
        else:
            newdir = parent
        return dao.StorageNode.factory(
            self, newdir, owner=self.owner,
            permissions=self._get_node_perms(newdir))

    @timing_metric
    def make_share(self, node_id, name, user_id=None, email=None,
                   readonly=True):
        """Create a direct share or a share offer."""
        assert user_id is not None or email, "user_id or an email required"
        if self.share:
            raise errors.NoPermission("Shares can not be nested.")
        to_user = None
        if user_id:
            to_user = self.get_user(user_id)
            if to_user is None or not to_user.is_active:
                raise errors.DoesNotExist(self.user_dne_error)
        node = self._get_directory_node(node_id, for_write=False)
        access_level = 'View' if readonly else 'Modify'
        share = model.Share(self.user.id, node.id, user_id, name, access_level,
                            email=email)
        self.user_store.add(share)
        share_dao = dao.SharedDirectory(
            share, by_user=self.user, to_user=to_user)
        share_dao._gateway = self.user._gateway
        self.queue_share_created(share_dao)
        return share_dao

    @with_notifications
    @timing_metric
    def delete_node(self, node_id, cascade=False):
        """Decorated _delete_node."""
        return self._delete_node(node_id, cascade)

    def _delete_node(self, node_id, cascade):
        """Delete a node."""
        if self.read_only:
            raise errors.NoPermission(self.readonly_error)

        node = self._get_node_simple(
            node_id, with_parent=True, with_content=True, with_volume=True)

        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        if not self._get_node_perms(node)["can_delete"]:
            raise errors.NoPermission(self.cannot_delete_error)
        if node.status == model.STATUS_DEAD:
            return
        if node.kind == 'Directory':
            self._delete_directory_node(node, cascade=cascade)
        else:
            self._delete_file_node(node)
        return dao.StorageNode.factory(self, node, owner=self.owner,
                                       permissions={})

    def _delete_file_node(self, node):
        """Internal function to delete a file node."""
        node.unlink()
        self.handle_node_change(node.parent)

    def _delete_directory_node(self, node, cascade=False):
        """Internal function to delete a directory node."""
        parent = self.shard_store.get(model.StorageObject, node.parent_id)
        self.owner_gateway.delete_related_shares(node)
        if cascade:
            node.unlink_tree()
        else:
            node.unlink()
        self.handle_node_change(parent)

    @with_notifications
    @timing_metric
    def restore_node(self, node_id, cascade=False):
        """Restore a deleted a node."""
        if self.read_only:
            raise errors.NoPermission(self.readonly_error)
        node = self._get_node_simple(node_id, live_only=False)
        if node.status == model.STATUS_LIVE:
            return
        node.undelete()
        parent = self._get_node_simple(node.parent_id)
        if parent:
            self.handle_node_change(parent)
        return dao.StorageNode.factory(self, node,
                                       owner=self.owner, permissions={})

    def _make_moves_from_shares(self, node, old_name, old_parent, new_parent):
        """Create moves from shares."""
        #get all accepted shares
        shares = self.owner_gateway.get_shared_by(accepted=True)
        share_info = {}
        for s in shares:
            share_info.setdefault(s.root_id, []).append(s.id)
        #if the user has no shares, just quit
        if not share_info:
            return
        #get the shared paths:
        shared_node_ids = share_info.keys()
        nodes = self.shard_store.find(
            model.StorageObject,
            model.UserVolume.id == model.StorageObject.volume_id,
            model.UserVolume.status == model.STATUS_LIVE,
            model.StorageObject.status == model.STATUS_LIVE,
            model.StorageObject.id.is_in(shared_node_ids)
        ).values(model.StorageObject.id,
                 model.StorageObject.path,
                 model.StorageObject.name)
        shared_paths = {}
        for id, path, name in nodes:
            shares = share_info[id]
            shared_paths[pypath.join(path, name)] = shares
        #make sure we have live nodes from these shares:
        if not shared_paths:
            return
        # get the paths that can see this node now.
        see_now = []
        see_after = []
        for path, share_ids in shared_paths.iteritems():
            if old_parent.full_path.startswith(path):
                see_now.extend(share_ids)
            if new_parent.full_path.startswith(path):
                see_after.extend(share_ids)
        # first delete any MoveFromShare where the share is going to see it
        # after the move
        self.shard_store.find(
            model.MoveFromShare,
            model.MoveFromShare.id == node.id,
            model.MoveFromShare.share_id.is_in(see_after)).remove()
        if see_now:
            # create a MoveFromShare for all the nodes shares that will
            # no longer see it
            for share_id in set(see_now).difference(set(see_after)):
                m = model.MoveFromShare.from_move(node, share_id)
                m.parent_id = old_parent.id
                m.name = old_name
                self.shard_store.add(m)

    @with_notifications
    @timing_metric
    def move_node(self, node_id, parent_id, new_name):
        """Move a node to a new parent, or rename it."""
        if self.read_only:
            raise errors.NoPermission(self.readonly_error)
        node = self._get_node_simple(node_id)
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        is_move = node.parent_id != parent_id
        if not is_move and node.name == new_name:
            return dao.StorageNode.factory(
                self, node, owner=self.owner,
                permissions=self._get_node_perms(node))
        new_parent = self._get_directory_node(parent_id)
        if is_move:
            old_parent = self._get_directory_node(node.parent_id)
            if not self._get_node_perms(node)["can_delete"]:
                raise errors.NoPermission(self.cannot_delete_error)
            if (node.kind == 'Directory' and
                    new_parent.full_path.startswith(node.full_path + '/')):
                raise errors.NoPermission("Can't move a directory to a child.")

        with db_timeout(TRANSACTION_MAX_TIME, force=True):
            # make room for the move, delete children with the same name
            conflicting_node = new_parent.get_child_by_name(new_name)
            if conflicting_node:
                self._delete_node(conflicting_node.id, cascade=True)

            old_name = node.name
            node.move(parent_id, new_name)
            if node.kind == 'File':
                mime = mimetypes.guess_type(node.name)[0]
                node.mimetype = unicode(mime) if mime else None
            if is_move:
                self._make_moves_from_shares(node, old_name,
                                             old_parent, new_parent)
            self.handle_node_change(new_parent)
        return dao.StorageNode.factory(self, node,
                                       owner=self.owner,
                                       permissions=self._get_node_perms(node))

    def _update_node_content(self, fnode, content, new, enforce):
        """Reusable function for updating file content."""
        old_content = fnode.content
        if fnode.content_hash == content.hash:
            return dao.StorageNode.factory(
                self, fnode,
                content=dao.FileNodeContent(old_content),
                owner=self.owner,
                permissions=self._get_node_perms(fnode))
        quota = self._get_quota()
        existing_size = old_content.size if old_content else 0
        if enforce and (content.size - existing_size > quota.free_bytes):
            raise errors.QuotaExceeded("Upload will exceed quota.",
                                       self.vol_id, quota.free_bytes)
        fnode.content = content
        content = dao.FileNodeContent(content)
        if new:
            self.handle_node_change(fnode.parent)
        else:
            self.handle_node_change(fnode)
        return dao.StorageNode.factory(self, fnode, content=content,
                                       owner=self.owner,
                                       permissions=self._get_node_perms(fnode))

    @timing_metric
    def make_uploadjob(self, node_id, node_hash, new_hash, crc32,
                       inflated_size, deflated_size, enforce_quota=True,
                       multipart_id=None, multipart_key=None):
        """Create an upload job for a FileNode."""
        if self.read_only:
            raise errors.NoPermission(self.readonly_error)
        node = self._get_node_simple(node_id)
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        if node.kind == 'Directory':
            raise NotImplementedError(
                "Uploading Directories is not supported.")
        self._check_can_write_node(node)
        # has file changed?
        if node.content_hash != node_hash and node.content_hash != new_hash:
            raise errors.HashMismatch("The file has changed.")

        old_content = self.shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == node.content_hash,
            model.ContentBlob.status == model.STATUS_LIVE).one()
        existing_size = old_content.size if old_content else 0

        #reload the owner to get the latest quota
        quota = self._get_quota()
        if enforce_quota and \
           (inflated_size - existing_size > quota.free_bytes):
            raise errors.QuotaExceeded("Upload will exceed quota.",
                                       self.vol_id, quota.free_bytes)

        # if we have multipart_key defined it's multipart
        if multipart_key:
            upload = model.UploadJob.new_multipart_uploadjob(
                self.shard_store, node.id, multipart_id, multipart_key)
        else:
            upload = model.UploadJob.new_uploadjob(self.shard_store, node.id)
        upload.hash_hint = new_hash
        upload.crc32_hint = crc32
        upload.inflated_size_hint = inflated_size
        upload.deflated_size_hint = deflated_size
        self.shard_store.flush()
        new_content = self.shard_store.find(
            model.ContentBlob,
            model.ContentBlob.hash == new_hash,
            model.ContentBlob.status == model.STATUS_LIVE).one()
        if new_content:
            new_content = dao.FileNodeContent(new_content)
        fnode = dao.StorageNode.factory(
            self, node, content=new_content,
            owner=self.owner, permissions={})
        upload_dao = dao.UploadJob(upload, file=fnode, gateway=self)
        return upload_dao

    @with_notifications
    @timing_metric
    def make_file_with_content(self, parent_id, name, hash, crc32, size,
                               deflated_size, storage_key, mimetype=None,
                               enforce_quota=True, is_public=False,
                               previous_hash=None, magic_hash=None):
        """Create or update a file with the given content.

        If a file node with name == name as a child of parent_id it will get
        updated with the new content.
        """
        parent = self._get_directory_node(parent_id)
        fnode = parent.get_child_by_name(name)
        is_new = False
        if fnode is None:
            is_new = True
            fnode = parent.make_file(name)
        elif fnode.kind != 'File':
            raise errors.AlreadyExists(
                "Node already exists but is not a File.")
        elif previous_hash and fnode.content_hash != previous_hash:
            raise errors.HashMismatch("File hash has changed.")

        if mimetype is None:
            mime = mimetypes.guess_type(name)[0]
            mimetype = unicode(mime) if mime else None
        if mimetype:
            fnode.mimetype = mimetype

        content = self.shard_store.get(model.ContentBlob, hash)
        if content is None:
            content = self._make_content(hash, crc32, size, deflated_size,
                                         storage_key, magic_hash)
        else:
            if content.magic_hash is None:
                # update magic hash now that we have it!
                content.magic_hash = magic_hash
        if is_public:
            self._make_public(fnode)
        # do we even need to do this?
        if fnode.content_hash == hash:
            return dao.StorageNode.factory(
                self, fnode,
                content=dao.FileNodeContent(fnode.content),
                owner=self.owner,
                permissions=self._get_node_perms(fnode))

        return self._update_node_content(fnode, content, is_new, enforce_quota)

    @with_notifications
    @timing_metric
    def make_content(self, file_id, original_hash, hash_hint, crc32_hint,
                     inflated_size_hint, deflated_size_hint,
                     storage_key, magic_hash=None):
        """Make content (if necessary) and update the magic hash (if have it).

        If there is no storage_key, we must have an existing content.
        """
        fnode = self._get_node_simple(file_id)
        if fnode is None:
            raise errors.DoesNotExist("The file no longer exists.")
        if fnode.content_hash != original_hash:
            raise errors.HashMismatch("The file's hash has changed.")
        self._check_can_write_node(fnode)
        content = self.shard_store.get(model.ContentBlob, hash_hint)
        if content is None:
            if storage_key is None:
                # we must have content since we have no storage_key
                raise errors.ContentMissing("The content does not exist.")
            content = self._make_content(
                hash_hint, crc32_hint, inflated_size_hint,
                deflated_size_hint, storage_key, magic_hash)
        else:
            if content.magic_hash is None:
                # update magic hash now that we have it!
                content.magic_hash = magic_hash
        return self._update_node_content(fnode, content, False, True)

    @timing_metric
    def delete_uploadjob(self, id):
        """Delete an upload job."""
        job = self._get_uploadjob(id)
        self.shard_store.remove(job)
        upload_dao = dao.UploadJob(job, gateway=self)
        return upload_dao

    @timing_metric
    def add_uploadjob_part(self, job_id, size, inflated_size, crc32,
                           hash_context, magic_hash_context,
                           decompress_context):
        """Add a part to an uploadjob with: size"""
        job = self._get_uploadjob(job_id)
        job.add_part(size, inflated_size, crc32, hash_context,
                     magic_hash_context, decompress_context)
        return dao.UploadJob(job, gateway=self)

    @timing_metric
    def set_uploadjob_multpart_id(self, job_id, multipart_id):
        """Set the multipart_id to the specified upload job."""
        job = self._get_uploadjob(job_id)
        job.multipart_id = multipart_id
        return dao.UploadJob(job, gateway=self)

    @timing_metric
    def set_uploadjob_when_last_active(self, job_id, datetime):
        """Set when_last_active to datetime.utcnow()."""
        job = self._get_uploadjob(job_id)
        job.when_last_active = datetime
        return dao.UploadJob(job, gateway=self)

    def _make_public(self, fnode):
        """Internal function to make a file public."""
        if fnode.publicfile_id is None:
            # Create a public file ID, if one does not already exist.
            publicfile = self.user_store.find(model.PublicNode,
                                              node_id=fnode.id).one()
            if publicfile is None:
                publicfile = self.user_store.add(
                    model.PublicNode(fnode.id, fnode.owner_id))
                # Flush the store to ensure the new PublicNode has
                # a database ID.
                self.user_store.flush()
            if (utils.set_public_uuid and fnode.public_uuid is None):
                fnode.public_uuid = uuid.uuid4()
                publicfile.public_uuid = fnode.public_uuid
            fnode.publicfile_id = publicfile.id

    @with_notifications
    @timing_metric
    def change_public_access(self, node_id, is_public, allow_directory=False):
        """Sets whether a node should be publicly available."""
        #we don't let user's make shared files public
        if self.share:
            raise errors.NoPermission("Can't make shared files public.")

        fnode = self._get_node_simple(node_id)
        if fnode is None:
            raise errors.DoesNotExist(self.node_dne_error)
        if fnode.kind != 'File' and allow_directory is False:
            raise errors.NoPermission("Only files can be made public.")

        if (fnode.publicfile_id is not None) != is_public:
            if is_public:
                self._make_public(fnode)
            else:
                fnode.publicfile_id = None
            self.handle_node_change(fnode)
        return dao.StorageNode.factory(
            self, fnode, owner=self.owner,
            permissions=self._get_node_perms(fnode))

    @timing_metric
    def get_node_parent_ids(self, node_id):
        """Get the parents of this node id."""
        node = self._get_node_simple(node_id)
        if node is None:
            raise errors.DoesNotExist(self.node_dne_error)
        return node.get_parent_ids()

    @with_notifications
    @timing_metric
    def undelete_volume(self, name, limit=100):
        """Undelete all user's data."""
        if self.user.id != self.owner.id:
            raise errors.NoPermission("You can only undelete your own files")
        #get the user's root volume
        root = self._get_root_node()
        parent = root.get_child_by_name(name)
        if parent and parent.kind != 'Directory':
            name = root.get_unique_childname(name)
            parent = None
        if parent is None:
            parent = root.make_subdirectory(name)
        model.undelete_volume(
            self.shard_store, self.owner.id, self.volume_id, parent,
            limit=limit)
        return dao.StorageNode.factory(self, parent, owner=self.owner)


def fix_udfs_with_generation_out_of_sync(store, user_ids, logger):
    """Find the UDFs that have an object whose generation is higher than the
    UDF's and update the UDF's generation to be the same as the Object's.

    Only UDFs owned by the given users are considered.
    """
    results = store.find(
        (model.UserVolume, model.StorageObject),
        model.UserVolume.id == model.StorageObject.volume_id,
        model.StorageObject.generation > model.UserVolume.generation,
        model.UserVolume.owner_id.is_in(user_ids))
    for udf, obj in results:
        # The query above will return all of a UDF's objects that have a higher
        # generation than the UDF itself, so we have this if block here to
        # make sure the UDF ends up with the highest generation of them all.
        if obj.generation > udf.generation:
            logger.info("Updating the generation of %s from %s to %s" % (
                udf.id, udf.generation, obj.generation))
            udf.generation = obj.generation


def fix_all_udfs_with_generation_out_of_sync(shard_id, logger, sleep=0,
                                             dry_run=False, batch_size=500):
    from backends.filesync.data.dbmanager import (
        get_shard_store, get_user_store)
    if dry_run:
        logger.info("Dry-run enabled; not committing any changes.")
    store = get_shard_store(shard_id)
    user_store = get_user_store()
    query = "SELECT id FROM StorageUser WHERE shard_id = '%s'" % shard_id
    user_ids = [row[0] for row in user_store.execute(query)]
    start = time.time()
    total_users = len(user_ids)
    total_done = 0
    while user_ids:
        batch = user_ids[:batch_size]
        user_ids = user_ids[batch_size:]
        fix_udfs_with_generation_out_of_sync(store, batch, logger)
        if dry_run:
            store.rollback()
        else:
            store.commit()
        total_time = time.time() - start
        total_done += len(batch)
        fraction_done = total_done / float(total_users)
        eta = (total_time / fraction_done) - total_time
        logger.info(
            "Processed UDFs for %.2f%% of this shard's users in %d seconds. "
            "ETA: %d seconds"
            % (fraction_done * 100, total_time, eta))
        time.sleep(sleep)
