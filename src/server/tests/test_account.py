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

"""Account info tests."""

from twisted.internet import defer

from ubuntuone.storageprotocol import request
from ubuntuone.storage.server.testing.testcase import TestWithDatabase
from backends.filesync.data import model, services, dbmanager


class QuotaTest(TestWithDatabase):
    """Test account and quota info."""

    def test_quota(self):
        """Test quota info """
        self.usr0.update(max_storage_bytes=2 ** 16)
        usr2 = services.make_storage_user(1, u"otheruser",
                                          u"Other User", 2 ** 17)
        share = usr2.root.share(self.usr0.id, u"a share", readonly=True)

        @defer.inlineCallbacks
        def do_test(client):
            """Do the actual test."""
            usr1 = self.usr0
            quota = usr1.get_quota()
            yield client.dummy_authenticate("open sesame")
            result = yield client.get_free_space(request.ROOT)
            self.assertEqual(quota.free_bytes, result.free_bytes)
            self.assertEqual(request.ROOT, result.share_id)
            result = yield client.get_free_space(str(share.id))
            quota = usr2.get_quota()
            self.assertEqual(quota.free_bytes, result.free_bytes)
            self.assertEqual(str(share.id), result.share_id)
        return self.callback_test(do_test,
                                  add_default_callbacks=True)

    def test_over_quota(self):
        """Test that 0 bytes free (versus a negative number) is reported
        when over quota."""
        self.usr0.update(max_storage_bytes=2 ** 16)
        #need to do something that just can't happen normally
        store = dbmanager.get_shard_store(self.usr0.shard_id)
        info = store.get(model.StorageUserInfo, 0)
        info.used_storage_bytes = 2 ** 17
        store.commit()

        @defer.inlineCallbacks
        def do_test(client):
            """Do the actual test."""
            yield client.dummy_authenticate("open sesame")
            result = yield client.get_free_space(request.ROOT)
            self.assertEqual(0, result.free_bytes)
            self.assertEqual(request.ROOT, result.share_id)
        return self.callback_test(do_test,
                                  add_default_callbacks=True)

    def test_account_info(self):
        """Test account info."""
        usr1 = self.usr0
        usr1.update(max_storage_bytes=2 ** 16)

        @defer.inlineCallbacks
        def do_test(client):
            """Do the actual test."""
            yield client.dummy_authenticate("open sesame")
            result = yield client.get_account_info()
            quota = usr1.get_quota()
            self.assertEqual(quota.max_storage_bytes, result.purchased_bytes)
        return self.callback_test(do_test, add_default_callbacks=True)
