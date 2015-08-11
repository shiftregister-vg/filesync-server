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

"""Test storage notifier."""

import unittest
import uuid

from backends.filesync.notifier import notifier
from backends.filesync.notifier.notifier import (
    EventNotifier,
    PendingNotifications,
    ShareAccepted,
    ShareCreated,
    ShareDeclined,
    ShareDeleted,
    UDFCreate,
    UDFDelete,
    VolumeNewGeneration,
)

from transaction import ThreadTransactionManager


class FakeNode(object):
    """Trivial fake storage node."""

    def __init__(self, owner_id=None, id=None):
        """Initialize an instance and give it an id."""
        self.owner_id = owner_id or 1
        self.id = id or uuid.uuid4()


class FakeShare(object):
    """Trivial fake share for testing."""
    def __init__(self):
        self.id = uuid.uuid4()
        self.name = u'FakeShare'
        self.root_id = uuid.uuid4()
        self.shared_by_id = 1
        self.shared_to_id = 2
        self.access = 'View'
        self.accepted = True

    @property
    def event_args(self):
        """Return the args for creating a ShareBaseEvent."""
        return (self.id, self.name, self.root_id, self.shared_by_id,
                self.shared_to_id, self.access, self.accepted)


class TestEventNotifier(unittest.TestCase):
    """Test the EventNotifier class."""

    def patch(self, obj, attr_name, new_val):
        """Patch!"""
        old_val = getattr(obj, attr_name)
        setattr(obj, attr_name, new_val)
        self.addCleanup(setattr, obj, attr_name, old_val)

    def setUp(self):
        """Set up."""
        super(TestEventNotifier, self).setUp()
        self.tx_manager = ThreadTransactionManager()
        self.notifier = EventNotifier(tx_manager=self.tx_manager)
        self.patch(notifier, 'get_notifier', lambda: self.notifier)

        self.notifications = []
        self.patch(self.notifier, 'on_event',
                   lambda event: self.notifications.append(event))

    def test_notify_properties(self):
        """Test getter and setter for notify handler."""
        handler = object()
        self.notifier.on_node_update = handler
        self.assert_(self.notifier.on_node_update is handler)

    def test_broadcast_share_created(self):
        """Test broadcast of an share creation."""
        share = FakeShare()
        self.notifier.queue_share_created(share)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [ShareCreated(*share.event_args)])

    def test_broadcast_share_deleted(self):
        """Test broadcast of an share deletion."""
        share = FakeShare()
        self.notifier.queue_share_deleted(share)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [ShareDeleted(*share.event_args)])

    def test_broadcast_share_accepted(self):
        """Test broadcast of an share accepted."""
        share = FakeShare()
        self.notifier.queue_share_accepted(share)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [ShareAccepted(*share.event_args)])

    def test_broadcast_share_declined(self):
        """Test broadcast of an share declined."""
        share = FakeShare()
        self.notifier.queue_share_declined(share)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [ShareDeclined(*share.event_args)])

    def test_broadcast_udf_create(self):
        """Test broadcast of an udf creation."""
        udf_id = uuid.uuid4()
        root_id = uuid.uuid4()
        suggested_path = u"foo"
        self.notifier.queue_udf_create(0, udf_id, root_id, suggested_path)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [UDFCreate(0, udf_id, root_id, suggested_path, None)])

    def test_broadcast_udf_delete(self):
        """Test broadcast of an udf creation."""
        udf_id = uuid.uuid4()
        self.notifier.queue_udf_delete(0, udf_id)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [UDFDelete(0, udf_id, None)])

    def test_broadcast_vol_new_gen(self):
        """Test the broadcast of a new generation for the volume."""
        user_id = 1
        volume_id = uuid.uuid4()
        new_gen = 77
        self.notifier.queue_volume_new_generation(user_id, volume_id, new_gen)
        self.tx_manager.commit()
        self.assertEqual(self.notifications,
                         [VolumeNewGeneration(user_id, volume_id,
                                              new_gen, None)])

    def test_deliver_after_commit(self):
        """Test that notifications are delivered after committing."""
        source_session = uuid.uuid4()
        udf_a = uuid.uuid4()
        udf_b = uuid.uuid4()
        udf_c = uuid.uuid4()
        self.tx_manager.begin()
        self.notifier.queue_udf_delete(1, udf_a, source_session)
        self.notifier.queue_udf_delete(1, udf_b)
        self.assertEqual([], self.notifications)
        self.tx_manager.commit()

        # should have sent notifications after commit
        event_a = self.notifications[0]
        event_b = self.notifications[1]
        self.assertEqual(event_a.udf_id, udf_a)
        self.assertEqual(event_a.source_session, source_session)
        self.assertEqual(event_b.udf_id, udf_b)
        self.assertEqual(event_b.source_session, None)
        self.notifications[:] = []

        self.tx_manager.begin()
        self.notifier.queue_udf_delete(1, udf_c)
        self.assertEqual([], self.notifications)
        self.tx_manager.commit()
        # make sure we don't re-send from previous transactions
        event_c = self.notifications[0]
        self.assertEqual(event_c.udf_id, udf_c)
        self.assertEqual(event_c.source_session, None)

    def test_no_delivery_after_abort(self):
        """Test that notifs queued for an aborted transaction are not sent."""
        source_session = uuid.uuid4()
        udf_a = uuid.uuid4()
        udf_b = uuid.uuid4()
        udf_c = uuid.uuid4()

        self.tx_manager.begin()
        self.notifier.queue_udf_delete(1, udf_a, source_session)
        self.notifier.queue_udf_delete(1, udf_b)
        self.assertEqual([], self.notifications)
        self.tx_manager.abort()
        # should not send any notifications
        self.assertEqual([], self.notifications)

        self.tx_manager.begin()
        self.notifier.queue_udf_delete(1, udf_c)
        self.assertEqual([], self.notifications)
        self.tx_manager.commit()
        # make sure we don't re-send from previous transactions
        event_c = self.notifications[0]
        self.assertEqual(event_c.udf_id, udf_c)
        self.assertEqual(event_c.source_session, None)


class TestRecipientIds(unittest.TestCase):
    """Check that each notification type has the appropriate recipient ids."""

    def test_udf_create(self):
        """Test that UDFCreate has the correct recipient_id list."""
        notif = UDFCreate(owner_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)

    def test_udf_delete(self):
        """Test that UDFDelete has the correct recipient_id list."""
        notif = UDFDelete(owner_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)

    def test_share_created(self):
        """Test that ShareCreated has the correct recipient_id list."""
        notif = ShareCreated(shared_to_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)

    def test_share_deleted(self):
        """Test that ShareDeleted has the correct recipient_id list."""
        notif = ShareDeleted(shared_to_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)

    def test_share_accepted(self):
        """Test that ShareAccepted has the correct recipient_id list."""
        notif = ShareAccepted(shared_to_id='user1', shared_by_id="user2")
        self.assertEquals({'user1', 'user2'}, notif.recipient_ids)

    def test_share_declined(self):
        """Test that ShareDeclined has the correct recipient_id list."""
        notif = ShareDeclined(shared_by_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)

    def test_volume_new_generation(self):
        """Test that VolumeNewGeneration has the correct recipient_id list."""
        notif = VolumeNewGeneration(user_id='user1')
        self.assertEquals({'user1'}, notif.recipient_ids)


class TestPendingNotifications(unittest.TestCase):
    """Test PendingNotifications."""

    def setUp(self):
        super(TestPendingNotifications, self).setUp()

        class FakeNotifier(object):
            def __init__(self):
                self.notifications = []

            def on_event(self, event):
                self.notifications.append(event)

        self.notifier = FakeNotifier()
        self.pending_notifications = PendingNotifications(self.notifier)

    def test_send_with_no_events(self):
        """If no events are pending, no notifications are sent."""
        self.pending_notifications.send()
        self.assertEqual(0, len(self.notifier.notifications))

    def test_send_does_send_single_event(self):
        """If a single event is pending, it should still be sent."""
        share = FakeShare()
        events = [ShareCreated(*share.event_args)]
        self.pending_notifications.add_events(events)
        self.pending_notifications.send()
        self.assertEqual(1, len(self.notifier.notifications))
        self.assertEqual(events, self.notifier.notifications)
