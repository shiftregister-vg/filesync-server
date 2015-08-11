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

"""A Notifier that will bind to transaction."""

import itertools
import re
import threading

from weakref import WeakKeyDictionary

from twisted.internet import reactor

from backends.filesync.data import storage_tm


class EventTypeDescriptor(object):
    """A descriptor to return event types for event classes."""

    def __get__(self, instance, cls):
        """Get event type string for this class."""
        return cls._get_event_type()

    def __set__(self, instance, value):
        """Raise AttributeError since the event type isn't settable."""
        raise AttributeError("Event type is not settable.")


def camelcase_to_underscores(name):
    """Convert a camelcased name to a lowercase name with underscores."""
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    name = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', name)
    return name.lower()


class Notification(object):
    """Base class for messages.

    Fields are dynamically set by args in the required field order or
    keyword args with names that match the field names.
    """

    event_type = EventTypeDescriptor()
    call_with_recipient = False

    def __init__(self, *args, **kw):
        """Initializes an object."""
        values = dict(itertools.izip_longest(self._fields, args))
        values.update(kw)
        if 'recipient_id' not in values:
            values['recipient_id'] = None
        for name, value in values.iteritems():
            setattr(self, name, value)

        self.recipient_ids = {getattr(self, name)
                              for name in self.__class__.recipient_id_fields}

    @classmethod
    def create_for_enqueue(cls, *args, **kw):
        """Create instance for queue_* methods."""
        return cls(*args, **kw)

    @classmethod
    def _get_event_type(cls):
        """Ensure that this class has an event type string defined."""
        # use vars() since we don't want to get values from parent classes
        event_type = vars(cls).get('_event_type')
        if event_type is None:
            event_type = camelcase_to_underscores(cls.__name__)
            cls._event_type = event_type
        return event_type

    def __repr__(self):
        """Return detailed string description for debugging purposes."""
        pairs = [(n, getattr(self, n)) for n in self._fields]
        pairs.append(('recipient_id', self.recipient_id))
        params = " ".join(["%s=%r" % p for p in pairs])
        return "<%s %s>" % (type(self).__name__, params)

    def __eq__(self, other):
        """Test equality."""
        if type(self) != type(other):
            return False
        for name in self._fields:
            if getattr(self, name) != getattr(other, name):
                return False
        return True


class UDFCreate(Notification):
    """Message indicating that an UDF was created."""
    _fields = ('owner_id', 'udf_id', 'root_id',
               'suggested_path', 'source_session')
    recipient_id_fields = ['owner_id']


class UDFDelete(Notification):
    """Message indicating that an UDF was deleted."""
    _fields = ('owner_id', 'udf_id', 'source_session')
    recipient_id_fields = ['owner_id']


class ShareBaseEvent(Notification):
    """Base class for all share events.

    Only a share_id is sent in the message since the share's state may change
    by the time the message is delivered. The final details can be determined
    once the share is retrieved from the when the event is received.
    """
    # override generated event type for backwards-compatibility
    _event_type = "share_base"

    _fields = ('share_id', 'name', 'root_id', 'shared_by_id',
               'shared_to_id', 'access', 'accepted', 'source_session')
    recipient_id_fields = []  # override in subclasses

    @classmethod
    def create_for_enqueue(cls, share, source_session=None):
        """Create instance for share-specific queue_* methods."""
        return cls(share.id, share.name, share.root_id, share.shared_by_id,
                   share.shared_to_id, share.access, share.accepted,
                   source_session)


class ShareCreated(ShareBaseEvent):
    """A share was created."""
    recipient_id_fields = ['shared_to_id']


class ShareDeleted(ShareBaseEvent):
    """A share was deleted."""
    recipient_id_fields = ['shared_to_id']


class ShareAccepted(ShareBaseEvent):
    """A share was accepted."""
    recipient_id_fields = ['shared_to_id', 'shared_by_id']
    call_with_recipient = True


class ShareDeclined(ShareBaseEvent):
    """A share was declined."""
    recipient_id_fields = ['shared_by_id']


class VolumeNewGeneration(Notification):
    """The volume is in a new generation."""
    _fields = ('user_id', 'client_volume_id',
               'new_generation', 'source_session')
    recipient_id_fields = ['user_id']


# this is the list of the different classes that describe events, used
# to automagically build and open the messages to be sent
event_classes = [
    UDFCreate,
    UDFDelete,
    ShareCreated,
    ShareDeleted,
    ShareAccepted,
    ShareDeclined,
    VolumeNewGeneration,
]


class PendingNotifications(object):
    """Accumulates notifications pending on commit."""

    def __init__(self, event_notifier):
        """Initializes an instance."""
        self.event_notifier = event_notifier
        self.pending_events = []

    def add_events(self, events):
        """Queues events notifications."""
        self.pending_events.extend(events)

    def send(self):
        """Broadcast pending notifications."""
        for event in self.pending_events:
            self.event_notifier.on_event(event)


class EventNotifier(object):
    """Notifies different events."""

    def __init__(self, tx_manager=storage_tm):
        self.tx_manager = tx_manager
        self.per_thread = threading.local()
        self._event_callbacks = {}

    def on_event(self, event):
        """An event was received, call the proper callback if any."""
        cback = self._event_callbacks.get(event.event_type)
        if cback is not None:
            if event.call_with_recipient:
                for recipient_id in event.recipient_ids:
                    reactor.callFromThread(cback, event,
                                           recipient_id=recipient_id)
            else:
                reactor.callFromThread(cback, event)

    def set_event_callback(self, event, callback):
        """Registers a callback for an event_type."""
        self._event_callbacks[event.event_type] = callback

    def __add_events(self, events):
        """Add events to the commit hook.

        The events added will be sent when the transaction manager
        successfully commits the changes.
        """
        per_transaction = getattr(self.per_thread,
                                  'notifications_by_transaction', None)
        if per_transaction is None:
            per_transaction = WeakKeyDictionary()
            self.per_thread.notifications_by_transaction = per_transaction

        transaction = self.tx_manager.get()

        notifications = per_transaction.get(transaction, None)
        if notifications is None:
            # create an object to collect pending notifications and
            # register it with the transaction
            notifications = PendingNotifications(self)
            per_transaction[transaction] = notifications
            transaction.addAfterCommitHook(self.__broadcast_after_commit,
                                           (notifications,))

        notifications.add_events(events)

    @classmethod
    def create_queue_method(cls, event_class):
        """Add an instance method to queue events of the given class."""
        underscore_name = camelcase_to_underscores(event_class.__name__)
        method_name = "queue_%s" % (underscore_name,)

        def method(self, *args, **kw):
            """Queue event."""
            event = event_class.create_for_enqueue(*args, **kw)
            self.__add_events([event])

        method.__doc__ = "Queue %s event." % (event_class.__name__,)
        setattr(cls, method_name, method)

    def __broadcast_after_commit(self, success, notifications):
        """Post-commit hook which sends notifications on successful commit."""
        if success:
            notifications.send()


# build EventNotifier.queue_* methods
for event_class in event_classes:
    EventNotifier.create_queue_method(event_class)


_event_notifier = None


def get_notifier():
    """Return always the same notifier."""
    global _event_notifier
    if _event_notifier is None:
        _event_notifier = EventNotifier()
    return _event_notifier
