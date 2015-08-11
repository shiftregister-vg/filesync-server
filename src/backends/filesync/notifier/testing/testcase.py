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

"""The Storage network server testcase.

Base classes to do all the testing.
"""

from backends.filesync.notifier.notifier import event_classes


class FakeNotifier(object):
    """A fake Event Notifier used for testing."""

    def __init__(self):
        """Initializes an instance of the storage controller."""
        self._event_callbacks = dict.fromkeys(cls.event_type
                                              for cls in event_classes)

    def set_event_callback(self, event, callback):
        """Registers a callback for an event_type."""
        self._event_callbacks[event.event_type] = callback

    def send_event(self, event):
        """Send an event to the appropriate callback."""
        cback = self._event_callbacks[event.event_type]
        if event.call_with_recipient:
            cback(event, recipient_id='test')
        else:
            cback(event)
