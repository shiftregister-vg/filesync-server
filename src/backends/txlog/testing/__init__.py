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

"""Testing helpers for txlog."""

import datetime
import uuid

from backends.txlog.model import TransactionLog


def txn_log_as_dict(txn_id=1, node_id=None, owner_id=1, volume_id=None,
                    op_type=TransactionLog.OP_PUT_CONTENT, path=None,
                    generation=2, timestamp=None, mimetype=u'text/plain',
                    old_path=None, extra_data=None):
    """Return a dict representing a txlog entry, overriding the default values
    with those specified.
    """
    if node_id is None:
        node_id = unicode(uuid.uuid4())
    if volume_id is None:
        volume_id = unicode(uuid.uuid4())
    if path is None:
        path = u'/%s' % unicode(uuid.uuid4())
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()
    return dict(
        txn_id=txn_id, node_id=node_id, owner_id=owner_id, volume_id=volume_id,
        op_type=op_type, path=path, generation=generation, timestamp=timestamp,
        mimetype=mimetype, old_path=old_path, extra_data=extra_data)
