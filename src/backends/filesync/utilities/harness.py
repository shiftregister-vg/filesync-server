#! /usr/bin/python -i

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

"""The Storage DAL test harness"""

import os
import readline

readline.parse_and_bind('tab: complete')

# Mimic the real interactive interpreter's loading of $PYTHONSTARTUP file.
startup = os.environ.get('PYTHONSTARTUP')
if startup:
    execfile(startup)

from backends.filesync.notifier.notifier import register_notifier_for_bus
from backends.filesync.notifier.testing.testcase import AccumulatingNotifyBus
from backends.filesync.data import services  # NOQA

nb = AccumulatingNotifyBus()
register_notifier_for_bus(nb)

print """
OH HAI HACKERS
This sets up an environment making it easy to play with the data access layer.

try this out:
bob = services.make_storage_user(101, u'bob', u'Bob the Builder', 30*(2**30))
tim = services.make_storage_user(102, u'tim', u'Tim the Enchanter', 30*(2**30))

udf = bob.make_udf(u"~/Documents")
dir = bob.volume(udf.id).root.make_subdirectory(u"Junk")
share = dir.share(tim.id, u"MyJunk")
tim.get_share(share.id).accept()

file = tim.volume(share.id).root.make_file(u"file.txt")

#You also can see events queued for MQ:
print nb.events
"""
