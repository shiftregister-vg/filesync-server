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

"""Utilities for testing code."""

from collections import defaultdict


class FakeMetrics(object):
    """Fake metrics behavior"""

    def __init__(self):
        self.namespace = None
        self.user = None
        self.mode = None
        self.meters = defaultdict(int)

    def meter(self, name, count=1, sample_rate=1):
        """Record call to meter()."""
        self.meters[name] += count

    def increment(self, counter):
        """Fake increment."""
        return

    def report(self, namespace, user, mode):
        """Fake report method"""
        self.namespace = namespace
        self.user = user
        self.mode = mode

    def make_all_assertions(self, testcase, namespace):
        """Make all the assertions on the metrics report."""
        testcase.assertEqual(namespace, self.namespace)
        testcase.assertEqual("d", self.mode)
        # assertIsInstance is not always available
        testcase.assertEqual(type(self.user), str)
