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

"""Test the config."""

from unittest import TestCase

from mock import patch

import config


class ConfigTestCase(TestCase):
    """The main test case."""

    def test_simple(self):
        conf = {'foo': 'bar'}
        with patch.object(config, '_load', return_value=conf):
            c = config._Config()
        self.assertEqual(c.foo, 'bar')

    def test_complex(self):
        conf = {'foo': {'bar': 'baz'}}
        with patch.object(config, '_load', return_value=conf):
            c = config._Config()
        self.assertEqual(c.foo.bar, 'baz')

    def test_setting_simple(self):
        conf = {'foo': 'bar'}
        with patch.object(config, '_load', return_value=conf):
            c = config._Config()
        c. foo = 'other'
        self.assertEqual(c.foo, 'other')

    def test_setting_complex(self):
        conf = {'foo': {'bar': 'baz'}}
        with patch.object(config, '_load', return_value=conf):
            c = config._Config()
        c.foo.bar = 'other'
        self.assertEqual(c.foo.bar, 'other')
