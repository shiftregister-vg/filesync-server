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

"""Tests for monitoring dump."""

import gc
import shutil
import tempfile

from config import config
from mocker import MockerTestCase, ANY

from ubuntuone.monitoring.dump import gc_dump, meliae_dump


class TestDump(MockerTestCase):
    """Test dump."""

    def setUp(self):
        super(TestDump, self).setUp()
        log_folder = config.general.log_folder
        temp_folder = tempfile.mkdtemp()
        config.general.update([("log_folder", temp_folder)])
        self.addCleanup(config.general.update, [("log_folder", log_folder)])
        self.addCleanup(shutil.rmtree, temp_folder)

    def test_meliae_dump(self):
        """Check that the dump works."""
        from meliae import scanner

        collect = self.mocker.replace(gc.collect)
        dump_all_objects = self.mocker.replace(scanner.dump_all_objects)

        collect()
        dump_all_objects(ANY)
        self.mocker.replay()

        self.assertIn("Output written to:", meliae_dump())

    def test_meliae_dump_error(self):
        """Check the error case."""
        from meliae import scanner

        dump_all_objects = self.mocker.replace(scanner.dump_all_objects)
        dump_all_objects(ANY)
        self.mocker.throw(ValueError)
        self.mocker.replay()

        self.assertIn("Error while trying to dump memory", meliae_dump())

    def test_gc_dumps_count_ok(self):
        """Check that the count dump works."""
        get_count = self.mocker.replace(gc.get_count)
        garbage = self.mocker.replace(gc.garbage)

        get_count()
        self.mocker.result((400, 20, 3))
        # we're exercising the mocker
        [x for x in garbage]
        self.mocker.result(iter([]))
        self.mocker.replay()

        self.assertIn("GC count is (400, 20, 3)", gc_dump())

    def test_gc_dumps_garbage_ok(self):
        """Check that the garbage dump works."""
        get_count = self.mocker.replace(gc.get_count)
        garbage = self.mocker.replace(gc.garbage)

        get_count()
        self.mocker.result(0)
        # we're exercising the mocker
        [x for x in garbage]
        self.mocker.result(iter(['foo', 666]))
        self.mocker.replay()

        self.assertIn("2 garbage items written", gc_dump())

    def test_gc_dump_error_generic(self):
        """Something bad happens when dumping gc."""
        get_count = self.mocker.replace(gc.get_count)

        get_count()
        self.mocker.throw(ValueError)
        self.mocker.replay()
        self.assertIn("Error while trying to dump GC", gc_dump())

    def test_gc_dump_error_garbage(self):
        """Support something that breaks in repr."""
        class Strange(object):
            """Weird object that breaks on repr."""
            def __repr__(self):
                raise ValueError('foo')

        garbage = self.mocker.replace(gc.garbage)
        # we're exercising the mocker
        [x for x in garbage]
        self.mocker.result(iter([Strange()]))
        self.mocker.replay()
        self.assertIn("1 garbage items written", gc_dump())
