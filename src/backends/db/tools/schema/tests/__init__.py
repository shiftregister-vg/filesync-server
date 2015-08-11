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

"""Tests for Database Schemas"""

import os
import shutil
import sys
import tempfile
import unittest


class Package(object):
    """This is a Package"""
    def __init__(self, package_dir, name):
        self.name = name
        self._package_dir = package_dir

    def create_module(self, filename, contents):
        """Create a module for the package"""
        filename = os.path.join(self._package_dir, filename)
        file = open(filename, "w")
        file.write(contents)
        file.close()


class MakePackage(unittest.TestCase):
    """ Make a package """
    def setUp(self):
        super(MakePackage, self).setUp()
        self._package_dirs = set()
        self._package_names = set()

    def tearDown(self):
        for package_dir in self._package_dirs:
            sys.path.remove(package_dir)

        for name in list(sys.modules):
            if name in self._package_names:
                del sys.modules[name]
            elif filter(None, [name.startswith("%s." % x)
                               for x in self._package_names]):
                del sys.modules[name]

        super(MakePackage, self).tearDown()

    def create_package(self, base_dir, name, init_module=None):
        """Create a Python package.

        Packages created using this method will be removed from L{sys.path}
        and L{sys.modules} during L{tearDown}.

        @param package_dir: The directory in which to create the new package.
        @param name: The name of the package.
        @param init_module: Optionally, the text to include in the __init__.py
            file.
        @return: A L{Package} instance that can be used to create modules.
        """
        package_dir = os.path.join(base_dir, name)
        self._package_names.add(name)
        os.makedirs(package_dir)

        file = open(os.path.join(package_dir, "__init__.py"), "w")
        if init_module:
            file.write(init_module)
        file.close()
        sys.path.append(base_dir)
        self._package_dirs.add(base_dir)

        return Package(package_dir, name)


class MakePath(unittest.TestCase):
    """ Make a path """
    def setUp(self):
        super(MakePath, self).setUp()
        self._dirname = tempfile.mkdtemp()
        self._counter = 0

    def tearDown(self):
        shutil.rmtree(self._dirname)
        super(MakePath, self).tearDown()

    def make_dir(self):
        """make the dir"""
        path = self.make_path()
        os.mkdir(path)
        return path

    def make_path(self, content=None, path=None):
        """make the path"""
        if path is None:
            self._counter += 1
            path = "%s/%03d" % (self._dirname, self._counter)
        if content is not None:
            file = open(path, "w")
            try:
                file.write(content)
            finally:
                file.close()
        return path
