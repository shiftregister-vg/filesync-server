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

"""Applies patches to a database schema

Patches come in the form of python modules named patch_<n>. The patches are
applied in sequential order (numerically). A special table is used to record
the current patches applied to a database.
"""
import sys
import os
import re

import transaction

from storm.locals import StormError, Int
from storm.zope.interfaces import IZStorm

from backends.db.tools.schema import print_elapsed_time


__all__ = ["PatchApplier", "UnknownPatchError", "BadPatchError"]


class UnknownPatchError(Exception):
    """
    This is raised if a patch is found in the database that doesn't exist
    in the local patch directory.
    """

    def __init__(self, store, patches):
        Exception.__init__(self)
        self._store = store
        self._patches = patches

    def __str__(self):
        try:
            from zope import component
            zstorm = component.getUtility(IZStorm)
            name = zstorm.get_name(self._store)
        except ImportError:
            name = "xxx"
        return "%s has patches the code doesn't know about: %s" % (
               name, ", ".join([str(version) for version in self._patches]))


class BadPatchError(Exception):
    """This is raised when a bad patch is found."""


def get_patch_class(patch_table):
    """Get the class for the storm model using the table name provided."""
    class Patch(object):
        """Represents a single patch to a schema"""
        __storm_table__ = patch_table or "patch"

        version = Int(primary=True, allow_none=False)

        def __init__(self, version):
            self.version = version
    return Patch


class PatchApplier(object):
    """Applies patches to a schema"""

    def __init__(self, store, package, patch_table='patch'):
        self._store = store
        self._package = package
        self._patch_table = patch_table
        self._patch_class = get_patch_class(patch_table)

    def _module(self, version):
        """Retuns a single patch module"""
        module_name = "patch_%d" % (version,)
        return __import__(self._package.__name__ + '.' + module_name,
                          None, None, [''])

    @print_elapsed_time
    def apply(self, version):
        """Apply a single patch that matches the version passed in"""
        sys.stderr.write("Applying patch patch_%s..." % version)
        patch = self._patch_class(version)
        self._store.add(patch)
        module = None
        try:
            module = self._module(version)
            module.apply(self._store)
        except StormError:
            transaction.abort()
            raise
        except:
            type, value, traceback = sys.exc_info()
            patch_repr = getattr(module, "__file__", version)
            exc = BadPatchError("Patch %s failed: %s: %s" % (
                patch_repr, type.__name__, str(value)))
            raise exc, None, traceback
        transaction.commit()

    def apply_all(self):
        """Applies all the patch modules in a package"""
        unknown_patches = self.get_unknown_patch_versions()
        if unknown_patches:
            raise UnknownPatchError(self._store, unknown_patches)
        for version in self._get_unapplied_versions():
            self.apply(version)

    def mark_applied(self, version):
        """Marks the patch as applied to the schema

        Basically this just adds the patch number to the patch table
        """
        self._store.add(self._patch_class(version))
        transaction.commit()

    def mark_applied_all(self):
        """Marks the all patches in the patch package as applied to the schema
        """
        for version in self._get_unapplied_versions():
            self.mark_applied(version)

    def has_pending_patches(self):
        """Returns true if all the patches in the patch package are applied"""
        for version in self._get_unapplied_versions():
            return True
        return False

    def get_unknown_patch_versions(self):
        """
        Return the list of Patch versions that have been applied to the
        database, but don't appear in the schema's patches module.
        """
        applied = self._get_applied_patches()
        known_patches = self._get_patch_versions()
        unknown_patches = set()

        for patch in applied:
            if not patch in known_patches:
                unknown_patches.add(patch)
        return unknown_patches

    def _get_unapplied_versions(self):
        """A generator that returns patches that haven't been applied"""
        applied = self._get_applied_patches()
        for version in self._get_patch_versions():
            if version not in applied:
                yield version

    def _get_applied_patches(self):
        """Returns a set of patch version numbers that have been applied"""
        applied = set()
        for patch in self._store.find(self._patch_class):
            applied.add(patch.version)
        return applied

    def _get_patch_versions(self):
        """Returns a list of patch version numbers that are in the package"""
        format = re.compile(r"^patch_(\d+).py$")

        filenames = os.listdir(os.path.dirname(self._package.__file__))
        matches = [(format.match(fn), fn) for fn in filenames]
        matches = sorted(filter(lambda x: x[0], matches),
                         key=lambda x: int(x[1][6:-3]))
        return [int(match.group(1)) for match, filename in matches]
