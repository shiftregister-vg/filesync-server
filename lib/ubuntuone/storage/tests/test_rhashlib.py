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

"""Tests for rhashlib."""

import os
import hashlib
import unittest

from ubuntuone.storage import rhashlib
from ubuntuone.storageprotocol.content_hash import magic_hash_factory


class TestRHashLib(unittest.TestCase):
    """Tests for resumable hashing."""

    def test_context(self):
        """get/set context work."""
        ctx = rhashlib.sha1()
        h0 = ctx.h0

        # get content works
        old = ctx.get_context()

        # change it
        ctx.h0 = 1
        self.assertEqual(ctx.h0, 1)

        # set works
        ctx.set_context(old)
        self.assertEquals(old, ctx.get_context())
        self.assertEqual(ctx.h0, h0)

    def test_hashing(self):
        """rhashlib produces the same results as hashlib."""
        first_results = []
        results = []
        hexresults = []
        data = "War is Peace" * 1000

        for hl in (hashlib, rhashlib):
            sha = hl.sha1()
            sha.update(data)
            first_results.append(sha.digest())
            sha.update(data)
            results.append(sha.digest())
            hexresults.append(sha.hexdigest())

        self.assertEqual(*first_results)
        self.assertEqual(*results)
        self.assertEqual(*hexresults)

    def test_resumable_hashing(self):
        """Resuming hash has the same result as hashlib."""
        results = []
        hexresults = []
        data = "Freedom is slavery" * 1000

        sha = hashlib.sha1()
        sha.update(data)
        results.append(sha.digest())
        hexresults.append(sha.hexdigest())

        sha = rhashlib.sha1()
        sha.update(data[:500])

        sha2 = rhashlib.sha1()
        sha2.set_context(sha.get_context())
        sha2.update(data[500:])

        results.append(sha2.digest())
        hexresults.append(sha2.hexdigest())

        self.assertEqual(*results)
        self.assertEqual(*hexresults)

    def test_hashcreate(self):
        """Data can be provided as an argument to the sha1 factory."""
        data = "Ignorance is strength" * 1000
        results = [hl.sha1(data).digest() for hl in (hashlib, rhashlib)]
        self.assertEqual(*results)

    def test_copy(self):
        """copied hashes have the same context."""
        s = rhashlib.sha1("Thoughtcrime does not entail death: "
                          "thoughtcrime is death.")
        self.assertEqual(s.get_context(), s.copy().get_context())

    def test_hashing_random(self):
        """rhashlib produces the same results as hashlib."""
        first_results = []
        results = []
        hexresults = []
        data = os.urandom(1024 * 1024 * 4)

        for hl in (hashlib, rhashlib):
            sha = hl.sha1()
            sha.update(data)
            first_results.append(sha.digest())
            sha.update(data)
            results.append(sha.digest())
            hexresults.append(sha.hexdigest())

        self.assertEqual(*first_results)
        self.assertEqual(*results)
        self.assertEqual(*hexresults)


class TestResumableMagicContentHash(unittest.TestCase):
    """Tests for resumable magic hashing."""

    def test_context(self):
        """get/set context work."""
        ctx = rhashlib.resumable_magic_hash_factory()
        h0 = ctx.hash_object.h0

        # get content works
        old = ctx.get_context()

        # change it
        ctx.hash_object.h0 = 1
        self.assertEqual(ctx.hash_object.h0, 1)

        # set works
        ctx.set_context(old)
        self.assertEquals(old, ctx.get_context())
        self.assertEqual(ctx.hash_object.h0, h0)

    def test_hashing(self):
        """rhashlib produces the same results as hashlib."""
        first_results = []
        results = []
        data = "War is Peace" * 1000

        for hf in (magic_hash_factory, rhashlib.resumable_magic_hash_factory):
            sha = hf()
            sha.update(data)
            first_results.append(sha.content_hash()._magic_hash)
            sha.update(data)
            results.append(sha.content_hash()._magic_hash)

        self.assertEqual(*first_results)
        self.assertEqual(*results)

    def test_resumable_hashing(self):
        """Resuming hash has the same result as hashlib."""
        results = []
        data = "Freedom is slavery" * 1000

        sha = magic_hash_factory()
        sha.update(data)
        results.append(sha.content_hash()._magic_hash)

        sha = rhashlib.resumable_magic_hash_factory()
        sha.update(data[:500])

        sha2 = rhashlib.resumable_magic_hash_factory()
        sha2.set_context(sha.get_context())
        sha2.update(data[500:])

        results.append(sha2.content_hash()._magic_hash)

        self.assertEqual(*results)

    def test_copy(self):
        """copied hashes have the same context."""
        s = rhashlib.resumable_magic_hash_factory()
        s.update("Thoughtcrime does not entail death: thoughtcrime is death.")
        self.assertEqual(s.get_context(), s.copy().get_context())

    def test_hashing_random(self):
        """rhashlib produces the same results as hashlib."""
        first_results = []
        results = []
        data = os.urandom(1024 * 1024 * 4)

        for hf in (magic_hash_factory, rhashlib.resumable_magic_hash_factory):
            sha = hf()
            sha.update(data)
            first_results.append(sha.content_hash()._magic_hash)
            sha.update(data)
            results.append(sha.content_hash()._magic_hash)

        self.assertEqual(*first_results)
        self.assertEqual(*results)
