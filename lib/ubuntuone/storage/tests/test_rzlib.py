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

"""Tests for rzlib."""

import cPickle
import ctypes
import os
import zlib
import unittest

from ubuntuone.storage import rzlib


class TestRZlib(unittest.TestCase):
    """Tests for resumable decompressing."""

    def test_initial_state(self):
        """get/set initial state work."""
        do = rzlib.decompressobj()
        # get  works
        old = do.get_state()
        # set works
        do1 = rzlib.decompressobj()
        do1.set_state(old)
        self.assertEqual(old, do1.get_state())

    def test_get_state(self):
        """get/set state work."""
        do = rzlib.decompressobj()
        state = cPickle.loads(do.get_state())
        fnames = [f[0]for f in rzlib.InflateState._fields_]
        for fname in fnames:
            self.assertTrue(fname in state['state'],
                            '%s not in InflateState' % fname)

        fnames = [f[0]for f in rzlib.ResumableZStream._fields_]
        for fname in fnames:
            if fname in ['next_in', 'next_out', 'zalloc', 'zfree', 'opaque']:
                continue
            self.assertTrue(fname in state,
                            '%s not in ResumableZStream' % fname)

    def test_get_state_version(self):
        """get/set state work."""
        do = rzlib.decompressobj()
        state = cPickle.loads(do.get_state())
        self.assertEqual(state['zlib_version'], zlib.ZLIB_VERSION)

    def test_get_state_with_error(self):
        """get/set state fails if there is an error in the state."""
        do = rzlib.decompressobj()
        try:
            do.decompress("Ho")
        except zlib.error:
            pass
        self.assertRaises(ValueError, do.get_state)

    def test_set_state(self):
        """set_state works as expected."""
        do = rzlib.decompressobj()
        # change the state to a unfinished decompress
        data = os.urandom(1024 * 64)
        deflated_data = zlib.compress(data)
        do.decompress(deflated_data[:len(deflated_data) / 2])
        new_do = rzlib.decompressobj()
        new_do.set_state(do.get_state())
        fnames = [f[0]for f in rzlib.InflateState._fields_]
        code_fields = [field[0] for field in rzlib.Code._fields_]
        new_state = new_do._c_do.zst.state.contents
        old_state = do._c_do.zst.state.contents
        for fname in fnames:
            # pointers needs special care
            # check Code pointers
            if fname in ['next', 'distcode', 'lencode']:
                new_code = getattr(new_state, fname).contents
                old_code = getattr(old_state, fname).contents
                for f in code_fields:
                    self.assertEqual(getattr(new_code, f),
                                     getattr(old_code, f))
                continue
            # check the window
            elif fname == 'window':
                if new_state.write == 0 and old_state.write == 0:
                    # the write index is 0, skip the window comparsion.
                    continue
                new_window = ctypes.string_at(new_state.window,
                                              new_state.whave)
                old_window = ctypes.string_at(old_state.window,
                                              old_state.whave)
                self.assertEqual(new_window, old_window, 'not equal')
                continue
            elif fname == 'head':
                self.assertFalse(new_state.head)
                self.assertFalse(old_state.head)
                continue
            # check arrays
            elif fname in ['lens', 'codes', 'work']:
                array = getattr(new_state, fname)
                new_array_content = ctypes.string_at(ctypes.pointer(array),
                                                     ctypes.sizeof(array))
                array = getattr(new_state, fname)
                old_array_content = ctypes.string_at(ctypes.pointer(array),
                                                     ctypes.sizeof(array))
                self.assertEqual(new_array_content, old_array_content)
                continue
            # compare simple types
            self.assertEqual(getattr(new_state, fname),
                             getattr(old_state, fname))

        fnames = [f[0]for f in rzlib.ResumableZStream._fields_]
        for fname in fnames:
            # skip data pointers, functions and the inflate state
            if fname in ['next_in', 'next_out', 'zalloc',
                         'zfree', 'opaque', 'state']:
                continue
            self.assertEqual(getattr(new_do._c_do.zst, fname),
                             getattr(do._c_do.zst, fname))

    def test_set_state_bad_version(self):
        """set_state with a different version fails."""
        do = rzlib.decompressobj()
        state = cPickle.loads(do.get_state())
        state['zlib_version'] = 'hola'
        pickled_state = cPickle.dumps(state)
        do1 = rzlib.decompressobj()
        self.assertRaises(rzlib.VersionError, do1.set_state, pickled_state)

    def test_decompress(self):
        """rzlib produces the same results as zlib."""
        results = []
        data = os.urandom(102400)
        deflated_data = zlib.compress(data)

        rdecompressor = rzlib.decompressobj()
        decompressor = rzlib.decompressobj()
        for do in (decompressor, rdecompressor):
            inflated_data = do.decompress(deflated_data)
            inflated_data += do.flush()
            results.append(inflated_data)
            self.assertEqual(inflated_data, data)

        self.assertEqual(*results)

    def test_resumable_decompress(self):
        """Resuming decompress has the same result as zlib."""
        results = []
        chunksz = 1024
        data = os.urandom(1024 * 64)
        deflated_data = zlib.compress(data)
        results.append(zlib.decompress(deflated_data))
        start = 0
        state = None
        rdata = ''
        for j, i in enumerate(range(0, len(deflated_data), chunksz)):
            if state is None:
                do2 = rzlib.decompressobj()
            else:
                do2 = rzlib.decompressobj(state)
            rdata += do2.decompress(deflated_data[start:i])
            state = do2.get_state()
            start = i
        # if there are some bytes left, decompress them.
        if start < len(deflated_data):
            rdata += do2.decompress(deflated_data[start:])
        results.append(rdata)
        self.assertEqual(results[0], results[1], "inflated data doesn't match")

    def test_copy(self):
        """Copied decompressobj have the same state."""
        data = "Thoughtcrime does not entail death: thoughtcrime is death."
        data = os.urandom(102400)
        deflated_data = zlib.compress(data)
        do = rzlib.decompressobj()
        do.decompress(deflated_data[:len(deflated_data) / 2])
        state = cPickle.loads(do.get_state())
        copy = cPickle.loads(do.copy().get_state())
        for k, v in state.iteritems():
            if k == 'state':
                for k1, v1 in v.iteritems():
                    self.assertEqual(v1, copy[k][k1],
                                     "%s -> %r != %r" % (k1, v1, copy[k][k1]))
                continue
            self.assertEqual(v, copy[k],
                             "%s -> %r != %r" % (k, v, copy[k]))

    def test_decompress_garbage_in_header(self):
        """rzlib handle garbage just like zlib."""
        data = os.urandom(102400)
        rdecompressor = rzlib.decompressobj()
        decompressor = rzlib.decompressobj()
        for do in (decompressor, rdecompressor):
            self.assertRaises(zlib.error, do.decompress, data)

    def test_decompress_garbage_in_middle(self):
        """rzlib handle garbage just like zlib."""
        data = os.urandom(102400)
        deflated_data = zlib.compress(data)

        rdecompressor = rzlib.decompressobj()
        decompressor = rzlib.decompressobj()
        for do in (decompressor, rdecompressor):
            data_size = len(deflated_data)
            part = data_size / 3
            do.decompress(deflated_data[:part])
            self.assertRaises(zlib.error, do.decompress, data[part:part * 2])

    def test_decompress_garbage_in_tail(self):
        """rzlib handle garbage just like zlib."""
        data = os.urandom(102400)
        deflated_data = zlib.compress(data)

        rdecompressor = rzlib.decompressobj()
        decompressor = rzlib.decompressobj()
        for do in (decompressor, rdecompressor):
            data_size = len(deflated_data)
            part = data_size / 3
            inflated_data = do.decompress(deflated_data[:part])
            inflated_data += do.decompress(deflated_data[part:part * 2])
            self.assertRaises(zlib.error, do.decompress, data[part * 2:])

    ##### stdlib zlib tests #####

    def test_decompressobj_badflush(self):
        """Verify failure on calling decompressobj.flush with bad params."""
        self.assertRaises(ValueError, rzlib.decompressobj().flush, 0)
        self.assertRaises(ValueError, rzlib.decompressobj().flush, -1)

    def test_decompinc(self, flush=False, source=None, cx=256, dcx=64):
        """Compress object in steps, decompress object in steps."""
        source = source or HAMLET_SCENE
        data = source * 128
        co = zlib.compressobj()
        bufs = []
        for i in range(0, len(data), cx):
            bufs.append(co.compress(data[i:i + cx]))
        bufs.append(co.flush())
        combuf = ''.join(bufs)

        self.assertEqual(data, zlib.decompress(combuf))

        dco = rzlib.decompressobj()
        bufs = []
        for i in range(0, len(combuf), dcx):
            bufs.append(dco.decompress(combuf[i:i + dcx]))
            self.assertEqual('', dco.unconsumed_tail,
                             "(A) uct should be '': not %d long" % len(
                                 dco.unconsumed_tail))
        if flush:
            bufs.append(dco.flush())
        else:
            while True:
                chunk = dco.decompress('')
                if chunk:
                    bufs.append(chunk)
                else:
                    break
        self.assertEqual('', dco.unconsumed_tail,
                         "(B) uct should be '': not %d long" % len(
                             dco.unconsumed_tail))
        self.assertEqual(data, ''.join(bufs))
        # Failure means: "decompressobj with init options failed"

    def test_decompincflush(self):
        """Compress object in steps, decompress object in steps + flush."""
        self.test_decompinc(flush=True)

    def test_decompimax(self, source=None, cx=256, dcx=64):
        """Compress in steps, decompress in length-restricted steps."""
        source = source or HAMLET_SCENE
        # Check a decompression object with max_length specified
        data = source * 128
        co = zlib.compressobj()
        bufs = []
        for i in range(0, len(data), cx):
            bufs.append(co.compress(data[i:i + cx]))
        bufs.append(co.flush())
        combuf = ''.join(bufs)
        self.assertEqual(data, zlib.decompress(combuf),
                         'compressed data failure')

        dco = rzlib.decompressobj()
        bufs = []
        cb = combuf
        while cb:
            #max_length = 1 + len(cb)//10
            chunk = dco.decompress(cb, dcx)
            self.failIf(len(chunk) > dcx,
                        'chunk too big (%d>%d)' % (len(chunk), dcx))
            bufs.append(chunk)
            cb = dco.unconsumed_tail
        bufs.append(dco.flush())
        self.assertEqual(data, ''.join(bufs), 'Wrong data retrieved')

    def test_decompressmaxlen(self, flush=False):
        """Check a decompression object with max_length specified."""
        data = HAMLET_SCENE * 128
        co = zlib.compressobj()
        bufs = []
        for i in range(0, len(data), 256):
            bufs.append(co.compress(data[i:i + 256]))
        bufs.append(co.flush())
        combuf = ''.join(bufs)
        self.assertEqual(data, zlib.decompress(combuf),
                         'compressed data failure')

        dco = rzlib.decompressobj()
        bufs = []
        cb = combuf
        while cb:
            max_length = 1 + len(cb) // 10
            chunk = dco.decompress(cb, max_length)
            self.failIf(len(chunk) > max_length,
                        'chunk too big (%d>%d)' % (len(chunk), max_length))
            bufs.append(chunk)
            cb = dco.unconsumed_tail
        if flush:
            bufs.append(dco.flush())
        else:
            while chunk:
                chunk = dco.decompress('', max_length)
                self.failIf(len(chunk) > max_length,
                            'chunk too big (%d>%d)' % (len(chunk), max_length))
                bufs.append(chunk)
        self.assertEqual(data, ''.join(bufs), 'Wrong data retrieved')

    def test_decompressmaxlenflush(self):
        """Check a decompression object forcing a flush."""
        self.test_decompressmaxlen(flush=True)

    def test_maxlenmisc(self):
        """Misc tests of max_length."""
        dco = rzlib.decompressobj()
        self.assertRaises(ValueError, dco.decompress, "", -1)
        self.assertEqual('', dco.unconsumed_tail)

    def test_decompresscopy(self):
        """Test copying a decompression object."""
        data = HAMLET_SCENE
        comp = zlib.compress(data)

        d0 = rzlib.decompressobj()
        bufs0 = []
        bufs0.append(d0.decompress(comp[:32]))

        d1 = d0.copy()
        bufs1 = bufs0[:]

        bufs0.append(d0.decompress(comp[32:]))
        s0 = ''.join(bufs0)

        bufs1.append(d1.decompress(comp[32:]))
        s1 = ''.join(bufs1)

        self.assertEqual(s0, s1)
        self.assertEqual(s0, data)

    def test_baddecompresscopy(self):
        """Test copying a compression object in an inconsistent state."""
        data = zlib.compress(HAMLET_SCENE)
        d = rzlib.decompressobj()
        d.decompress(data)
        d.flush()
        self.assertRaises(ValueError, d.copy)


HAMLET_SCENE = """
LAERTES

       O, fear me not.
       I stay too long: but here my father comes.

       Enter POLONIUS

       A double blessing is a double grace,
       Occasion smiles upon a second leave.

LORD POLONIUS

       Yet here, Laertes! aboard, aboard, for shame!
       The wind sits in the shoulder of your sail,
       And you are stay'd for. There; my blessing with thee!
       And these few precepts in thy memory
       See thou character. Give thy thoughts no tongue,
       Nor any unproportioned thought his act.
       Be thou familiar, but by no means vulgar.
       Those friends thou hast, and their adoption tried,
       Grapple them to thy soul with hoops of steel;
       But do not dull thy palm with entertainment
       Of each new-hatch'd, unfledged comrade. Beware
       Of entrance to a quarrel, but being in,
       Bear't that the opposed may beware of thee.
       Give every man thy ear, but few thy voice;
       Take each man's censure, but reserve thy judgment.
       Costly thy habit as thy purse can buy,
       But not express'd in fancy; rich, not gaudy;
       For the apparel oft proclaims the man,
       And they in France of the best rank and station
       Are of a most select and generous chief in that.
       Neither a borrower nor a lender be;
       For loan oft loses both itself and friend,
       And borrowing dulls the edge of husbandry.
       This above all: to thine ownself be true,
       And it must follow, as the night the day,
       Thou canst not then be false to any man.
       Farewell: my blessing season this in thee!

LAERTES

       Most humbly do I take my leave, my lord.

LORD POLONIUS

       The time invites you; go; your servants tend.

LAERTES

       Farewell, Ophelia; and remember well
       What I have said to you.

OPHELIA

       'Tis in my memory lock'd,
       And you yourself shall keep the key of it.

LAERTES

       Farewell.
"""
