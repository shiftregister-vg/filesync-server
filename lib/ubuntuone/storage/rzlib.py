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

"""Resumable decompression

A ctypes interface to zlib decompress/inflate functions that mimics
zlib.decompressobj interface but also supports getting and setting the
z_stream state to suspend/serialize it and then resume the decompression
at a later time.
"""

import cPickle
import ctypes
import zlib

if zlib.ZLIB_VERSION != '1.2.3.3' and zlib.ZLIB_VERSION != '1.2.3.4':
    raise zlib.error("zlib version not supported: %s" % (zlib.ZLIB_VERSION))

if zlib.ZLIB_VERSION == '1.2.3.3':
    # from inftrees.h
    ENOUGH = 2048
elif zlib.ZLIB_VERSION == '1.2.3.4':
    ENOUGH_LENS = 852
    ENOUGH_DISTS = 592
    ENOUGH = ENOUGH_LENS + ENOUGH_DISTS


# from inflate.h
#/*
#     gzip header information passed to and from zlib routines.  See RFC 1952
#  for more details on the meanings of these fields.
#*/
#typedef struct gz_header_s {
#    int     text;       /* true if compressed data believed to be text */
#    uLong   time;       /* modification time */
#    int     xflags;     /* extra flags (not used when writing a gzip file) */
#    int     os;         /* operating system */
#    Bytef   *extra;     /* pointer to extra field or Z_NULL if none */
#    uInt    extra_len;  /* extra field length (valid if extra != Z_NULL) */
#    uInt    extra_max;  /* space at extra (only when reading header) */
#    Bytef   *name;      /* pointer to zero-terminated file name or Z_NULL */
#    uInt    name_max;   /* space at name (only when reading header) */
#    Bytef   *comment;   /* pointer to zero-terminated comment or Z_NULL */
#    uInt    comm_max;   /* space at comment (only when reading header) */
#    int     hcrc;       /* true if there was or will be a header crc */
#    int     done;       /* true when done reading gzip header (not used
#                           when writing a gzip file) */
#} gz_header;

Bytefp = ctypes.POINTER(ctypes.c_ubyte)


class GZHeader(ctypes.Structure):
    """gz_header_s structure."""

    _fields_ = [
        ('text', ctypes.c_int),
        ('time', ctypes.c_ulong),
        ('xflags', ctypes.c_int),
        ('os', ctypes.c_int),
        ('extra', Bytefp),
        ('extra_len', ctypes.c_uint),
        ('extra_max', ctypes.c_uint),
        ('name', Bytefp),
        ('name_max', ctypes.c_uint),
        ('comment', Bytefp),
        ('comm_max', ctypes.c_uint),
        ('hcrc', ctypes.c_int),
        ('done', ctypes.c_int),
    ]


#/* Structure for decoding tables.  Each entry provides either the
#   information needed to do the operation requested by the code that
#   indexed that table entry, or it provides a pointer to another
#   table that indexes more bits of the code.  op indicates whether
#   the entry is a pointer to another table, a literal, a length or
#   distance, an end-of-block, or an invalid code.  For a table
#   pointer, the low four bits of op is the number of index bits of
#   that table.  For a length or distance, the low four bits of op
#   is the number of extra bits to get after the code.  bits is
#   the number of bits in this code or part of the code to drop off
#   of the bit buffer.  val is the actual byte to output in the case
#   of a literal, the base length or distance, or the offset from
#   the current table to the next table.  Each entry is four bytes. */
#typedef struct {
#    unsigned char op;           /* operation, extra bits, table bits */
#    unsigned char bits;         /* bits in this part of the code */
#    unsigned short val;         /* offset in table or code value */
#} code;

class Code(ctypes.Structure):
    """code structure."""

    _fields_ = [
        ('op', ctypes.c_ubyte),
        ('bits', ctypes.c_ubyte),
        ('val', ctypes.c_ushort),
    ]

#/* state maintained between inflate() calls.  Approximately 7K bytes. */
#struct inflate_state {
#    inflate_mode mode;          /* current inflate mode */
#    int last;                   /* true if processing last block */
#    int wrap;                   /* bit 0 true for zlib, bit 1 true for gzip */
#    int havedict;               /* true if dictionary provided */
#    int flags;                  /* gzip header method and flags (0 if zlib) */
#    unsigned dmax;              /* zlib header max distance (INFLATE_STRICT)*/
#    unsigned long check;        /* protected copy of check value */
#    unsigned long total;        /* protected copy of output count */
#    gz_headerp head;            /* where to save gzip header information */
#        /* sliding window */
#    unsigned wbits;             /* log base 2 of requested window size */
#    unsigned wsize;             /* window size or zero if not using window */
#    unsigned whave;             /* valid bytes in the window */
#    unsigned write;             /* window write index */
#    unsigned char FAR *window;  /* allocated sliding window, if needed */
#        /* bit accumulator */
#    unsigned long hold;         /* input bit accumulator */
#    unsigned bits;              /* number of bits in "in" */
#        /* for string and stored block copying */
#    unsigned length;            /* literal or length of data to copy */
#    unsigned offset;            /* distance back to copy string from */
#        /* for table and code decoding */
#    unsigned extra;             /* extra bits needed */
#        /* fixed and dynamic code tables */
#    code const FAR *lencode;    /* starting table for length/literal codes */
#    code const FAR *distcode;   /* starting table for distance codes */
#    unsigned lenbits;           /* index bits for lencode */
#    unsigned distbits;          /* index bits for distcode */
#        /* dynamic table building */
#    unsigned ncode;             /* number of code length code lengths */
#    unsigned nlen;              /* number of length code lengths */
#    unsigned ndist;             /* number of distance code lengths */
#    unsigned have;              /* number of code lengths in lens[] */
#    code FAR *next;             /* next available space in codes[] */
#    unsigned short lens[320];   /* temporary storage for code lengths */
#    unsigned short work[288];   /* work area for code table building */
#    code codes[ENOUGH];         /* space for code tables */
#};
if zlib.ZLIB_VERSION == '1.2.3.4':
    extra_fields = [
        ('sane', ctypes.c_int),
        ('back', ctypes.c_int),
        ('was', ctypes.c_uint),
    ]
    extra_attr = tuple([i[0] for i in extra_fields])
else:
    extra_fields = []
    extra_attr = ()


class InflateState(ctypes.Structure):
    """inflate_state structure."""

    _fields_ = [
        ('mode', ctypes.c_int),
        ('last', ctypes.c_int),
        ('wrap', ctypes.c_int),
        ('havedict', ctypes.c_int),
        ('flags', ctypes.c_int),
        ('dmax', ctypes.c_uint),
        ('check', ctypes.c_ulong),
        ('total', ctypes.c_ulong),
        ('head', ctypes.POINTER(GZHeader)),

        ('wbits', ctypes.c_uint),
        ('wsize', ctypes.c_uint),
        ('whave', ctypes.c_uint),
        ('write', ctypes.c_uint),
        ('window', ctypes.POINTER(ctypes.c_ubyte)),

        ('hold', ctypes.c_ulong),
        ('bits', ctypes.c_uint),

        ('length', ctypes.c_uint),
        ('offset', ctypes.c_uint),

        ('extra', ctypes.c_uint),
        ('lencode', ctypes.POINTER(Code)),
        ('distcode', ctypes.POINTER(Code)),
        ('lenbits', ctypes.c_uint),
        ('distbits', ctypes.c_uint),
        ('ncode', ctypes.c_uint),
        ('nlen', ctypes.c_uint),
        ('ndist', ctypes.c_uint),
        ('have', ctypes.c_uint),
        ('next', ctypes.POINTER(Code)),
        ('lens', ctypes.c_ushort * 320),
        ('work', ctypes.c_ushort * 288),
        ('codes', Code * ENOUGH)
    ] + extra_fields

    simple_attr = ('last', 'wrap', 'havedict', 'flags', 'dmax',
                   'check', 'total', 'wbits', 'wsize', 'whave', 'write',
                   'hold', 'bits', 'length', 'offset', 'offset',
                   'extra', 'lenbits', 'distbits', 'ncode', 'nlen',
                   'ndist', 'have', 'mode') + extra_attr

    def get_state(self):
        """Get the state of inflate_state struct."""
        # head will be always a NULL pointer, as we use raw in/delfate
        state = {}
        # first get the pointers offsets
        #lencode = ctypes.string_at(self.lencode, ctypes.sizeof(Code))
        lencode_addr = ctypes.addressof(self.lencode.contents)
        codes_start = ctypes.addressof(self.codes)
        lencode = lencode_addr - codes_start

        #distcode = ctypes.string_at(self.distcode, ctypes.sizeof(Code))
        distcode = ctypes.addressof(self.distcode.contents) - codes_start

        #next = ctypes.string_at(self.next, ctypes.sizeof(Code))
        next = ctypes.addressof(self.next.contents) - codes_start

        # now get the raw memory data
        codes = ctypes.string_at(ctypes.pointer(self.codes),
                                 ctypes.sizeof(self.codes))
        lens = ctypes.string_at(ctypes.pointer(self.lens),
                                ctypes.sizeof(self.lens))
        work = ctypes.string_at(ctypes.pointer(self.work),
                                ctypes.sizeof(self.work))
        if self.window:
            window = ctypes.string_at(self.window, self.wsize)
        else:
            window = None
        if self.head:
            raise ValueError("gzip resume isn't supported.")
        state = {'lencode': lencode, 'distcode': distcode, 'codes': codes,
                 'window': window, 'lens': lens, 'work': work, 'next': next,
                 'head': None}
        # now add the basic type attributes to the state dict
        for attr_name in self.simple_attr:
            state[attr_name] = getattr(self, attr_name)
        return state

    def set_state(self, old_state, zalloc):
        """Set the state of this inflate state.

        @param old_state: the old state dict.
        @param zalloc: the zalloc function (in case we need to allocate space
                       for the window).
        """
        if old_state['head']:
            raise ValueError("gzip resume isn't supported.")
        # set the basic type attributes from the old state dict
        for attr_name in self.simple_attr:
            setattr(self, attr_name, old_state[attr_name])
        # set the data from the array attributes.
        ctypes.memmove(ctypes.pointer(self.codes),
                       ctypes.c_char_p(old_state['codes']),
                       ctypes.sizeof(self.codes))
        ctypes.memmove(ctypes.pointer(self.lens),
                       ctypes.c_char_p(old_state['lens']),
                       ctypes.sizeof(self.lens))
        ctypes.memmove(ctypes.pointer(self.work),
                       ctypes.c_char_p(old_state['work']),
                       ctypes.sizeof(self.work))
        # fix the Code pointers
        codes_start = ctypes.addressof(self.codes)
        self.lencode = ctypes.pointer(
            Code.from_address(codes_start + old_state['lencode']))
        self.distcode = ctypes.pointer(
            Code.from_address(codes_start + old_state['distcode']))
        self.next = ctypes.pointer(
            Code.from_address(codes_start + old_state['next']))
        # set the window
        if old_state['window']:
            if not self.window:
                # we don't have the window mem allocated
                addr = zalloc(ctypes.c_uint(1 << self.wbits),
                              ctypes.sizeof(ctypes.c_ubyte))
                self.window = ctypes.cast(addr, ctypes.POINTER(ctypes.c_ubyte))
            # set the contents of the window, we don't care about the size as
            # in our use case it's always 1<<zlib.MAX_WBITS.
            ctypes.memmove(self.window, ctypes.c_char_p(old_state['window']),
                           1 << self.wbits)


# this structure is based on this lines from /usr/include/zlib.h
#
# typedef struct z_stream_s {
#     Bytef    *next_in;  /* next input byte */
#     uInt     avail_in;  /* number of bytes available at next_in */
#     uLong    total_in;  /* total nb of input bytes read so far */
#
#     Bytef    *next_out; /* next output byte should be put there */
#     uInt     avail_out; /* remaining free space at next_out */
#     uLong    total_out; /* total nb of bytes output so far */
#
#     char     *msg;      /* last error message, NULL if no error */
#     struct internal_state FAR *state; /* not visible by applications */
#
#     alloc_func zalloc;  /* used to allocate the internal state */
#     free_func  zfree;   /* used to free the internal state */
#     voidpf     opaque;  /* private data object passed to zalloc and zfree */
#
#     int     data_type;  /* best guess about the data type: binary or text */
#     uLong   adler;      /* adler32 value of the uncompressed data */
#     uLong   reserved;   /* reserved for future use */
# } z_stream;


class ResumableZStream(ctypes.Structure):
    """z_stream structure."""

    _fields_ = [
        ("next_in", ctypes.POINTER(ctypes.c_ubyte)),
        ("avail_in", ctypes.c_uint),
        ("total_in", ctypes.c_ulong),
        ("next_out", ctypes.POINTER(ctypes.c_ubyte)),
        ("avail_out", ctypes.c_uint),
        ("total_out", ctypes.c_ulong),
        ("msg", ctypes.c_char_p),
        ("state", ctypes.POINTER(InflateState)),
        ("zalloc", ctypes.c_void_p),
        ("zfree", ctypes.c_void_p),
        ("opaque", ctypes.c_void_p),
        ("data_type", ctypes.c_int),
        ("adler", ctypes.c_ulong),
        ("reserved", ctypes.c_ulong),
    ]

    def get_state(self):
        """Returns the context as a string."""
        # sanity checks
        if self.next_in and self.avail_in > 0:
            raise ValueError("There are pending bytes to process in next_in")
        if self.msg:
            raise ValueError("Can't serialize a stream in a error state.")
        if self.state:
            inflate_state = self.state.contents.get_state()
        else:
            inflate_state = {}
        state = {'total_in': self.total_in,
                 'total_out': self.total_out,
                 'avail_in': self.avail_in,
                 'avail_out': self.avail_out,
                 'data_type': self.data_type,
                 'adler': self.adler,
                 'reserved': self.reserved,
                 'msg': self.msg,
                 'state': inflate_state,
                 'zlib_version': zlib.ZLIB_VERSION}
        return cPickle.dumps(state)

    def set_state(self, old_state):
        """Set the context with a string of data."""
        old_state = cPickle.loads(old_state)
        # first check the version
        if old_state['zlib_version'] != zlib.ZLIB_VERSION:
            raise VersionError("zlib_version: %s, not supported (%s)" %
                               (old_state['zlib_version'], zlib.ZLIB_VERSION))
        # set the data
        self.total_in = old_state['total_in']
        self.total_out = old_state['total_out']
        self.avail_in = old_state['avail_in']
        self.avail_out = old_state['avail_out']
        self.data_type = old_state['data_type']
        self.adler = old_state['adler']
        self.reserved = old_state['reserved']
        inflate_state = old_state['state']
        # build the zalloc function, see zutil.c
        zcalloc = ctypes.CFUNCTYPE(ctypes.c_void_p)(self.zalloc)
        zalloc = lambda items, size: zcalloc(self.opaque, items, size)
        if self.state and inflate_state:
            # set the inflate_state state
            self.state.contents.set_state(inflate_state, zalloc)


class PyTypeObject(ctypes.Structure):
    """PyTypeObject structure."""

    _fields_ = [
        ("ob_refcnt", ctypes.c_size_t),
        ("ob_type", ctypes.c_void_p),
        ("ob_size", ctypes.c_size_t),
        ("tp_name", ctypes.c_char_p)
    ]


class PyObject(ctypes.Structure):
    """PyObject structure."""
    _fields_ = [
        ("ob_refcnt", ctypes.c_size_t),
        ("ob_type", ctypes.POINTER(PyTypeObject))
    ]

# PyObject *
PyObjectPtr = ctypes.POINTER(PyObject)


class CompObject(PyObject):
    """zlibmodule.c CompObject structure."""
    _fields_ = [
        ('zst', ResumableZStream),
        ('unused_data', PyObjectPtr),
        ('unconsumed_tail', PyObjectPtr),
        ('is_initialised', ctypes.c_int)
    ]


class Decompress(object):
    """A zlib.Decompress wrapper that supports get/setting the state."""

    def __init__(self, decompress_obj=None):
        if decompress_obj is None:
            decompress_obj = zlib.decompressobj()
        self._do = decompress_obj
        # get the C Decompress object
        self._c_do = ctypes.cast(ctypes.c_void_p(id(self._do)),
                                 ctypes.POINTER(CompObject)).contents

    @property
    def unconsumed_tail(self):
        """The uncosumed tail."""
        return self._do.unconsumed_tail

    @property
    def unused_data(self):
        """The unused_data."""
        return self._do.unused_data

    def decompress(self, *args, **kwargs):
        """See zlib.decompressobj().decompress method."""
        return self._do.decompress(*args, **kwargs)

    def flush(self, *args, **kwargs):
        """See zlib.decompressobj().flush method."""
        return self._do.flush(*args, **kwargs)

    def copy(self):
        """See zlib.decompressobj().copy method."""
        return Decompress(self._do.copy())

    def set_state(self, z_stream_state):
        """Set the specified z_stream state."""
        self._c_do.zst.set_state(z_stream_state)

    def get_state(self):
        """Get the current z_stream state."""
        return self._c_do.zst.get_state()


def decompressobj(z_stream_state=None, wbits=zlib.MAX_WBITS):
    """Returns a custom Decompress object instance."""
    do = Decompress(decompress_obj=zlib.decompressobj(wbits))
    if z_stream_state is not None:
        do.set_state(z_stream_state)
    return do


class VersionError(Exception):
    """Exception used for version mismatch in z_stream.set_state."""
