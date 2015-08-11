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

"""Resumable hashing

A ctypes interface to libcrypto's sha1 functions that mimics hashlib's sha1
interface but also supports getting and setting the context as a string to
suspend/serialize it and then resume the hashing at a later time.
"""

import ctypes
crypto = ctypes.cdll.LoadLibrary("libcrypto.so")

# this structure is based on this lines from /usr/include/openssl/sha.h
#
# #define SHA_LONG unsigned int
# #define SHA_LBLOCK      16
# typedef struct SHAstate_st
#        {
#        SHA_LONG h0,h1,h2,h3,h4;
#        SHA_LONG Nl,Nh;
#        SHA_LONG data[SHA_LBLOCK];
#        unsigned int num;
#        } SHA_CTX;


class ResumableSHA1(ctypes.Structure):
    """A sha1 hash calculator with get_context/set_context."""

    method_name = "sha1"

    _fields_ = [
        ("h0", ctypes.c_uint),
        ("h1", ctypes.c_uint),
        ("h2", ctypes.c_uint),
        ("h3", ctypes.c_uint),
        ("h4", ctypes.c_uint),
        ("Nl", ctypes.c_uint),
        ("Nh", ctypes.c_uint),
        ("data", ctypes.c_uint * 16),
        ("num", ctypes.c_uint),
    ]

    def __init__(self):
        super(ResumableSHA1, self).__init__()
        self._digest = None

    def set_context(self, data):
        """Set the context with a string of data."""
        return ctypes.memmove(ctypes.pointer(self),
                              ctypes.c_char_p(data),
                              ctypes.sizeof(self))

    def get_context(self):
        """Returns the context as a string."""
        return ctypes.string_at(ctypes.pointer(self), ctypes.sizeof(self))

    def update(self, data):
        """Update this hash object's state with the provided string."""
        self._digest = None
        crypto.SHA1_Update(ctypes.pointer(self),
                           ctypes.c_char_p(data),
                           len(data))

    def digest(self):
        """Return the digest value as a string of binary data."""
        if self._digest is not None:
            return self._digest
        res = ctypes.create_string_buffer(20)
        crypto.SHA1_Final(res, ctypes.pointer(self.copy()))

        self._digest = res.raw
        return self._digest

    def hexdigest(self):
        """Return the digest value as a string of hexadecimal digits."""
        digest = self.digest()
        return digest.encode('hex')

    def content_hash(self):
        """Adds hex digest to content hash."""
        return self.method_name + ":" + self.hexdigest()

    def copy(self):
        """Return a copy of the hash object."""
        other = ResumableSHA1()
        other.set_context(self.get_context())
        return other

# prototype definitions for the sha1 functions
# from /usr/include/openssl/sha.h

# int SHA1_Init(SHA_CTX *c);
# int SHA1_Update(SHA_CTX *c, const void *data, size_t len);
# int SHA1_Final(unsigned char *md, SHA_CTX *c);

# int SHA1_Init(SHA_CTX *c);
crypto.SHA1_Init.argtypes = [ctypes.POINTER(ResumableSHA1)]
crypto.SHA1_Init.restype = ctypes.c_int

# int SHA1_Update(SHA_CTX *c, const void *data, size_t len);
crypto.SHA1_Update.argtypes = \
    [ctypes.POINTER(ResumableSHA1), ctypes.c_void_p, ctypes.c_size_t]
crypto.SHA1_Update.restype = ctypes.c_int

# int SHA1_Final(unsigned char *md, SHA_CTX *c);
crypto.SHA1_Final.argtypes = [ctypes.c_char_p, ctypes.POINTER(ResumableSHA1)]
crypto.SHA1_Final.restype = ctypes.c_int


def sha1(data=None):
    """Returns a sha1 hash object; optionally initialized with a string."""
    ctx = ResumableSHA1()
    crypto.SHA1_Init(ctypes.pointer(ctx))
    if data is not None:
        ctx.update(data)
    return ctx


from ubuntuone.storageprotocol import content_hash


class ResumableMagicContentHash(content_hash.MagicContentHash):
    """Resumable MagicContentHash."""

    method = staticmethod(sha1)

    def set_context(self, data):
        """Set the context with a string of data."""
        self.hash_object.set_context(data)

    def get_context(self):
        """Returns the context as a string."""
        return self.hash_object.get_context()

    def copy(self):
        """Return a copy of the hash object."""
        other = ResumableMagicContentHash()
        other.set_context(self.get_context())
        return other

resumable_magic_hash_factory = ResumableMagicContentHash
