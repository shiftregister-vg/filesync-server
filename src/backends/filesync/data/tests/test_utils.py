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

"""Tests for backends.filesync.data.utils."""

import unittest

import uuid
from backends.filesync.data.utils import (
    decode_base62,
    decode_uuid,
    encode_base62,
    encode_uuid,
    get_keywords_from_path,
    get_node_public_key,
    get_public_file_url,
    make_nodekey,
    parse_nodekey,
    split_nodekey,
    Base62Error,
    NodeKeyParseError,
)

VOLUME_UUID = uuid.UUID('157b4a51-2c88-4fe6-8ce3-08b98153054d')
NODE_UUID = uuid.UUID('5b64bc7e-0ac1-4da6-875b-a063bdd62bac')
VOLUME_KEY = encode_uuid(VOLUME_UUID)
NODE_KEY = encode_uuid(NODE_UUID)


class NodeKeyTests(unittest.TestCase):
    """Tests for the parse_nodekey() helper."""

    def test_encode_decode(self):
        """Test encode and decode uuid."""
        self.assertEquals(encode_uuid(VOLUME_UUID), VOLUME_KEY)
        self.assertEquals(decode_uuid(VOLUME_KEY), VOLUME_UUID)

    def test_make_split_nodekey(self):
        """Test make and split_nodekey."""
        key = make_nodekey(VOLUME_UUID, NODE_UUID)
        self.assertEqual(key, "%s:%s" % (VOLUME_KEY, NODE_KEY))
        volume_id, node_id = split_nodekey(key)
        self.assertEqual(volume_id, VOLUME_UUID)
        self.assertEqual(node_id, NODE_UUID)

    def test_parse_nodekey(self):
        """parse_nodekey() handles a plain encoded node ID."""
        volume_id, node_id = parse_nodekey(NODE_KEY)
        self.assertEqual(volume_id, None)
        self.assertEqual(node_id, NODE_UUID)

    def test_parse_nodekey_with_volume_id(self):
        """parse_nodekey() handles an encoded volume ID and node ID."""
        volume_id, node_id = parse_nodekey("%s:%s" % (VOLUME_KEY, NODE_KEY))
        self.assertEqual(volume_id, VOLUME_UUID)
        self.assertEqual(node_id, NODE_UUID)

    def test_parse_nodekey_unicode(self):
        """parse_nodekey() accepts unicode strings."""
        volume_id, node_id = parse_nodekey(u"%s:%s" % (VOLUME_KEY, NODE_KEY))
        self.assertEqual(volume_id, VOLUME_UUID)
        self.assertEqual(node_id, NODE_UUID)

    def test_parse_nodekey_non_ascii(self):
        """parse_nodekey() fails on non-ASCII compatible unicode strings."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          u"\u00A0")

    def test_parse_nodekey_short_node_id(self):
        """paerse_nodekey() fails if the node ID is too short."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          "FXtKUSyIT-aM4wi5gVMFTQ:W2S8fgrBTa")

    def test_parse_nodekey_long_node_id(self):
        """paerse_nodekey() fails if the node ID is too long."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          u"%s:%sAAA" % (VOLUME_KEY, NODE_KEY))

    def test_parse_nodekey_short_volume_id(self):
        """paerse_nodekey() fails if the volume ID is too short."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          "FXtKUSyIT-aM4wi5gV:W2S8fgrBTaaHW6BjvdYrrA")

    def test_parse_nodekey_long_volume_id(self):
        """paerse_nodekey() fails if the volume ID is too long."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          u"%sAAA:%s" % (VOLUME_KEY, NODE_KEY))

    def test_parse_nodekey_empty_volume_id(self):
        """parse_nodekey() accepts an empty volume ID component."""
        volume_id, node_id = parse_nodekey(":%s" % NODE_KEY)
        self.assertEqual(volume_id, None)
        self.assertEqual(node_id, NODE_UUID)

    def test_parse_nodekey_empty_node_id(self):
        """parse_nodekey() fails on an empty node ID component."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          "%s:" % VOLUME_KEY)

    def test_parse_nodekey_bad_chars(self):
        """parse_nodekey() fails on bad characters."""
        self.assertRaises(NodeKeyParseError, parse_nodekey,
                          "$%W2S8fgrBTaaHW6BjvdYr")

    def test_get_node_public_key(self):
        """Test get_node_public_key."""
        node = FakeNode()
        node.public_id = 1
        node.public_uuid = uuid.UUID(int=12)
        self.assertTrue(get_node_public_key(node, False),
                        encode_base62(node.public_id))
        self.assertTrue(get_node_public_key(node, True),
                        encode_base62(node.public_uuid.int, padded_to=22))

    def test_get_public_url(self):
        """Test get_public_url function."""
        node = FakeNode()
        self.assertTrue(get_public_file_url(node) is None)
        node.public_id = 1
        self.assertTrue(get_public_file_url(node).endswith(
            "/%s/" % encode_base62(node.public_id)))
        #using a short value here to make sure padding works
        node.public_uuid = uuid.UUID(int=12)
        self.assertTrue(get_public_file_url(node).endswith(
            "/%s" % encode_base62(node.public_uuid.int, padded_to=22)))


class Base62Tests(unittest.TestCase):
    """Tests for the base-62 encoder and decoder."""

    def test_encode_base62(self):
        """Tests for encode_base62."""
        self.assertEqual("1", encode_base62(1))
        self.assertEqual("A", encode_base62(10))
        self.assertEqual("a", encode_base62(36))
        self.assertEqual("10", encode_base62(62))
        self.assertEqual("100", encode_base62(62 * 62))
        self.assertEqual("00100", encode_base62(62 * 62, padded_to=5))
        # Only non-negative values a can be encoded.
        self.assertRaises(Base62Error, encode_base62, -42)
        self.assertRaises(Base62Error, encode_base62, 0)
        self.assertRaises(Base62Error, encode_base62, 62, padded_to=1)

    def test_decode_base62(self):
        """Tests for decode_base62."""
        self.assertEqual(1, decode_base62("1"))
        self.assertEqual(10, decode_base62("A"))
        self.assertEqual(36, decode_base62("a"))
        self.assertEqual(62, decode_base62("10"))
        self.assertEqual(62 * 62, decode_base62("100"))
        self.assertEqual(62 * 62, decode_base62("000100", allow_padding=True))

        # The empty string is not allowed.
        self.assertRaises(Base62Error, decode_base62, "")
        # zero-prefixed strings are not allowed
        self.assertRaises(Base62Error, decode_base62, "05")
        # Bad characters are also not allowed:
        self.assertRaises(Base62Error, decode_base62, "#!abc")
        # Try with a value to large for a UUID
        val = encode_base62(1 << 128, padded_to=22)
        self.assertRaises(Base62Error, decode_base62, val)


class FakeNode(object):
    """A fake node for testing."""
    public_id = None
    public_uuid = None

    @property
    def publicfile_id(self):
        return self.public_id


class KeywordsTests(unittest.TestCase):
    """Test keyword function."""

    def test_basic_path(self):
        kw = get_keywords_from_path(
            u"~/Ubuntu One/is/the path/path! for/my%$files/here.txt")
        # result should not include base directory and should be sorted
        self.assertEqual(
            list(sorted(kw)),
            sorted(['files', 'for', 'is', 'here', 'path', 'the', 'txt', 'my']))

    def test_documents_path(self):
        kw = get_keywords_from_path(
            u"~/Documents/is/the path/path! for/my%$files/here.txt")
        # result should include documents and should be sorted
        self.assertEqual(
            sorted(kw),
            sorted(['documents', 'files', 'for', 'is', 'here', 'path', 'the',
                    'txt', 'my']))

    def test_normalized_path(self):
        kw = get_keywords_from_path(
            u"~/Do\u0304cuments/is/the path/path!"
            u" fo\u0304r/my%$files/hero\u0304.txt")
        # unicode character should be normalized
        self.assertEqual(
            sorted(kw),
            sorted(['documents', 'files', 'for', 'is', 'hero', 'path', 'the',
                    'txt', 'my']))
