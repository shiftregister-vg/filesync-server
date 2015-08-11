# -*- coding: utf-8 -*-

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

"""Check the message validation step."""

import re

from ubuntuone.storageprotocol import request
from ubuntuone.storage.server.testing.testcase import TestWithDatabase


def is_protocol_error(failure):
    """
    Returns whether the failure is a PROTOCOL_ERROR
    """
    return (failure.check(request.StorageProtocolProtocolError) or
            failure.getErrorMessage() == 'PROTOCOL_ERROR'
            or re.search(r'^\s*type: PROTOCOL_ERROR\s*$',
                         failure.getErrorMessage(),
                         re.MULTILINE))


def fail_check(client):
    """
    Returns a closure to be used as an errback that checks that the
    failure is a PROTOCOL_ERROR, and passes the test if so.
    """
    def fail_check_cb(f):
        """
        Check the failure is a PROTOCOL_ERROR
        """
        if is_protocol_error(f):
            return client.test_done('ok')
        else:
            return f
    return fail_check_cb


class TestValidation(TestWithDatabase):
    """Tests for message validation."""

    def test_make_invalid_share(self):
        """
        Test validation for invalid MAKE_DIR works
        """
        def _worker(client):
            """
            async test
            """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir('invalid share', r, "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, fail_check(client))
            return d
        return self.callback_test(_worker)

    def test_make_invalid_parent(self):
        """
        Test validation for invalid MAKE_DIR works
        """
        def _worker(client):
            """
            async test
            """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_dir('', 'invalid parent', "hola"),
                client.test_fail)
            d.addCallbacks(client.test_fail, fail_check(client))
            return d
        return self.callback_test(_worker)

    def test_move_invalid_new_parent_node(self):
        """
        Test we fail moves to invalid nodes
        """
        def _worker(client):
            """
            async test
            """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.get_root(), client.test_fail)
            d.addCallbacks(
                lambda r: client.make_file('', r, "hola"),
                client.test_fail)
            d.addCallbacks(lambda mk: client.move('', mk.new_id,
                                                  'invalid new parent node',
                                                  'chau'),
                           client.test_fail)
            d.addCallbacks(client.test_fail, fail_check(client))
            return d
        return self.callback_test(_worker)

    def test_accept_share_invalid_share_id(self):
        """
        Test we fail accept_shares with invalid share_ids
        """
        def _worker(client):
            """
            async test
            """
            d = client.dummy_authenticate("open sesame")
            d.addCallbacks(lambda r: client.accept_share('invalid share id',
                                                         'Yes'),
                           client.test_fail)
            d.addCallbacks(client.test_fail, fail_check(client))
            return d
        return self.callback_test(_worker)
