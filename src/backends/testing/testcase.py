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

"""TestCases for testing with backends."""

from django.test import TestCase, utils
from django.test.client import RequestFactory
from testresources import ResourcedTestCase

from backends.testing.resources import get_all_db_resources


class BaseTestCase(TestCase):
    """Base TestCase: provides a Factory, RequestFactory and a mock SSO."""

    request_factory = RequestFactory()

    def setUp(self):
        super(BaseTestCase, self).setUp()

        # django's pre_setup, currently not being called because trial test
        # runner will not __call__ every test case -- assign client manually
        self.client = self.client_class()

        # the following is usually called from the DjangoTestSuiteRunner
        # (django/test/simple.py) -- since we use trial, need to call by hand
        utils.setup_test_environment()
        self.addCleanup(utils.teardown_test_environment)


class DatabaseResourceTestCase(BaseTestCase, ResourcedTestCase):
    """Base TestCase for Tests that use the database."""

    resources = get_all_db_resources()
