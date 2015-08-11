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

"""Tests for metric services."""

from mock import patch
from testtools import TestCase

from metrics import METER_UTILITY, NamespaceMeter
from metrics.services import oops_saved, revno
from versioninfo import version_info


class ServicesTest(TestCase):
    """Tests for metric services."""

    def test_meters_oops_by_service(self):
        """The service is able to meter an oops scoped to services."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(METER_UTILITY, 'get_service_meter'):
            METER_UTILITY.get_service_meter.return_value = service_meter

            oops_saved()

            self.assertTrue(METER_UTILITY.get_service_meter.called)

    def test_meters_oops_with_normal_meter(self):
        """The service is able to meter an oops by normal meter."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(service_meter, 'meter',
                          spec=NamespaceMeter) as meter:
            oops_saved()

            meter.assert_called_with('oops_saved')

    def test_meters_oops_passing_a_report(self):
        """The service is able to meter an oops passing a report."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(service_meter, 'meter',
                          spec=NamespaceMeter) as meter:
            oops_saved(report=dict())

            meter.assert_called_with('oops_saved')

    def test_meters_oops_passing_a_context(self):
        """The service is able to meter an oops passing a context."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(service_meter, 'meter',
                          spec=NamespaceMeter) as meter:
            oops_saved(context='some oops context')

            meter.assert_called_with('oops_saved')

    def test_oops_saved_with_no_report(self):
        """oops_saved returns an empty list with no report."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(METER_UTILITY, 'get_service_meter'):
            METER_UTILITY.get_service_meter.return_value = service_meter

            self.assertEqual([], oops_saved())

    def test_oops_saved_with_report_with_no_id(self):
        """oops_saved returns an empty list with no id in the report."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(METER_UTILITY, 'get_service_meter'):
            METER_UTILITY.get_service_meter.return_value = service_meter

            self.assertEqual([], oops_saved(report=dict()))

    def test_oops_saved_with_report_with_id(self):
        """oops_saved returns a non-empty list with an id in the report."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(METER_UTILITY, 'get_service_meter'):
            METER_UTILITY.get_service_meter.return_value = service_meter
            the_id = 'an id'
            self.assertEqual([the_id], oops_saved(report=dict(id=the_id)))

    def test_meters_revision_by_service(self):
        """The service is able to meter a revision scoped to services."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(METER_UTILITY, 'get_service_meter'):
            METER_UTILITY.get_service_meter.return_value = service_meter

            revno()

            self.assertTrue(METER_UTILITY.get_service_meter.called)

    def test_meters_revno_with_gauge_meter(self):
        """The service is able to meter a revision by gauge meter."""

        service_meter = METER_UTILITY.get_service_meter()

        with patch.object(service_meter, 'gauge',
                          spec=NamespaceMeter) as gauge:
            revno()

            gauge.assert_called_with('revno', version_info['revno'])
