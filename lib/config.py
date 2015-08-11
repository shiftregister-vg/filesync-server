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

"""A config management layer."""

import os
import yaml

from utilities import devconfig


def _load():
    """Load configuration from a file."""
    fpath = os.environ["CONFIG"]
    with open(fpath, "rt") as fh:
        data = yaml.load(fh)
    return data


class _Config(dict):
    """The configuration holder."""
    def __init__(self, data=None):
        if data is None:
            data = _load()
        super(_Config, self).__init__(data)

    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict) and not isinstance(value, _Config):
            wrapped = _Config(value)
            self[name] = wrapped
            return wrapped
        else:
            return value

    def __setattr__(self, name, value):
        self[name] = value

    def __str__(self):
        return "<Config at %d: %s>" % (
            id(self), super(_Config, self).__str__())


# instantiate the config and dynamically load the active ports
config = _Config()
devconfig.development_ports(config)
