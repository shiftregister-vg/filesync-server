#!/usr/bin/python -Wignore

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

"""Script to execute client stuff in dev environment."""

import os
import sys

import _pythonpath

# fix environment before further imports
os.environ['CONFIG'] = os.path.join("configs/development.yaml")
os.environ["DJANGO_SETTINGS_MODULE"] = "backends.django_settings"

from utilities import dev_launcher


def usage():
    """Return the usage."""
    return ("dev_launcher PROGNAME USERNAME [--machine=N] [args*]\n"
            "runs PROGNAME with args plus:\n"
            "     --port with the local server port\n"
            "     --host localhost\n"
            "     --oauth keys for user USERNAME\n")


def main(args):
    """run devlauncher"""
    from optparse import OptionParser
    if len(args) < 3:
        print usage()
        return

    parser = OptionParser(usage=usage())
    parser.add_option('-p', '--port', dest='port',
                      help="The port to where connect")
    parser.add_option('-t', '--host', dest='host',
                      help="The host to where connect")
    parser.add_option('-a', '--auth', dest='auth',
                      help="Auth data")
    parser.add_option('-m', '--machine', dest='machine',
                      help="Machine number (to have multiple launchers with "
                      "the same user)")

    # check to see if we have extra params for the command to execute
    if "--" in args:
        pos = args.index("--")
        params = args[pos + 1:]
        args = args[:pos]
    else:
        params = []

    (options, pargs) = parser.parse_args(args[1:])
    progname, username = pargs
    if options.machine:
        machine_suffix = "_" + options.machine
    else:
        machine_suffix = None

    lib_dir = _pythonpath.get_lib_dir()
    d = dict(
        PYTHONPATH=lib_dir,
        XDG_CACHE_HOME="tmp/xdg_cache",
    )

    dev_launcher.launch(progname, username, params, environ=d, verbose=True,
                        machine_suffix=machine_suffix, host=options.host,
                        api_port=options.port, auth_data=options.auth)

if __name__ == "__main__":
    main(sys.argv)
