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

from utilities import utils


def launch(progname, username, params=None, environ=None, verbose=False,
           homedir=None, machine_suffix=None, host=None, api_port=None,
           auth_data=None):
    """Run a program in a developer context.

    @param progname: the program name
    @param username: the user name
    @param params: extra parameters to pass to the program
    @param environ: a dict of environment variables to set before calling
        the program

    """

    if auth_data is None:
        # import the function here, as it needs the DB up... so, if we
        # already have auth_data, don't need the DB ;)
        from utilities.userutils import get_auth_info_from_keyfile

        data = get_auth_info_from_keyfile(username)
        if not data:
            raise ValueError()
        # build the auth_data with the file info
        auth_data = data['username'] + ":" + data['password']

    if host is None:
        host = "localhost"

    if api_port is None:
        api_port = int(open('tmp/filesyncserver.port.ssl').read())

    extra_args = ["--port", str(api_port), "--host", host,
                  "--auth", auth_data]
    env = os.environ.copy()

    if 'ubuntuone-syncdaemon' in progname:
        if machine_suffix is not None:
            username += machine_suffix
        if homedir is None:
            base = os.path.join(utils.get_rootdir(), 'tmp',
                                'syncdaemon_homes', username)
            home = os.path.join(base, 'home')
        else:
            base = os.path.dirname(homedir)
            home = homedir

        data = os.path.join(home, '.local/share/ubuntuone/syncdaemon')
        root = os.path.join(home, 'Ubuntu One')
        shares = os.path.join(home, '.local/share/ubuntuone/shares')
        env['HOME'] = home
        env['XDG_DATA_HOME'] = os.path.join(home, '.local/share')
        env['XDG_CONFIG_HOME'] = os.path.join(home, '.config')
        env['XDG_CACHE_HOME'] = os.path.join(home, '.cache')
        if not os.path.exists(base):
            os.makedirs(base)
        if not os.path.exists(data):
            os.makedirs(data)
        # get the progname root (two levels up)
        progname_root = os.path.dirname(os.path.dirname(progname))
        config_files_root = os.path.join(progname_root, 'data')
        config_file = os.path.join(config_files_root, "syncdaemon.conf")
        dev_config_file = os.path.join(config_files_root,
                                       "syncdaemon-dev.conf")
        log_config_file = os.path.join(config_files_root,
                                       "logging.conf")
        if not os.path.exists(log_config_file):
            msg = "Please run: cd %s && ./setup.py build; cd -\n"
            print msg % progname_root
            return
        extra_args.insert(0, config_file)
        extra_args.insert(1, dev_config_file)
        extra_args.insert(2, log_config_file)
        extra_args += ["--data_dir", data, "--root_dir", root,
                       '--shares_dir', shares]

    args = [progname] + extra_args
    if params:
        args += params

    if environ:
        env.update(environ)

    if verbose:
        print "Executing:", " ".join(args)
    os.execvpe(progname, args, env)
