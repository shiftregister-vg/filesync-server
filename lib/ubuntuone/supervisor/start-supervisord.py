#!/usr/bin/env python
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

"""Start supervisord."""

import os
import sys

from utilities.utils import os_exec, is_running, get_tmpdir, get_rootdir
import utilities.localendpoints as local


ROOT_DIR = get_rootdir()
TMP_DIR = get_tmpdir()


def write_conf(tpl, inet_http_server_port):
    """Write out filled in conf template in tmp."""
    template = open(os.path.join(ROOT_DIR, tpl)).read()

    template_vars = {}
    template_vars['inet_http_server_port'] = inet_http_server_port
    template_vars['basepath'] = ROOT_DIR
    template_vars['tmp_dir'] = TMP_DIR

    conf = template % template_vars

    conf_file_path = os.path.join(TMP_DIR,
                                  os.path.basename(tpl).replace('.tpl', ''))

    with open(conf_file_path, 'w') as conf_file:
        conf_file.write(conf)
    return conf_file_path


def main(tpls):
    """main."""
    main_tpl = tpls[0]
    main_name = os.path.basename(main_tpl).replace('.conf.tpl', '')
    pid_file = os.path.join(TMP_DIR, main_name + '.pid')
    port_name = main_name + '-rpc'

    # first, check if it's already running
    if is_running("supervisor", pid_file, port_name):
        print "Supervisor [%s] already running" % main_name
        sys.exit(0)

    print "Starting supervisor [%s]" % main_name

    # get port
    inet_http_server_port, = local.allocate_ports(1)
    local.register_local_port(port_name, inet_http_server_port)

    conf_file_path = write_conf(main_tpl, inet_http_server_port)
    for tpl in tpls[1:]:
        write_conf(tpl, inet_http_server_port)

    os.chdir(ROOT_DIR)
    try:
        os.mkdir(os.path.join(TMP_DIR, main_name + '-childlog'))
    except OSError:
        pass

    os_exec("/usr/bin/supervisord", "-c", conf_file_path)


if __name__ == "__main__":
    main(sys.argv[1:])
