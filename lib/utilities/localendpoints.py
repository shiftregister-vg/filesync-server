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

"""Utilities for endpoints which are per-development-tree"""

import os
import errno
import socket

from os.path import abspath, isdir
from warnings import warn
from bzrlib.workingtree import WorkingTree


def _bad_rootdir(rootdir):
    """Tell whether the given rootdir is bad"""
    return (rootdir is None                      # shouldn't happen
            or not isdir(rootdir)                # garbage in env or bzr error
            or not os.access(rootdir, os.R_OK))  # perms are wrong


def get_rootdir():
    """Work out what the rootdir should be"""
    # strange things can happen when you mess up your ROOTDIR, so it pays
    # to be careful
    rootdir = None
    if 'ROOTDIR' in os.environ:
        rootdir = os.environ['ROOTDIR']
        if _bad_rootdir(rootdir):
            warn('Environment variable ROOTDIR is bad, falling back to bzr')
            rootdir = None
        elif not abspath(__file__).startswith(abspath(rootdir)):
            warn('Environment variable ROOTDIR is pointing somewhere else')

    if rootdir is None:
        rootdir = WorkingTree.open_containing(__file__)[0].basedir
        if _bad_rootdir(rootdir):
            raise RuntimeError("Bad ROOTDIR %r (bzr trouble?)" % (rootdir,))
    return rootdir


def service_name_to_file(service_name, ssl=False):
    """Maps a service name to a .port file in the tree tmp/ dir"""
    if ssl:
        suffix = ".ssl"
    else:
        suffix = ""
    filename = "%s.port%s" % (service_name, suffix)
    return os.path.join(get_rootdir(), 'tmp', filename)


def get_local_port(service_name, ssl=False):
    """Returns the port number for the named service"""
    port_file = service_name_to_file(service_name, ssl=ssl)
    if os.path.exists(port_file):
        with open(port_file, 'r') as stream:
            return int(stream.read().strip())


def resolve_port(port_or_service_name, ssl=False):
    """Returns port if a number or the port number for the named service"""
    try:
        return int(port_or_service_name)
    except ValueError:
        return get_local_port(port_or_service_name, ssl=ssl)


def register_local_port(service_name, port, ssl=False):
    """Registers a port number for a given service name"""
    port_file = service_name_to_file(service_name, ssl=ssl)
    with open(port_file, 'w') as stream:
        stream.write("%d\n" % (port,))


def unregister_local_port(service_name, ssl=False):
    """Unregisters the port for the given service name"""
    port_file = service_name_to_file(service_name, ssl=ssl)
    try:
        os.unlink(port_file)
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise


def allocate_ports(n=1):
    """
    Allocate n unused ports

    There is a small race condition here (between the time we allocate
    the port, and the time it actually gets used), but for the purposes
    for which this function gets used it isn't a problem in practice.
    """
    sockets = map(lambda _: socket.socket(), xrange(n))
    try:
        for s in sockets:
            s.bind(('localhost', 0))
        ports = map(lambda s: s.getsockname()[1], sockets)
    finally:
        for s in sockets:
            s.close()
    return ports


def get_local_server(name):
    """Like get_local_port, but return a host:port instead."""
    port = get_local_port(name)
    if port is None:
        return None
    return "127.0.0.1:%d" % port
