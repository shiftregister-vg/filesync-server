#!/usr/bin/env python

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

"""A simple ping client."""

import warnings
warnings.simplefilter("ignore")

import Queue
import cmd
import os
import shlex
import tempfile
import time
import traceback
import uuid
import zlib

from threading import Thread
from optparse import OptionParser

try:
    from twisted.internet import gireactor
    gireactor.install()
except ImportError:
    from twisted.internet import glib2reactor
    glib2reactor.install()

from dbus.mainloop.glib import DBusGMainLoop
DBusGMainLoop(set_as_default=True)

from twisted.internet import reactor, defer, ssl
from twisted.python.failure import Failure
from twisted.python.util import mergeFunctionMetadata
try:
    import gobject
    gobject.set_application_name('cmd_client')
except ImportError:
    pass

import _pythonpath  # NOQA

from ubuntuone.storageprotocol.client import (
    StorageClientFactory, StorageClient)
from ubuntuone.storageprotocol import request, dircontent_pb2, volumes
from ubuntuone.storageprotocol.dircontent_pb2 import \
    DirectoryContent, DIRECTORY
from ubuntuone.storageprotocol.content_hash import content_hash_factory, crc32


def show_volume(volume):
    """Show a volume."""
    if isinstance(volume, volumes.ShareVolume):
        print "Share %r (other: %s, access: %s, id: %s)" % (
            volume.share_name, volume.other_username,
            volume.access_level, volume.volume_id)
    elif isinstance(volume, volumes.UDFVolume):
        print "UDF %r (id: %s)" % (volume.suggested_path, volume.volume_id)


class CmdStorageClient(StorageClient):
    """Simple client that calls a callback on connection."""

    def connectionMade(self):
        """Setup and call callback."""

        StorageClient.connectionMade(self)
        if self.factory.current_protocol not in (None, self):
            self.factory.current_protocol.transport.loseConnection()
        self.factory.current_protocol = self
        self.factory.cmd.status = "connected"
        print "Connected."

    def connectionLost(self, reason=None):
        """Callback for connection lost"""

        if self.factory.current_protocol is self:
            self.factory.current_protocol = None
            self.factory.cmd.status = "disconnected"
            if reason is not None:
                print "Disconnected: %s" % reason.value
            else:
                print "Disconnected: no reason"


class CmdClientFactory(StorageClientFactory):
    """A cmd protocol factory."""

    protocol = CmdStorageClient

    def __init__(self, cmd):
        """Create the factory"""
        self.cmd = cmd
        self.current_protocol = None

    def clientConnectionFailed(self, connector, reason):
        """We failed at connecting."""
        print 'ERROR: Connection failed. Reason:', reason.value
        self.current_protocol = None
        self.cmd.status = "disconnected"


def split_args(args):
    """Split a string using shlex."""
    sh = shlex.shlex(args, "args", True)
    sh.wordchars = sh.wordchars + '-./'
    result = []
    part = sh.get_token()
    while part is not None:
        result.append(part)
        part = sh.get_token()
    return result


def parse_args(*args, **kwargs):
    """Decorates a method so that we can parse its arguments:
    Example:
    @parse_args(int, int):
    def p(self, one, two):
        print one + two
    o.p("10 10")
    will print 20.
    """
    def inner(method):
        """the actual decorator"""
        def parser(self, rest):
            """the parser"""
            parts = split_args(rest)

            if len(parts) != len(args):
                print (
                    "ERROR: Wrong number of arguments. Expected %i, got %i" % (
                        len(args), len(parts)))
                return

            result = []
            for i, (constructor, part) in enumerate(zip(args, parts)):
                try:
                    value = constructor(part)
                except ValueError:
                    print "ERROR: cant convert argument %i to %s" % (
                        i, constructor)
                    return
                result.append(value)

            return method(self, *result)
        return mergeFunctionMetadata(method, parser)
    return inner


def require_connection(method):
    """This decorator ensures functions that require a connection dont
    get called without one"""

    def decorator(self, *args):
        """inner"""
        if self.status != "connected":
            print "ERROR: Must be connected."
            return
        else:
            return method(self, *args)
    return mergeFunctionMetadata(method, decorator)


def show_exception(function):
    """Trap exceptions and print them."""
    def decorator(*args, **kwargs):
        """inner"""
        # we do want to catch all
        try:
            function(*args, **kwargs)
        except Exception:
            traceback.print_exc()
    return mergeFunctionMetadata(function, decorator)


class ClientCmd(cmd.Cmd):
    """An interactive shell to manipulate the server."""

    use_rawinput = False

    def __init__(self, username, password):
        """Create the instance."""

        cmd.Cmd.__init__(self)
        self.thread = Thread(target=self._run)
        self.thread.setDaemon(True)
        self.thread.start()
        self.factory = CmdClientFactory(self)
        self.connected = False
        self.status = "disconnected"
        self.cwd = "/"
        self.volume = request.ROOT
        self.volume_root = None
        self.queue = Queue.Queue()

        self.username = username
        self.password = password

        self.volumes = set()
        self.shares = set()

    def _run(self):
        """Run the reactor in bg."""
        reactor.run(installSignalHandlers=False)

    @property
    def prompt(self):
        """Our prompt is our path."""
        return "%s $ " % self.cwd

    def emptyline(self):
        """We do nothing on an empty line."""
        return

    def defer_from_thread(self, function, *args, **kwargs):
        """Do twisted defer magic to get results and show exceptions."""

        queue = Queue.Queue()

        def runner():
            """inner."""
            # we do want to catch all
            try:
                d = function(*args, **kwargs)
                if isinstance(d, defer.Deferred):
                    d.addBoth(queue.put)
                else:
                    queue.put(d)
            except Exception, e:
                queue.put(e)

        reactor.callFromThread(runner)
        result = queue.get()
        if isinstance(result, Exception):
            raise result
        elif isinstance(result, Failure):
            result.raiseException()
        else:
            return result

    def get_cwd_id(self):
        """Get the id of the current working directory."""

        parts = [part for part in self.cwd.split("/") if part]
        # this will block forever if we didnt authenticate
        parent_id = self.get_root()
        for part in parts:
            if not self.is_dir(parent_id, part):
                raise ValueError("cwd is not a directory")
            parent_id = self.get_child_id(parent_id, part)

        return parent_id

    def get_root(self):
        """Get the root id."""
        if self.volume_root:
            return self.volume_root
        else:
            return self.defer_from_thread(
                self.factory.current_protocol.get_root)

    def get_id_from_filename(self, filename):
        """Get a node id from a filename."""

        root = self.cwd
        parent_id = self.get_root()
        if filename and filename[0] == "/":
            newdir = os.path.normpath(filename)
        else:
            newdir = os.path.normpath(os.path.join(root, filename))
        parts = [part for part in newdir.split("/") if part]
        if not parts:
            return parent_id
        file = parts[-1]
        parts = parts[:-1]

        for part in parts:
            if not self.is_dir(parent_id, part):
                raise ValueError("not a directory")

            parent_id = self.get_child_id(parent_id, part)
        return self.get_child_id(parent_id, file)

    def is_dir(self, parent_id, name):
        """Is name inside of parent_id a directory?"""

        content = self.get_content(parent_id)
        unserialized_content = DirectoryContent()
        unserialized_content.ParseFromString(content)
        for entry in unserialized_content.entries:
            if entry.name == name and entry.node_type == DIRECTORY:
                return True
        return False

    def get_child_id(self, parent_id, name):
        """Get the node id of name inside of parent_id."""
        content = self.get_content(parent_id)
        unserialized_content = DirectoryContent()
        unserialized_content.ParseFromString(content)
        for entry in unserialized_content.entries:
            if entry.name == name:
                return entry.node
        raise ValueError("not found")

    def get_file(self, filename):
        """Get the content of filename."""

        node_id = self.get_id_from_filename(filename)
        content = self.get_content(node_id)
        return content

    def get_hash(self, node_id):
        """Get the hash of node_id."""
        def _got_query(query):
            """deferred part."""
            message = query[0][1].response[0]
            return message.hash

        def _query():
            """deferred part."""
            d = self.factory.current_protocol.query(
                [(self.volume, node_id, request.UNKNOWN_HASH)]
            )
            d.addCallback(_got_query)
            return d
        return self.defer_from_thread(_query)

    def get_content(self, node_id):
        """Get the content of node_id."""

        hash = self.get_hash(node_id)

        def _get_content():
            """deferred part."""
            d = self.factory.current_protocol.get_content(self.volume,
                                                          node_id, hash)
            return d

        content = self.defer_from_thread(_get_content)
        return zlib.decompress(content.data)

    def unlink(self, node_id):
        """unlink a node."""

        def _unlink():
            """deferred part."""
            d = self.factory.current_protocol.unlink(self.volume, node_id)
            return d

        return self.defer_from_thread(_unlink)

    def move(self, node_id, new_parent_id, new_name):
        """move a node."""

        def _move():
            """deferred part."""
            d = self.factory.current_protocol.move(
                self.volume, node_id, new_parent_id, new_name)
            return d

        return self.defer_from_thread(_move)

    @parse_args(str, int)
    def do_connect(self, host, port):
        """Connect to host/port."""
        def _connect():
            """deferred part."""
            reactor.connectTCP(host, port, self.factory)
        self.status = "connecting"
        reactor.callFromThread(_connect)

    @parse_args(str, int)
    def do_connect_ssl(self, host, port):
        """Connect to host/port using ssl."""
        def _connect():
            """deferred part."""
            reactor.connectSSL(host, port, self.factory,
                               ssl.ClientContextFactory())
        self.status = "connecting"
        reactor.callFromThread(_connect)

    @parse_args()
    def do_status(self):
        """Print the status string."""
        print "STATUS: %s" % self.status

    @parse_args()
    def do_disconnect(self):
        """Disconnect."""
        if self.status != "connected":
            print "ERROR: Not connecting."
            return
        reactor.callFromThread(
            self.factory.current_protocol.transport.loseConnection)

    @parse_args(str)
    @require_connection
    @show_exception
    def do_dummy_auth(self, token):
        """Perform dummy authentication."""
        self.defer_from_thread(
            self.factory.current_protocol.dummy_authenticate, token)

    @parse_args()
    @require_connection
    @show_exception
    def do_shares(self):
        """Perform dummy authentication."""
        r = self.defer_from_thread(
            self.factory.current_protocol.list_shares)
        for share in r.shares:
            print share
            if share.accepted and share.direction == 'to_me':
                self.shares.add(str(share.id))

    @parse_args(str)
    @require_connection
    @show_exception
    def do_set_share(self, sharename):
        """Perform dummy authentication."""
        r = self.defer_from_thread(
            self.factory.current_protocol.list_shares)
        for share in r.shares:
            if str(share.id) == sharename:
                self.volume_root = share.subtree
                break
        else:
            print "BAD SHARE NAME"
            return
        self.volume = sharename
        self.cwd = '/'

    @parse_args()
    @require_connection
    @show_exception
    def do_volumes(self):
        """Perform dummy authentication."""
        r = self.defer_from_thread(
            self.factory.current_protocol.list_volumes)
        for volume in r.volumes:
            show_volume(volume)
            if not isinstance(volume, volumes.RootVolume):
                self.volumes.add(str(volume.volume_id))

    @parse_args(str)
    @require_connection
    @show_exception
    def do_set_volume(self, volume_id):
        """Perform dummy authentication."""
        r = self.defer_from_thread(
            self.factory.current_protocol.list_volumes)
        for volume in r.volumes:
            if str(volume.volume_id) == volume_id:
                self.volume_root = volume.node_id
                break
        else:
            print "BAD Volume ID"
            return
        self.volume = volume_id
        self.cwd = '/'

    @parse_args()
    @require_connection
    @show_exception
    def do_root(self):
        """Perform dummy authentication."""
        self.volume = request.ROOT
        self.cwd = '/'
        self.volume_root = None
        root = self.get_root()
        print "root is", root

    @require_connection
    def _list_dir(self, node_id):
        """Return the content of a directory."""
        content = self.get_content(node_id)
        unserialized_content = DirectoryContent()
        # TODO: what exceptions can protobuf's parser raise?
        unserialized_content.ParseFromString(content)
        return unserialized_content.entries

    @parse_args()
    @require_connection
    @show_exception
    def do_ls(self):
        """Get a listing of the current working directory."""
        node_id = self.get_cwd_id()
        entries = self._list_dir(node_id)
        for entry in entries:
            node_type = dircontent_pb2._NODETYPE. \
                values_by_number[entry.node_type].name
            print "%s %10s %s" % (entry.node, node_type, entry.name)

    @parse_args(str)
    @require_connection
    @show_exception
    def do_mkfile(self, name):
        """Create a file named name on the current working directory."""
        node_id = self.get_cwd_id()
        self.defer_from_thread(
            self.factory.current_protocol.make_file,
            self.volume, node_id, name)

    @parse_args(str)
    @show_exception
    def do_mkdir(self, name):
        """Create a directory named name on the current working directory."""
        self.mkdir(name)

    @require_connection
    def mkdir(self, name):
        """Create a directory named name on the current working directory."""
        node_id = self.get_cwd_id()
        self.defer_from_thread(
            self.factory.current_protocol.make_dir,
            self.volume, node_id, name)

    @parse_args(int)
    @show_exception
    def do_storm(self, intensity):
        """Storm operations to the server.

        It creates N directories, with N (empty) files in each dir.
        """
        self.storm(intensity)

    @require_connection
    def storm(self, intensity):
        """Storm operations to the server."""
        subroot_id = self.get_cwd_id()

        make_dir = self.factory.current_protocol.make_dir
        make_file = self.factory.current_protocol.make_file

        @defer.inlineCallbacks
        def go():
            """Actually do it."""
            tini = time.time()
            for _ in xrange(intensity):
                name = u"testdir-" + unicode(uuid.uuid4())
                req = yield make_dir(self.volume, subroot_id, name)
                for _ in xrange(intensity):
                    name = u"testfile-" + unicode(uuid.uuid4())
                    yield make_file(self.volume, req.new_id, name)
            tend = time.time()
            print "%d dirs and %d files created in %.2f seconds" % (
                intensity, intensity ** 2, tend - tini)

        self.defer_from_thread(go)

    @parse_args(str)
    @show_exception
    def do_cd(self, name):
        """CD to name."""
        self.cd(name)

    @require_connection
    def cd(self, name):
        """CD to name."""

        root = self.cwd
        newdir = os.path.normpath(os.path.join(root, name))
        parts = [part for part in newdir.split("/") if part]
        parent_id = self.get_root()

        for part in parts:
            if not self.is_dir(parent_id, part):
                print "ERROR: Not a directory"
                return

            parent_id = self.get_child_id(parent_id, part)

        self.cwd = newdir

    @parse_args(str, str)
    @show_exception
    def do_put(self, local, remote):
        """Put local file into remote file."""
        self.put(local, remote)

    @require_connection
    @show_exception
    def put(self, local, remote):
        """Put local file into remote file."""
        try:
            node_id = self.get_id_from_filename(remote)
        except ValueError:
            parent_id = self.get_cwd_id()
            r = self.defer_from_thread(
                self.factory.current_protocol.make_file,
                self.volume, parent_id, remote.split("/")[-1])
            node_id = r.new_id

        old_hash = self.get_hash(node_id)

        ho = content_hash_factory()
        zipper = zlib.compressobj()
        crc32_value = 0
        size = 0
        deflated_size = 0
        temp_file_name = None
        with open(local) as fh:
            with tempfile.NamedTemporaryFile(mode='w', prefix='cmd_client-',
                                             delete=False) as dest:
                temp_file_name = dest.name
                while True:
                    cont = fh.read(1024 ** 2)
                    if not cont:
                        dest.write(zipper.flush())
                        deflated_size = dest.tell()
                        break
                    ho.update(cont)
                    crc32_value = crc32(cont, crc32_value)
                    size += len(cont)
                    dest.write(zipper.compress(cont))
        hash_value = ho.content_hash()
        try:
            self.defer_from_thread(
                self.factory.current_protocol.put_content,
                self.volume, node_id, old_hash, hash_value,
                crc32_value, size, deflated_size, open(temp_file_name, 'r'))
        finally:
            if os.path.exists(temp_file_name):
                os.unlink(temp_file_name)

    @parse_args(str, str)
    @require_connection
    @show_exception
    def do_rput(self, local, remote):
        """Put local directory and it's files into remote directory."""
        def get_server_path(path):
            """ returns the server relative path """
            return path.rpartition(os.path.dirname(local))[2].lstrip('/')
        cwd = self.cwd
        for dirpath, dirnames, fnames in os.walk(local):
            server_path = get_server_path(dirpath)
            self.cd(os.path.dirname(server_path))
            leaf = os.path.basename(server_path)
            self.mkdir(leaf)
            self.cd(leaf)
            for filename in fnames:
                local_path = os.path.join(dirpath, filename)
                self.put(local_path, filename)
            self.cd(cwd)

    @parse_args(str, str)
    @require_connection
    @show_exception
    def do_get(self, remote, local):
        """Get remote file into local file."""
        data = self.get_file(remote)
        f = open(local, "w")
        f.write(data)
        f.close()

    @parse_args(str)
    @require_connection
    @show_exception
    def do_cat(self, remote):
        """Show the contents of remote file on screen."""
        data = self.get_file(remote)
        print data

    @parse_args(str)
    @require_connection
    @show_exception
    def do_hash(self, filename):
        """Print the hash of filename."""
        node_id = self.get_id_from_filename(filename)
        hash_value = self.get_hash(node_id)
        print hash_value

    @parse_args(str)
    @require_connection
    @show_exception
    def do_unlink(self, filename):
        """Print the hash of filename."""
        node_id = self.get_id_from_filename(filename)
        self.unlink(node_id)

    @parse_args(str, str)
    @require_connection
    @show_exception
    def do_move(self, source, dest):
        """Move file source to dest."""
        source_node_id = self.get_id_from_filename(source)
        try:
            dest_node_id = self.get_id_from_filename(dest)
        except ValueError:
            parent_name, node_name = os.path.split(dest)
            dest_node_id = self.get_id_from_filename(parent_name)
            self.move(source_node_id, dest_node_id, node_name)
        else:
            parent_name, node_name = os.path.split(source)
            self.move(source_node_id, dest_node_id, node_name)

    @defer.inlineCallbacks
    def _auth(self, consumer, token):
        """Really authenticate, and show the session id."""
        auth_method = self.factory.current_protocol.simple_authenticate
        req = yield auth_method(self.username, self.password)
        print "Authenticated ok, session:", req.session_id

    @parse_args(str, str)
    @require_connection
    @show_exception
    def do_oauth(self):
        """Perform authorisation."""
        self.defer_from_thread(self._auth)

    def do_shell(self, cmd):
        """Execute a shell command."""
        os.system(cmd)

    def do_quit(self, rest):
        """Exit the shell."""
        print "Goodbye", rest
        return True
    do_EOF = do_quit

    @require_connection
    def complete_set_volume(self, text, line, begidx, endidx):
        """Completion for set_volume."""
        if not self.volumes:
            r = self.defer_from_thread(
                self.factory.current_protocol.list_volumes)
            for volume in r.volumes:
                if not isinstance(volume, volumes.RootVolume):
                    self.volumes.add(str(volume.volume_id))
        return [vol_id for vol_id in sorted(self.volumes)
                if vol_id.startswith(text)]

    @require_connection
    def complete_set_share(self, text, line, begidx, endidx):
        """Completion for set_share."""
        if not self.shares:
            r = self.defer_from_thread(
                self.factory.current_protocol.list_shares)
            for share in r.shares:
                if share.accepted and share.direction == 'to_me':
                    self.shares.add(str(share.id))
        return [share_id for share_id in sorted(self.shares)
                if share_id.startswith(text)]

    @require_connection
    def _complete_single_filename(self, text, line, begidx, endidx):
        """Completion for remote filename for single argument commands."""
        node_id = self.get_cwd_id()
        entries = self._list_dir(node_id)
        return [entry.name for entry in entries
                if entry.name.startswith(text)]

    complete_cat = complete_unlink = _complete_single_filename

    @require_connection
    def complete_get(self, text, line, begidx, endidx):
        """Completion for get command."""
        if len(line.split(' ')) < 3:
            node_id = self.get_cwd_id()
            entries = self._list_dir(node_id)
            return [entry.name for entry in entries
                    if entry.node_type != dircontent_pb2.DIRECTORY
                    and entry.name.startswith(text)]

    def _complete_local(self, text, include_dirs=False):
        """Return the list of possible local filenames."""
        isdir = os.path.isdir

        def filter_files(files):
            """Firlter files/dirs."""
            return [f for f in files if include_dirs or not isdir(f)]
        if not os.path.exists(text):
            head, tail = os.path.split(text)
            while head and tail and not os.path.exists(head):
                head, tail = os.path.split(text)
            dirs = os.listdir(head or '.')
            return filter_files([d for d in dirs if d.startswith(tail)])
        elif os.path.exists(text) and isdir(text):
            return filter_files(os.listdir(text))
        else:
            return []

    @require_connection
    def complete_put(self, text, line, begidx, endidx):
        """Completion for put command."""
        if len(line.split(' ')) < 3:
            # local
            return self._complete_local(text)
        else:  # remote
            node_id = self.get_cwd_id()
            entries = self._list_dir(node_id)
            return [entry.name for entry in entries
                    if entry.node_type != dircontent_pb2.DIRECTORY
                    and entry.name.startswith(text)]

    @require_connection
    def complete_cd(self, text, line, begidx, endidx):
        """Completion for cd command."""
        node_id = self.get_cwd_id()
        entries = self._list_dir(node_id)
        return [entry.name for entry in entries
                if entry.node_type == dircontent_pb2.DIRECTORY
                and entry.name.startswith(text)]


def main():
    """run the cmd_client parsing cmd line options"""
    usage = "usage: %prog [options] [CMD]"
    parser = OptionParser(usage=usage)
    parser.add_option("--port", dest="port", metavar="PORT",
                      default=443,
                      help="The port on which to connect to the server")
    parser.add_option("--host", dest="host", metavar="HOST",
                      default='localhost',
                      help="The server address")
    parser.add_option("--username", dest="username", metavar="USERNAME",
                      help="The username")
    parser.add_option("--password", dest="password", metavar="PASSWORD",
                      help="The password")
    parser.add_option("-f", "--file", dest="filename",
                      help="write report to FILE", metavar="FILE")

    (options, args) = parser.parse_args()

    client = ClientCmd(options.username, options.password)
    client.onecmd('connect_ssl "%s" %s' % (options.host, options.port))

    while client.status != 'connected':
        time.sleep(.5)

    client.onecmd("auth")

    if args:
        client.onecmd(" ".join(args))
    else:
        client.cmdloop()

if __name__ == "__main__":
    main()
