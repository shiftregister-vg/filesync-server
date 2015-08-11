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

"""Run the graphite development server."""

import sys
import os
import commands
import signal
import subprocess

from config import config
import utilities.localendpoints as local
from utilities.utils import kill_group, get_tmpdir


TMP_DIR = get_tmpdir()
CARBON_LOG_DIR = os.path.join(TMP_DIR, "carbon_logs")
GRAPHITE_LOG_DIR = os.path.join(TMP_DIR, "graphite_logs")
CARBON_PID_FILE = os.path.join(TMP_DIR, "carbon.pid")

try:
    os.makedirs(GRAPHITE_LOG_DIR)
except OSError:
    pass
try:
    os.makedirs(CARBON_LOG_DIR)
except OSError:
    pass

# User our settings instead of graphite.settings
os.environ['DJANGO_SETTINGS_MODULE'] = \
    'metrics.graphite_frontend.local_settings'

ADMIN_USER = config.secret.graphite_admin_user
ADMIN_PASS = config.secret.graphite_admin_password

(carbon_line_port, carbon_pickle_port,
 carbon_query_port) = local.allocate_ports(3)
local.register_local_port("carbon_line_receiver", carbon_line_port)
local.register_local_port("carbon_pickle_receiver", carbon_pickle_port)
local.register_local_port("carbon_query_port", carbon_query_port)

port = local.allocate_ports()[0]
local.register_local_port("graphite-devserver", port)

syncdb = "/usr/bin/django-admin syncdb --noinput"

status, output = commands.getstatusoutput(syncdb)
if status > 0:
    print >> sys.stderr, "Error: Database doesn't appear to be running!"
    print >> sys.stderr, "Did you run  \"make start\" first?"
    print output
    sys.exit(1)

# Create the user
from django.contrib.auth.models import User
# delete if exists
User.objects.filter(username=ADMIN_USER).delete()
# Create user with correct credentials
user = User.objects.create_user(ADMIN_USER, "noreply@somemail.com", ADMIN_PASS)
user.is_staff = True
user.save()

with open(os.path.join(TMP_DIR, "storage-schemas.conf"), "w") as schema:
    schema.write("""[everything_1min_7days]
priority = 100
pattern = .*
retentions = 60:7d
""")

# Build a conf
with open(os.path.join(os.path.dirname(sys.argv[0]), 'carbon.conf.tpl')) as f:
    template = f.read()

template_vars = {}
template_vars['data_dir'] = os.path.join(TMP_DIR, "whisper")
template_vars['line_receiver_port'] = carbon_line_port
template_vars['pickle_receiver_port'] = carbon_pickle_port
template_vars['cache_query_port'] = carbon_query_port

conf = template % template_vars

conf_file_path = os.path.join(TMP_DIR, 'carbon.conf')
with open(conf_file_path, 'w') as conf_file:
    conf_file.write(conf)


# SIGTERM -> kill process group/carbon
def shutdown(signum, frame):
    """Shutdown carbon."""
    if os.path.exists(CARBON_PID_FILE):
        os.kill(int(open(CARBON_PID_FILE).read()), signal.SIGTERM)
    kill_group()
signal.signal(signal.SIGTERM, shutdown)

carbon_cache_command = [
    "/usr/bin/twistd", "--pidfile", CARBON_PID_FILE,
    "--reactor", "epoll", "carbon-cache", "--config", conf_file_path,
    "--logdir", CARBON_LOG_DIR
]

subprocess.Popen(carbon_cache_command + ['start'],
                 env={"PYTHONPATH": os.pathsep.join(sys.path),
                      "GRAPHITE_ROOT": TMP_DIR}).wait()
try:
    subprocess.Popen(["/usr/bin/django-admin",
                      "runserver", "0.0.0.0:%s" % port]).wait()
except KeyboardInterrupt:
    pass
subprocess.Popen(carbon_cache_command + ['stop'],
                 env={"PYTHONPATH": os.pathsep.join(sys.path),
                      "GRAPHITE_ROOT": TMP_DIR}).wait()
