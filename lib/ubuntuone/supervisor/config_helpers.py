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

"""Supervisord config helpers."""

import os
import platform

from utilities.utils import get_rootdir


NICK_TEMPLATE = "%(nick)s-%(nick_extra)s%(instance)s.%(hostname)s"

GROUP_TEMPLATE = """
[group:%(group_name)s]
programs=%(programs)s
"""

HEARTBEAT_LISTENER_TEMPLATE = '\n'.join((
    "[eventlistener:heartbeat]",
    "command=python %(basepath)s/lib/ubuntuone/supervisor/heartbeat_listener.py --interval=%(interval)s --timeout=%(timeout)s --log_level=%(log_level)s --log_file=%(log_folder)s/heartbeat_listener.log --groups=%(groups)s %(processes)s",  # NOQA
    'environment=PYTHONPATH="%(basepath)s/lib",CONFIG="%(basepath)s/configs/development.yaml"',  # NOQA
    "events=PROCESS_COMMUNICATION,TICK_5",
    "buffer_size=%(buffer_size)s",
))

STATS_WORKER_TEMPLATE = '\n'.join((
    "[program:stats-worker]",
    "command=python %(basepath)s/lib/ubuntuone/monitoring/stats_worker.py --log_file=%(log_folder)s/stats_worker.log --metric_namespace_prefix=%(env)s.%(hostname)s",  # NOQA
    'environment=PYTHONPATH="%(basepath)s/lib",CONFIG="%(basepath)s/configs/development.yaml"',  # NOQA
))

CONFIG_TEMPLATE = """; supervisor config file
[supervisord]
logfile=%(log_folder)s/supervisord.log
pidfile=%(pid_folder)s/supervisord.pid
childlogdir=%(log_folder)s/supervisor-childlog/
logfile_maxbytes = 50MB
logfile_backups = 10
loglevel = info
nodaemon = false
minfds = 8192
minprocs = 200

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = \
    supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix://%(pid_folder)s/run/supervisor.sock

[unix_http_server]
file = %(pid_folder)s/run/supervisor.sock
"""

base_spec = {
    "basepath": "/srv/%(base_dir)s/%(env)s/fsync",
    "log_folder": "/srv/%(base_dir)s/%(env)s-logs",
    "pid_folder": "/srv/%(base_dir)s/var",
    "stdout_capture_maxbytes": 16384,
}

base_heartbeat_listener_spec = {
    "interval": 60,
    "timeout": 160,
    "log_level": "DEBUG",
    "buffer_size": 50,
    "processes": "",
    "processes_re": ["rpcdb"],  # don't restart the group when rpcdb dies
}


def get_config(environ, config_spec, base_dir):
    """Return the base config for environ."""
    # start with base_spec
    config = base_spec.copy()

    if config_spec is None:
        config_spec = {}

    config.update(config_spec)

    # now get the routing scheme for the env, if any
    if "routing_scheme" in config:
        config["routing_scheme"] = config["routing-schemes"][environ]

    config["env"] = environ
    config["base_dir"] = base_dir

    # build the environment vars, if any
    if "env_vars" in config:
        env_vars = config["env_vars"][environ]
        config["environment_vars"] = ',' + ','.join(
            ['%s="%s"' % (k, v) for k, v in env_vars.items()])
    else:
        config["environment_vars"] = ''

    for k, v in config.items():
        # only apply the env to string values
        if isinstance(v, basestring) and '%' in v:
            config[k] = v % {"env": environ,
                             "host_env": config["host_env"],
                             "base_dir": base_dir}
        else:
            config[k] = v

    return config


# add the groups sections
def get_sort_key(value):
    """Get the sort key from the group name."""
    name = value[0]
    parts = name.rsplit("-", 1)
    try:
        return int(parts[1]), name
    except (IndexError, ValueError):
        return name


def generate_server_config(server_name, env_service_map, config_spec,
                           templates, service_group, with_heartbeat,
                           with_stats_worker, with_header):
    """Generate the config for a specific server/machine."""
    # Generally we have only one env per machine, except when production and
    # edge are colocated. In which case we default everything that doesn't have
    # a env-specific key to "production".
    envs = env_service_map.keys()
    default_env = "production" if "production" in envs else envs[0]
    base_dir = env_service_map[default_env].get("base_dir")
    config_spec = config_spec.copy()
    config_spec["server_name"] = config_spec["hostname"] = server_name
    server_spec = get_config(default_env, config_spec, base_dir)
    output = []
    if with_header:
        output.append(CONFIG_TEMPLATE % server_spec)
    for env, server_config in env_service_map.items():
        service_map = server_config.pop("services")
        base_dir = server_config.get("base_dir")
        service_spec = get_config(env, config_spec, base_dir)
        service_spec["nick_extra"] = "edge" if env == "edge" else ""
        service_output, workers, groups = generate_service_config(
            service_map, service_spec, service_group,
            server_config, templates)
        processes = sorted(workers.keys())
        output.extend(service_output)

    # add the heartbeat listener
    if groups and with_heartbeat:
        heartbeat_listener_spec = base_heartbeat_listener_spec.copy()
        heartbeat_listener_spec.update(server_spec)
        hb_groups = sorted([gn for gn in groups.keys()])
        heartbeat_listener_spec['groups'] = ','.join(hb_groups)
        procs_to_watch = []
        if "processes_re" in heartbeat_listener_spec:
            for proc_re in heartbeat_listener_spec['processes_re']:
                to_watch = [proc for proc in processes if proc_re in proc]
                procs_to_watch.extend(to_watch)
        if procs_to_watch:
            heartbeat_listener_spec['processes'] = '--processes=' + \
                ','.join(procs_to_watch)
        output.append(HEARTBEAT_LISTENER_TEMPLATE % heartbeat_listener_spec)

    if with_stats_worker:
        output.append(STATS_WORKER_TEMPLATE % server_spec)

    for group, progs in sorted(groups.items(), key=get_sort_key):
        group_spec = {"group_name": group,
                      "programs": ",".join(progs)}
        output.append(GROUP_TEMPLATE % group_spec)

    return ''.join(output)


def generate_service_config(service_map, config_spec, service_group=None,
                            server_config=None, templates=None):
    """Generate a config and a mapping of worker name to worker config."""
    if config_spec.get("nick_extra") is None:
        config_spec["nick_extra"] = ""

    groups = {}
    output = []
    output_by_name = {}
    workers = {}

    config_spec = config_spec.copy()
    environ = config_spec["env"]
    if not "hostname" in config_spec:
        config_spec["hostname"] = platform.node()

    for service_name, instances in sorted(service_map.items()):
        service_name, nick = service_name.split(".", 1)
        parts = nick.split("/", 1)
        partition_id = None
        if len(parts) == 2:
            nick, partition_id = parts
        elif len(parts) != 1:
            raise ValueError("Invalid service nick: %r." % nick)

        instance_name_template = templates[service_name]['name']
        instance_template = templates[service_name]['config']

        # find nick-specific values in config spec and replace them by non-nick
        # prefixed values.
        service_config_spec = config_spec.copy()
        for key, value in service_config_spec.items()[:]:
            if key.startswith(nick + "."):
                del service_config_spec[key]
                key_name = key[len(nick) + 1:]
                service_config_spec[key_name] = value

        if partition_id is not None:
            service_config_spec["partition_id"] = int(partition_id)

        if "tacfile" in templates[service_name]:
            # have a tacfile for this service
            tac_template = templates[service_name]["tacfile"]
            service_config_spec["tacfile"] = tac_template % service_config_spec

        for instance in instances:
            instance_config = service_config_spec.copy()
            instance_config["nick"] = nick
            instance_config["instance"] = instance

            # build the environment vars, if any
            env_vars = {"PGAPPNAME": "%(program_name)s"}
            env_vars.update(service_config_spec.get(
                "env_vars", {}).get(environ, {}))
            env_vars.update(service_config_spec.get(
                "%s.env_vars" % service_name, {}).get(environ, {}))
            instance_config["environment_vars"] = ',' + ','.join(
                ['%s="%s"' % (k, v) for k, v in env_vars.items()])

            instance_name = instance_name_template % instance_config
            instance_config["instance_name"] = instance_name
            instance_config["node_name"] = NICK_TEMPLATE % instance_config

            workers[instance_name] = instance_config
            output_by_name[instance_name] = instance_template % instance_config

            if service_group is not None:
                group_name = "%s-%d" % (service_group, instance)
                group_services = groups.setdefault(group_name, [])
                group_services.append(instance_name)

    for instance_name, instance_output in sorted(output_by_name.items(),
                                                 key=get_sort_key):
        output.append(instance_output)

    return output, workers, groups


def main(instance_map, templates, service_group=None,
         with_heartbeat=False, with_stats_worker=False, config_spec=None):
    """Entry point"""
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-n", "--dry-run", dest="dry_run", action="store_true",
                      help="Show the generated config via stdout.")
    options, args = parser.parse_args()

    for server_name, env_service_map in instance_map.items():
        config_content = generate_server_config(server_name, env_service_map,
                                                config_spec, templates,
                                                service_group, with_heartbeat,
                                                with_stats_worker,
                                                with_header=True)
        if options.dry_run:
            print config_content
        else:
            filename = "configs/supervisor-%s.conf" % (server_name,)
            with open(os.path.join(get_rootdir(), filename), 'w') as f:
                f.write(config_content)
