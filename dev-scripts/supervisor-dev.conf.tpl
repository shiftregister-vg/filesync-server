; supervisor config file
[supervisord]
logfile=%(basepath)s/tmp/supervisor-dev.log          ; (main log file;default $CWD/supervisord.log)
pidfile=%(basepath)s/tmp/supervisor-dev.pid          ; (supervisord pidfile;default supervisord.pid)
childlogdir=%(basepath)s/tmp/supervisor-dev-childlog/ ; ('AUTO' child log dir, default $TEMP)
logfile_maxbytes = 50MB
logfile_backups = 10
loglevel = info
nodaemon = false
minfds = 1024
minprocs = 200
identifier = supervisor-dev

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=http://localhost:%(inet_http_server_port)s

[inet_http_server]
port=%(inet_http_server_port)s

[include]
files=metrics-processors.conf services-supervisor.conf workers-supervisor.conf

[eventlistener:heartbeat]
command=python %(basepath)s/lib/ubuntuone/supervisor/heartbeat_listener.py --interval=10 --timeout=20 --log_level=DEBUG --log_file=%(tmp_dir)s/heartbeat.log --groups=filesync-dummy,filesync-oauth
environment=PYTHONPATH="%(basepath)s/lib"
events=PROCESS_COMMUNICATION,TICK_5
buffer_size=42

[program:filesync]
command=/usr/bin/twistd --pidfile %(tmp_dir)s/filesync.pid -n -y %(basepath)s/lib/ubuntuone/storage/server/server.tac --reactor=epoll
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp,CONFIG="%(basepath)s/configs/development.yaml"
stdout_capture_maxbytes=16384
autostart=false
stopsignal=INT

[program:filesync-dummy-auth]
command=python %(basepath)s/dev-scripts/deploy_api_server.py
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp,CONFIG="%(basepath)s/configs/development.yaml"
stdout_capture_maxbytes=16384
autostart=false
redirect_stderr=true                          ; send stderr to the log file
stdout_logfile=%(tmp_dir)s/filesync.log
stdout_logfile_maxbytes=0

[program:ssl-proxy]
command=/usr/bin/twistd --pidfile %(tmp_dir)s/ssl-proxy.pid -n -y %(basepath)s/lib/ubuntuone/storage/server/ssl_proxy.tac  --reactor=epoll
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",CONFIG="%(basepath)s/configs/development.yaml"
stdout_capture_maxbytes=16384
autostart=false
stopsignal=INT

[program:s4]
command=/usr/bin/twistd --pidfile %(tmp_dir)s/s4.pid -n -y %(basepath)s/lib/s4/S4.tac -l %(tmp_dir)s/s4_stdout.log
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",CONFIG="%(basepath)s/configs/development.yaml"
autostart=false
stopsignal=INT

[program:storage-proxy]
command=python %(basepath)s/dev-scripts/squid.py storage-proxy %(basepath)s/dev-scripts/storage-proxy.conf.tmpl
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",CONFIG="%(basepath)s/configs/development.yaml"
autostart=false
stopsignal=INT

[program:stats_worker]
command=python %(basepath)s/lib/ubuntuone/monitoring/stats_worker.py --log_file=%(tmp_dir)s/stats_worker.log
environment=PYTHONPATH="%(basepath)s/lib",DJANGO_SETTINGS_MODULE="backends.django_settings",CONFIG="%(basepath)s/configs/development.yaml"
autostart=false

[group:filesync-dummy]
programs=filesync-dummy-auth

[group:filesync-oauth]
programs=filesync,ssl-proxy
