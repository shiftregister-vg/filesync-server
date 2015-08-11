[program:postgres]
command=python %(basepath)s/lib/backends/db/scripts/dev/start-postgres.py
environment=PYTHONPATH="%(basepath)s/lib",CONFIG="%(basepath)s/configs/development.yaml"
redirect_stderr=true                          ; send stderr to the log file
stdout_logfile=%(tmp_dir)s/postgres.log
autostart=false
stopsignal=INT
