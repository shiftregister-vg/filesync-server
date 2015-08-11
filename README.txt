How to setup Filesync Server and Client
=======================================

These are the general instructions to do all the needed setup to have
a proper file synchronization service up and running.


Before server or client
-----------------------

Create the SSL certificates for client to communicate with server
securely. First, the private key:

    openssl genrsa -out privkey.pem


Then, then self-signed certificate. Note that at some point it will
ask you to write the "Common Name (e.g. server FQDN or YOUR name)", I
found that what you put there needs to match the '--host' parameter
you pass to the client (see below, in the part where the client
is started), so this host name must be such the client machine
can ping it.

    openssl req -new -x509 -key privkey.pem -out cacert.pem -days 1095


The just generated 'privkey.pem' and 'cacert.pem' will be used below.


Server setup
------------

Start with a clean Ubuntu Precise environment (for example, in a VPS).

Install tools and dependencies:

    sudo apt-get install bzr make python-transaction protobuf-compiler \
        python-setuptools gcc python-dev python-twisted-web postgresql-9.1 \
        python-yaml python-psycopg2 postgresql-contrib supervisor \
        postgresql-plpython-9.1 python-django python-boto squid \
        python-protobuf python-psutil python-testresources


Branch the project and get into that dir:

    bzr branch lp:filesync-server
    cd filesync-server


Edit the `config/development.yaml` file and in the `secret` section set
the `api_server_crt` key with the content of `cacert.pem` file, and the
`api_server_key` key with the content of `privkey.pem` file (both files
produced in the "Before server or client" section).

Then, start the server:

    make start-oauth

Note that the server will listen on port 21101, so you need to assure
than the client could reach it (open it in your firewall config, etc).

Finally, create all the users you want:

    dev-scripts/user-mgr.py create testuser John Doe jdoe@gmail.com testpass

(with this script you'll be able to also retrieve and update user data,
and delete users)



Client setup
------------

This is to be repeated in all places that you want the system to run.
Instructions are for an Ubuntu Trusty environment, adapt as needed. It's
assuming you're starting from a clean machine (e.g.: a just installed one,
or an LXC), if you're not you may have some of the needed parts
already installed.

First, install tools and dependencies:

    sudo apt-get install python-twisted-bin python-twisted-core \
        python-dirspec python-pyinotify python-configglue \
        python-twisted-names python-ubuntu-sso-client \
        python-distutils-extra protobuf-compiler python-protobuf


Go to a new directory (anyone you chooses), let's call it $SOMEDIR, and there,
after following the steps below, you'll get the following structure:

    $SOMEDIR/storage-protocol   <-- this is a subproject needed by the client
    $SOMEDIR/client   <-- this is the proper filesync client
    $SOMEDIR/certifs   <-- this is where you'll store the SSL certifs for the client


Just to follow the next instructions, it's easier if you do:

    export SOMEDIR=<the path where you want to put everything>
    mkdir -p $SOMEDIR


So, branch and build the storage protocol:

    cd $SOMEDIR
    bzr branch lp:~facundo/ubuntuone-storage-protocol/opensourcing storage-protocol
    cd storage-protocol
    ./setup.py build


Put the SSL certificate in a separate directory (wherever you want):

    cd $SOMEDIR
    mkdir certif
    cp <path of cacert.pem file created above> certif/


Also branch and build the client:

    cd $SOMEDIR
    bzr branch lp:~facundo/ubuntuone-client/opensourcing client
    cd client/ubuntuone
    ln -s $SOMEDIR/storage-protocol/ubuntuone/storageprotocol .
    cd ..
    ./setup.py build


Finally, start the client

    export $(dbus-launch)  # seems this is needed if you're inside a LXC or VPS
    PYTHONPATH=. SSL_CERTIFICATES_DIR=$SOMEDIR/certif bin/ubuntuone-syncdaemon \
        --auth=testuser:testpass --host=testfsyncserver \
        --port=21101 --logging-level=DEBUG


If you want, check logs to see all went ok

    less $HOME/.cache/ubuntuone/log/syncdaemon.log


There, this line will show that the client started ok:

    ubuntuone.SyncDaemon.Main - NOTE - ---- MARK (state: <State: 'INIT' ...


And this line will show that the client reached the server ok (so no network issues):

    ubuntuone.SyncDaemon.StateManager - DEBUG - received event 'SYS_CONNECTION_MADE'


Finally, this line will show that client authenticated OK to the server
(no username/password issues):

    ubuntuone.SyncDaemon.StateManager - DEBUG - received event 'SYS_AUTH_OK'


Enjoy.
