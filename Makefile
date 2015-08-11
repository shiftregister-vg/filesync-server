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

PYTHON = python
MAKEFLAGS:=$(MAKEFLAGS) --no-print-directory
HERE:=$(shell pwd)
PYTHONPATH:=$(HERE):$(HERE)/lib:${PYTHONPATH}
# use protobuf cpp
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp

START_SUPERVISORD = lib/ubuntuone/supervisor/start-supervisord.py

export PYTHONPATH
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION
export ROOTDIR ?= $(HERE)
export CONFIG = $(HERE)/configs/development.yaml

SOURCEDEPS_TAG = .sourcecode/sourcedeps-tag
SOURCEDEPS_DIR ?= ../sourcedeps
SOURCEDEPS_SOURCECODE_DIR = $(SOURCEDEPS_DIR)/sourcecode
TARGET_SOURCECODE_DIR = $(HERE)/.sourcecode

BUILD_DEPLOY_SOURCEDEPS=storm zope.interface ubuntuone-storage-protocol

TESTFLAGS=

TAR_EXTRA = --exclude 'tmp/*' --exclude tags

ifneq ($(strip $(STRIP_BZR)),)
TAR_EXTRA += --exclude .bzr
endif

sourcedeps: $(SOURCEDEPS_TAG)

clean-sourcedeps:
	rm -rf .sourcecode/*

$(SOURCEDEPS_TAG):
ifndef EXPORT_FROM_BZR
	$(MAKE) link-sourcedeps
endif
	$(MAKE) build-sourcedeps
	touch $(SOURCEDEPS_TAG)

DB_TAG=tmp/db1/.s.PGSQL.5432.lock

build: link-sourcedeps build-sourcedeps version

link-sourcedeps:
	@echo "Checking out external source dependencies..."
	build/link-external-sourcecode \
		-p $(SOURCEDEPS_SOURCECODE_DIR)/ \
		-t $(TARGET_SOURCECODE_DIR) \
		-c build/config-manager.txt

# no need to link sourcedeps before building them, as rollout process
# handles config-manager.txt automatically
build-for-deployment: build-deploy-sourcedeps version

build-sourcedeps: build-deploy-sourcedeps
	@echo "Building client clientdefs.py"
	@cd .sourcecode/ubuntuone-client/ubuntuone/ && sed \
		-e 's|\@localedir\@|/usr/local/share/locale|g' \
		-e 's|\@libexecdir\@|/usr/local/libexec|g' \
		-e 's|\@GETTEXT_PACKAGE\@|ubuntuone-client|g' \
		-e 's|\@VERSION\@|0.0.0|g' < clientdefs.py.in > clientdefs.py


build-deploy-sourcedeps:
	@echo "Building Python extensions"

	@for sourcedep in $(BUILD_DEPLOY_SOURCEDEPS) ; do \
            d=".sourcecode/$$sourcedep" ; \
            if test -e "$$d/setup.py" ; then \
	        (cd "$$d" && $(PYTHON) \
	        setup.py build build_ext --inplace > /dev/null) ; \
            fi ; \
	done

	@echo "Generating twistd plugin cache"
	@$(PYTHON) -c "from twisted.plugin import IPlugin, getPlugins; list(getPlugins(IPlugin));"

tarball: build-for-deployment
	tar czf ../filesync-server.tgz $(TAR_EXTRA) .

raw-test:
	./test $(TESTFLAGS)

test: sourcedeps clean lint version start-base start-dbus raw-test stop

ci-test:
	$(MAKE) test TESTFLAGS="-1 $(TESTFLAGS)"

clean: stop
	rm -rf tmp/* _trial_temp

lint:
	dev-scripts/lint.sh

etags: sourcedeps
	# Generate tags for emacs
	ctags-exuberant -e -R --language-force=python -f tags lib src

tags: sourcedeps
	# Generate tags for vim
	ctags-exuberant -R --language-force=python -f tags lib src

version:
	build/makeversion.sh

start: build start-base start-filesync-dummy-group load-sample-data publish-api-port

start-oauth: build start-base start-filesync-oauth-group load-sample-data publish-api-port

start-oauth-heapy:
	USE_HEAPY=1 $(MAKE) start-oauth

start-base:
	$(MAKE) start-db && $(MAKE) start-supervisor && $(MAKE) start-dbus && \
	$(MAKE) start-statsd && $(MAKE) start-s4 && $(MAKE) start-storage-proxy || \
	( $(MAKE) stop ; exit 1 )

stop:  stop-filesync-dummy-group stop-supervisor stop-db stop-statsd stop-dbus

$(DB_TAG):
	lib/backends/db/scripts/dev/start-database.sh

start-db: $(DB_TAG) schema

schema:
	./lib/backends/db/scripts/schema --all

stop-db:
	lib/backends/db/scripts/dev/stop-database.sh

start-dbus:
	dev-scripts/start-dbus.sh

stop-dbus:
	dev-scripts/stop-dbus.sh

start-supervisor:
	@python dev-scripts/supervisor-config-dev.py
	-@$(START_SUPERVISORD) dev-scripts/supervisor-dev.conf.tpl \
	dev-scripts/metrics-processors.conf.tpl

stop-supervisor:
	-@dev-scripts/supervisorctl-dev shutdown

start-statsd:
	-@dev-scripts/supervisorctl-dev start graphite
	-@dev-scripts/supervisorctl-dev start statsd

stop-statsd:
	-@dev-scripts/supervisorctl-dev stop statsd
	-@dev-scripts/supervisorctl-dev stop graphite

start-%-group:
	-@dev-scripts/supervisorctl-dev start $*:

stop-%-group:
	-@dev-scripts/supervisorctl-dev stop $*:

start-%:
	-@dev-scripts/supervisorctl-dev start $*

stop-%:
	-@dev-scripts/supervisorctl-dev stop $*

publish-api-port:
	python -c 'from config import config; print >> file("tmp/filesyncserver.port", "w"), config.api_server.tcp_port; print >> file("tmp/filesyncserver.port.ssl", "w"), config.ssl_proxy.port; print >> file("tmp/filesyncserver-status.port", "w"), config.api_server.status_port'

load-sample-data:
	DJANGO_SETTINGS_MODULE=backends.django_settings $(PYTHON) dev-scripts/load_sample_data.py

.PHONY: sourcedeps link-sourcedeps build-sourcedeps build-deploy-sourcedeps \
	build clean version lint test ci-test schema \
	build-for-deployment clean-sourcedeps tarball \
	start stop load-sample-data publish-api-port \
	start-supervisor stop-supervisor \
	start-dbus stop-dbus \
	start-oauth start-oauth-heapy start-statsd stop-statsd
