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

"""A data access layer for storage data

The data access layer serves as an abstraction to the data model. Storm models,
referencesets, recordsets are converted to Data Access Objects (DAO)
or lists of DAOs and all access is subsequently performed via the DAO.

The majority of data access will be performed via a StorageUser DAO object
retrieved from SystemServices for a specific user. There are also a few
functions in the SystemServices which are typically for external systems to use
directly.

DOA methods that retrieve and update handle thier own transaction management
so all transaction management is handled in the DataServices Layer
|
|                                            +-----+
|                 +--------------------------| DAO |
|                 |                          +-----+
|                 |                             |
|                 |                      +--------------+
|                 |                      | DataServices |
|   Data          |                      +--------------+
|   Services      |                             ^
|- - - - - - - - -|- - - - - - - - - - - - - - -|- - - - - - - - - - -
|   Data Access   |               +-----+       |
|   Layer         |       +-------| DAO |-------+
|                 |       |       +-----+
|                 |       |
|          +----------------+
|          | Gateway        |
|          +----------------+
|                  ^
|                  |                    +--------------+
|             Storm Stores-------------| Storm Models |
|                  |                    +--------------+
|                  |
|                  |
|                  |
|          +----------------+
|          | Database       |
|          +----------------+
|
|----------------------------------------------------------------------------
|
| Here is a typical scenario:
|    When a system has autenticated a user:
|         controller = SystemServices()
|         user = controller.get_user(user_id)
|
|    Later in the code:
|
|
|         user.share(share_id).dir(dir_id).make_file(u"file_name")
|
|   In this example, no database access is performed until the call
|   to make_file. This way, this would translate to the following
|   steps within a single transaction:
|             * get the user and verify it's Live
|             * get the share_id and verify it's Live and accepted
|             * get the directory from the share and verify that it's Live
|             * if the user has write access to the share, make the file and
|               return it
|
|   The benefit if this method is that methods like make_file make_subdir are
|   only methods of a directory node and we don't end up with a user class with
|   lots of functions that behave differently based on the parameters passed
|
"""

from ubuntuone.storageprotocol.content_hash import content_hash_factory

from backends.filesync.data.dbmanager import get_storage_store  # NOQA
from backends.filesync.data.dbmanager import storage_tm  # NOQA

EMPTY_CONTENT_HASH = content_hash_factory().content_hash()
