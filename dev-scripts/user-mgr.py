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

"""Script to manage the users in the system."""

import argparse
import os

import warnings
warnings.simplefilter("ignore")

import _pythonpath  # NOQA

# fix environment before further imports
os.environ['CONFIG'] = os.path.join("configs/development.yaml")
os.environ["DJANGO_SETTINGS_MODULE"] = "backends.django_settings"

from django.contrib.auth.models import User

from backends.filesync.data import services


def create(args):
    """Create a user."""
    username = args.username.decode("utf8")
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        pass
    else:
        print "ERROR: There is already an user with that username"
        return

    # let's create it
    user = User(username=username,
                email=args.email.decode("utf8"),
                first_name=args.firstname.decode("utf8"),
                last_name=args.lastname.decode("utf8"))
    user.set_password(args.password.decode("utf8"))
    user.save()

    # refresh the user object to ensure permissions caches are reloaded
    user = User.objects.get(username=username)

    # create also the storage user
    visible_name = "%s %s" % (user.first_name, user.last_name)
    services.make_storage_user(user.id, username, visible_name, 2 ** 20)

    print "Success: User created ok"


def update(args):
    """Change information for a user."""
    username = args.username.decode("utf8")
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        print "ERROR: User does not exist"
        return

    if args.email is not None:
        user.email = args.email.decode("utf8")
    if args.firstname is not None:
        user.first_name = args.firstname.decode("utf8")
    if args.lastname is not None:
        user.last_name = args.lastname.decode("utf8")
    if args.password is not None:
        user.set_password(args.password.decode("utf8"))
    user.save()

    print "Success: User updated ok"


def delete(args):
    """Remove a user from the system."""
    username = args.username.decode("utf8")
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        print "ERROR: User does not exist"
        return

    user.delete()
    print "Success: User deleted ok"


def show(args):
    """Show information about a user."""
    username = args.username.decode("utf8")
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        print "ERROR: User does not exist"
        return

    print "Username:   '%s'" % (user.username,)
    print "E-mail:     '%s'" % (user.email,)
    print "Name:       ", user.first_name, user.last_name
    print "Id:         ", user.id
    print "Joined:     ", user.date_joined.ctime()
    print "Last Login: ", user.last_login.ctime()
    print "Active:     ", user.is_active


parser = argparse.ArgumentParser(description="Filesync Server User Manager")
subparsers = parser.add_subparsers()

p_create = subparsers.add_parser('create', help="Create a user.")
p_create.set_defaults(func=create)
p_create.add_argument("username")
p_create.add_argument("firstname")
p_create.add_argument("lastname")
p_create.add_argument("email")
p_create.add_argument("password")

p_update = subparsers.add_parser('update',
                                 help="Change information for a user.")
p_update.add_argument("username")
p_update.set_defaults(func=update)
p_update.add_argument("--email")
p_update.add_argument("--firstname")
p_update.add_argument("--lastname")
p_update.add_argument("--password")

p_delete = subparsers.add_parser('delete',
                                 help="Remove a user from the system.")
p_delete.set_defaults(func=delete)
p_delete.add_argument("username")

p_show = subparsers.add_parser('show',
                               help="Show information about an user.")
p_show.set_defaults(func=show)
p_show.add_argument("username")

args = parser.parse_args()
args.func(args)
