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

"""Integration Tests for UDFs."""


import os
import random
import shutil

from copy import copy

from ubuntuone.platform.linux import tools
from twisted.internet import defer

from helpers import (
    create_file_and_add_content,
    debug,
    walk_and_list_dir,
)


@defer.inlineCallbacks
def create_udf(udf_name, sd, prefix, basedir=None):
    """Create an UDF on SD's home."""
    if basedir is None:
        basedir = sd.homedir

    folderdir = os.path.join(basedir, udf_name)
    os.mkdir(folderdir)

    dirpath = os.path.join(folderdir, 'a_dir')
    os.makedirs(dirpath)

    filepath = os.path.join(dirpath, 'a_file.txt')
    create_file_and_add_content(filepath)

    debug(prefix, 'Attempting to create folder for path %r' % folderdir)
    folder = yield sd.sdt.create_folder(path=folderdir)
    folder, = folder
    debug(prefix, 'folder created with id %s' % (folder['volume_id'],))

    yield sd.sdt.wait_for_nirvana(.5)
    defer.returnValue(folder)


# all tests start with "test_"
@defer.inlineCallbacks
def test_create_folder(udf_name, sd1, sd2, sd3, prefix):
    """Test 1: Assert correct folder creation."""
    wait_for_udf_created = sd2.wait_for_event('VM_UDF_CREATED')

    yield create_udf(udf_name, sd1, prefix)
    yield wait_for_udf_created
    yield sd2.sdt.wait_for_nirvana(.5)

    folderdir1 = os.path.join(sd1.homedir, udf_name)
    folderdir2 = os.path.join(sd2.homedir, udf_name)
    expected = walk_and_list_dir(folderdir1)
    actual = walk_and_list_dir(folderdir2)
    debug(prefix, 'expected', expected)
    debug(prefix, 'actual', actual)
    assert expected == actual, 'UDF must be replicated correctly'

    with open(os.path.join(folderdir1, expected[-1])) as fd:
        content1 = fd.read()
    with open(os.path.join(folderdir1, actual[-1])) as fd:
        content2 = fd.read()
    assert content1 == content2, 'file content macth'


@defer.inlineCallbacks
def test_get_folders(udf_name, sd1, sd2, sd3, prefix):
    """Test 2: Assert folder list is correct."""
    udf_values = yield create_udf(udf_name, sd1, prefix)

    yield sd2.sdt.wait_for_nirvana(.5)

    folders = yield sd2.sdt.get_folders()
    debug(prefix, 'get_folders completed!', folders)

    assert len(folders) == 1, 'only 1 folder'

    folder = folders[0]
    assert folder['path'] == os.path.join(sd2.homedir, udf_name), \
        'path correct'
    assert folder['subscribed'], 'udf must be subscribed'
    assert folder['suggested_path'] == os.path.join('~', udf_name), \
        'suggested_path must be correct'
    assert folder['node_id'] == udf_values['node_id'], \
        'node_id mut be correct'
    assert folder['volume_id'] == udf_values['volume_id'], \
        'volume id must be correct'


@defer.inlineCallbacks
def test_unsubscribe_no_side_effects(udf_name, sd1, sd2, sd3, prefix):
    """Test 3: Assert sd1 can unsubscribe from an UDF without side effects."""
    folder = yield create_udf(udf_name, sd1, prefix)
    assert folder['subscribed'], 'sd1 subscribed'

    yield sd2.sdt.wait_for_nirvana(.5)

    # is UDF created already?
    folders = yield sd1.sdt.get_folders()
    debug(prefix, 'folders for SD1', folders)

    fid = folder['volume_id']
    folder = yield sd1.sdt.unsubscribe_folder(fid)
    folder, = folder
    debug(prefix, 'unsubscribe_folder completed!', folder)
    assert not folder['subscribed'], 'sd1 no longer subscribed'

    folders = yield sd2.sdt.get_folders()
    debug(prefix, 'folders for SD2', folders)
    assert len(folders) == 1, 'only 1 folder'
    assert folders[0]['subscribed'], 'sd2 subscribed'


@defer.inlineCallbacks
def test_merge_directories_no_overlap(udf_name, sd1, sd2, sd3, prefix):
    """Test 4: Assert directories are correctly merged if no overlapping."""
    folderdir1 = os.path.join(sd1.homedir, udf_name)
    folderdir2 = os.path.join(sd2.homedir, udf_name)
    expected = []

    os.mkdir(folderdir1)
    # add folders and files to folderdir1
    on_sd1 = []
    for d in ('a', 'b', 'c'):
        dirpath = os.path.join(folderdir1, d)
        os.makedirs(dirpath)
        expected.append(d)
        for f in ('foo1.txt', 'bar1.txt', 'syncdaemon1.log'):
            # flip a coin
            if random.random() < 0.5:
                filepath = os.path.join(dirpath, f)
                open(filepath, 'w').close()
                on_sd1.append(os.path.join(d, f))
    debug(prefix, "created in sd1", on_sd1)
    expected.extend(on_sd1)

    os.mkdir(folderdir2)
    on_sd2 = []
    # add folders and files to folderdir2
    for d in ('z', 'y', 'x'):
        dirpath = os.path.join(folderdir2, d)
        os.makedirs(dirpath)
        expected.append(d)
        for f in ('foo2.txt', 'bar2.txt', 'syncdaemon2.log'):
            # flip a coin
            if random.random() < 0.5:
                filepath = os.path.join(dirpath, f)
                open(filepath, 'w').close()
                on_sd2.append(os.path.join(d, f))
    debug(prefix, "created in sd2", on_sd2)
    expected.extend(on_sd2)

    expected.sort()
    debug(prefix, "Expected", expected)

    # create the folder on sd1 and wait sd2 to finish working
    yield sd1.sdt.create_folder(path=folderdir1)
    yield sd1.sdt.wait_for_nirvana(.5)

    folders = yield sd1.sdt.get_folders()
    debug(prefix, 'get_folders completed!', folders)
    assert len(folders) == 1  # UDF was reported as expected

    yield sd2.sdt.wait_for_nirvana(.5)

    actual = walk_and_list_dir(folderdir2)
    debug(prefix, 'Found in SD2', actual)

    assert expected == actual, 'directory merge successful'


@defer.inlineCallbacks
def test_merge_directories_with_overlap(udf_name, sd1, sd2, sd3, prefix):
    """Test 5: Assert directories are correctly merge with overlapping."""

    # Structure to start
    #
    #   client 1:
    #     .../a
    #     .../a/conflict.txt   (content random)
    #     .../a/noconflict.txt (same content that 2)
    #     .../a/bar.txt
    #     .../b
    #
    #   client 2:
    #     .../a
    #     .../a/conflict.txt   (content random)
    #     .../a/noconflict.txt (same content that 1)
    #     .../a/beer.txt
    #     .../c
    #
    # Result after UDF creation and merge:
    #
    #  .../a/bar.txt and .../b are synced to client 2
    #  .../a/beer.txt and .../c are synced to client 1
    #  .../a/conflict.txt stays the same in one client, and in the other it
    #      goes to conflict (depending on which got first to the server)
    #  .../a/noconflict.txt stays ok in both clients
    #
    folderdir1 = os.path.join(sd1.homedir, udf_name)
    folderdir2 = os.path.join(sd2.homedir, udf_name)

    os.mkdir(folderdir1)
    # add folders and files to folderdir1
    dirpath = os.path.join(folderdir1, 'a')
    os.makedirs(dirpath)

    filepath = os.path.join(dirpath, 'conflict.txt')
    create_file_and_add_content(filepath, content='content from SD1')

    filepath = os.path.join(dirpath, 'noconflict.txt')
    with open(filepath, "w") as fh:
        fh.write("same content")

    filepath = os.path.join(dirpath, 'bar.txt')
    create_file_and_add_content(filepath)

    dirpath = os.path.join(folderdir1, 'b')
    os.makedirs(dirpath)

    os.mkdir(folderdir2)

    # add folders and files to folderdir2
    dirpath = os.path.join(folderdir2, 'a')
    os.makedirs(dirpath)

    filepath = os.path.join(dirpath, 'conflict.txt')
    create_file_and_add_content(filepath, content='content from SD2')

    filepath = os.path.join(dirpath, 'noconflict.txt')
    with open(filepath, "w") as fh:
        fh.write("same content")

    filepath = os.path.join(dirpath, 'beer.txt')
    create_file_and_add_content(filepath)

    dirpath = os.path.join(folderdir1, 'c')
    os.makedirs(dirpath)

    # wait for all changes to settle down
    yield sd2.sdt.wait_for_nirvana(.5)
    yield sd1.sdt.wait_for_nirvana(.5)

    # prepare the info to compare
    expected_no_conflict = ['a', 'b', 'c',
                            os.path.join('a', 'bar.txt'),
                            os.path.join('a', 'beer.txt'),
                            os.path.join('a', 'noconflict.txt'),
                            os.path.join('a', 'conflict.txt')]
    expected_no_conflict.sort()
    debug(prefix, 'expected without conflict', expected_no_conflict)

    expected_with_conflict = copy(expected_no_conflict)
    expected_with_conflict.append(os.path.join('a', 'conflict.txt.u1conflict'))
    expected_with_conflict.sort()
    debug(prefix, 'expected with conflict', expected_with_conflict)

    # create the UDF and wait everything to stop
    yield sd1.sdt.create_folder(path=folderdir1)
    yield sd2.sdt.wait_for_nirvana(.5)
    yield sd1.sdt.wait_for_nirvana(.5)

    actual1 = walk_and_list_dir(folderdir1)
    debug(prefix, 'actual content from SD1', actual1)
    actual2 = walk_and_list_dir(folderdir2)
    debug(prefix, 'actual content from SD2', actual2)

    # we don't know which client will enter in conflict, so we
    # tested both ways.
    if actual1 != expected_no_conflict:
        assert actual1 == expected_with_conflict, \
            'directory merge must be correct for SD1'
        assert actual2 == expected_no_conflict, \
            'directory merge must be correct for SD2'
    else:
        assert actual1 == expected_no_conflict, \
            'directory merge must be correct for SD1'
        assert actual2 == expected_with_conflict, \
            'directory merge must be correct for SD2'

test_merge_directories_with_overlap.skip = """
    The noconflict.txt file gets into conflict! Bug: #711389
"""


@defer.inlineCallbacks
def test_renaming_ancestor(udf_name, sd1, sd2, sd3, prefix):
    """Test 6: Assert correct unsubscription when an ancestor is renamed."""
    folder1 = os.path.join(sd1.homedir, udf_name)
    folder2 = os.path.join(folder1, "udf_parent_dir")
    folder3 = os.path.join(folder2, "udf_dir")
    debug(prefix, 'test_create_folder using UDF at', folder3)
    os.makedirs(folder3)

    folder = yield sd1.sdt.create_folder(path=folder3)
    debug(prefix, 'create_folder completed!', folder)

    yield sd2.sdt.wait_for_nirvana(.5)

    # FIXME: this signal is sometimes lost.
    events = [event['event_name'] for event in sd2.events]
    if 'VM_UDF_CREATED' not in events:
        yield sd2.wait_for_event('VM_UDF_CREATED')

    # rename and wait for nirvana
    debug(prefix, 'rename!')
    os.rename(folder2, folder2 + ".renamed")
    debug(prefix, 'wait for nirvanas')
    yield sd1.sdt.wait_for_nirvana(1)
    yield sd2.sdt.wait_for_nirvana(1)

    # both SDs should have the UDF, the first one should be unsuscribed
    folders = yield sd1.sdt.get_folders()
    udfs = [f for f in folders if f['path'] == folder3]
    assert len(udfs) == 1, "SD1 has udfs != 1 (%d)" % len(udfs)
    assert not udfs[0]['subscribed'], "The UDF of SD1 is subscribed!"

    folders = yield sd2.sdt.get_folders()
    folder_in_sd2 = os.path.join(sd2.homedir, udf_name,
                                 "udf_parent_dir", "udf_dir")
    udfs = [f for f in folders if f['path'] == folder_in_sd2]
    assert len(udfs) == 1, "SD1 has udfs != 1 (%d)" % len(udfs)
    assert udfs[0]['subscribed'], "The UDF of SD1 is not subscribed!"


@defer.inlineCallbacks
def test_renaming_the_udf_itself(udf_name, sd1, sd2, sd3, prefix):
    """Test 7: Assert correct unsubcription when the UDF is renamed."""
    folder1 = os.path.join(sd1.homedir, udf_name)
    folder2 = os.path.join(folder1, "udf_parent_dir")
    folder3 = os.path.join(folder2, "udf_dir")
    debug(prefix, 'test_create_folder using UDF at', folder3)
    os.makedirs(folder3)

    folder = yield sd1.sdt.create_folder(path=folder3)
    debug(prefix, 'create_folder completed!', folder)

    # FIXME: this signal is sometimes lost.
    events = [event['event_name'] for event in sd2.events]
    if 'VM_UDF_CREATED' not in events:
        yield sd2.wait_for_event('VM_UDF_CREATED')

    # rename and wait for nirvana
    #
    # FIXME: this will generate this message from pyinotify:
    #     ERROR: The path <folder3>  of this watch
    #     <Watch ... > must not be trusted anymore
    # this is because the "general" watch manager doesn't have a watch on the
    # parent of the UDF, so it doesn't know how it was moved; actually we
    # don't care, because the UDF is being un-subscribed...
    #
    debug(prefix, 'rename!')
    os.rename(folder3, folder3 + ".renamed")

    debug(prefix, 'wait for nirvanas')
    yield sd1.sdt.wait_for_nirvana(1)
    yield sd2.sdt.wait_for_nirvana(1)

    # both SDs should have the UDF, the first one should have it unsuscribed
    folders = yield sd1.sdt.get_folders()
    udfs = [f for f in folders if f['path'] == folder3]
    assert len(udfs) == 1, "SD1 has udfs != 1 (%d)" % len(udfs)
    assert not udfs[0]['subscribed'], "The UDF of SD1 is subscribed!"

    folders = yield sd2.sdt.get_folders()
    folder_in_sd2 = os.path.join(sd2.homedir, udf_name,
                                 "udf_parent_dir", "udf_dir")
    udfs = [f for f in folders if f['path'] == folder_in_sd2]
    assert len(udfs) == 1, "SD1 has udfs != 1 (%d)" % len(udfs)
    assert udfs[0]['subscribed'], "The UDF of SD1 is not subscribed!"


@defer.inlineCallbacks
def test_remove_udf(udf_name, sd1, sd2, sd3, prefix):
    """Test 8: Remove an UDF, assert correct deletion on both clients."""
    yield create_udf(udf_name, sd1, prefix)
    yield sd2.sdt.wait_for_nirvana(.5)

    assert os.path.exists(os.path.join(sd2.homedir, udf_name))
    actual1 = walk_and_list_dir(os.path.join(sd1.homedir, udf_name))
    actual2 = walk_and_list_dir(os.path.join(sd2.homedir, udf_name))
    debug(prefix, "contents for SD1", actual1)
    assert actual1 == actual2

    debug(prefix, "Removing the UDF:", udf_name)
    shutil.rmtree(os.path.join(sd1.homedir, udf_name))

    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)

    events = [event['event_name'] for event in sd2.events]
    assert 'VM_VOLUME_DELETED' in events, 'VM_UDF_DELETED in sd2.events'
    debug(prefix, "VM_VOLUME_DELETED found on SD2")

    msg = 'UDF\'s contents must be deleted from file system on SD2.'
    udf_content = os.listdir(os.path.join(sd2.homedir, udf_name))
    debug(prefix, 'contents for SD2', udf_content)
    assert udf_content == [], msg

test_remove_udf.skip = """
    This test exposes a problem we have now in SD: deleting everything
    under the UDF depends on timing of operations between one client
    and other, the behaviour should be inforced somehow, we need to
    take care of this when generations be in place (most probably will
    not have this problem), but until then this may fail sometimes.
"""


@defer.inlineCallbacks
def test_can_not_create_inside_root(udf_name, sd1, sd2, sd3, prefix):
    """Tets 9: A folder can not be created inside the ultimate root."""
    d = create_udf(udf_name, sd1, prefix, basedir=sd1.rootdir)

    def fail(result):
        """Fail since this callback should not be called.."""
        assert False, 'latest call to create_folder should have failed.'

    def check(failure):
        """Error must have been occurred. Analyze it."""
        is_error = failure.type == tools.ErrorSignal
        debug(prefix, 'UDF creation failed. Error:', failure.type)
        assert is_error, 'failure must be a tools.ErrorSignal'

    d.addCallback(fail)
    d.addErrback(check)

    yield d


@defer.inlineCallbacks
def test_can_not_create_inside_other_udf(udf_name, sd1, sd2, sd3, prefix):
    """Test 10: A folder can not be created inside another udf."""
    # create UDF 1
    folder1 = yield create_udf(udf_name, sd1, prefix)

    # create UDF 2 (nested)
    folderdir = os.path.join(folder1['path'], 'nested_udf')
    os.mkdir(folderdir)

    d = sd1.sdt.create_folder(path=folderdir)

    def fail(result):
        """Fail since this callback should not be called.."""
        assert False, 'latest call to create_folder should have failed.'

    def check(failure):
        """Error must have been occurred. Analyze it."""
        is_error = failure.type == tools.ErrorSignal
        debug(prefix, 'UDF creation failed. Error:', failure.type)
        assert is_error, 'failure must be a tools.ErrorSignal'

    d.addCallback(fail)
    d.addErrback(check)

    yield d


@defer.inlineCallbacks
def test_sharing(udf_name, sd1, sd2, sd3, prefix):
    """Test 12: Shares inside UDF."""
    folder = yield create_udf(udf_name, sd1, prefix)
    dir1 = os.path.join(folder["path"], "a_dir")
    dir2 = os.path.join(folder["path"], "other_dir")
    os.mkdir(dir2)
    yield sd1.sdt.wait_for_nirvana(1)

    # offer one and accept it
    debug(prefix, "Offering share 1 from SD1")
    share_name_1 = "share_1_" + udf_name
    d_wait_share = sd3.wait_for_event('SV_SHARE_CHANGED')
    sd1.sdt.offer_share(dir1, sd3.username, share_name_1, "View")
    yield d_wait_share
    debug(prefix, "Received share in SD3")
    shares = yield sd3.sdt.get_shares()
    share = [x for x in shares if x['name'] == share_name_1][0]
    share_id = share['volume_id']
    debug(prefix, "Accepting share")
    yield sd3.sdt.accept_share(share_id)

    # offer a second one
    debug(prefix, "Offering share 2 from SD1")
    share_name_2 = "share_2_" + udf_name
    d_wait_share = sd3.wait_for_event('SV_SHARE_CHANGED')
    sd1.sdt.offer_share(dir2, sd3.username, share_name_2, "Modify")
    yield d_wait_share
    debug(prefix, "Received share in SD3")

    # check the shares of sd1
    shared = yield sd1.sdt.list_shared()
    share = [x for x in shared if x['name'] == share_name_1][0]
    assert share['access_level'] == "View", "share 1 in sd1 should be View!"
    assert share['accepted'], "share 1 in sd1 should be accepted"
    share = [x for x in shared if x['name'] == share_name_2][0]
    assert share['access_level'] == "Modify", "share 2 in sd1 should be Modif!"
    assert not share['accepted'], "share 2 in sd1 should NOT be accepted"

    # check the shared of sd3
    shares = yield sd3.sdt.get_shares()
    share = [x for x in shares if x['name'] == share_name_1][0]
    assert share['access_level'] == "View", "share 1 in sd2 should be View!"
    assert share['accepted'], "share 1 in sd2 should be accepted"
    share = [x for x in shares if x['name'] == share_name_2][0]
    assert share['access_level'] == "Modify", "share 2 in sd2 should be Modif!"
    assert not share['accepted'], "share 2 in sd2 should NOT be accepted"


@defer.inlineCallbacks
def test_disconnect_modify_connect(udf_name, sd1, sd2, sd3, prefix):
    """Test 13: Create UDF, disconnect the SD, do stuff, and then reconnect."""
    folder = yield create_udf(udf_name, sd1, prefix)
    folder_path = folder['path']
    other_dir = os.path.join(folder_path, 'other_dir')
    os.mkdir(other_dir)
    third_dir = os.path.join(folder_path, 'third_dir')
    os.mkdir(third_dir)

    yield sd1.sdt.wait_for_nirvana(.5)

    debug(prefix, 'Disconnecting SD1.')
    yield sd1.sdt.disconnect()  # disconnect SD1

    debug(prefix, 'Doing stuff in the file system of SD1.')
    # do stuff in the file system
    xyz_dir = os.path.join(folder_path, 'x', 'y', 'z')
    os.makedirs(xyz_dir)
    create_file_and_add_content(os.path.join(xyz_dir, 'new.txt'))

    # move a file within the UDF
    os.rename(os.path.join(folder_path, 'a_dir', 'a_file.txt'),
              os.path.join(xyz_dir, 'renamed_file.txt'))

    # move a folder outside the UDF to the root dir
    os.rename(os.path.join(folder_path, 'other_dir'),
              os.path.join(sd1.rootdir, udf_name + 'renamed_other_dir'))

    # move a folder outside the UDF to the home dir
    renamed_third_dir = os.path.join(sd1.homedir, 'renamed_third_dir')
    os.rename(os.path.join(folder_path, 'third_dir'),
              renamed_third_dir)

    expected = set(walk_and_list_dir(sd1.homedir))
    debug(prefix, "Expected to have", expected)

    debug(prefix, 'Re connecting SD1.')
    yield sd1.sdt.connect()  # re-connect SD1
    yield sd1.sdt.wait_for_nirvana(.5)

    debug(prefix, 'Waiting for nirvana for SD2.')
    yield sd2.sdt.wait_for_nirvana(.5)  # wait for SD2 to get all the changes

    actual = set(walk_and_list_dir(sd2.homedir))
    debug(prefix, "Currently found", actual)

    debug(prefix, 'expected sym diff actual',
          expected.symmetric_difference(actual))
    assert expected.difference(actual) == set([u'renamed_third_dir']), \
        'SD1 home must have the same as SD2 except for renamed_third_dir.'
    assert actual.difference(expected) == set([]), \
        'SD2 home must have nothing extra than the SD1\'s.'

#    Disabled the following lines, because they try to create a folder while
#    being disconnected, and right now the SDT interface waits for the
#    FolderCreated signal, so it blocks until dead... we need to decide if
#    we will change this functionality, or remove this tests
#
#    yield sd1.sdt.wait_for_nirvana(.5)
#    debug(prefix, 'Disconnecting SD1.')
#    yield sd1.sdt.disconnect() # disconnect SD1
#
#    # convert renamed_third_dir to an UDF
#    yield sd1.sdt.create_folder(path=renamed_third_dir)
#    debug(prefix, 'folder created for renamed_third_dir!')
#
#    debug(prefix, 'Re connecting SD1.')
#    yield sd1.sdt.connect() # re-connect SD1
#    yield sd1.sdt.wait_for_nirvana(.5)
#
#    debug(prefix, 'Waiting for nirvana for SD2.')
#    yield sd2.sdt.wait_for_nirvana(.5) # wait for SD2 to get all the changes
#
#    expected = walk_and_list_dir(sd1.homedir)
#    debug(prefix, "Expected to have", expected)
#    actual = walk_and_list_dir(sd2.homedir)
#    debug(prefix, "Currently found", actual)
#    assert expected == actual


@defer.inlineCallbacks
def test_unsuscribe_delete_subscribe(udf_name, sd1, sd2, sd3, prefix):
    """Test 14: unsubscribe and subsc., removing everything in the middle."""
    # create udf
    folder = yield create_udf(udf_name, sd1, prefix)
    folder_id = folder["volume_id"]
    udf_path = folder["path"]
    debug(prefix, 'create_folder completed!', folder)
    assert folder['subscribed'], 'sd1 must be subscribed'
    yield sd1.sdt.wait_for_nirvana(.5)

    # un-subscribe and check
    yield sd1.sdt.unsubscribe_folder(folder_id)
    folders = yield sd1.sdt.get_folders()
    assert len(folders) == 1, "SD1 has udfs != 1 (%d)" % len(folders)
    assert not folders[0]['subscribed'], 'sd1 must NOT be subscribed'
    debug(prefix, 'unsubscribed!')

    # remove everything
    shutil.rmtree(udf_path)
    debug(prefix, 'everything removed from disk')
    yield sd1.sdt.wait_for_nirvana(.5)

    # subscribe and wait
    yield sd1.sdt.subscribe_folder(folder_id)
    folders = yield sd1.sdt.get_folders()
    assert len(folders) == 1, "SD1 has udfs != 1 (%d)" % len(folders)
    assert folders[0]['subscribed'], 'sd1 must be subscribed'
    yield sd1.sdt.wait_for_nirvana(.5)
    debug(prefix, 'subscribed!')

    # check stuff in disk for sd1
    in_disk = walk_and_list_dir(udf_path)
    expected = ['a_dir', os.path.join('a_dir', 'a_file.txt')]
    assert in_disk == expected, \
        "Wrong stuff in disk: %s (expected: %s)" % (in_disk, expected)


@defer.inlineCallbacks
def test_unsuscribe_subscribe(udf_name, sd1, sd2, sd3, prefix):
    """Test 15: unsubscribe and subscribe."""
    folder = yield create_udf(udf_name, sd1, prefix)
    folder_id = folder["volume_id"]
    debug(prefix, 'create_folder completed!', folder)
    assert folder['subscribed'], 'sd1 must be subscribed'

    # un-subscribe and check
    yield sd1.sdt.unsubscribe_folder(folder_id)
    folders = yield sd1.sdt.get_folders()
    assert len(folders) == 1  # UDF was reported as expected
    assert not folders[0]['subscribed'], 'sd1 must NOT be subscribed'

    # subscribe and check
    yield sd1.sdt.subscribe_folder(folder_id)
    folders = yield sd1.sdt.get_folders()
    assert len(folders) == 1  # UDF was reported as expected
    assert folders[0]['subscribed'], 'sd1 must be subscribed'


@defer.inlineCallbacks
def test_renaming_ancestor_of_two(udf_name, sd1, sd2, sd3, prefix):
    """Test 16: Check behavior when an ancestor of more than one is renamed."""
    udfdir1 = os.path.join(sd1.homedir, udf_name)
    udfdir2 = os.path.join(udfdir1, "udf_parent_dir")
    os.makedirs(udfdir2)
    udfdir3 = os.path.join(udfdir2, "udf_dir1")
    udfdir4 = os.path.join(udfdir2, "udf_dir2")

    yield sd1.sdt.create_folder(path=udfdir3)
    yield sd1.sdt.create_folder(path=udfdir4)
    debug(prefix, 'create_folders completed!')

    yield sd1.sdt.wait_for_nirvana(.5)

    # FIXME: this signal is sometimes lost.
    events = [event['event_name'] for event in sd2.events]
    if 'VM_UDF_CREATED' not in events:
        yield sd2.wait_for_event('VM_UDF_CREATED')

    # rename and wait for nirvana
    debug(prefix, 'rename!')
    os.rename(udfdir2, udfdir2 + ".renamed")
    debug(prefix, 'wait for nirvanas')
    yield sd1.sdt.wait_for_nirvana(1)
    yield sd2.sdt.wait_for_nirvana(1)

    # both SDs should have UDFs, the first one "unsuscribed"
    folders = yield sd1.sdt.get_folders()
    assert len(folders) == 2, "SD1 has udfs != 2 (%d)" % len(folders)
    for udf in folders:
        assert not udf['subscribed'], "%s of SD1 is subscribed!" % udf

    folders = yield sd2.sdt.get_folders()
    assert len(folders) == 2, "SD2 has udfs != 2 (%d)" % len(folders)
    for udf in folders:
        assert udf['subscribed'], "%s of SD2 is NOT subscribed!" % udf


@defer.inlineCallbacks
def test_no_events_from_ancestors_if_unsubsc(udf_name, sd1, sd2, sd3, prefix):
    """Test 17: Watches are removed in ancestors."""
    # structure:
    # base_dir
    #    \---parent
    #         |--- udf_dir1
    #         \--- middle
    #                \--- udf_dir2
    #
    # unsubscribing udf2 should remove the watch of "middle", but not from
    # "homedir", as the later is also an ancestor of other udf

    base_dir = os.path.join(sd1.homedir, udf_name)
    parent = os.path.join(base_dir, "parent")
    udf_dir1 = os.path.join(parent, "udf_dir1")
    middle = os.path.join(parent, "middle")
    udf_dir2 = os.path.join(middle, "udf_dir2")
    os.makedirs(udf_dir1)
    os.makedirs(udf_dir2)

    yield sd1.sdt.create_folder(path=udf_dir1)
    yield sd1.sdt.create_folder(path=udf_dir2)
    debug(prefix, 'create_folders completed!')
    yield sd1.sdt.wait_for_nirvana(.5)

    # rename udf2 and wait for nirvana
    debug(prefix, 'rename!')
    os.rename(udf_dir2, udf_dir2 + ".renamed")
    debug(prefix, 'wait for nirvana')
    yield sd1.sdt.wait_for_nirvana(1)

    # check that UDF1 ancestors still have the watches by renaming
    # 'parent' and verifying that UDF1 is unsubscribed; there's no
    # way to check that 'middle' lost its watch
    debug(prefix, 'check!')
    os.rename(parent, parent + ".renamed")
    folders = yield sd1.sdt.get_folders()
    udf = [x for x in folders if x['path'] == udf_dir1][0]
    assert not udf['subscribed'], "%s of SD1 is subscribed!" % udf


@defer.inlineCallbacks
def test_sharing_udfitself(udf_name, sd1, sd2, sd3, prefix):
    """Test 18: Sharing the UDF itself."""
    folder = yield create_udf(udf_name, sd1, prefix)

    # offer one and accept it
    debug(prefix, "Offering share 1 from SD1")
    share_name = "share_" + udf_name
    d_wait_share = sd3.wait_for_event('SV_SHARE_CHANGED')
    sd1.sdt.offer_share(folder["path"], sd3.username, share_name, "Modify")
    yield d_wait_share
    debug(prefix, "Received share in SD3")
    shares = yield sd3.sdt.get_shares()
    share = [x for x in shares if x['name'] == share_name][0]
    share_id = share['volume_id']

    debug(prefix, "Accepting share and wait acceptance to propagate")
    yield sd3.sdt.accept_share(share_id)
    yield sd1.sdt.wait_for_nirvana(.5)

    # check the shares of sd1
    shared = yield sd1.sdt.list_shared()
    share = [x for x in shared if x['name'] == share_name][0]
    assert share['access_level'] == "Modify", "share in sd1 should be Modify!"
    assert share['accepted'], "share in sd1 should be accepted"

    # check the shared of sd3
    shares = yield sd3.sdt.get_shares()
    share = [x for x in shares if x['name'] == share_name][0]
    assert share['access_level'] == "Modify", "share in sd2 should be Modify!"
    assert share['accepted'], "share in sd2 should be accepted"


@defer.inlineCallbacks
def test_unsusc_lotofchanges_subsc(udf_name, sd1, sd2, sd3, prefix):
    """Test 19: Merge should be done correctly."""
    # some dirs and files
    udf_dir = os.path.join(sd1.homedir, udf_name)
    dir_a = os.path.join(udf_dir, "a")
    file_1 = os.path.join(dir_a, "file1")
    file_2 = os.path.join(dir_a, "file2")
    file_3 = os.path.join(dir_a, "file3")
    dir_b = os.path.join(udf_dir, "b")
    dir_c = os.path.join(udf_dir, "c")
    dir_d = os.path.join(udf_dir, "d")

    # we create an UDF and put:
    #  - dir_a, with:
    #  -     file_1
    #  -     file_2
    #  - dir_b
    #  - dir_c
    folder, = yield sd1.sdt.create_folder(path=udf_dir)
    folder_id = folder["volume_id"]
    for d in (dir_a, dir_b, dir_c):
        os.mkdir(d)
    for f in (file_1, file_2):
        open(f, "w").close()
    debug(prefix, 'initial UDF completed!')
    yield sd1.sdt.wait_for_nirvana(.5)

    # unsubscribe
    yield sd1.sdt.unsubscribe_folder(folder_id)
    debug(prefix, 'unsubscribed!')

    # some changes:
    os.rmdir(dir_c)
    os.mkdir(dir_d)
    os.remove(file_2)
    open(file_3, "w").close()
    debug(prefix, 'changes made!')
    yield sd1.sdt.wait_for_nirvana(1)

    # subscribe again
    yield sd1.sdt.subscribe_folder(folder_id)
    debug(prefix, 'subscribed!')

    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, 'changes propagated')

    # what the merge should do:
    #  - dir_c is back from the server
    #  - dir_d uploaded
    #  - file_2 is back from the server
    #  - file_3 uploaded to the server
    #  - the rest should remain unchanged

    # to check, we verify everything in both clients
    expected = ['a', 'a/file1', 'a/file2', 'a/file3', 'b', 'c', 'd']
    for which, sd in enumerate((sd1, sd2)):
        debug(prefix, 'check SD', sd)
        udf_dir = os.path.join(sd.homedir, udf_name)
        in_disk = walk_and_list_dir(udf_dir)
        assert in_disk == expected, "sd %s has bad stuff in "\
                                    "disk: %s" % (which, in_disk)
