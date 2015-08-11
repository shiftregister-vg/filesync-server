# -*- coding: utf8 -*-

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

"""Basic Integration Tests."""


import os
import shutil

from os.path import join

from twisted.internet import defer

from ubuntuone.storage.tests.integration.helpers import (
    create_file_and_add_content,
    debug,
    walk_and_list_dir,
)


@defer.inlineCallbacks
def test_empty_file(test_name, sd1, sd2, sd3, prefix):
    """01. Create an empty file. Check."""
    # create the file in sd1 and wait
    open(join(sd1.rootdir, 'file.txt'), 'w').close()
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2
    assert os.path.exists(join(sd2.rootdir, 'file.txt'))


@defer.inlineCallbacks
def test_empty_file_funny_name(test_name, sd1, sd2, sd3, prefix):
    """02. Create an empty file with a non-ascii name. Check."""
    # create the file in sd1 and wait
    open(join(sd1.rootdir, u'moño.txt'), 'w').close()
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2
    assert os.path.exists(join(sd2.rootdir, u'moño.txt'))


@defer.inlineCallbacks
def test_file_with_content(test_name, sd1, sd2, sd3, prefix):
    """02. Create a file with data in it. Check."""
    # create the file in sd1 and wait
    filepath = join(sd1.rootdir, 'file.txt')
    content = create_file_and_add_content(filepath)
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2 with correct content
    data = open(join(sd2.rootdir, 'file.txt')).read()
    assert data == content


@defer.inlineCallbacks
def test_file_with_content_changing(test_name, sd1, sd2, sd3, prefix):
    """03. Create a file with data. Check. Modify file's content. Check."""
    # create the file in sd1 and wait
    filepath = join(sd1.rootdir, 'file.txt')
    content = create_file_and_add_content(filepath)
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2 with correct content
    data = open(join(sd2.rootdir, 'file.txt')).read()
    assert data == content

    # change the content in sd2, and wait for both to settle
    newcontent = os.urandom(1000)
    with open(join(sd2.rootdir, 'file.txt'), 'w') as fh:
        fh.write(newcontent)
    yield sd2.sdt.wait_for_nirvana(.5)
    yield sd1.sdt.wait_for_nirvana(.5)

    # check that the file in sd1 has the correct content
    data = open(join(sd1.rootdir, 'file.txt')).read()
    assert data == newcontent


@defer.inlineCallbacks
def test_rename_file(test_name, sd1, sd2, sd3, prefix):
    """04. Create a file. Check. Rename it. Check."""
    # create the file in sd1 and wait
    open(join(sd1.rootdir, 'file.txt'), 'w').close()
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2
    assert os.path.exists(join(sd2.rootdir, 'file.txt'))

    # rename the file, wait for both to settle
    os.rename(join(sd1.rootdir, 'file.txt'), join(sd1.rootdir, 'filenew.txt'))
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)

    # check the new file is there, and the old one isn't
    assert os.path.exists(join(sd2.rootdir, 'filenew.txt'))
    assert not os.path.exists(join(sd2.rootdir, 'file.txt'))


@defer.inlineCallbacks
def test_rename_file_with_content(test_name, sd1, sd2, sd3, prefix):
    """05. Rename a file with content.

    Create a file with some data, and immediately after rename it to
    something else (it still will be in the creation/uploading process).
    Check.
    """
    # create the file in sd1 and rename
    content = create_file_and_add_content(join(sd1.rootdir, 'file.txt'))
    os.rename(join(sd1.rootdir, 'file.txt'), join(sd1.rootdir, 'filenew.txt'))
    debug(prefix, "File created and renamed")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2 ok (and old file is not there)
    assert not os.path.exists(join(sd2.rootdir, 'file.txt'))
    data = open(join(sd2.rootdir, 'filenew.txt')).read()
    assert data == content


@defer.inlineCallbacks
def test_unlink_file(test_name, sd1, sd2, sd3, prefix):
    """06. Create a file. Check. Unlink it. Check."""
    # create the file in sd1 and wait
    open(join(sd1.rootdir, 'file.txt'), 'w').close()
    debug(prefix, "File created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the file is in sd2
    assert os.path.exists(join(sd2.rootdir, 'file.txt'))

    # remove the file, wait for both to settle
    os.remove(join(sd1.rootdir, 'file.txt'))
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)

    # check
    assert not os.path.exists(join(sd2.rootdir, 'file.txt'))


@defer.inlineCallbacks
def test_fast_unlink_file(test_name, sd1, sd2, sd3, prefix):
    """07. Create a file and immediately after unlink it. Check."""
    # create the file in sd1 and delete it
    open(join(sd1.rootdir, 'file.txt'), 'w').close()
    os.remove(join(sd1.rootdir, 'file.txt'))
    debug(prefix, "File created and deleted")

    # wait and check
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    assert not os.path.exists(join(sd1.rootdir, 'file.txt'))
    assert not os.path.exists(join(sd2.rootdir, 'file.txt'))


@defer.inlineCallbacks
def test_create_directory(test_name, sd1, sd2, sd3, prefix):
    """08. Create a directory. Check."""
    # create the dir in sd1 and wait
    os.mkdir(join(sd1.rootdir, 'dir'))
    debug(prefix, "Directory created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the dir is in sd2
    assert os.path.exists(join(sd2.rootdir, 'dir'))


@defer.inlineCallbacks
def test_create_and_remove_directory(test_name, sd1, sd2, sd3, prefix):
    """09. Remove an empty directory. Check."""
    # create the dir in sd1 and wait
    os.mkdir(join(sd1.rootdir, 'dir'))
    debug(prefix, "Directory created")
    yield sd2.sdt.wait_for_nirvana(.5)

    # check that the dir is in sd2
    assert os.path.exists(join(sd2.rootdir, 'dir'))
    debug(prefix, "Directory is ok in sd2")

    # remove it and wait
    os.rmdir(join(sd2.rootdir, 'dir'))
    debug(prefix, "Directory removed")
    yield sd2.sdt.wait_for_nirvana(.5)
    yield sd1.sdt.wait_for_nirvana(.5)

    # check it's gone also in sd1
    assert not os.path.exists(join(sd1.rootdir, 'dir'))


@defer.inlineCallbacks
def test_create_tree_and_rename_parent_fast(test_name, sd1, sd2, sd3, prefix):
    """10. Create tree and rename the parent while uploading.

    Create a directory, put three files inside with data, and immediately
    after rename the directory. Check.
    """
    # create the root/a/b/c/d tree in sd1, put some files in it, rename the dir
    deepdir = join(sd1.rootdir, *'abcd')
    os.makedirs(deepdir)
    c1 = create_file_and_add_content(join(deepdir, 'file1.txt'))
    c2 = create_file_and_add_content(join(deepdir, 'file2.txt'))
    c3 = create_file_and_add_content(join(deepdir, 'file3.txt'))
    os.rename(join(sd1.rootdir, 'a'), join(sd1.rootdir, 'nuevo'))
    debug(prefix, "Tree structure created and dir removed")

    # wait for both to settle, and check
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)

    data = open(join(sd2.rootdir, 'nuevo', 'b', 'c', 'd', 'file1.txt')).read()
    assert data == c1
    data = open(join(sd2.rootdir, 'nuevo', 'b', 'c', 'd', 'file2.txt')).read()
    assert data == c2
    data = open(join(sd2.rootdir, 'nuevo', 'b', 'c', 'd', 'file3.txt')).read()
    assert data == c3


@defer.inlineCallbacks
def test_create_both_sd_different_content(test_name, sd1, sd2, sd3, prefix):
    """11. Create files with different content in both SDs.

    Create at the same time on each client two files with same name and
    different content. Check that one of both gets conflicted.
    """
    # create both files with content
    c1 = os.urandom(1000)
    c2 = c1[::-1]
    create_file_and_add_content(join(sd1.rootdir, 'file.txt'), content=c1)
    create_file_and_add_content(join(sd2.rootdir, 'file.txt'), content=c2)
    debug(prefix, "Files with different content created in both SDs")

    # wait for both to settle, and get the files in both
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(sd1.rootdir)
    debug(prefix, "Files in SD1", files1)
    files2 = walk_and_list_dir(sd2.rootdir)
    debug(prefix, "Files in SD2", files2)

    # file.txt should be on both
    assert "file.txt" in files1, "the file.txt should be in SD1"
    assert "file.txt" in files2, "the file.txt should be in SD2"

    # one of them should have a conflict, and the other should not
    conflict1 = "file.txt.u1conflict" in files1
    conflict2 = "file.txt.u1conflict" in files2
    assert conflict1 and not conflict2 or conflict2 and not conflict1, \
        "Only one of both should have conflict!"


@defer.inlineCallbacks
def test_create_both_sd_empty(test_name, sd1, sd2, sd3, prefix):
    """12. Create empty files in both SDs.

    Create at the same time on each client two empty files with the same
    name. Check.
    """
    # create two empty files in the SDs
    open(join(sd1.rootdir, 'file.txt'), 'w').close()
    open(join(sd2.rootdir, 'file.txt'), 'w').close()
    debug(prefix, "Empty files created in both SDs")

    # wait for both to settle, and get the files in both
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(sd1.rootdir)
    files2 = walk_and_list_dir(sd2.rootdir)

    # file.txt should be on both, and no conflict at all
    assert "file.txt" in files1, "the file should be in SD1: %s" % (files1,)
    assert "file.txt.u1conflict" not in files1, "conflict SD1: %s" % (files1,)
    assert "file.txt" in files2, "the file should be in SD2: %s" % (files2,)
    assert "file.txt.u1conflict" not in files2, "conflict SD2: %s" % (files2,)


@defer.inlineCallbacks
def test_create_both_sd_same_content(test_name, sd1, sd2, sd3, prefix):
    """13. Create files with same content in both SDs.

    Create at the same time on each client two files with same name and
    same content. Check.
    """
    # create both files with same content
    data = os.urandom(1000)
    create_file_and_add_content(join(sd1.rootdir, 'file.txt'), content=data)
    create_file_and_add_content(join(sd2.rootdir, 'file.txt'), content=data)
    debug(prefix, "Files with same content created in both SDs")

    # wait for both to settle, and get the files in both
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(sd1.rootdir)
    files2 = walk_and_list_dir(sd2.rootdir)

    # file.txt should be on both, and no conflict at all
    assert "file.txt" in files1, "the file should be in SD1: %s" % (files1,)
    assert "file.txt.u1conflict" not in files1, "conflict SD1: %s" % (files1,)
    assert "file.txt" in files2, "the file should be in SD2: %s" % (files2,)
    assert "file.txt.u1conflict" not in files2, "conflict SD2: %s" % (files2,)


@defer.inlineCallbacks
def test_create_both_sd_same_dir_diff_file(test_name, sd1, sd2, sd3, prefix):
    """14. Create different files in same dir in both SDs.

    Create at the same time on one client a directory with file A, and on
    the other client the same directory with file B. Check that both files
    are propagated ok.
    """
    # create both dirs with different files
    os.mkdir(join(sd1.rootdir, 'dir'))
    os.mkdir(join(sd2.rootdir, 'dir'))
    open(join(sd1.rootdir, 'dir', 'file1.txt'), 'w').close()
    open(join(sd2.rootdir, 'dir', 'file2.txt'), 'w').close()
    debug(prefix, "Dir and files created in both SDs")

    # wait for both to settle, and get the files in both
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(join(sd1.rootdir, 'dir'))
    files2 = walk_and_list_dir(join(sd2.rootdir, 'dir'))

    # both files should be on both
    assert "file1.txt" in files1, "file 1 should be in SD1: %s" % (files1,)
    assert "file2.txt" in files1, "file 2 should be in SD1: %s" % (files1,)
    assert "file1.txt" in files2, "file 1 should be in SD2: %s" % (files2,)
    assert "file2.txt" in files2, "file 2 should be in SD2: %s" % (files2,)


@defer.inlineCallbacks
def test_create_tree_and_rename(test_name, sd1, sd2, sd3, prefix):
    """15. Create a tree and rename parent.

    Create a directory, with one subdir, and other subdir under the later,
    and a file as a leaf (root/a/b/c.txt). Wait for nirvana. Rename the
    first subdir (a). Check.
    """
    # create the tree and wait for it to finish
    os.makedirs(join(sd1.rootdir, 'a', 'b'))
    open(join(sd1.rootdir, 'a', 'b', 'c.txt'), 'w').close()
    debug(prefix, "Dir and files created in SD1")
    yield sd1.sdt.wait_for_nirvana(.5)

    # rename the parent, wait for both to settle, and get the files in both
    os.rename(join(sd1.rootdir, 'a'), join(sd1.rootdir, 'nuevo'))
    debug(prefix, "Parent renamed")
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(sd1.rootdir)
    files2 = walk_and_list_dir(sd2.rootdir)

    # both trees should be on both, and old 'a' on neither of them
    assert "nuevo/b/c.txt" in files1, "tree not in SD1: %s" % (files1,)
    assert "nuevo/b/c.txt" in files2, "tree not in SD2: %s" % (files2,)
    assert 'a' not in files1, "'a' should not be in SD1: %s" % (files1,)
    assert 'a' not in files2, "'a' should not be in SD1: %s" % (files2,)


@defer.inlineCallbacks
def test_create_tree_and_remove_it(test_name, sd1, sd2, sd3, prefix):
    """16. Create tree and remove it.

    Create a tree structure with some dirs and some files in some of
    them. Wait for nirvana. Remove everything as fast as possible. Check.
    """
    # create some tree structure with files
    deepdir = join(sd1.rootdir, *'abcd')
    os.makedirs(deepdir)
    create_file_and_add_content(join(deepdir, 'file1.txt'))
    create_file_and_add_content(join(deepdir, 'file2.txt'))
    open(join(deepdir, 'file2.txt'), 'w').close()

    deepdir = join(sd1.rootdir, *'abjk')
    os.makedirs(deepdir)
    create_file_and_add_content(join(deepdir, 'file3.txt'))
    open(join(deepdir, 'file4.txt'), 'w').close()

    deepdir = join(sd1.rootdir, 'a')
    create_file_and_add_content(join(deepdir, 'file5.txt'))
    open(join(deepdir, 'file6.txt'), 'w').close()
    debug(prefix, "Tree structure created with files in it")

    # wait for both to settle, and remove everything
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    shutil.rmtree(join(sd1.rootdir, 'a'))
    debug(prefix, "rmtree finished")

    # wait for everything to finish, get both files list and check
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    files1 = walk_and_list_dir(sd1.rootdir)
    files2 = walk_and_list_dir(sd2.rootdir)
    assert files1 == [], "bad info in SD1: %s" % (files1,)
    assert files2 == [], "bad info in SD1: %s" % (files2,)


@defer.inlineCallbacks
def test_rename_dir_create_same_name(test_name, sd1, sd2, sd3, prefix):
    """17. Create directory, rename it and create other with same name.

    Create a directory with a file A in it. Wait for nirvana. Rename
    the directory and immediately after create other directory with same
    name, and file B in it. Check.
    """
    # create the directory, file, and wait
    os.mkdir(join(sd1.rootdir, 'direct'))
    open(join(sd1.rootdir, 'direct', 'fileA.txt'), 'w').close()
    debug(prefix, "Directory with file A created")
    yield sd1.sdt.wait_for_nirvana(.5)
    debug(prefix, "Nirvana reached")

    # rename the directory, create again, and a new file
    os.rename(join(sd1.rootdir, 'direct'), join(sd1.rootdir, 'newdir'))
    os.mkdir(join(sd1.rootdir, 'direct'))
    open(join(sd1.rootdir, 'direct', 'fileB.txt'), 'w').close()
    debug(prefix, "Directory renamed and created new one with file B")

    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, "Nirvana reached on both")

    # check in sd2
    files = walk_and_list_dir(sd2.rootdir)
    shouldbe = ['direct', 'direct/fileB.txt', 'newdir', 'newdir/fileA.txt']
    assert files == shouldbe, "Bad files in SD2: %s" % (files,)


@defer.inlineCallbacks
def test_create_tree_internal_move(test_name, sd1, sd2, sd3, prefix):
    """18. Create tree and do internal move.

    Create a directory, with a subdir and a file in the later; immediately
    after move the file one dir up. Check.
    """
    # create the tree and do the move
    os.makedirs(join(sd1.rootdir, 'dir1', 'dir2'))
    content = create_file_and_add_content(join(sd1.rootdir, 'dir1',
                                               'dir2', 'file.txt'))
    os.rename(join(sd1.rootdir, 'dir1', 'dir2', 'file.txt'),
              join(sd1.rootdir, 'dir1', 'file.txt'))
    debug(prefix, "Tree created and file moved")

    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, "Nirvana reached on both")

    # check files in sd2
    files = walk_and_list_dir(sd2.rootdir)
    shouldbe = ['dir1', 'dir1/dir2', 'dir1/file.txt']
    assert files == shouldbe, "Bad files in SD2: %s" % (files,)

    # check content is ok
    assert content == open(join(sd2.rootdir, 'dir1', 'file.txt')).read()


@defer.inlineCallbacks
def test_delete_tree_touched_ok(test_name, sd1, sd2, sd3, prefix):
    """19. Delete a tree on one side that has a deleted file in the other.

    Create a directory, with a subdir and a file in the later. Wait for
    nirvana. At the same time on one client unlink the file, and on the
    other client remove everything. Check that all is deleted ok.
    """
    # create the tree and wait
    os.makedirs(join(sd1.rootdir, 'dir1', 'dir2'))
    create_file_and_add_content(join(sd1.rootdir, 'dir1', 'dir2', 'file.txt'))
    yield sd1.sdt.wait_for_nirvana(.5)
    debug(prefix, "Tree created and propagated")

    # remove everything on one side, and unlink on the other
    shutil.rmtree(join(sd2.rootdir, 'dir1'))
    os.remove(join(sd1.rootdir, 'dir1', 'dir2', 'file.txt'))
    debug(prefix, "Deletes executed")

    # wait
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, "Nirvana reached on both")

    # check files
    files1 = walk_and_list_dir(sd1.rootdir)
    files2 = walk_and_list_dir(sd2.rootdir)
    assert files1 == [], "Bad files in SD1: %s" % (files1,)
    assert files2 == [], "Bad files in SD2: %s" % (files2,)


@defer.inlineCallbacks
def test_delete_tree_touched_conflict(test_name, sd1, sd2, sd3, prefix):
    """20. Delete a tree on one side that has a renamed file in the other.

    Create a directory, with a subdir and a file in the later. Wait for
    nirvana. Disconnect SD1 and remove everything in SD2. Write something
    to the file in SD1 and connect again.  Check that in SD2 all is gone,
    but in SD1 the directory is not deleted but conflicted.
    """
    # create the tree and wait
    os.makedirs(join(sd1.rootdir, 'dir1', 'dir2'))
    create_file_and_add_content(join(sd1.rootdir, 'dir1', 'dir2', 'file.txt'))
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    yield sd1.sdt.disconnect()
    debug(prefix, "Tree created and propagated; sd1 disconnected")

    # remove everything on one side
    shutil.rmtree(join(sd2.rootdir, 'dir1'))
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, "Delete executed")

    # change sd1 and reconnect
    with open(join(sd1.rootdir, 'dir1', 'dir2', 'file.txt'), 'w') as fh:
        fh.write(os.urandom(10))
    yield sd1.sdt.connect()
    yield sd1.sdt.wait_for_nirvana(.5)
    debug(prefix, "Wrote in SD1 and reconnected")

    # check files
    files2 = walk_and_list_dir(sd2.rootdir)
    assert files2 == [], "Bad files in SD2: %s" % (files2,)

    files1 = walk_and_list_dir(sd1.rootdir)
    shouldbe = ['dir1.u1conflict', 'dir1.u1conflict/dir2.u1conflict',
                'dir1.u1conflict/dir2.u1conflict/file.txt.u1conflict']
    assert files1 == shouldbe, "Bad files in SD1: %s" % (files1,)


@defer.inlineCallbacks
def test_overwrite_file_with_content(test_name, sd1, sd2, sd3, prefix):
    """21. Create a file with data in it and overwrite it.

    Create a file 1 with content, and a file 2 with other content. Wait
    for nirvana. Move 2 into 1 (overwriting it). Check.
    """
    # create the file in sd1 and wait
    file1 = join(sd1.rootdir, 'file1.txt')
    create_file_and_add_content(file1)
    file2 = join(sd1.rootdir, 'file2.txt')
    content2 = create_file_and_add_content(file2)
    yield sd1.sdt.wait_for_nirvana(.5)
    debug(prefix, "Files created")

    # overwrite it
    os.rename(file2, file1)
    debug(prefix, "Overwritten")

    # wait propagation
    yield sd1.sdt.wait_for_nirvana(.5)
    yield sd2.sdt.wait_for_nirvana(.5)
    debug(prefix, "All changes propagated")

    # check that the file is in sd2 with correct content
    assert not os.path.exists(join(sd1.rootdir, 'file2.txt'))
    data = open(join(sd2.rootdir, 'file1.txt')).read()
    assert data == content2
