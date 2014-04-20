import unittest
from unittest import TestCase
from dbfs import Dbfs
import errno
import os

__author__ = 'yermak'

class TestDbfs(TestCase):
    def setUp(self):
        self.fs = Dbfs();
        self.fs.db.open_connection()
        self.fs.db.conn().execute('drop table if exists options')
        self.fs.db.conn().execute('drop table if exists indices')
        self.fs.db.conn().execute('drop table if exists hashes')
        self.fs.db.conn().execute('drop table if exists links')
        self.fs.db.conn().execute('drop table if exists inodes')
        self.fs.db.conn().execute('drop table if exists tree')
        self.fs.db.conn().execute('drop table if exists names')

        self.fs.db.initialize(1, 2, 0)
        self.fs.db.commit()

    def tearDown(self):
        pass

    def assertZero(self, actual, message):
        self.assertEquals(actual, 0, message)


    def test_mkdir_access_normal(self):
        self.fs.mkdir({'uid': 1, 'gid': 2}, 'first', 777)
        access = self.fs.access({'uid': 1, 'gid': 2}, 'first', 0)
        self.assertZero(access, 'Access failed')


    def test_mkdir_access_nested(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, "parent_dir", 777)
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, "parent_dir/child_dir", 777)
        self.assertZero(mkdir, 'mkdir failed')
        access = self.fs.access({'uid': 1, 'gid': 2}, "parent_dir/child_dir", 0)
        self.assertZero(access, 'Access failed')


    def test_chmod(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, "mod_dir", 0777)
        self.assertZero(mkdir, 'mkdir failed')
        chmod = self.fs.chmod('mod_dir', 0000)
        self.assertZero(chmod, 'Chmod failed')

        access = self.fs.access({'uid': 1, 'gid': 2}, 'mod_dir', os.W_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        access = self.fs.access({'uid': 1, 'gid': 3}, 'mod_dir', os.R_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        access = self.fs.access({'uid': 3, 'gid': 2}, 'mod_dir', os.X_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        access = self.fs.access({'uid': 3, 'gid': 3}, 'mod_dir', os.X_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        chmod = self.fs.chmod('mod_dir', 0751)
        self.assertZero(chmod, 'Chmod failed')

        access = self.fs.access({'uid': 1, 'gid': 3}, 'mod_dir', os.R_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 1, 'gid': 3}, 'mod_dir', os.W_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 1, 'gid': 3}, 'mod_dir', os.X_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 3, 'gid': 2}, 'mod_dir', os.R_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 3, 'gid': 2}, 'mod_dir', os.X_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 3, 'gid': 3}, 'mod_dir', os.X_OK)
        self.assertZero(access, 'Access denied')

        self.fs.read_only = True
        access = self.fs.access({'uid': 1, 'gid': 3}, 'mod_dir', os.W_OK)
        self.assertEqual(access, -errno.EACCES, 'Access denied')


    def test_chown(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, "own_dir", 0751)
        self.assertZero(mkdir, 'mkdir failed')
        access = self.fs.access({'uid': 1, 'gid': 4}, 'own_dir', os.W_OK)
        self.assertZero(access, 'Access denied')
        access = self.fs.access({'uid': 3, 'gid': 2}, 'own_dir', os.R_OK)
        self.assertZero(access, 'Access denied')
        access = self.fs.access({'uid': 5, 'gid': 6}, 'own_dir', os.X_OK)
        self.assertZero(access, 'Access denied')

        chown = self.fs.chown('own_dir', 3, 4)
        self.assertZero(chown, 'Chown failed')

        access = self.fs.access({'uid': 1, 'gid': 2}, 'own_dir', os.X_OK)
        self.assertZero(access, 'Access denied')

        access = self.fs.access({'uid': 1, 'gid': 2}, 'own_dir', os.R_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        access = self.fs.access({'uid': 1, 'gid': 2}, 'own_dir', os.W_OK)
        self.assertEqual(access, -errno.EACCES, 'Access violation')

        access = self.fs.access({'uid': 3, 'gid': 5}, 'own_dir', os.R_OK)
        self.assertZero(access, 'Access violation')

        access = self.fs.access({'uid': 3, 'gid': 5}, 'own_dir', os.W_OK)
        self.assertZero(access, 'Access violation')

        access = self.fs.access({'uid': 3, 'gid': 5}, 'own_dir', os.X_OK)
        self.assertZero(access, 'Access violation')

        access = self.fs.access({'uid': 1, 'gid': 4}, 'own_dir', os.X_OK)
        self.assertZero(access, 'Access violation')


    def test_rmdir(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'rm_dir', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        rmdir = self.fs.rmdir('rm_dir')
        self.assertZero(rmdir, 'rmdir failed')
        access = self.fs.access({'uid': 1, 'gid': 2}, 'rm_dir', os.R_OK)
        self.assertNotEqual(access, 0, 'Accessed non existing directory')


    def test_rmdir_nested(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'rm_dir_parent', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'rm_dir_parent/rm_dir_child', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        #     with self.assertRaises(OSError) as cm:
        #       rmdir = self.fs.rmdir('rm_dir_parent')
        #       the_exception = cm.exception
        #       self.assertEqual(the_exception.error_code, errno.ENOTEMPTY)
        rmdir = self.fs.rmdir('rm_dir_parent')
        self.assertEqual(rmdir, -errno.ENOTEMPTY,
                         'Proper Error was not raised, expected %s found %s' % (-errno.ENOTEMPTY, rmdir))


    def test_rmdir_readonly(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'rm_dir_readonly', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        self.fs.read_only = True
        rmdir = self.fs.rmdir('rm_dir_readonly')
        self.assertEqual(rmdir, -errno.EROFS, 'Readonly error was not raised')


    def test_symlink(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'real_dir_parent', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'real_dir_parent/real_dir_child', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'sym_dir_parent', 0777)
        self.assertZero(mkdir, 'mkdir failed')

        symlink = self.fs.symlink({'uid': 1, 'gid': 2}, 'real_dir_parent/real_dir_child',
                                  'sym_dir_parent/sym_dir_child')
        self.assertZero(symlink, 'Symlink failed')

    def test_readlink(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'real_dir_parent', 0777)
        self.assertZero(mkdir, 'mkdir failed')
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'real_dir_parent/real_dir_child', 0777)

        symlink = self.fs.symlink({'uid': 1, 'gid': 2}, 'real_dir_parent/real_dir_child', 'sym_dir')
        target = self.fs.readlink('sym_dir')
        self.assertEqual(target, 'real_dir_parent/real_dir_child', 'Readlink failed')

    def test_link(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'source_dir_parent', 0777)
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'source_dir_parent/source_dir_child', 0777)

        link = self.fs.link('source_dir_parent/source_dir_child', 'dist_dir')
        self.assertZero(link, 'Failed to create hard link')
        access = self.fs.access({'uid': 1, 'gid': 2}, 'dist_dir', os.R_OK)
        self.assertZero(access, 'Failed to access linked dir')

    def test_rename(self):
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'old_dir_parent', 0777)
        mkdir = self.fs.mkdir({'uid': 1, 'gid': 2}, 'old_dir_parent/child', 0777)
        rename = self.fs.rename('old_dir_parent', 'new_dir_parent')
        self.assertZero(rename, 'Failed to rename dir')

        access_new_parent = self.fs.access({'uid': 1, 'gid': 2}, 'new_dir_parent', os.R_OK)
        self.assertZero(access_new_parent, 'Failed to access new parent dir')

        access_new_child = self.fs.access({'uid': 1, 'gid': 2}, 'new_dir_parent/child', os.R_OK)
        self.assertZero(access_new_child, 'Failed to access new child dir with error code: %s' % access_new_child)

        # access_old_parent = self.fs.access({'uid': 1, 'gid': 2}, 'old_dir_parent', os.R_OK)
        # self.assertNotEqual(0, access_old_parent, 'Still able to access old parent dir')
        #
        # access_old_child = self.fs.access({'uid': 1, 'gid': 2}, 'old_dir_parent/child', os.R_OK)
        # self.assertNotEqual(0, access_old_child, 'Still able to access old parent dir')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()