#!/usr/bin/python

# Documentation. {{{1

"""
This Python script implements a file system in user space using FUSE.
This module was based on DedupFS filesystem but was refactored to add
more features and add higher abstraction level.



==========================================================================
It's called DedupFS because the file system's primary feature is deduplication,
which enables it to store virtually unlimited copies of files because data
is only stored once.

In addition to deduplication the file system also supports transparent
compression using any of the compression methods lzo, zlib and bz2.

These two properties make the file system ideal for backups: I'm currently
storing 250 GB worth of backups using only 8 GB of disk space.
============================================================================

DedupFS is licensed under the MIT license.
The latest version is available at http://peterodding.com/code/dedupfs/
Copyright 2010 Peter Odding <peter@peterodding.com>.

dbfs-fuse is licensed under the MIT license.
Copyright 2014 Yaroslav Yermak <yermak@gmail.com>.

"""

# Imports. {{{1

# http://www.rath.org/llfuse-docs/example.html#example-file-system


# Check the Python version, warn the user if untested.
import sys

if sys.version_info[:2] != (2, 6):
    sys.stderr.write('Warning: DedupFS has only been tested on Python 2.6, while you''re running Python %d.%d!\n' % (
    sys.version_info[0], sys.version_info[1]))

# Try to load the required modules from Python's standard library.
try:
    import cStringIO
    import errno
    import hashlib
    import logging
    import math
    import os
    import sqlite3
    import stat
    import time
    import traceback
    import dbfs
except ImportError, e:
    sys.stderr.write('Error: Failed to load one of the required Python modules! (%s)\n' % str(e))
    sys.exit(1)

# Try to load the Python FUSE binding.
try:
    import fuse
except ImportError:
    sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
                     "If you're on Ubuntu try running `sudo apt-get install python-fuse'.\n")
    sys.exit(1)

# Local modules that are mostly useful for debugging.

# from my_formats import format_size, format_timespan
# from get_memory_usage import get_memory_usage
# from db import Db


def main():  # {{{1
    """
    This function enables using dedupfs.py as a shell script that creates FUSE
    mount points. Execute "dedupfs -h" for a list of valid command line options.
    """

    dfs = DedupFS()

    # A short usage message with the command line options defined by dedupfs
    # itself (see the __init__() method of the DedupFS class) is automatically
    # printed by the following call when sys.argv contains -h or --help.
    fuse_opts = dfs.parse(['-o', 'use_ino,default_permissions,fsname=dedupfs'] + sys.argv[1:])

    dfs_opts = dfs.cmdline[0]
    if dfs_opts.print_stats:
        dfs.fs.read_only = True
        dfs.fs.fsinit(silent=True)
        dfs.fs.report_disk_usage()
        dfs.fsdestroy(silent=True)

    # If the user didn't pass -h or --help and also didn't supply a mount point
    # as a positional argument, print the short usage message and exit (I don't
    # agree with the Python FUSE binding's default behavior, which is something
    # nonsensical like using the working directory as a mount point).
    elif dfs.fuse_args.mount_expected() and not fuse_opts.mountpoint:
        dfs.parse(['-h'])
    elif fuse_opts.mountpoint or not dfs.fuse_args.mount_expected():
        # Don't print all options unless the user passed -h or --help explicitly
        # because this listing includes the 20+ options defined by the Python FUSE
        # binding (which is kind of intimidating at first).
        dfs.main()


class DedupFS(fuse.Fuse):  # {{{1

    def __init__(self, *args, **kw):  # {{{2

        try:

            # Set the Python FUSE API version.
            fuse.fuse_python_api = (0, 2)

            # Initialize the FUSE binding's internal state.
            fuse.Fuse.__init__(self, *args, **kw)

            # Set some options required by the Python FUSE binding.
            self.flags = 0
            self.multithreaded = 0

            # Register some custom command line options with the option parser.
            option_stored_in_db = " (this option is only useful when creating a new database, because your choice is stored in the database and can't be changed after that)"
            self.parser.set_conflict_handler('resolve')  # enable overriding the --help message.
            self.parser.add_option('-h', '--help', action='help',
                                   help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
            self.parser.add_option('-v', '--verbose', action='count', dest='verbosity', default=0,
                                   help="increase verbosity")
            self.parser.add_option('--print-stats', dest='print_stats', action='store_true', default=False,
                                   help="print the total apparent size and the actual disk usage of the file system and exit")
            self.parser.add_option('--log-file', dest='log_file', help="specify log file location")
            self.parser.add_option('--metastore', dest='metastore', metavar='FILE', default=self.metastore_file,
                                   help="specify the location of the file in which metadata is stored")
            self.parser.add_option('--datastore', dest='datastore', metavar='FILE', default=self.datastore_file,
                                   help="specify the location of the file in which data blocks are stored")
            self.parser.add_option('--block-size', dest='block_size', metavar='BYTES', default=self.block_size,
                                   type='int', help="specify the maximum block size in bytes" + option_stored_in_db)
            self.parser.add_option('--no-transactions', dest='use_transactions', action='store_false', default=True,
                                   help="don't use transactions when making multiple related changes, this might make the file system faster or slower (?)")
            self.parser.add_option('--nosync', dest='synchronous', action='store_false', default=True,
                                   help="disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted)")
            self.parser.add_option('--nogc', dest='gc_enabled', action='store_false', default=True,
                                   help="disable the periodic garbage collection because it degrades performance (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced)")
            self.parser.add_option('--verify-writes', dest='verify_writes', action='store_true', default=False,
                                   help="after writing a new data block to the database, check that the block was written correctly by reading it back again and checking for differences")

            # Dynamically check for supported hashing algorithms.
            msg = "specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s" + option_stored_in_db
            hash_functions = filter(lambda m: m[0] != '_' and m != 'new', dir(hashlib))
            msg %= ', '.join('%r' % fun for fun in hash_functions)
            self.parser.add_option('--hash', dest='hash_function', metavar='FUNCTION', type='choice',
                                   choices=hash_functions, default='sha1', help=msg)

            # Dynamically check for supported compression methods.
            def noop(s):
                return s

            self.compressors = {'none': (noop, noop)}
            compression_methods = ['none']
            for modname in 'lzo', 'zlib', 'bz2':
                try:
                    module = __import__(modname)
                    if hasattr(module, 'compress') and hasattr(module, 'decompress'):
                        self.compressors[modname] = (module.compress, module.decompress)
                        compression_methods.append(modname)
                except ImportError:
                    pass
            msg = "enable compression of data blocks using one of the supported compression methods: one of %s" + option_stored_in_db
            msg %= ', '.join('%r' % mth for mth in compression_methods[1:])
            self.parser.add_option('--compress', dest='compression_method', metavar='METHOD', type='choice',
                                   choices=compression_methods, default='none', help=msg)

            # Dynamically check for profiling support.
            try:
                # Using __import__() here because of pyflakes.
                for p in 'cProfile', 'pstats': __import__(p)
                self.parser.add_option('--profile', action='store_true', default=False,
                                       help="use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots)")
            except ImportError:
                self.logger.warning("No profiling support available, --profile option disabled.")
                self.logger.warning("If you're on Ubuntu try `sudo apt-get install python-profiler'.")

            self.fs = dbfs.Dbfs()
        except Exception, e:
            self.__except_to_status('__init__', e)
            sys.exit(1)


    # FUSE API implementation: {{{2

    def access(self, path, flags):  # {{{3
        return self.fs.acccess(fuse.FuseGetContext(), path, flags)

    def chmod(self, path, mode):  # {{{3
        return self.fs.chmod(path, mode)

    def chown(self, path, uid, gid):  # {{{3
        return self.fs.chown(path, uid, gid)

    def create(self, path, flags, mode):  # {{{3
        return self.fs.create(path, flags, mode)

    def fsdestroy(self, silent=False):  # {{{3
        return self.fs.fsdestroy(silent)

    def getattr(self, path):  # {{{3
        return self.fs.getattr()

    def link(self, target_path, link_path, nested=False):  # {{{3
        # From the link(2) manual page: "If link_path names a directory, link()
        # shall fail unless the process has appropriate privileges and the
        # implementation supports using link() on directories." ... :-)
        # However I've read that FUSE doesn't like multiple directory pathnames
        # with the same inode number (maybe because of internal caching based on
        # inode numbers?).
        return self.fs.link(target_path, link_path, nested)

    def mkdir(self, path, mode):
        return self.fs.mkdir(self, fuse.FuseGetContext(), path, mode)

    def mknod(self, path, mode, rdev):  # {{{3
        return self.fs.mknod(fuse.FuseGetContext(), path, mode, rdev)

    def open(self, path, flags, nested=None, inode=None):  # {{{3
        return self.fs.open(fuse.FuseGetContext(), path, flags, nested, inode)

    def read(self, path, length, offset):  # {{{3
        return self.fs.read(path, length, offset)

    def readdir(self, path, offset):  # {{{3
        # Bug fix: When you use the -o use_ino option, directory entries must have
        # an "ino" field, otherwise not a single directory entry will be listed!
        try:
            self.__log_call('readdir', 'readdir(%r, %i)', path, offset)
            node_id, inode = self.__path2keys(path)
            yield fuse.Direntry('.', ino=inode)
            yield fuse.Direntry('..')
            for inode, name in self.db.list_childs(node_id):
                yield fuse.Direntry(str(name), ino=inode)
        except Exception, e:
            self.__except_to_status('readdir', e)

    def readlink(self, path):  # {{{3
        return self.fs.readlink(path)

    def release(self, path, flags):  # {{{3
        return self.fs.release(path, flags)

    def rename(self, old_path, new_path):  # {{{3
        return self.fs.rename(old_path, new_path)

    def rmdir(self, path):  # {{{3
        return self.fs.rmdir(path)

    def statfs(self):  # {{{3
        return self.fs.statfs()

    def symlink(self, target_path, link_path):  # {{{3
        return self.fs.symlink(fuse.FuseGetContext(), target_path, link_path)

    def truncate(self, path, size):  # {{{3
        return self.fs.truncate(path, size)

    def unlink(self, path, nested=False):  # {{{3
        return self.fs.unlink(path, nested)

    def utime(self, path, times):  # {{{3
        return self.fs.utime(path, times)

    def utimens(self, path, ts_acc, ts_mod):  # {{{3
        return self.fs.utimens(path, ts_acc, ts_mod)

    def write(self, path, data, offset):  # {{{3
        return self.fs.write(path, data, offset)

# }}}1

if __name__ == '__main__':

    if '--profile' in sys.argv:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats

        profile = '.dedupfs.cprofile-%i' % time.time()
        cProfile.run('main()', profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('time')
        s.print_stats(0.1)
        os.unlink(profile)
    else:
        main()

# vim: ts=2 sw=2 et
