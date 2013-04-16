'''
Created on 26.03.2013

@author: Yermak
'''
import sys  
import cStringIO
import errno
import hashlib
import logging
import math
import os
import stat
import time
import traceback


# Local modules that are mostly useful for debugging.
from my_formats import format_size, format_timespan
# from get_memory_usage import get_memory_usage
#from db.sqlite import Db
from db.mysql import MysqlDb

class Dbfs:
  '''
  classdocs
  '''


  def __init__(self):
      '''
      Constructor
      '''
          # Initialize instance attributes.
      self.block_size = 1024 * 128
      self.buffers = {}
      self.bytes_read = 0
      self.bytes_written = 0
      self.cache_gc_last_run = time.time()
      self.cache_requests = 0
      self.cache_timeout = 60 # TODO Make this a command line option!
      self.cached_nodes = {}
      self.calls_log_filter = []
      self.datastore_file = '~/.dedupfs-datastore.db'
      self.fs_mounted_at = time.time()
      self.gc_enabled = True
      self.gc_hook_last_run = time.time()
      self.gc_interval = 60
      self.link_mode = stat.S_IFLNK | 0777
      self.memory_usage = 0
      self.metastore_file = '~/.dedupfs-metastore.sqlite3'
      self.opcount = 0
      self.read_only = False
      self.root_mode = stat.S_IFDIR | 0755
      self.time_spent_caching_nodes = 0
      self.time_spent_hashing = 0
      self.time_spent_interning = 0
      self.time_spent_querying_tree = 0
      self.time_spent_reading = 0
      self.time_spent_traversing_tree = 0
      self.time_spent_writing = 0
      self.time_spent_writing_blocks = 0
      self.__NODE_KEY_VALUE = 0
      self.__NODE_KEY_LAST_USED = 1

      # Initialize a Logger() object to handle logging.
      self.logger = logging.getLogger('fuse-dbfs.main')
      self.logger.setLevel(logging.DEBUG)
#      self.logger.addHandler(logging.StreamHandler(sys.stdout))
      fh = logging.FileHandler('fuse.log')
      fh.setLevel(logging.DEBUG)
      self.logger.addHandler(fh);

      self.db = MysqlDb('localhost','dbfs','root', 'BISKVIT')
    
  def mkdir(self, ctx, path, mode): 
    try:
      self.__log_call('mkdir', 'mkdir(%r, %o)', path, mode)
      if self.read_only: return -errno.EROFS
      self.db.open_connection()
      inode, parent_ino = self.__insert(ctx, path, mode | stat.S_IFDIR, 4*1024)
      self.db.inc_links(parent_ino);
      self.db.commit()
      self.__gc_hook()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('mkdir', e, errno.EIO)

  def access(self, ctx, path, flags):
    try:
      self.__log_call('access', 'access(%r, %o)', path, flags)
      inode = self.__path2keys(path)[1]
      if flags == os.F_OK or self.__access(ctx, inode, flags):
        return 0
      else:
        return -errno.EACCES
    except Exception, e:
      return self.__except_to_status('access', e, errno.ENOENT)

  def chmod(self, path, mode):
    try:
      self.__log_call('chmod', 'chmod(%r, %o)', path, mode)
      if self.read_only: return -errno.EROFS
      inode = self.__path2keys(path)[1]
      self.db.update_mode(mode, inode)
      self.__gc_hook()
      return 0
    except Exception, e:
      return self.__except_to_status('chmod', e, errno.EIO)

  def chown(self, path, uid, gid):
    try:
      self.__log_call('chown', 'chown(%r, %i, %i)', path, uid, gid)
      if self.read_only: return -errno.EROFS
      inode = self.__path2keys(path)[1]
      self.db.update_uid_gid(uid, gid, inode)
      self.__gc_hook()
      return 0
    except Exception, e:
      return self.__except_to_status('chown', e, errno.EIO)

  def create(self, ctx, path, flags, mode): # {{{3
    try:
      self.__log_call('create', 'create(%r, %o, %o)', path, flags, mode)
      if self.read_only: return -errno.EROFS
      try:
        # If the file already exists, just open it.
        status = self.open(path, flags, nested=True)
      except OSError, e:
        if e.errno != errno.ENOENT: raise
        # Otherwise create a new file and open that.
        inode, parent_ino = self.__insert(ctx, path, mode, 0)
        status = self.open(path, flags, nested=True, inode=inode)
      self.db.commit()
      self.__gc_hook()
      return status
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('create', e, errno.EIO)

  def fsdestroy(self, silent=False): # {{{3
    try:
      self.__log_call('fsdestroy', 'fsdestroy()')
      self.__collect_garbage()
      if not silent:
        self.__print_stats()
      if not self.read_only:
        self.logger.info("Committing outstanding changes to `%s'.", self.metastore_file)
        self.db.dbmcall('sync')
        self.db.commit()
      self.db.close()
      return 0
    except Exception, e:
      return self.__except_to_status('fsdestroy', e, errno.EIO)



  def fsinit(self, silent=False): # {{{3
    self.logger.debug('fsinit started')
    try:
      # Process the custom command line options defined in __init__().
      options = self.cmdline[0]
      self.block_size = options.block_size
      self.compression_method = options.compression_method
      self.datastore_file = self.__check_data_file(options.datastore, silent)
      self.gc_enabled = options.gc_enabled
      self.hash_function = options.hash_function
      self.metastore_file = self.__check_data_file(options.metastore, silent)
      self.synchronous = options.synchronous
      
      self.verify_writes = options.verify_writes
      # Initialize the logging and database subsystems.
#      self.__init_logging(options)
#      self.__log_call('fsinit', 'fsinit()')
      
      self.logger.debug('about to start db init')
      self.__setup_database_connections(options.use_transactions, silent, options.synchronous)
      if not self.read_only:
        self.__init_metastore()
      self.__get_opts_from_db(options)
      # Make sure the hash function is (still) valid (since the database was created).
      if not hasattr(hashlib, self.hash_function):
        self.logger.critical("Error: The selected hash function %r doesn't exist!", self.hash_function)
        sys.exit(1)
      # Get a reference to the hash function.
      self.hash_function_impl = getattr(hashlib, self.hash_function)
      # Select the compression method (if any) after potentially reading the
      # configured block size that was used to create the database (see the
      # set_block_size() call).
      self.__select_compress_method(options, silent)
      return 0
    except Exception, e:
      self.__except_to_status('fsinit', e, errno.EIO)
      # Bug fix: Break the mount point when initialization failed with an
      # exception, because self.conn might not be valid, which results in
      # an internal error message for every FUSE API call...
      os._exit(1)

  def getattr(self, path): # {{{3
    try:
      self.__log_call('getattr', 'getattr(%r)', path)
      inode = self.__path2keys(path)[1]
      attrs = self.db.gett_attr()
      result = Stat(st_ino     = attrs['inode'],
                    st_nlink   = attrs['nlinks'],
                    st_mode    = attrs['mode'],
                    st_uid     = attrs['uid'],
                    st_gid     = attrs['gid'],
                    st_rdev    = attrs['rdev'],
                    st_size    = attrs['size'],
                    st_atime   = attrs['atime'],
                    st_mtime   = attrs['mtime'],
                    st_ctime   = attrs['ctime'],
                    st_blksize = self.block_size,
                    st_blocks  = attrs[6] / 512,
                    st_dev     = 0)
      self.logger.debug("getattr(%r) returning %s", path, result)
      return result
    except Exception, e:
      self.logger.debug("getattr(%r) returning ENOENT", path)
      return self.__except_to_status('getattr', e, errno.ENOENT)

  def link(self, target_path, link_path, nested=False): # {{{3
    try:
      self.__log_call('link', '%slink(%r -> %r)', nested and ' ' or '', target_path, link_path)
      if self.read_only: return -errno.EROFS
      target_ino = self.__path2keys(target_path)[1]
      link_parent, link_name = os.path.split(link_path)
      link_parent_id, link_parent_ino = self.__path2keys(link_parent)
      string_id = self.db.get_node_by_name(link_name)
      node_id = self.db.add_leaf(link_parent_id, string_id, target_ino)
      
      self.db.inc_links(target_ino)
      
      mode, uid, gid = self.db.get_mode_uid_gid(target_ino)
      if mode & stat.S_IFDIR:
        self.db.inc_links(link_parent_ino)
        
      self.__cache_set(link_path, (node_id, target_ino))
      self.db.commit(nested)
      self.__gc_hook(nested)
      return 0
    except Exception, e:
      self.db.rollback(nested)
      if nested: raise
      return self.__except_to_status('link', e, errno.EIO)

  def mknod(self, ctx, path, mode, rdev): # {{{3
    try:
      self.__log_call('mknod', 'mknod(%r, %o)', path, mode)
      if self.read_only: return -errno.EROFS
      self.__insert(ctx, path, mode, 0, rdev)
      self.db.commit()
      self.__gc_hook()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('mknod', e, errno.EIO)

  def open(self, ctx, path, flags, nested=None, inode=None): # {{{3
    try:
      self.__log_call('open', 'open(%r, %o)', path, flags)
      # Make sure the file exists?
      inode = inode or self.__path2keys(path)[1]
      # Make sure the file is readable and/or writable.
      access_flags = 0
      if flags & (os.O_RDONLY | os.O_RDWR): access_flags |= os.R_OK
      if flags & (os.O_WRONLY | os.O_RDWR): access_flags |= os.W_OK
      if not self.__access(ctx, inode, access_flags):
        return -errno.EACCES
      return 0
    except Exception, e:
      if nested: raise
      return self.__except_to_status('open', e, errno.ENOENT)

  def read(self, path, length, offset): # {{{3
    try:
      self.__log_call('read', 'read(%r, %i, %i)', path, length, offset)
      start_time = time.time()
      buf = self.__get_file_buffer(path)
      buf.seek(offset)
      data = buf.read(length)
      self.time_spent_reading += time.time() - start_time
      self.bytes_read += len(data)
      return data
    except Exception, e:
      return self.__except_to_status('read', e, code=errno.EIO)

  def readlink(self, path): # {{{3
    try:
      self.__log_call('readlink', 'readlink(%r)', path)
      inode = self.__path2keys(path)[1]
      return self.db.get_target(inode)
    except Exception, e:
      return self.__except_to_status('readlink', e, errno.ENOENT)

  def release(self, path, flags): # {{{3
    try:
      self.__log_call('release', 'release(%r, %o)', path, flags)
      # Flush the write buffer?!
      if path in self.buffers:
        buf = self.buffers[path]
        # Flush the write buffer?
        if buf.dirty:
          # Record start time so we can calculate average write speed.
          start_time = time.time()
          # Make sure the file exists and get its inode number.
          inode = self.__path2keys(path)[1]
          # Save apparent file size before possibly compressing data.
          apparent_size = len(buf)
          # Split up that string in the configured block size, hash the
          # resulting blocks and store any new blocks.
          try:
            self.__write_blocks(inode, buf, apparent_size)
            self.db.commit()
          except Exception, e:
            self.db.rollback()
            raise
          # Record the number of bytes written and the elapsed time.
          self.bytes_written += apparent_size
          self.time_spent_writing += time.time() - start_time
          self.__gc_hook()
        # Delete the buffer.
        buf.close()
        del self.buffers[path]
      return 0
    except Exception, e:
      return self.__except_to_status('release', e, errno.EIO)

  def rename(self, old_path, new_path): # {{{3
    try:
      self.__log_call('rename', 'rename(%r -> %r)', old_path, new_path)
      if self.read_only: return -errno.EROFS
      # Try to remove the existing target path (if if exists).
      # NB: This also makes sure target directories are empty.
      try:
        self.__remove(new_path, check_empty=True)
      except OSError, e:
        # Ignore errno.ENOENT, re raise other exceptions.
        if e.errno != errno.ENOENT: raise
      # Link the new path to the same inode as the old path.
      self.link(old_path, new_path, nested=True)
      # Finally unlink the old path.
      self.unlink(old_path, nested=True)
      self.db.commit()
      self.__gc_hook()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('rename', e, errno.ENOENT)

  def rmdir(self, path): # {{{3
    try:
      self.__log_call('rmdir', 'rmdir(%r)', path)
      if self.read_only: return -errno.EROFS
      self.__remove(path, check_empty=True)
      self.db.commit()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('rmdir', e, errno.ENOENT)

  def statfs(self): # {{{3
    try:
      self.__log_call('statfs', 'statfs()')
      # Use os.statvfs() to report the host file system's storage capacity.
      host_fs = os.statvfs(self.metastore_file)
      return StatVFS(f_bavail  = (host_fs.f_bsize * host_fs.f_bavail) / self.block_size, # The total number of free blocks available to a non privileged process.
                     f_bfree   = (host_fs.f_frsize * host_fs.f_bfree) / self.block_size, # The total number of free blocks in the file system.
                     f_blocks  = (host_fs.f_frsize * host_fs.f_blocks) / self.block_size, # The total number of blocks in the file system in terms of f_frsize.
                     f_bsize   = self.block_size, # The file system block size in bytes.
                     f_favail  = 0, # The number of free file serial numbers available to a non privileged process.
                     f_ffree   = 0, # The total number of free file serial numbers.
                     f_files   = 0, # The total number of file serial numbers.
                     f_flag    = 0, # File system flags. Symbols are defined in the <sys/statvfs.h> header file to refer to bits in this field (see The f_flags field).
                     f_frsize  = self.block_size, # The fundamental file system block size in bytes.
                     f_namemax = 4294967295) # The maximum file name length in the file system. Some file systems may return the maximum value that can be stored in an unsigned long to indicate the file system has no maximum file name length. The maximum value that can be stored in an unsigned long is defined in <limits.h> as ULONG_MAX.
    except Exception, e:
      return self.__except_to_status('statfs', e, errno.EIO)

  def symlink(self, ctx, target_path, link_path): # {{{3
    try:
      self.__log_call('symlink', 'symlink(%r -> %r)', link_path, target_path)
      if self.read_only: return -errno.EROFS
      # Create an inode to hold the symbolic link.
      inode, parent_ino = self.__insert(ctx, link_path, self.link_mode, len(target_path))
      # Save the symbolic link's target.
      self.db.add_link(inode, target_path)
      self.db.commit()
      self.__gc_hook()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('symlink', e, errno.EIO)

  def truncate(self, path, size): # {{{3
    try:
      self.__log_call('truncate', 'truncate(%r, %i)', path, size)
      if self.read_only: return -errno.EROFS
      inode = self.__path2keys(path)[1]
      last_block = size / self.block_size
      self.db.clear_index(inode, last_block)
      self.db.update_inode_size(inode, size, last_block);
      self.__gc_hook()
      self.db.commit()
      return 0
    except Exception, e:
      self.db.rollback()
      return self.__except_to_status('truncate', e, errno.ENOENT)

  def unlink(self, path, nested=False): # {{{3
    try:
      self.__log_call('unlink', '%sunlink(%r)', nested and ' ' or '', path)
      if self.read_only: return -errno.EROFS
      self.__remove(path)
      self.db.commit(nested)
    except Exception, e:
      self.db.rollback(nested)
      if nested: raise
      return self.__except_to_status('unlink', e, errno.ENOENT)

  def utime(self, path, times): # {{{3
    try:
      self.__log_call('utime', 'utime(%r, %i, %i)', path, *times)
      if self.read_only: return -errno.EROFS
      inode = self.__path2keys(path)[1]
      atime, mtime = times
      self.db.update_time(inode, atime, mtime)
      self.__gc_hook()
      return 0
    except Exception, e:
      return self.__except_to_status('utime', e, errno.ENOENT)

  def utimens(self, path, ts_acc, ts_mod): # {{{3
    try:
      self.__log_call('utimens', 'utimens(%r, %i.%i, %i.%i)', path, ts_acc.tv_sec, ts_acc.tv_nsec, ts_mod.tv_sec, ts_mod.tv_nsec)
      if self.read_only: return -errno.EROFS
      inode = self.__path2keys(path)[1]
      atime = ts_acc.tv_sec + (ts_acc.tv_nsec / 1000000.0)
      mtime = ts_mod.tv_sec + (ts_mod.tv_nsec / 1000000.0)
      self.db.update_time(inode, atime, mtime)
      self.__gc_hook()
      return 0
    except Exception, e:
      return self.__except_to_status('utimens', e, errno.ENOENT)

  def write(self, path, data, offset): # {{{3
    try:
      length = len(data)
      self.__log_call('write', 'write(%r, %i, %i)', path, offset, length)
      start_time = time.time()
      buf = self.__get_file_buffer(path)
      buf.seek(offset)
      buf.write(data)
      time_time = time.time()
      self.time_spent_writing += time_time - start_time
      # self.bytes_written is incremented from release().
      return length
    except Exception, e:
      return self.__except_to_status('write', e, errno.EIO)

  # Miscellaneous methods: {{{2

  def __init_logging(self, options): # {{{3
    # Configure logging of messages to a file.
    if options.log_file:
      handler = logging.StreamHandler(open(options.log_file, 'w'))
      self.logger.addHandler(handler)
    # Convert verbosity argument to logging level?
    if options.verbosity > 0:
      if options.verbosity <= 1:
        self.logger.setLevel(logging.INFO)
      elif options.verbosity <= 2:
        self.logger.setLevel(logging.DEBUG)
      else:
        self.logger.setLevel(logging.NOTSET)

  def __init_metastore(self): # {{{3
    # Bug fix: At this point fuse.FuseGetContext() returns uid = 0 and gid = 0
    # which differs from the info returned in later calls. The simple fix is to
    # use Python's os.getuid() and os.getgid() library functions instead of
    # fuse.FuseGetContext().
    self.db.initialize(os.getuid(), os.getgid(), self.root_mode)

  def __setup_database_connections(self, use_transactions, silent, synchronous): # {{{3
    self.logger.debug("Using data files %r and %r.", self.metastore_file, self.datastore_file)
#    self.db = Db(self.metastore_file, use_transactions, synchronous)
    self.db = MysqlDb('localhost','dbfs','root', 'BISKVIT')
#    self.blocks = self.db.blocks()
#    self.conn = self.db.conn()
   

  def __check_data_file(self, pathname, silent): # {{{3
    pathname = os.path.expanduser(pathname)
    if os.access(pathname, os.F_OK):
      # If the datafile already exists make sure it's readable,
      # otherwise the file system would be completely unusable.
      if not os.access(pathname, os.R_OK):
        self.logger.critical("Error: Datafile %r exists but isn't readable!", pathname)
        os._exit(1)
      # Check and respect whether the datafile is writable (e.g. when it was
      # created by root but is currently being accessed by another user).
      if not os.access(pathname, os.W_OK):
        if not silent:
          self.logger.warning("File %r exists but isn't writable! Switching to read only mode.", pathname)
        self.read_only = True
    return pathname

  def __log_call(self, fun, msg, *args): # {{{3
    # To disable all __log_call() invocations:
    #  :%s/^\(\s\+\)\(self\.__log_call\)/\1#\2
    # To re enable them:
    #  :%s/^\(\s\+\)#\(self\.__log_call\)/\1\2
    if self.calls_log_filter == [] or fun in self.calls_log_filter:
      self.logger.debug(msg, *args)

  def __get_opts_from_db(self, options): # {{{3
    for name, value in self.db.get_options():
      if name == 'synchronous':
        self.synchronous = int(value) != 0
        # If the user passed --nosync, override the value stored in the database.
        if not options.synchronous:
          self.synchronous = False
      elif name == 'block_size' and int(value) != self.block_size:
        self.logger.warning("Ignoring --block-size=%i argument, using previously chosen block size %i instead", self.block_size, int(value))
        self.block_size = int(value)
      elif name == 'compression_method' and value != self.compression_method:
        if self.compression_method != 'none':
          self.logger.warning("Ignoring --compress=%s argument, using previously chosen compression method %r instead", self.compression_method, value)
        self.compression_method = value
      elif name == 'hash_function' and value != self.hash_function:
        self.logger.warning("Ignoring --hash=%s argument, using previously chosen hash function %r instead", self.hash_function, value)
        self.hash_function = value

  def __select_compress_method(self, options, silent): # {{{3
    valid_formats = self.compressors.keys()
    selected_format = self.compression_method.lower()
    if selected_format not in valid_formats:
      self.logger.warning("Invalid compression format `%s' selected!", selected_format)
      selected_format = 'none'
    if selected_format != 'none':
      if not silent:
        self.logger.debug("Using the %s compression method.", selected_format)
      # My custom LZO binding defines set_block_size() which enables
      # optimizations like preallocating a buffer that can be reused for
      # every call to compress() and decompress().
      if selected_format == 'lzo':
        module = __import__('lzo')
        if hasattr(module, 'set_block_size'):
          module.set_block_size(self.block_size)
    self.compress, self.decompress = self.compressors[selected_format]

  def __write_blocks(self, inode, buf, apparent_size): # {{{3
    start_time = time.time()
    # Delete existing index entries for file.
    self.db.clear_index(inode)
    
    # Store any changed blocks and rebuild the file index.
    storage_size = len(buf)
    for block_nr in xrange(int(math.ceil(storage_size / float(self.block_size)))):
      buf.seek(self.block_size * block_nr, os.SEEK_SET)
      new_block = buf.read(self.block_size)
      digest = self.__hash(new_block)
      encoded_digest = sqlite3.Binary(digest)
      row = self.db.get_by_hash(encoded_digest)
      if row:
        hash_id = row[0]
        existing_block = self.decompress(self.db.get_data(digest))
        # Check for hash collisions.
        if new_block != existing_block:
          # Found a hash collision: dump debugging info and exit.
          dumpfile_collision = '/tmp/dedupfs-collision-%i' % time.time()
          handle = open(dumpfile_collision, 'w')
          handle.write('Content of existing block is %r.\n' % existing_block)
          handle.write('Content of new block is %r.\n' % new_block)
          handle.close()
          self.logger.critical(
              "Found a hash collision on block number %i of inode %i!\n" + \
              "The existing block is %i bytes and hashes to %s.\n"   + \
              "The new block is %i bytes and hashes to %s.\n"        + \
              "Saved existing and conflicting data blocks to %r.",
              block_nr, inode, len(existing_block), digest,
              len(new_block), digest, dumpfile_collision)
          os._exit(1)
          self.db.add_hash_to_index(inode, hash_id, block_nr)
      else:
        self.db.set_data(digest, self.compress(new_block))
        
        hash_id = self.db.add_hash(encoded_digest);
        self.db.add_hash_to_index(inode, hash_id, block_nr)
        # Check that the data was properly stored in the database?
        self.__verify_write(new_block, digest, block_nr, inode)
      block_nr += 1
    # Update file size and last modified time.
    self.db.update_inode_size(inode, apparent_size)
    self.time_spent_writing_blocks += time.time() - start_time

  def __insert(self, ctx, path, mode, size, rdev=0):
    parent, name = os.path.split(path)
    parent_id, parent_ino = self.__path2keys(parent)
    nlinks = mode & stat.S_IFDIR and 2 or 1
    t = time.time()
    uid = ctx['uid']
    gid = ctx['gid']
    node_id, inode = self.db.insert_node_to_tree(name, parent_id, nlinks, mode, uid, gid, rdev, size, t)
#     self.__cache_set(path, (node_id, inode))
    return inode, parent_ino

  def __remove(self, path, check_empty=False): # {{{3
    node_id, inode = self.__path2keys(path)
    # Make sure directories are empty before deleting them to avoid orphaned inodes.
    if check_empty and self.db.count_of_children(inode) > 0:
      raise OSError, (errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY), path)
    self.__cache_set(path, None)
    self.db.remove_leaf(node_id, inode);
    # Inodes with nlinks = 0 are purged periodically from __collect_garbage() so
    # we don't have to do that here.
    mode, uid, gid = self.db.get_mode_uid_gid(inode)
    if mode & stat.S_IFDIR:
      parent_id, parent_ino = self.__path2keys(os.path.split(path)[0])
      self.db.dec_links(parent_ino)
      

  def __verify_write(self, block, digest, block_nr, inode): # {{{3
    if self.verify_writes:
      saved_value = self.decompress(self.db.get_data(digest))
      if saved_value != block:
        # The data block was corrupted when it was written or read.
        dumpfile_corruption = '/tmp/dedupfs-corruption-%i' % time.time()
        handle = open(dumpfile_corruption, 'w')
        handle.write('The content that should have been stored is %r.\n' % block)
        handle.write('The content that was retrieved from the database is %r.\n' % saved_value)
        handle.close()
        self.logger.critical(
            "Failed to verify data with block number %i of inode %i!\n" + \
            "Saved original and corrupted data blocks to %i.",
            block_nr, inode, dumpfile_corruption)
        os._exit(1)

  def __access(self, ctx, inode, flags): # {{{3
    # Check if the flags include writing while the database is read only.
    if self.read_only and flags & os.W_OK:
      return False
    inode_mode, inode_uid, inode_gid = self.db.get_mode_uid_gid(inode);
    # Determine by whom the request is being made.
    uid = ctx['uid']
    gid = ctx['gid']
    o = uid == inode_uid # access by same user id?
    g = gid == inode_gid and not o # access by same group id?
    # Note: "and not o" added after experimenting with EXT4.
    w = not (o or g) # anything else
    m = inode_mode
    # The essence of UNIX file permissions. Did I miss anything?! (Probably...)
    return (not (flags & os.R_OK) or ((o and (m & 0400)) or (g and (m & 0040)) or (w and (m & 0004)))) \
       and (not (flags & os.W_OK) or ((o and (m & 0200)) or (g and (m & 0020)) or (w and (m & 0002)))) \
       and (not (flags & os.X_OK) or ((o and (m & 0100)) or (g and (m & 0010)) or (w and (m & 0001))))

  def __path2keys(self, path): # {{{3
    node_id, inode = 1, 1
    if path == '/':
      return node_id, inode
    parent_id = node_id
    for segment in self.__split_segments(path):
      result = self.db.get_node_id_inode_by_parrent_and_name(parent_id, segment)
      if result == None:
#         self.__cache_check_gc()
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT), path)
      node_id, inode = result
      parent_id = node_id
#     self.__cache_check_gc()
    return node_id, inode

  def __cache_set(self, key, value):
    segments = self.__split_segments(key)
    last_segment = segments.pop(-1)
    node = self.cached_nodes
    time_now = time.time()
    for segment in segments:
      # Check that the keys of the sub path have been cached.
      if segment not in node:
        self.time_spent_caching_nodes += time.time() - time_now
        return False
      # Resolve the next path segment.
      node = node[segment]
      # Update the last used time of the sub path.
      node[self.__NODE_KEY_LAST_USED] = time_now
    if not value:
      # Delete the path's keys.
      if last_segment in node:
        del node[last_segment]
    elif last_segment not in node:
      # Create the path's keys.
      node[last_segment] = { self.__NODE_KEY_VALUE: value, self.__NODE_KEY_LAST_USED: time_now }
    else:
      # Update the path's keys.
      node = node[last_segment]
      node[self.__NODE_KEY_VALUE] = value
      node[self.__NODE_KEY_LAST_USED] = time_now
    self.__cache_check_gc()
    self.time_spent_caching_nodes += time.time() - time_now
    return True

  def __cache_check_gc(self): # {{{3
    self.cache_requests += 1
    if self.cache_requests >= 2500:
      time_now = time.time()
      if time_now - self.cache_gc_last_run >= self.cache_timeout:
        self.__cache_do_gc(self.cached_nodes, time_now)
        self.cache_gc_last_run = time_now
      self.cache_requests = 0

  def __cache_do_gc(self, node, time_now): # {{{3
    for key in node.keys():
      child = node[key]
      if isinstance(child, dict):
        last_used = time_now - child[self.__NODE_KEY_LAST_USED]
        if last_used > self.cache_timeout:
          del node[key]
        else:
          self.__cache_do_gc(child, time_now)

  def __split_segments(self, key): # {{{3
    return filter(None, key.split('/'))


  def __hash(self, data): # {{{3
    start_time = time.time()
    context = self.hash_function_impl()
    context.update(data)
    digest = context.digest()
    self.time_spent_hashing += time.time() - start_time
    return digest

  def __print_stats(self): # {{{3
    self.logger.info('-' * 79)
    self.__report_memory_usage()
    self.__report_throughput()
    self.__report_timings()

  def __report_timings(self): # {{{3
    if self.logger.isEnabledFor(logging.DEBUG):
      timings = [(self.time_spent_traversing_tree, 'Traversing the tree'),
                 (self.time_spent_caching_nodes, 'Caching tree nodes'),
                 (self.time_spent_interning, 'Interning path components'),
                 (self.time_spent_writing_blocks, 'Writing data blocks'),
                 (self.time_spent_hashing, 'Hashing data blocks'),
                 (self.time_spent_querying_tree, 'Querying the tree')]
      maxdescwidth = max([len(l) for t, l in timings]) + 3
      timings.sort(reverse=True)
      uptime = time.time() - self.fs_mounted_at
      printed_heading = False
      for timespan, description in timings:
        percentage = timespan / (uptime / 100)
        if percentage >= 1:
          if not printed_heading:
            self.logger.debug("Cumulative timings of slowest operations:")
            printed_heading = True
          self.logger.debug(" - %-*s%s (%i%%)" % (maxdescwidth, description + ':', format_timespan(timespan), percentage))

  def report_disk_usage(self): # {{{3
    disk_usage = self.db.get_disk_usage()
    disk_usage += os.stat(self.datastore_file).st_size
    apparent_size = self.db.get_used_space()
    self.logger.info("The total apparent size is %s while the databases take up %s (that's %.2f%%).",
        format_size(apparent_size), format_size(disk_usage), float(disk_usage) / (apparent_size / 100))

  def __report_memory_usage(self): # {{{3
    memory_usage = get_memory_usage()
    msg = "Current memory usage is " + format_size(memory_usage)
    difference = abs(memory_usage - self.memory_usage)
    if self.memory_usage != 0 and difference:
      direction = self.memory_usage < memory_usage and 'up' or 'down'
      msg += " (%s by %s)" % (direction, format_size(difference))
    self.logger.info(msg + '.')
    self.memory_usage = memory_usage

  def __report_throughput(self, nbytes=None, nseconds=None, label=None): # {{{3
    if nbytes == None:
      self.bytes_read, self.time_spent_reading = \
          self.__report_throughput(self.bytes_read, self.time_spent_reading, "read")
      self.bytes_written, self.time_spent_writing = \
          self.__report_throughput(self.bytes_written, self.time_spent_writing, "write")
    else:
      if nbytes > 0:
        average = format_size(nbytes / max(1, nseconds))
        self.logger.info("Average %s speed is %s/s.", label, average)
        # Decrease the influence of previous measurements over time?
        if nseconds > 60 and nbytes > 1024**2:
          return nbytes / 2, nseconds / 2
      return nbytes, nseconds

  def __report_top_blocks(self): # {{{3
    if self.logger.isEnabledFor(logging.DEBUG):
      printed_header = False
      for row in self.db.get_top_blocks():
        if not printed_header:
          self.logger.debug("A listing of the most used blocks follows:")
          printed_header = True
        msg = "Block #%s of %s has been used %i times: %r"
        preview = row['value']
        max_length = 60
        if len(preview) < max_length:
          preview = str(preview)
        else:
          preview = preview[0 : max_length] + '...'
        nbytes = format_size(len(row['value']))
        self.logger.debug(msg, row['hash_id'], nbytes, row['count'], preview)

  def __gc_hook(self, nested=False): # {{{3
    # Don't collect any garbage for nested calls.
    if not nested:
      # Don't call time.time() more than once every 500th FUSE call.
      self.opcount += 1
      if self.opcount % 500 == 0:
        # Every minute the other statistics are reported and garbage
        # collection is performed when it isn't disabled.
        if time.time() - self.gc_hook_last_run >= self.gc_interval:
          self.__collect_garbage()
          self.__print_stats()
          self.gc_hook_last_run = time.time()

  def __collect_garbage(self): # {{{3
    if self.gc_enabled and not self.read_only:
      start_time = time.time()
      self.logger.info("Performing garbage collection (this might take a while) ..")
      self.should_vacuum = False
      for method in self.__collect_strings, self.__collect_inodes, \
          self.__collect_indices, self.__collect_blocks, self.__vacuum_metastore:
        sub_start_time = time.time()
        msg = method()
        if msg:
          elapsed_time = time.time() - sub_start_time
          self.logger.info(msg, format_timespan(elapsed_time))
      elapsed_time = time.time() - start_time
      self.logger.info("Finished garbage collection in %s.", format_timespan(elapsed_time))

  def __collect_strings(self): # {{{4
    count = self.db.clean_strings()
    if count > 0:
      self.should_vacuum = True
      return "Cleaned up %i unused path segment%s in %%s." % (count, count != 1 and 's' or '')

  def __collect_inodes(self): # {{{4
    count = self.db.clean_inodes()
    if count > 0:
      self.should_vacuum = True
      return "Cleaned up %i unused inode%s in %%s." % (count, count != 1 and 's' or '')

  def __collect_indices(self): # {{{4
    count = self.db.clean_indices()
    if count > 0:
      self.should_vacuum = True
      return "Cleaned up %i unused index entr%s in %%s." % (count, count != 1 and 'ies' or 'y')

  def __collect_blocks(self): # {{{4
    should_reorganize = False
    for row in self.db.find_unused_hashes():
      digest = str(row[0]) 
      self.db.remove_data(digest)
      should_reorganize = True
    if should_reorganize:
      self.db.dbmcall('reorganize')
    count = self.db.clean_hashes() 
    if count > 0:
      self.should_vacuum = True
      return "Cleaned up %i unused data block%s in %%s." % (count, count != 1 and 's' or '')

  def __vacuum_metastore(self): # {{{4
    if self.should_vacuum:
      self.db.vacuum();
      return "Vacuumed SQLite metadata store in %s."
  
  def __get_file_buffer(self, path): # {{{3
    if path in self.buffers:
      return self.buffers[path]
    else:
      buf = Buffer()
      inode = self.__path2keys(path)[1]
      for row in self.db.list_hash(inode):
        # TODO Make the file system more robust against failure by doing
        # something sensible when self.blocks.has_key(digest) is false.
        digest = str(row[0])
        buf.write(self.decompress(self.db.get_data(digest)))
      self.buffers[path] = buf
      return buf

  def __except_to_status(self, method, exception, code=errno.ENOENT): # {{{3
    # Don't report ENOENT raised from getattr().
    if method != 'getattr' or code != errno.ENOENT:
      sys.stderr.write('%s\n' % ('-' * 50))
      sys.stderr.write("Caught exception in %s(): %s\n" % (method, exception))
      traceback.print_exc(file=sys.stderr)
      sys.stderr.write('%s\n' % ('-' * 50))
      sys.stderr.write("Returning %i\n" % -code)
      sys.stderr.flush()
    # Convert the exception to a FUSE error code.
    if isinstance(exception, OSError):
      return -exception.errno
    else:
      return -code

class Buffer: # {{{1

  """
  This class wraps cStringIO.StringIO with two additions: The __len__
  method and a dirty flag to determine whether a buffer has changed.
  """

  def __init__(self):
    self.buf = cStringIO.StringIO()
    self.dirty = False

  def __getattr__(self, attr, default=None):
    """ Delegate to the StringIO object. """
    return getattr(self.buf, attr, default)

  def __len__(self):
    """ Get the total size of the buffer in bytes. """
    position = self.buf.tell()
    self.buf.seek(0, os.SEEK_END)
    length = self.buf.tell()
    self.buf.seek(position, os.SEEK_SET)
    return length

  def truncate(self, *args):
    """ Truncate the file at the current position and set the dirty flag. """
    if len(self) > self.buf.tell():
      self.dirty = True
    return self.buf.truncate(*args)

  def write(self, *args):
    """ Write a string to the file and set the dirty flag. """
    self.dirty = True
    return self.buf.write(*args)







# Named tuples used to return complex objects to FUSE. {{{1

try:
  import collections
  Stat = collections.namedtuple('Stat', 'st_atime st_blksize st_blocks \
      st_ctime st_dev st_gid st_ino st_mode st_mtime st_nlink st_rdev \
      st_size st_uid')
  StatVFS = collections.namedtuple('StatVFS', 'f_bavail f_bfree f_blocks \
      f_bsize f_favail f_ffree f_files f_flag f_frsize f_namemax')
except ImportError:
  # Fall back to regular Python classes instead of named tuples.
  class __Struct:
    def __init__(self, **kw):
      for k, v in kw.iteritems():
        setattr(self, k, v)
  class Stat(__Struct): pass
  class StatVFS(__Struct): pass
