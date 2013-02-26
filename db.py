'''
Created on Feb 25, 2013

@author: yermak
'''

import sys

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
except ImportError, e:
  msg = "Error: Failed to load one of the required Python modules! (%s)\n"
  sys.stderr.write(msg % str(e))
  sys.exit(1)

class Db:
  def __init__(self, metastore_file, use_transactions, synchronous ):
    self.use_transactions = use_transactions
    try:
      # Initialize a Logger() object to handle logging.
      self.logger = logging.getLogger('fuse-dbfs.db')
      self.logger.setLevel(logging.INFO)
      self.logger.addHandler(logging.StreamHandler(sys.stderr))
      
      # Open the key/value store containing the data blocks.
      if not os.path.exists(self.metastore_file):
        self.__blocks = self.__open_datastore(True)
      else:
        from whichdb import whichdb
        created_by_gdbm = whichdb(self.metastore_file) == 'gdbm'
        self.__blocks = self.__open_datastore(created_by_gdbm)
      
      # Open an SQLite database connection with manual transaction management. 
      self.__conn = sqlite3.connect(metastore_file, isolation_level=None)
      # Use the built in row factory to enable named attributes.
      self.__conn.row_factory = sqlite3.Row
      # Return regular strings instead of Unicode objects.
      self.__conn.text_factory = str
      # Don't bother releasing any locks since there's currently no point in
      # having concurrent reading/writing of the file system database.
      self.__conn.execute('PRAGMA locking_mode = EXCLUSIVE')
      
      # Disable synchronous operation. This is supposed to make SQLite perform
      # MUCH better but it has to be enabled wit --nosync because you might
      # lose data when the file system isn't cleanly unmounted...
      if not synchronous and not self.read_only:
        self.logger.warning("Warning: Disabling synchronous operation, you might lose data..")
        self.conn.execute('PRAGMA synchronous = OFF')

      
    except ImportError, e:
      msg = "Error: Failed to load one of the required Python modules! (%s)\n"
      sys.stderr.write(msg % str(e))
      sys.exit(1)
    

  def blocks(self):
    return self.__blocks


  def conn(self):
    return self.__conn;    
  

  def initialize(self, uid, gid):
    t = time.time()
    self.__conn.executescript("""

      -- Create the required tables?
      CREATE TABLE IF NOT EXISTS tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name INTEGER NOT NULL, inode INTEGER NOT NULL, UNIQUE (parent_id, name));
      CREATE TABLE IF NOT EXISTS strings (id INTEGER PRIMARY KEY, value BLOB NOT NULL UNIQUE);
      CREATE TABLE IF NOT EXISTS inodes (inode INTEGER PRIMARY KEY, nlinks INTEGER NOT NULL, mode INTEGER NOT NULL, uid INTEGER, gid INTEGER, rdev INTEGER, size INTEGER, atime INTEGER, mtime INTEGER, ctime INTEGER);
      CREATE TABLE IF NOT EXISTS links (inode INTEGER UNIQUE, target BLOB NOT NULL);
      CREATE TABLE IF NOT EXISTS hashes (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE);
      CREATE TABLE IF NOT EXISTS "index" (inode INTEGER, hash_id INTEGER, block_nr INTEGER, PRIMARY KEY (inode, hash_id, block_nr));
      CREATE TABLE IF NOT EXISTS options (name TEXT PRIMARY KEY, value TEXT NOT NULL);

      -- Create the root node of the file system?
      INSERT OR IGNORE INTO strings (id, value) VALUES (1, '');
      INSERT OR IGNORE INTO tree (id, parent_id, name, inode) VALUES (1, NULL, 1, 1);
      INSERT OR IGNORE INTO inodes (nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime) VALUES (2, %i, %i, %i, 0, 1024*4, %f, %f, %f);

      -- Save the command line options used to initialize the database?
      INSERT OR IGNORE INTO options (name, value) VALUES ('synchronous', %i);
      INSERT OR IGNORE INTO options (name, value) VALUES ('block_size', %i);
      INSERT OR IGNORE INTO options (name, value) VALUES ('compression_method', %r);
      INSERT OR IGNORE INTO options (name, value) VALUES ('hash_function', %r);

    """ % (self.root_mode, uid, gid, t, t, t, self.synchronous and 1 or 0,
           self.block_size, self.compression_method, self.hash_function))


  
  def update_mode(self, mode, inode):
    self.__conn.execute('UPDATE inodes SET mode = ? WHERE inode = ?', (mode, inode))
  

  def update_uid_gid(self, uid, gid, inode):
    self.__conn.execute('UPDATE inodes SET uid = ?, gid = ? WHERE inode = ?', (uid, gid, inode))

    
  def add_leaf(self, link_parent_id, string_id, target_ino):
    self.__conn.execute('INSERT INTO tree (parent_id, name, inode) VALUES (?, ?, ?)', (link_parent_id, string_id, target_ino))
    return self.__conn.execute('SELECT last_insert_rowid()').fetchone()[0]

  def remove_leaf(self, node_id, inode):
    self.__conn.execute('DELETE FROM tree WHERE id = ?', (node_id,))
    self.__conn.execute('UPDATE inodes SET nlinks = nlinks - 1 WHERE inode = ?', (inode,))

  
  def inc_links(self, target_ino):
    self.__conn.execute('UPDATE inodes SET nlinks = nlinks + 1 WHERE inode = ?', (target_ino,))


  def dec_links(self, parent_ino):
    self.__conn.execute('UPDATE inodes SET nlinks = nlinks - 1 WHERE inode = ?', (parent_ino,))

  
  def list_childs(self,node_id):
    return self.__conn.execute('SELECT t.inode, s.value FROM tree t, strings s WHERE t.parent_id = ? AND t.name = s.id', (node_id,)).fetchall()

  
  def get_target(self, inode):
    return str(self.__conn.execute('SELECT target FROM links WHERE inode = ?', (inode)).fetchone()[0])

  
  def insert_node_to_tree(self, name, parent_id, nlinks, mode, uid, gid, rdev, size, t):
    self.__conn.execute('INSERT INTO inodes (nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                       (nlinks, mode, uid, gid, rdev, size, t, t, t))
    inode = self.__conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    string_id = self.get_node_by_name(name)
    self.__conn.execute('INSERT INTO tree (parent_id, name, inode) VALUES (?, ?, ?)', (parent_id, string_id, inode))
    node_id = self.__conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    return node_id, inode

  
  def get_node_by_name(self, string):
    start_time = time.time()
    args = (sqlite3.Binary(string),)
    result = self.__conn.execute('SELECT id FROM strings WHERE value = ?', args).fetchone()
    if not result:
      self.__conn.execute('INSERT INTO strings (id, value) VALUES (NULL, ?)', args)
      result = self.__conn.execute('SELECT last_insert_rowid()').fetchone()
    self.time_spent_interning += time.time() - start_time
    return int(result[0])

  # Get the path's mode, owner and group through the inode.
  def get_mode_uid_gid(self, inode):
    result = self.__conn.execute('SELECT mode, uid, gid FROM inodes WHERE inode = ?', (inode,)).fetchone()
    return result['mode'], result['uid'], result['gid']

  
  def get_options(self):
    return self.__conn.execute('SELECT name, value FROM options')

  
  def get_by_hash(self, encoded_digest):
    return self.__conn.execute('SELECT id FROM hashes WHERE hash = ?', (encoded_digest,)).fetchone()

  
  def add_hash_to_index(self, inode, hash_id, block_nr):
    self.__conn.execute('INSERT INTO "index" (inode, hash_id, block_nr) VALUES (?, ?, ?)', (inode, hash_id, block_nr))

  
  def add_hash(self, encoded_digest):
    self.__conn.execute('INSERT INTO hashes (id, hash) VALUES (NULL, ?)', (encoded_digest,))
    return self.__conn.execute('SELECT last_insert_rowid()').fetchone()[0]

  
  def add_link(self, inode, target_path):
    self.__conn.execute('INSERT INTO links (inode, target) VALUES (?, ?)', (inode, sqlite3.Binary(target_path)))

  
  def update_inode_size(self, inode, size):
    self.__conn.execute('UPDATE inodes SET size = ?, mtime=? WHERE inode = ?', (size, time.time(), inode))

  
  def count_of_children(self, inode):
    query = 'SELECT COUNT(t.id) FROM tree t, inodes i WHERE t.parent_id = ? AND i.inode = t.inode AND i.nlinks > 0'
    self.__conn.execute(query, inode).fetchone()[0]

  
  def clear_index(self, inode, block_nr = -1):
    self.__conn.execute('DELETE FROM "index" WHERE inode = ? and block_nr > ?', (inode, block_nr))

  
  def update_time(self, inode, atime, mtime):
    self.__conn.execute('UPDATE inodes SET atime = ?, mtime = ? WHERE inode = ?', (atime, mtime, inode))

  
  def clean_strings(self):
    return self.__conn.execute('DELETE FROM strings WHERE id NOT IN (SELECT name FROM tree)').rowcount

  
  def clean_inodes(self):
    return self.__conn.execute('DELETE FROM inodes WHERE nlinks = 0').rowcount

  
  def clean_indices(self):
    return self.__conn.execute('DELETE FROM "index" WHERE inode NOT IN (SELECT inode FROM inodes)').rowcount

  
  def find_unused_hashes(self):
    return self.__conn.execute('SELECT hash FROM hashes WHERE id NOT IN (SELECT hash_id FROM "index")')

  
  def clean_hashes(self):
    return self.__conn.execute('DELETE FROM hashes WHERE id NOT IN (SELECT hash_id FROM "index")').rowcount

  
  def list_hash(self, inode):
    query = 'SELECT h.hash FROM hashes h, "index" i WHERE i.inode = ? AND h.id = i.hash_id  ORDER BY i.block_nr ASC'
    return self.__conn.execute(query, (inode,)).fetchall()

  
  def get_used_space(self):
    return self.__conn.execute('SELECT SUM(inodes.size) FROM tree, inodes WHERE tree.inode = inodes.inode').fetchone()[0]

  
  def get_disk_usage(self):
    return self.__conn.execute('PRAGMA page_size').fetchone()[0] * self.__conn.execute('PRAGMA page_count').fetchone()[0]

  
  def gett_attr(self, inode):
    query = 'SELECT inode, nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime FROM inodes WHERE inode = ?'
    return self.__conn.execute(query, (inode,)).fetchone()

  
  def get_node_id_inode_by_parrent_and_name(self, parent_id, name):
    query = 'SELECT t.id, t.inode FROM tree t, strings s WHERE t.parent_id = ? AND t.name = s.id AND s.value = ? LIMIT 1'
    return self.__conn.execute(query, (parent_id, sqlite3.Binary(name))).fetchone()

  
  def get_top_blocks(self):
    query = """
      SELECT * FROM (
        SELECT *, COUNT(*) AS "count" FROM "index"
        GROUP BY hash_id ORDER BY "count" DESC
      ), hashes WHERE
        "count" > 1 AND
        hash_id = hashes.id
        LIMIT 10 """
    return self.__conn.execute(query)


  def __open_datastore(self, use_gdbm):
    # gdbm is preferred over other dbm implementations because it supports fast
    # vs. synchronous modes, however any other dedicated key/value store should
    # work just fine (albeit not as fast). Note though that existing key/value
    # stores are always accessed through the library that created them.
    mode = self.read_only and 'r' or 'c'
    if use_gdbm:
      try:
        import gdbm
        mode += self.synchronous and 's' or 'f'
        return gdbm.open(self.datastore_file, mode)
      except ImportError:
        pass
    import anydbm
    return anydbm.open(self.datastore_file, mode)

  def get_data(self, digest):
    return self.__blocks[digest]

  
  def set_data(self, digest, new_block):
    self.__blocks[digest] = self.compress(new_block)

  
  def remove_data(self, digest):
    del self.__blocks[digest]
    
  def dbmcall(self, fun): # {{{3
    # I simply cannot find any freakin' documentation on the type of objects
    # returned by anydbm and gdbm, so cannot verify that any single method will
    # always be there, although most seem to...
    if hasattr(self.__blocks, fun):
      getattr(self.__blocks, fun)()

  def commit(self, nested=False): # {{{3
    if self.use_transactions and not nested:
      self.__conn.commit()
  
  def rollback_(self, nested=False): # {{{3
    if self.use_transactions and not nested:
      self.logger.info('Rolling back changes')
      self.__conn.rollback()

  def close(self):
    self._conn.close()
    self.dbmcall('close')

  
  def vacuum(self):
    self.__conn.execute('VACUUM')
  
  
  
  
