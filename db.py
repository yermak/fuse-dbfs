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
  def __init__(self, metastore_file):
    try:
      # Open an SQLite database connection with manual transaction management. 
      self.__conn = sqlite3.connect(metastore_file, isolation_level=None)
      # Use the built in row factory to enable named attributes.
      self.__conn.row_factory = sqlite3.Row
      # Return regular strings instead of Unicode objects.
      self.__conn.text_factory = str
      # Don't bother releasing any locks since there's currently no point in
      # having concurrent reading/writing of the file system database.
      self.__conn.execute('PRAGMA locking_mode = EXCLUSIVE')
    except ImportError, e:
      msg = "Error: Failed to load one of the required Python modules! (%s)\n"
      sys.stderr.write(msg % str(e))
      sys.exit(1)
    

  def conn(self):
    return self.__conn;    
  
  
  def update_mode(self, mode, inode):
    self.__conn.execute('UPDATE inodes SET mode = ? WHERE inode = ?', (mode, inode))
  

  def update_uid_gid(self, uid, gid, inode):
    self.__conn.execute('UPDATE inodes SET uid = ?, gid = ? WHERE inode = ?', (uid, gid, inode))

    
  def add_leaf(self, link_parent_id, string_id, target_ino):
    self.__conn.execute('INSERT INTO tree (parent_id, name, inode) VALUES (?, ?, ?)', (link_parent_id, string_id, target_ino))
    return self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]

  
  def inc_links(self, target_ino):
    self.__conn.execute('UPDATE inodes SET nlinks = nlinks + 1 WHERE inode = ?', (target_ino,))

  
  def get_inode_mode(self,target_ino):
    return self.__conn.execute('SELECT mode FROM inodes WHERE inode = ?', target_ino).fetchone()[0]

  
  def list_childs(self,node_id):
    return self.__conn.execute('SELECT t.inode, s.value FROM tree t, strings s WHERE t.parent_id = ? AND t.name = s.id', (node_id,)).fetchall()

  
  def get_target(self, inode):
    return str(self.conn.execute('SELECT target FROM links WHERE inode = ?', (inode)).fetchone()[0])

  
  def insert_node_to_tree(self, name, parent_id, nlinks, mode, uid, gid, rdev, size, t):
    self.conn.execute('INSERT INTO inodes (nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
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
      result = self.conn.execute('SELECT last_insert_rowid()').fetchone()
    self.time_spent_interning += time.time() - start_time
    return int(result[0])

  # Get the path's mode, owner and group through the inode.
  def get_mode_uid_gid(self, inode):
    result = self.conn.execute('SELECT mode, uid, gid FROM inodes WHERE inode = ?', (inode,)).fetchone()
    return result['mode'], result['uid'], result['gid']

  
  
    
  
  
  
  
  
  
  
  
