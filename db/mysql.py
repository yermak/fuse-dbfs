'''
Created on 26.02.2013

@author: YErmak
'''
import MySQLdb
import threading
import time
import json
import logging
import sys


class MysqlDb():
  def __init__(self, host, database, user, password):
    self.logger = logging.getLogger('fuse-dbfs.mysql')
    self.logger.setLevel(logging.DEBUG)
    self.logger.addHandler(logging.StreamHandler(sys.stdout))
    self.__db = MySQLdb.connect(host=host, user=user, passwd=password, db=database)
    self.__threadlocal = threading.local()
    self.__sql = self.load_sql();

  def sql(self, name):
    return self.__sql[name]

  def load_sql(self):
    with open('sql/sql.json') as data_file:    
      data = json.load(data_file)
    self.logger.debug(data)
    return data

  def open_connection(self):
    self.__threadlocal.cursor=self.__db.cursor()

  def conn(self):
    return self.__threadlocal.cursor

  def close(self):
    self.__threadlocal.cursor.close()
    self.__threadlocal.cursor = None

  def execute_named_stmt(self, query_name, **kwargs):
    query = self.sql(query_name)
    self.logger.debug('Executing stmt: %s' , query)
    if kwargs:
      self.logger.debug('With parameters:')
      for key in kwargs:
        self.logger.debug('\t%s: %s' %(key, kwargs[key]))
#    if kwargs:
#      self.logger.debug('Prepared query: %s',  query.format(kwargs))
    self.conn().execute(query, kwargs)
    last_id = self.conn().lastrowid
    if last_id:
      self.logger.debug('Inserted id: %s', last_id)
      return last_id
    return self.conn().rowcount

  def execute_named_query(self, query_name, limit=0, **kwargs):
    query = self.sql(query_name)
    self.logger.debug('Executing query: %s' , query)
    if kwargs:
      self.logger.debug('With parameters:')
      for key in kwargs:
        self.logger.debug('\t%s: %s' %(key, kwargs[key]))
#    if kwargs:
#      self.logger.debug('Prepared query: %s',  query.format(kwargs))
    count = self.conn().execute(query, kwargs)
    if count == 0:
      return None;
    else:
      if limit==0:
        return self.conn().fetchall()
      elif limit==1:
        return self.conn().fetchone()
      else:
        return self.conn().fetchall()[0:limit]

  def initialize(self, uid, gid, root_mode):
    t = time.time()
    self.execute_named_stmt('create_tree')
    self.execute_named_stmt('create_strings')
    self.execute_named_stmt('create_inodes')
    self.execute_named_stmt('create_links')
    self.execute_named_stmt('create_hashes')
    self.execute_named_stmt('create_indices')
    self.execute_named_stmt('create_options')
    string_id = self.execute_named_stmt('insert_string', string='')
    inode_id = self.execute_named_stmt('insert_inode', nlinks=2, mode=root_mode, uid=uid, gid=gid,rdev=0, size=1024*4, time=t)
    self.execute_named_stmt('insert_tree_item', parent_id=None, string_id=string_id, inode_id=inode_id)

  def update_mode(self, mode, inode):
    self.execute_named_stmt('update_inode_mode', mode=mode, inode=inode)

  def update_uid_gid(self, uid, gid, inode):
    self.execute_named_stmt('update_inode_uid_gid', uid=uid, gid=gid, inode=inode)

  def add_leaf(self, link_parent_id, string_id, target_ino):
    return self.execute_named_stmt('insert_tree_item', parent_id=link_parent_id,  string_id=string_id, inode_id=target_ino)

  def remove_leaf(self, node_id, inode):
    self.execute_named_stmt('delete_tree_item', id=node_id)
    self.execute_named_stmt('dec_inode_nlinks', inode=inode)
  
  def inc_links(self, target_ino):
    self.execute_named_stmt('inc_inode_nlinks', inode=target_ino)

  def dec_links(self, parent_ino):
    self.execute_named_stmt('dec_inode_nlinks', inode=parent_ino)

  def list_childs(self,node_id):
    return self.execute_named_query('query_nodes_names', node_id = node_id)

  def get_target(self, inode):
    return str(self.execute_named_query('query_link_target', limit=1, inode=inode)[0])

  def insert_node_to_tree(self, name, parent_id, nlinks, mode, uid, gid, rdev, size, t):
    inode = self.execute_named_stmt('insert_inode', nlinks=nlinks, mode=mode, uid=uid, gid=gid, rdev=rdev, size=size, time=t)
    string_id = self.get_string_id_by_name(name)
    node_id = self.execute_named_stmt('insert_tree_item', parent_id=parent_id, string_id=string_id, inode_id=inode)
    return node_id, inode

  def get_string_id_by_name(self, string):
    result = self.execute_named_query('query_string_id', limit=1, string=string)
    if not result:
      result = self.execute_named_stmt('insert_string', string=string)
      return int(result)
    else:
      return result[0]

  # Get the path's mode, owner and group through the inode.
  def get_mode_uid_gid(self, inode):
    result = self.execute_named_query('query_inode_mode_uid_gid', limit=1, inode=inode)
    return result[0], result[1], result[2]

  def get_options(self):
    return self.execute_named_query('query_options')

  def get_by_hash(self, encoded_digest):
    return self.execute_named_query('query_hash_id', limit=1, hash= encoded_digest)

  def add_hash_to_index(self, inode, hash_id, block_nr):
    self.execute_named_stmt('insert_index', inode=inode, hash_id=hash_id, block_nr=block_nr)

  def add_hash(self, encoded_digest):
    return self.execute_named_stmt('insert_hash', hash = encoded_digest)

  def add_link(self, inode, target_path):
    self.execute_named_stmt('insert_link', inode=inode, target=target_path)

  def update_inode_size(self, inode, size):
    self.execute_named_stmt('update_inode_size', size= size, mtime=time.time(), inode=inode)

  def count_of_children(self, inode):
    return self.execute_named_query('query_inode_children_count', limit = 1, parent_id=inode)[0]

  def clear_index(self, inode, block_nr = -1):
    self.execute_named_stmt('delete_indices_by_node_and_block_nr', inode=inode, block_nr=block_nr)

  def update_time(self, inode, atime, mtime):
    self.execute_named_stmt('update_inode_time', inode=inode, atime=atime, mtime=mtime)

  def clean_strings(self):
    return self.execute_named_stmt('delete_strings')

  def clean_inodes(self):
    return self.execute_named_stmt('delete_inodes')

  def clean_indices(self):
    return self.execute_named_stmt('delete_indices')

  def find_unused_hashes(self):
    return self.execute_named_query('query_hashes_unused')

  def clean_hashes(self):
    return self.execute_named_stmt('delete_hashes')

  def list_hash(self, inode):
    return self.execute_named_query('query_hashes_by_inode',inode=inode)

  def get_used_space(self):
    return self.execute_named_query('query_used_space', limit=1)

  def get_disk_usage(self):
    return 0
    #TODO
    #return self.__conn.execute('PRAGMA page_size').fetchone()[0] * self.__conn.execute('PRAGMA page_count').fetchone()[0]

  def gett_attr(self, inode):
    return self.execute_named_query('query_inode_attr', limit=1, inode=inode)

  def get_node_id_inode_by_parrent_and_name(self, parent_id, name):
    return self.execute_named_query('query_inode_by_parent_and_name', limit=1, parent_id=parent_id, name=name)

  def get_top_blocks(self):
    return self.execute_named_query('query_top_blocks')

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
    return None

  def set_data(self, digest, new_block):
    pass

  def remove_data(self, digest):
    pass
    
  def dbmcall(self, fun): # {{{3
    # I simply cannot find any freakin' documentation on the type of objects
    # returned by anydbm and gdbm, so cannot verify that any single method will
    # always be there, although most seem to...
    if hasattr(self.__blocks, fun):
      getattr(self.__blocks, fun)()

  def commit(self, nested=False):
    if not nested:
      self.__db.commit()
#      self.conn().commit()
  
  def rollback(self, nested=False):
    if not nested:
      self.logger.info('Rolling back changes')
      self.__db.rollback()
#      self.conn().rollback()
 
  def vacuum(self):
    self.__conn.execute('VACUUM')
