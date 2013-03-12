'''
Created on 26.02.2013

@author: YErmak
'''
import MySQLdb
`import threading
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
    pass
  
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


  def execute_named_query(self, query_name, limit=0, **kwargs):
    query = self.sql(query_name)
    self.logger.debug('Executing query: %s' , query)
    if kwargs:
      self.logger.debug('With parameters:')
      for key in kwargs:
        self.logger.debug('\t%s: %s' %(key, kwargs[key]))
#    if kwargs:
#      self.logger.debug('Prepared query: %s',  query.format(kwargs))
    if not limit:
      return self.conn().execute(query, kwargs).fetchall()
    elif limit=1:
      return self.conn().execute(query, kwargs).fetchone()
    else:
      return self.conn().execute(query, kwargs).fetchall()[0:limit]

  
  def initialize(self, uid, gid, root_mode):
    t = time.time()
    self.execute_named_stmt('create_tree')
    self.execute_named_stmt('create_strings')
    self.execute_named_stmt('create_inodes')
    self.execute_named_stmt('create_links')
    self.execute_named_stmt('create_hashes')
    self.execute_named_stmt('create_indices')
    self.execute_named_stmt('create_options')
    
    string_id = self.execute_named_stmt('insert_string_root')
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
    return str(self.__conn.execute('SELECT target FROM links WHERE inode = ?', (inode)).fetchone()[0])

  
  def insert_node_to_tree(self, name, parent_id, nlinks, mode, uid, gid, rdev, size, t):
    inode = self.execute_named_stmt('insert_inode', nlinks=nlinks, mode=mode, uid=uid, gid=gid, rdev=rdev, size=size, time=t, time=t, time=t))
    string_id = self.get_node_by_name(name)
    node_id = self.execute_named_stmt('insert_tree_item', parent_id=parent_id, string_id=string_id, inode=inode)
    return node_id, inode

  
  def get_node_by_name(self, string):
    start_time = time.time()
    args = (string,)
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
    return self.execute_named_query('query_options')

  
  def get_by_hash(self, encoded_digest):
    return self.execute_named_query('query_hash_id', limit=1, hash= encoded_digest)

  
  def add_hash_to_index(self, inode, hash_id, block_nr):
    self.execute_named_stmt('insert_index', inode=inode, hash_id=hash_id, block_nr=block_nr)

  
  def add_hash(self, encoded_digest):
    return self.execute_named_stmt('insert_hash', hash = encoded_digest)
  
  def add_link(self, inode, target_path):
    self.__conn.execute('insert_link', inode=inode, target=target_path)

  
  def update_inode_size(self, inode, size):
    self.__conn.execute('UPDATE inodes SET size = ?, mtime=? WHERE inode = ?', (size, time.time(), inode))

  
  def count_of_children(self, inode):
    query = 'SELECT COUNT(t.id) FROM tree t, inodes i WHERE t.parent_id = ? AND i.inode = t.inode AND i.nlinks > 0'
    self.__conn.execute(query, inode).fetchone()[0]

  
  def clear_index(self, inode, block_nr = -1):
    self.__conn.execute('DELETE FROM indices WHERE inode = ? and block_nr > ?', (inode, block_nr))

  
  def update_time(self, inode, atime, mtime):
    self.execute_named_stmt('update_inode_time', inode=inode, atime=atime, mtime=mtime)

  
  def clean_strings(self):
    return self.__conn.execute('DELETE FROM strings WHERE id NOT IN (SELECT name FROM tree)').rowcount

  
  def clean_inodes(self):
    return self.__conn.execute('DELETE FROM inodes WHERE nlinks = 0').rowcount

  
  def clean_indices(self):
    return self.__conn.execute('DELETE FROM indices WHERE inode NOT IN (SELECT inode FROM inodes)').rowcount

  
  def find_unused_hashes(self):
    return self.__conn.execute('SELECT hash FROM hashes WHERE id NOT IN (SELECT hash_id FROM indices)')

  
  def clean_hashes(self):
    return self.__conn.execute('DELETE FROM hashes WHERE id NOT IN (SELECT hash_id FROM indices)').rowcount

  
  def list_hash(self, inode):
    query = 'SELECT h.hash FROM hashes h, indices i WHERE i.inode = ? AND h.id = i.hash_id  ORDER BY i.block_nr ASC'
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
    return self.__conn.execute(query, (parent_id, name)).fetchone()

  
  def get_top_blocks(self):
    query = """
      SELECT * FROM (
        SELECT *, COUNT(*) AS "count" FROM indices
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
    if self.use_transactions and not nested:
      self.__conn.commit()
  
  def rollback_(self, nested=False):
    if self.use_transactions and not nested:
      self.logger.info('Rolling back changes')
      self.__conn.rollback()
 
  def vacuum(self):
    self.__conn.execute('VACUUM')
  
 
