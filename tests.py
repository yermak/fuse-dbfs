'''
Created on Feb 28, 2013

@author: yermak
'''
import unittest
import json
from dbfs import Dbfs

class Test(unittest.TestCase):

  def setUp(self):
    self.fs = Dbfs();
    self.fs.db.open_connection()

    self.fs.db.conn().execute('drop table if exists options')
    self.fs.db.conn().execute('drop table if exists indices')
    self.fs.db.conn().execute('drop table if exists hashes')
    self.fs.db.conn().execute('drop table if exists links')
    self.fs.db.conn().execute('drop table if exists inodes')
    self.fs.db.conn().execute('drop table if exists tree')
    self.fs.db.conn().execute('drop table if exists strings')
  
    self.fs.db.initialize(1, 2, 0)
    self.fs.db.commit()

  def tearDown(self):
    pass
    
#  SELECT t.id, t.inode FROM tree t, strings s WHERE t.parent_id = %(parent_id) AND t.name = s.id AND s.string = %(name) LIMIT 1   
  def test_mkdir_access_normal(self):
    self.fs.mkdir({'uid':1, 'gid':2},'first', 777)
    access = self.fs.access({'uid':1, 'gid':2}, 'first', 0)
    self.assertEqual(0, access, 'Access failed')
    
    
  def test_mkdir_access_nested(self):    
    mkdir = self.fs.mkdir({'uid':1, 'gid':2},"parent_dir", 777)
    mkdir = self.fs.mkdir({'uid':1, 'gid':2},"parent_dir/child_dir", 777)
    self.assertEqual(0, mkdir, 'mkdir failed')
    access = self.fs.access({'uid':1, 'gid':2}, "parent_dir/child_dir", 0)
    self.assertEqual(0, access, 'Access failed')
    
    
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()