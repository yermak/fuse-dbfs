'''
Created on 03.03.2013

@author: YErmak
'''
import unittest
import mysql
import logging
import sys
import json

class Test(unittest.TestCase):


    def setUp(self):
      self.logger = logging.getLogger('fuse-dbfs.main')
      self.logger.setLevel(logging.DEBUG)
      self.logger.addHandler(logging.StreamHandler(sys.stdout))
#      fh = logging.FileHandler('fuse.log')
#      fh.setLevel(logging.DEBUG)



    def tearDown(self):
        pass


    def testMysql(self):
        mysqlDb = mysql.MysqlDb('localhost','dbfs','root', 'BISKVIT')
        self.logger.debug("Created MySQL server")
        mysqlDb.open_connection()                  
        mysqlDb.initialize(1, 2, 3)
        
        
        
#        mysqlDb.conn().execute('select * from test')
#        result = mysqlDb.conn().fetchall()
#        self.logger.debug(result)
#        mysql.
      
          


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()