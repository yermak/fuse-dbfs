'''
Created on Feb 28, 2013

@author: yermak
'''
import unittest
import json
import db.mysql

class Test(unittest.TestCase):


    def setUp(self):
      pass


    def tearDown(self):
      pass


    def testName(self):
      pass
      
    def testJson(self):
        sql = db.mysql.load_sql('sql/query.json')
        self.assertEqual(sql, '1')
      
  
    def main(self):
      self.testJson()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()