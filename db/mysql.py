'''
Created on 26.02.2013

@author: YErmak
'''
import MySQLdb
import threading

class MysqlDB:
  def __init__(self, host, database, user, password):
    self.__db = MySQLdb.connect(host=host, user=user, passwd=password, db=database)
    self.__threadlocal = threading.local()
    pass
  
  def open_connection(self):
    self.__threadlocal.cursor=self._db.cursor()
  
  def conn(self):
    return self.__threadlocal.cursor
    
  def close(self):
    self.__threadlocal.cursor.close()
    self.__threadlocal.cursor = None
      

