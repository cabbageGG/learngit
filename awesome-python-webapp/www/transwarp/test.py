#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading, MySQLdb

def connect():
    conn = MySQLdb.connect(
        host='localhost',
        port= 3306,
        user='root',
        passwd='123456',
        db = 'test',
    )
    return conn

class User(object):
    def __init__(self):
        self.connection = connect()
        self.cursor = self.connection.cursor()
    
    def close(self):
        self.cursor.close()
        self.connection.commit()
        self.connection.close()
    
    def commit(self):
        self.connection.commit()

    def select_int(self,sql):
        return self.cursor.execute(sql)

    def select(self,sql):
        tables = self.select_int(sql)
        return self.cursor.fetchmany(tables)

    def select_one(self,sql,num):
        aa = self.select(sql)
        return aa[num]

    def update(self,sql,*args):
        self.cursor.execute(sql,args)
    
    def rollback(self):
        pass

class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict,self).__init__(**kw)
        for k, v in zip(names,values):
            self[k] = v
    
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r" 'Dict' has no attribute %s " % key )
    
    def __setattr__(self, key, value):
        print 'set'
        self[key] = value

    
if __name__ == '__main__':
    user1 = User()
    user2 = User()
    sql = 'select * from user'
    #re1 = user1.select_one(sql,0)
    re2 = user2.select(sql)
    user1.close()
    user2.close()
   # print re1
    for i in re2:
        print i

