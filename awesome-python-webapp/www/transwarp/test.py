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

    def update(self,sql):
        self.cursor.execute(sql)
    
    def rollback(self):
        pass
    
    
if __name__ == '__main__':
    user1 = User()
    user2 = User()
    sql = 'select * from student'
    re1 = user1.select_one(sql,1)
    re2 = user2.select(sql)
    user1.close()
    user2.close()
    print re1
    print re2 

