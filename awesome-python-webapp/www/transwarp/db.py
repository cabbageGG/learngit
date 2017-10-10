#!/usr/bin/env python
# －*－coding:utf-8 -*-

__author__ = 'cabbageGG li'

import threading, logging, functools, time, uuid

class Dict(dict):
    '''
    封装dict 可以有a.c的形式 方便ORM模块
    example:
    >>>a = Dict(a=1,b=2,c=3)
    >>>a.c
    3
    >>>a = dict(a=1,b=2,c=3)
    >>>a.c
    error
     
    '''
    def __init__(self, names=(), values=(), **kw):
        super(Dict,self).__init__()
        for k, v in zip(names,values):
            self[k] = v
    
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' has no attribute %s" % key)
    
    def __setattr__(self, key, value):
        self[key] = value

def next_id(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

def _profiling(start, sql=''):  #打印事务时间，暂时先不管
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

class _LasyConnection(object):   # 封装了mysql连接的类
    def __init__(self):  
        self.connection=None  
  
    def cursor(self):  
        if self.connection is None:  
            connection=engine.connect()  
            logging.info('open connection <%s>...' % hex(id(connection)))  
            self.connection = connection    # 将引擎对象的mysql连接 赋给 connection
        return self.connection.cursor()  
  
    def commit(self):  
        self.connection.commit()  
  
    def rollback(self):  
        #print '================='  
        #print self.connection  
        self.connection.rollback()  
  
    def cleanup(self):    #cleanup : 就是关闭连接
        if self.connection:  
            connection = self.connection  
            self.connection=None  
            logging.info('colse connection <%s>...' %hex(id(connection)))  
            connection.close() 

#持有数据库连接的上下文对象
class _DbCtx(threading.local):  # threading.local 不懂 ？？应该是当前线程的全部局部数据 比如：engine对象
    def __init__(self):
        self.connection = None  #此处的连接不是真正的mysql连接，而是一个封装了mysql连接的类
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):    # 关闭连接
        self.connection.cleanup()
        self.connection = None

    def cursor(self):     # 获得 连接的 cursor
        return self.connection.cursor()

_db_ctx = _DbCtx()

engine = None

#数据库引擎对象
class _Engine(object):
    def __init__(self,connect):
        self._connect = connect
    def connect(self):
        return self._connect()
def create_engine(user='root', passwd='123456', db='test', host='127.0.0.1', port=3306 ,**kw):
    import MySQLdb
    global engine 
    if engine is not None:
        raise DBError('Engine is already initialized')
    params = dict(user=user, passwd=passwd, db=db, host=host, port=port)
    engine = _Engine(lambda:MySQLdb.connect(**params)) # **把params当作一个字典传入 why send a func return : 必须的，传递一个object
    logging.info("Init mysql engine <%s> ok" % hex(id(engine)))



#封装_Dbctx 对象 ，使得每次 mysql 操作 都能够 ，自动初始化连接 和 断开
class _ConnectionCtx(object):    
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self
    
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

#封装每次mysql操作－－－可以是一次sql操作如：select，update,delete 等, 也可以是多次sql操作
#使用修饰器，来实现这个功能－－－就是写出sql函数，在执行sql函数时，附带执行上面封装的_Dbctx对象的，自动连接和断开操作
def with_connection(func):
    '''
    定义修饰器,后面可以如下使用修饰器 。 ps：差不多可以说是一个模版声明
    @with_connection
    def func():
        pass
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():   # 表示自动执行 _ConnectionCtx 的enter和exit 函数
            return func(*args, **kw)
    return _wrapper

#事务类     
class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')

#封装每次事物操作－－－一般是多次sql操作
#使用修饰器，来实现这个功能－－－就是写出sql函数，在执行sql函数时，附带执行上面封装的_TransactionCtx对象的，自动连接和断开操作
def with_transaction(func):
    '''
    定义修饰器,后面可以如下使用修饰器 。 ps：差不多可以说是一个模版声明
    @with_transaction
    def func():
        pass
    '''
    @functools.wraps(func)
    def wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():   # 表示自动执行 _TransactionCtx 的enter和exit 函数
            return func(*args, **kw)
        _profiling(_start)
    return wrapper

def _select(sql, first, *args):  #原生的select语句，支持一条结果与多条结果的查询 返回的list结果封装成 dict
    global _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('sql is: %s , args is: %s' % (sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names,x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql,*args):
    '''
    返回单行查询结果
    '''
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    '''
    返回查询结果的行数
    '''
    d = _select(sql, False, *args)
    print d   # 需要测试
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]

@with_connection
def select(sql, *args):  
    '''
    默认返回全部查询结果的dict
    '''
    return _select(sql, False, *args)

@with_connection
def _update(sql,*args):     
    '''
    执行sql指令，insert, update 等 ，没有事务时，自动提交
    '''
    global _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('sql is: %s , args is %s' % (sql, args))
    try:             #注意：需要判断下，当前是否有事务在执行，没有的话，可以自动提交 
        cursor = _db_ctx.connection.cursor()
        #print args
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    '''
    插入一张表  #这条sql语句不太懂，需要测试
    '''
    cols, args = zip(*kw.iteritems())  
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    #print 'insert test sql: %s' % sql
    return _update(sql, *args)
def update(sql, *args):
    return _update(sql, *args)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine()
    print callable(update)
    # update('drop table if exists user')
    # update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    a = select('select * from user')
    print a
    print 'hello'
    u1 = dict(id=2001, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    insert('user', **u1)

