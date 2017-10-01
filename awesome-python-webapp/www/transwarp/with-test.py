#!/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'cabbageGG li'

import functools

class Demo(object):
    def __enter__(self):
        print 'enter'

    def __exit__(self,exctype, excvalue,traceback):
        print 'exit'

def with_test(func):
    #@functools.wraps(func)
    def _wrapper(*args, **kw):
        with Demo():
            return func(*args, **kw)
    return _wrapper

@with_test
def test():
    print 'test'

if __name__ == '__main__':
    print 'test begin'
    test()
