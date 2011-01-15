#!/usr/bin/env python
import random
import unittest
import shutil
import os
from cachefs import FileDataCache, CacheMiss

class TestFileDataCache(unittest.TestCase):

    #decorator to give tests a cache object
    def fdc(f):
        return lambda s: f.__call__(s, FileDataCache(s.cache_base, f.__name__))

    def assertData(self, cache, data, offset = 0):
        self.assertEqual(cache.read(len(data), offset), data)
        
    def setUp(self):
        self.cache_base = ".test_dir"
        try:
            shutil.rmtree(self.cache_base)
        except OSError:
            pass

    @fdc
    def test_simple_update_read(self, cache):
        cache.update("foo", 0)
        self.assertData(cache, "foo", 0)

    @fdc
    def test_multi_update_read(self, cache):
        data = bytes([1, 2, 3, 4, 5])
        cache.update(data, 0)
        cache.update(data, len(data))

        self.assertData(cache, data, 0)
        self.assertData(cache, data, len(data))

    @fdc
    def test_simple_miss(self, cache):
        self.assertRaises(CacheMiss, 
                          cache.read, cache, (1, 0))


    @fdc
    def test_not_enough_data_mss(self, cache):
        data = bytes(range(10))
        cache.update(data, 0)

        self.assertRaises(CacheMiss, 
                          cache.read, cache, (2*len(data), 0))
        
    @fdc
    def test_inner_read(self, cache):
        data = bytes(range(10))
        cache.update(data, 0)
        
        self.assertData(cache, data[1:], 1)

    @fdc
    def test_sparce_file(self, cache):
        data = bytes(range(10))
        seek_to = 1000000000000
        cache.update(data, seek_to)
        cache.cache.flush()
        st = os.stat(cache.cache.name)
        self.assertTrue( seek_to > st.st_blocks * st.st_blksize)

if __name__ == '__main__':
    unittest.main()

