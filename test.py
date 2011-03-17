#!/usr/bin/env python
import random
import unittest
import shutil
import os
from cachefs import FileDataCache, CacheMiss, create_db, open_db
cache_base = ".test_dir"

try:
    shutil.rmtree(cache_base)
except OSError:
    pass

os.mkdir(cache_base)

db = create_db(cache_base)

class TestFileDataCache(unittest.TestCase):
    #decorator to give tests a cache object
    def setUp(self):
        self.db = db
        open(os.path.join(cache_base,self._testMethodName), 'a+').close()
        self.cache = FileDataCache(self.db, os.path.join(cache_base, 'cache.db'), os.path.join(cache_base,self._testMethodName))

    def assertData(self, data, offset = 0):
        self.assertEqual(self.cache.read(len(data), offset), data)
        
    
    def test_multi_update_read(self):
        data = bytes([1, 2, 3, 4, 5])
        self.cache.update(data, 0)
        self.cache.update(data, len(data))

        try:
            self.assertData(data, 0)
            self.assertData(data, len(data))
        except:
            print "\nData: %s\nlen: %d" % (data, len(data))
            print "olb: ", self.cache.__overlapping_block__(len(data))
            self.cache.report()
            raise

    def test_simple_miss(self):
        self.assertRaises(CacheMiss, 
                          self.cache.read, 1, 0)

    
    def test_not_enough_data_mss(self):
        data = bytes(range(10))
        self.cache.update(data, 0)

        self.assertRaises(CacheMiss, 
                          self.cache.read, 2*len(data), 0)
        
    
    def test_inner_read(self):
        data = bytes(range(10))
        self.cache.update(data, 0)
        
        self.assertData(data[1:], 1)

    
    def test_sparce_file(self):
        data = bytes(b'1234567890')
        seek_to = 1000000000000
        self.cache.update(data, seek_to)
        
        self.cache.open()
        self.cache.cache.flush()

        st = os.stat(self.cache.cache.name)
        self.cache.close()

        self.assertTrue( seek_to > st.st_blocks * st.st_blksize)


    
    def test_add_block_1(self):
        data1 =      b'1234567890'
        data2 = b'1234567890'
        result= b'123456789067890'
        
        self.cache.update(data1, 10)
        self.cache.update(data2, 5)

        self.assertTrue(len(self.cache.known_offsets()) == 1)
        self.assertTrue(self.cache.known_offsets()[5] == 15)

        self.assertTrue(self.cache.read(15, 5) == result)

    
    def test_add_block_2(self):
        data1 =      b'1234567890'
        data2 = b'12345678901234567890'
        result= b'12345678901234567890'
        
        self.cache.update(data1, 10)
        self.cache.update(data2, 5)

        self.assertTrue(len(self.cache.known_offsets()) == 1)
        self.assertTrue(self.cache.known_offsets()[5] == len(result))

        self.assertTrue(self.cache.read(len(result), 5) == result)

    
    def test_add_block_3(self):
        data1 =      b'1234567890'
        data2 = b'12345'
        result= b'123451234567890'
        
        self.cache.update(data1, 10)
        self.cache.update(data2, 5)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 1)
            self.assertTrue(self.cache.known_offsets()[5] == len(result))
            
            self.assertTrue(self.cache.read(len(result), 5) == result)
        except:
            self.cache.report()
            raise

    
    def test_add_block_4(self):
        data1 =       b'1234567890'
        data2 = b'12345'
        results= ((b'12345', 5),(b'1234567890', 11))
        
        self.cache.update(data1, 11)
        self.cache.update(data2, 5)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 2)
            for result, offset in results: 
                self.assertTrue(self.cache.known_offsets()[offset] == len(result))
                
                self.assertTrue(self.cache.read(len(result), offset) == result)
        except:
            self.cache.report()
            raise

    
    def test_add_block_6(self):
        data1 = b'1234567890'
        data2 = b'54321'
        result= b'5432167890'
        
        self.cache.update(data1, 0)
        self.cache.update(data2, 0)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 1)
            self.assertTrue(self.cache.known_offsets()[0] == len(result))
            
            self.assertTrue(self.cache.read(len(result), 0) == result)
        except:
            self.cache.report()
            raise

    
    def test_add_block_7(self):
        data1 = b'1234567890'
        data2 =    b'54321'
        result= b'1235432190'
        
        self.cache.update(data1, 0)
        self.cache.update(data2, 3)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 1)
            self.assertTrue(self.cache.known_offsets()[0] == len(result))
            
            self.assertTrue(self.cache.read(len(result), 0) == result)
        except:
            self.cache.report()
            raise

    
    def test_add_block_8(self):
        data1 = b'1234567890'
        data2 =      b'54321'
        result= b'1234554321'
        
        self.cache.update(data1, 0)
        self.cache.update(data2, 5)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 1)
            self.assertTrue(self.cache.known_offsets()[0] == len(result))
            
            self.assertTrue(self.cache.read(len(result), 0) == result)
        except:
            self.cache.report()
            raise

    
    def test_add_block_9(self):
        data1 = b'1234567890'
        data2 =                  b'54321'
        data3 =           b'54321'
        
        results= ((b'123456789054321', 0),(b'54321', 17))
        
        
        self.cache.update(data1, 0)
        self.cache.update(data2, 17)
        self.cache.update(data3, 10)
        try:
            self.assertTrue(len(self.cache.known_offsets()) == 2)
            for result, offset in results: 
                self.assertTrue(self.cache.known_offsets()[offset] == len(result))
                
                self.assertTrue(self.cache.read(len(result), offset) == result)
        except:
            self.cache.report()
            raise

    def cmp_bufs(self, buf1, buf2):
        if len(buf1) != len(buf2):
            return False

        for i in range(len(buf1)):
            if buf1[i] != buf2[i]:
                return False

        return True

    def verify_add_blocks(self, inputs, results, truncate = None):
        for space, bytes in inputs:
            self.cache.update(bytes, len(space))

        if truncate:
            self.cache.truncate(truncate)

        try:
            self.assertTrue(len(self.cache.known_offsets()) == len(results))
            
            for space, result in results.items():
                try:
                    offset = len(space)
                    self.assertTrue(self.cache.known_offsets()[offset] == len(result))
                    self.assertTrue(self.cmp_bufs(self.cache.read(len(result), 
                                                                  offset), 
                                                  result))
                except:
                    print "\n\nresult: %s, offset: %d, len: %d" % (result,
                                                                   offset, 
                                                                   len(result))
                    print "buffer: %s\n" % self.cache.read(len(result), offset)
                    print self.cmp_bufs(self.cache.read(len(result), offset), 
                                        result)
                    raise
        except:
            self.cache.report()
            raise
        
    
    def test_add_block_10(self):
        inputs = (('', b'1234567890'),
                  ('          ', b'54321'),
                  ('                 ', b'54321'))
        
        results= {'': b'123456789054321',
                  '                 ': b'54321'}
        
        self.verify_add_blocks(inputs, results)

    
    def test_add_block_11(self):
        inputs = (('', b'54321'),
                  ('               ', b'54321'),
                  ('     ', b'1234567890'))
        
        results= {'': b'54321123456789054321'}

        self.verify_add_blocks(inputs, results)

    
    def test_add_block_12(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'),
                  ('    ', b'1234567890'))
        
        results= {'':  b'543212345678904321'}
        
        self.verify_add_blocks(inputs, results)

    
    def test_add_block_13(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'),
                  ('    ', b'12345678901234567890'))
        
        results= {'': b'543212345678901234567890'}
        
        self.verify_add_blocks(inputs, results)

    
    def test_add_truncate_1(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'),
                  ('    ', b'12345678901234567890'))
        truncate = len('            ')    
        results = {'': b'543212345678'}
        
        self.verify_add_blocks(inputs, results, truncate)

    
    def test_add_truncate_2(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'))
        truncate =  len('      ')    
        results = {'': b'54321'}

    
    def test_add_truncate_3(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'))
        truncate =  len('              ')    
        results = {'': b'54321',
                   '             ': b'5'}
    
    def test_add_truncate_4(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'))
        truncate =  len('                  ')    
        results = {'': b'54321',
                   '             ': b'54321'}
        
        
        self.verify_add_blocks(inputs, results, truncate)

    
    def test_perf(self):
        inputs = (('', b'54321'),
                  ('             ', b'54321'),
                  ('    ', b'1234567890'))
        
        results= {'':  b'543212345678904321'}
        
        self.verify_add_blocks(inputs, results)

        for i in range(1000):
            self.cache.read(0, 10)

if __name__ == '__main__':
#    import cProfile
#    cProfile.run('unittest.main()')
    unittest.main()

