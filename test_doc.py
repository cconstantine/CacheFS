#!/usr/bin/env python
import random
import unittest
import shutil
import os

import doc
cache_base = ".test_dir"

try:
    shutil.rmtree(cache_base)
except OSError:
    pass

os.mkdir(cache_base)


class TestDoc(unittest.TestCase):
    def setUp(self):
        self.doc = doc.doc(os.path.join(cache_base, self._testMethodName))
        self.dic = None
        self.db = self.doc.db

    def tearDown(self):
        if self.dic != None:
            self.assertDocEqual(self.doc, self.dic)

    def assertDocEqual(self, doc, dic):
        if ('keys' not in dir(doc)) or ('keys' not in dir(dic)):
            self.assertEqual(doc, dic)
            return

        self.assertEqual(set(doc.keys()), set(dic.keys()))
        
        for k in doc.keys():
            self.assertDocEqual(doc[k], dic[k])

    def test_simple_insert(self):
        self.doc['foo'] = '1'

        self.dic = {"foo" : '1'}

    def test_simple_multiple_insert(self):
        self.doc['foo0'] = '1'
        self.doc['foo1'] = '2'
        self.doc['foo2'] = '3'
        
        self.dic = {
            'foo0' : '1',
            'foo1' : '2',
            'foo2' : '3'
            }

    def test_simple_doc_insert(self):
        self.dic = {"foo" : 
                    {'bob': "bob"}}

        self.doc['foo'] = self.dic['foo']
        
    def test_simple_doc_single_insert(self):
        self.dic = {}
        self.dic["foo"] = {
            'bob1': "bob1",
            'bob2': "bob2",
            'bob3': "bob3"}

        self.doc['foo'] = self.dic['foo']

    def test_nested_doc_single_insert(self):
        self.dic = {}
        self.dic["foo"] = {
            'bob': {"1": 'bob1',
                    '2': "bob2",
                    '3': "bob3"}}

        self.doc['foo'] = self.dic['foo']

    def test_multi_doc_delete(self):
        self.dic = {}
        self.dic["foo1"] = {
            'bob': {"a1": 'abob1',
                    'a2': "abob2",
                    'a3': "abob3"}}
        self.dic["foo2"] = {
            'bob': {"1": 'bob1',
                    '2': "bob2",
                    '3': "bob3"}}

        self.doc['foo1'] = self.dic['foo1']
        self.doc['foo2'] = self.dic['foo2']

        del self.doc['foo1']['bob']
        del self.dic['foo1']['bob']

    def test_multi_doc(self):
        self.dic = {}
        self.dic["foo1"] = {
            'bob': {"a1": 'abob1',
                    'a2': "abob2",
                    'a3': "abob3"}}
        self.dic["foo2"] = {
            'bob': {"1": 'bob1',
                    '2': "bob2",
                    '3': "bob3"}}

        self.doc['foo1'] = self.dic['foo1']
        self.doc['foo2'] = self.dic['foo2']

    def test_path_insert(self):
        self.dic = {}
        self.doc["foo1/bob"] = {"1": 'bob1',
                                '2': "bob2",
                                '3': "bob3"}

        self.dic["foo1"] = {
            'bob': {"1": 'bob1',
                    '2': "bob2",
                    '3': "bob3"}}


    def test_path_delete(self):
        self.dic = {}
        self.doc["foo1/bob"] = {"1": 'bob1',
                                '2': "bob2",
                                '3': "bob3"}

        self.dic["foo1"] = {
            'bob': {"1": 'bob1',
                    '2': "bob2",
                    '3': "bob3"}}

        del self.doc['foo1']['bob']
        del self.dic['foo1']['bob']



if __name__ == '__main__':
    unittest.main()

