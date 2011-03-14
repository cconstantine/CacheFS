import gdbm
import os

class doc:
    def __init__(self, filename=None, db=None, key=""):
        self.key = key + '/'
        if db != None:
            self.db = db
        elif filename != None:
            self.db = gdbm.open(filename, "nf")
        else:
            raise Exception("Must give a db or a filename")
        self.db[self.key] = ''


    def __setitem__(self, k, val):
        if type(k).__name__!='str':
            raise Exception("Key must be a string")

        keys = k.split('/')
        while len(keys) > 1:
            k = keys[0]
            keys = keys[1:]
            path = os.path.join(self.key, k)
            self = doc(None, self.db, path)
        k = keys[0]
            
        path = os.path.join(self.key, k)

        if type(val).__name__=='dict':
            a = doc(None, self.db, path)
            for k,v in val.iteritems():
                a[k] = v
        else:
            self.db[path] = val

    def __getitem__(self, k):
        path = os.path.join(self.key, k)
        try:
            r = self.db[path] 
            return r
        except:
            try:
                if self.db[path+'/'] == '':
                    return doc(None, self.db, path)
            except:
                pass
            raise 

    def __delitem__(self, k):
        path = os.path.join(self.key, k)
        try:
            del self.db[path] 
        except:
            try:
                if self.db[path+'/'] == '':
                    a= doc(None, self.db, path)
                    for k in set(a.keys()):
                        del a[k]
                del self.db[path+'/'] 
                return
            except:
                pass
            raise 

        
        
    def close(self):
        self.db.close()

    def sync(self):
        self.db.sync()

    def keys(self):
        key = self.db.firstkey()
        key_len = len(self.key)
        while key != None:
            k = key[key_len:]
            if k != '' and key.startswith(self.key):
                if '/' not in k:
                    yield k
                elif '/' not in  k[:-1] and k[-1] == '/':
                    yield k[:-1]
            key = self.db.nextkey(key)
        

    def iteritems(self):
        for k in self.keys():
            yield (k,self[k])


