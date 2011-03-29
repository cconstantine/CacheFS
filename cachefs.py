#!/usr/bin/env python

import shutil
import time
import os
import stat
import errno
import sys
import sqlite3

CACHE_FS_VERSION = '0.0.1'
import fuse
fuse.fuse_python_api = (0, 2)

log_file = sys.stdout

def debug(text):
    pass
#    log_file.write(text)
#    log_file.write('\n')
#    log_file.flush()

def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

cache = None


class CacheMiss(Exception):
    def __init__(self):
        debug(">> CACHE MISS")
    pass

class FileDataCache:
    def cache_file(self, path):
        return os.path.join(self.cachebase, "file_data") + path

    def __init__(self, db, cachebase, path, node_id = None):
        self.cachebase = cachebase
        self.full_path = self.cache_file(path)

        try:
            os.makedirs(os.path.dirname(self.full_path))
        except OSError:
            pass

        self.path = path
        self.db = db
        self.cache = None
        #self.open()

        self.node_id = node_id

        with self.db:
            if self.node_id != None:
                self.db.execute('INSERT OR REPLACE INTO nodes (id, last_use) values (?,?)', (self.node_id,time.time()))
                self.db.execute('INSERT OR REPLACE INTO paths (node_id,path) values (?,?)', (self.node_id,self.path))
            else:
                for row in self.db.execute('SELECT node_id FROM paths WHERE path = ?', self.path):
                    self.node_id = row['node_id']

                if self.node_id == None:
                    raise Exception("Unable to find path in db and no node_id given, unable to open cache")

            for other_path, in self.db.execute('SELECT path FROM paths WHERE node_id = ? AND path != ?', (self.node_id, self.path)):
                try:
                    if not os.path.exists(self.full_path) and os.path.exists(self.cache_file(other_path)):
                        os.link(self.cache_file(other_path), self.full_path)
                except Exception, e:
                    print "link error: %s" % e
                    raise e

        self.misses = 0
        self.hits = 0

    def known_offsets(self):
        ret = {}
        c = self.db.cursor()
        c.execute('select offset, end from blocks where node_id = ?', (self.node_id,))
        for offset, end in c:
            ret[offset] = end - offset

        return ret

    def open(self):
        if self.cache == None:
            try:
                self.cache = open(self.full_path, "r+")
            except:
                self.cache = open(self.full_path, "w+")


    def report(self):
        rate = 0.0
        if self.hits + self.misses:
            rate = 100*float(self.hits)/(self.hits + self.misses)
        import pprint
        pprint.pprint(self.known_offsets())
        print ">> %s Hits: %d, Misses: %d, Rate: %f%%" % (
            self.path, self.hits, self.misses, rate)

    def __del__(self):
        self.close()

    def close(self):
        if self.cache:
            self.cache.close()
            self.cache = None


    def __conditions__(self, offset, length = None):
        if length != None:
            return ["node_id = ? AND (offset = ? OR "
                    "(offset > ? AND offset <= ?) OR (offset < ? AND end >= ?))",
                    (self.node_id,
                     offset,
                     offset, offset + length,
                     offset, offset)]
        else:
            return ["node_id = ? AND offset <= ? AND end > ?",
                    (self.node_id,            offset,     offset)]

    def __overlapping_block__(self, offset):
        conditions = self.__conditions__(offset)
                      
        query = "select offset, end from blocks where %s" % conditions[0]
        result = self.db.execute(query, conditions[1])
        for db_offset, db_end in result:
            return (db_offset, db_end - db_offset)
        return (None, None)

    def __add_block___(self, offset, length):
        end = offset + length

        conditions = self.__conditions__(offset, length)
        query = "select min(offset), max(end) from blocks where %s" % conditions[0]
        for db_offset, db_end in self.db.execute(query, conditions[1]):
            if db_offset == None or db_end == None:
                continue
            offset = min(offset, db_offset)
            end = max(end, db_end)

        with self.db:
            self.db.execute("delete from blocks where %s" % conditions[0], conditions[1])
            self.db.execute('insert into blocks values (?, ?, ?)', (self.node_id, offset, end))


        return

    def read(self, size, offset):
        #print ">>> READ (size: %s, offset: %s" % (size, offset)
        (addr, s) = self.__overlapping_block__(offset)
        if addr == None or addr + s < offset + size:
            self.misses += size
            raise CacheMiss
    
        self.hits += size

        self.open()
        self.cache.seek(offset)
        buf = self.cache.read(size)
        self.close()

        return buf

    def update(self, buff, offset):
        #print ">>> UPDATE (len: %s, offset, %s)" % (len(buff), offset)
        self.open()
        self.cache.seek(offset)
        self.cache.write(buff)
        self.__add_block___(offset, len(buff))
        self.close()

    def truncate(self, len):
        try:
            self.open()
            self.cache.truncate(len)
            self.close()
            
            with self.db:
                self.db.execute("DELETE FROM blocks WHERE node_id = ? AND offset >= ?", (self.node_id, len))
                self.db.execute("UPDATE blocks SET end = ? WHERE node_id = ? AND end > ?", (len, self.node_id, len))
        except Exception, e:
            print "Error truncating: %s" % e
        
        return

    def unlink(self):
        try:
            shutil.rmtree(self.full_path)
            with self.db:
                self.db.execute("DELETE FROM paths WHERE path = ?", self.path)
                count = self.db.execute("SELECT COUNT(*) FROM paths WHERE paths.path = ? ", self.path).fetchone()[0]
                if count == 0:
                    self.db.execute("DELETE FROM nodes WHERE node_id = ? ", self.node_id)

        except:
            pass

    @staticmethod
    def rmdir(self, cache_base, path):
        try:
            shutil.rmtree(cache_base + path) 
        except:
            pass

    @staticmethod
    def symlink(self, cache_base, target, name):
        cache_target = cache_base + target
        cache_name = cache_base + name

        try:
            os.makedirs(os.path.dirname(cache_name))
        except OSError:
            pass
        
        os.symlink(cache_target, cache_name)

    @staticmethod
    def rename(self, cache_base, old_name, new_name):
        shutil.rmtree(cache_base + new_name) 
        os.rename(cache_base + old_name, cache_base + new_name)


        


def make_file_class(file_system):
    class CacheFile(object):
        direct_io = False
        keep_cache = False
    
        def __init__(self, path, flags, *mode):
            self.path = path
            m = flag2mode(flags)
            pp = file_system._physical_path(self.path)
            print('>> file<%s>.open(flags=%d, mode=%s)' % (pp, flags, m))
            self.f = open(pp, m)
            inode_id = os.stat(pp).st_ino
            self.data_cache = FileDataCache(file_system.cache_db, file_system.cache, path, inode_id)

        def read(self, size, offset):
            try:
                buf = self.data_cache.read(size, offset)
            except CacheMiss:
                self.f.seek(offset)
                buf = self.f.read(size)
                self.data_cache.update(buf, offset)
            return buf
        
        def write(self, buf, offset):
            self.f.seek(offset)
            self.f.write(buf)

            self.data_cache.update(buf, offset)

            return len(buf)


        def release(self, flags):
            debug('>> file<%s>.release()' % self.path)
            self.f.close()
            self.data_cache.report()
            return 0

        def flush(self):
            self.f.flush()

    return CacheFile

class CacheFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.file_class = make_file_class(self)
        self.caches = {}

    def _physical_path(self, path):
        phys_path = os.path.join(self.target, path.lstrip('/'))
        return phys_path

    def path_cache(self, path):
        try:
            return self.caches[path]
        except:
            inode_id = os.stat(path).st_ino
            fdc = FileDataCache(self.cache_db, self.cache, path, inode_id)
            self.caches[path] = fdc
            return fdc
        
    
    def getattr(self, path):
        try:
           pp = self._physical_path(path)
           # Hide non-public files (except root)

           return os.lstat(pp)
        except Exception, e:
           debug(str(e))
           raise e

    def readdir(self, path, offset):
        phys_path = self._physical_path(path).rstrip('/') + '/'
        for r in ('..', '.'):
            yield fuse.Direntry(r)
        for r in os.listdir(phys_path):
            virt_path = r
            debug('readdir yield: ' + virt_path)
            yield fuse.Direntry(virt_path)

    def readlink(self, path):
        debug('>> readlink("%s")' % path)
        phys_resolved = os.readlink(self._physical_path(path))
        debug('   resolves to physical "%s"' % phys_resolved)
        return phys_resolved


    def unlink(self, path):
        debug('>> unlink("%s")' % path)
        os.remove(self._physical_path(path))
        try:
            FileDataCache(self.cache_db, self.cache, path).unlink()
        except:
            pass
        return 0

    # Note: utime is deprecated in favour of utimens.
    def utime(self, path, times):
        """
        Sets the access and modification times on a file.
        times: (atime, mtime) pair. Both ints, in seconds since epoch.
        Deprecated in favour of utimens.
        """
        debug('>> utime("%s", %s)' % (path, times))
        os.utime(self._physical_path(path), times)
        return 0

    def access(self, path, flags):
        path = self._physical_path(path)
        os.access(path, flags)

    def mkdir(self, path, mode):
        path = self._physical_path(path)
        os.mkdir(path, mode)
        
    def rmdir(self, path):
        os.rmdir( self._physical_path(path) )
        FileDataCache.rmdir(self.cache, path)

    def symlink(self, target, name):
        os.symlink(self._physical_path(target), self._physical_path(name))
        FileDataCache.symlink(self.cache, target, name)

    def link(self, target, name):
        print('>> link(%s, %s)' % (target, name))
        os.link(self._physical_path(target), self._physical_path(name))
        FileDataCache(self.cache_db, self.cache, name, os.stat(self._physical_path(name)).st_ino)

    def rename(self, old_name, new_name):
        os.rename(self._physical_path(old_name),
                  self._physical_path(new_name))
        FileDataCache.rename(self.cache, old_name, new_name)
        FileDataCache.rename(self.cache, old_name)
        
    def chmod(self, path, mode):
        os.chmod(self._physical_path(path), mode)
        
    def chown(self, path, user, group):
        os.chown(self._physical_path(path), user, group)
	
    def truncate(self, path, len):
        f = open(self._physical_path(path), "a")
        f.truncate(len)
        f.close()
        try:
            cache = FileDataCache(self.cache, path)
            cache.truncate(len)
        except:
            pass


def open_db(cache_dir):
    return sqlite3.connect(os.path.join(cache_dir, "metadata.db"), isolation_level="DEFERRED")

def create_db(cache_dir):
    open_db(cache_dir)
    cache_db = sqlite3.connect(os.path.join(cache_dir, "metadata.db"), isolation_level="DEFERRED")

    cache_db.execute("""
CREATE TABLE IF NOT EXISTS paths (
  id   INTEGER NOT NULL, 
  node_id INTEGER,
  path STRING,
  FOREIGN KEY(node_id) REFERENCES nodes(id)
  UNIQUE(path),
  PRIMARY KEY(id)
)
"""
                     )
    cache_db.execute("""
CREATE TABLE IF NOT EXISTS nodes (
  id        INTEGER PRIMARY KEY, 
  last_use  INTEGER
)
"""
                     )
    
    
    cache_db.execute("""
CREATE TABLE IF NOT EXISTS blocks (
  node_id INTEGER NOT NULL,
  offset  INTEGER,
  end     INTEGER,
  FOREIGN KEY(node_id) REFERENCES nodes(id)
)"""
                     )
#    cache_db.execute('create index if not exists meta on blocks (path_id, offset, end)') 
    cache_db.execute("PRAGMA synchronous=OFF")
    cache_db.execute("PRAGMA journal_mode=OFF")

    return cache_db


def main():
    usage='%prog MOUNTPOINT -o target=SOURCE cache=SOURCE [options]'
    server = CacheFS(version='CacheFS %s' % CACHE_FS_VERSION,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parser.add_option(
        mountopt="cache", metavar="PATH",
        default=None,
        help="Path to place the cache")

    # Wire server.target to command line options
    server.parser.add_option(
        mountopt="target", metavar="PATH",
        default=None,
        help="Path to be cached")


    server.parse(values=server, errex=1)
#    try:
    server.target = os.path.abspath(server.target)
    try:
        cache_dir = server.cache
    except:
        import hashlib
        cache_dir = os.path.join(os.path.expanduser("~"),
                                 ".cachefs",
                                 hashlib.md5(server.target).hexdigest())
    server.multithreaded = 0
    server.cache = os.path.abspath(cache_dir)
    try:
        os.mkdir(server.cache)
    except OSError:
        pass
    
    server.cache_db = create_db(cache_dir)
    
        

    #except AttributeError as e:
    #    print e
    #    server.parser.print_help()
    #    sys.exit(1)
    #except AttributeError as e:
    #    pass

    print 'Setting up CacheFS %s ...' % CACHE_FS_VERSION
    print '  Target       : %s' % server.target
    print '  Cache        : %s' % server.cache
    print '  Mount Point  : %s' % os.path.abspath(server.fuse_args.mountpoint)
    print
    print 'Unmount through:'
    print '  fusermount -u %s' % server.fuse_args.mountpoint
    print
    print 'Done.'
    server.main()

if __name__ == '__main__':
    main()
