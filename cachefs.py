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

cache = None


class CacheMiss(Exception):
    def __init__(self):
        debug(">> CACHE MISS")
    pass

class FileDataCache:
    def cache_file(self, path):
        return os.path.join(self.cachebase, "file_data") + path

    def __init__(self, db, cachebase, path, flags =  os.O_RDWR, node_id = None):
        self.cachebase = cachebase
        self.full_path = self.cache_file(path)

        try:
            os.makedirs(os.path.dirname(self.full_path))
        except OSError:
            pass

        self.path = path
        self.db = db

        self.cache = None

        self.flags = os.O_RDWR | ( flags &
                                   os.O_CREAT &
                                   os.O_EXCL )
        self.node_id = node_id

        self.open()

        if flags & os.O_TRUNC:
            self.truncate(0)

        with self.db:
            if self.node_id != None:
                self.db.execute('INSERT OR REPLACE INTO nodes (id, last_use) values (?,?)', (self.node_id,time.time()))
                self.db.execute('INSERT OR REPLACE INTO paths (node_id,path) values (?,?)', (self.node_id,self.path))
            else:
                for nid, in self.db.execute('SELECT node_id FROM paths WHERE path = ?', (self.path,)):
                    self.node_id = nid

                if self.node_id == None:
                    print "Unable to find path in db and no node_id given, unable to open cache"
                    raise CacheMiss

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
                self.cache = os.open(self.full_path, self.flags)
            except Exception, e:
                self.cache = os.open(self.full_path, self.flags | os.O_CREAT )


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
            os.close(self.cache)
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
                      
        query = "select offset, end, last_block from blocks where %s" % conditions[0]
        result = self.db.execute(query, conditions[1])
        for db_offset, db_end, last_block in result:
            return (db_offset, db_end - db_offset, last_block)
        return (None, None, False)

    def __add_block___(self, offset, length, last_bytes):
        end = offset + length

        conditions = self.__conditions__(offset, length)
        query = "select min(offset), max(end) from blocks where %s" % conditions[0]
        with self.db:
            for db_offset, db_end in self.db.execute(query, conditions[1]):
                if db_offset == None or db_end == None:
                    continue
                offset = min(offset, db_offset)
                end = max(end, db_end)

            self.db.execute("delete from blocks where %s" % conditions[0], conditions[1])
            self.db.execute('insert into blocks values (?, ?, ?, ?)', (self.node_id, offset, end, last_bytes))
        return

    def read(self, size, offset):
        #print ">>> READ (size: %s, offset: %s" % (size, offset)
        (addr, s, last) = self.__overlapping_block__(offset)
        if addr == None or (addr + s < offset + size and not last):
            self.misses += size
            raise CacheMiss
    
#        self.open()
        os.lseek(self.cache, offset, os.SEEK_SET)
        buf = os.read(self.cache, size)
        self.hits += len(buf)
#        self.close()

        return buf

    def update(self, buff, offset, last_bytes=False):
        print ">>> UPDATE (len: %s, offset, %s)" % (len(buff), offset)
#        self.open()
        os.lseek(self.cache, offset, os.SEEK_SET)
        os.write(self.cache, buff)

        self.__add_block___(offset, len(buff), last_bytes)
#        self.close()

    def truncate(self, l):
        print ">>> TRUNCATE (cache: %s, len: %s)" % (self.cache, l)
        try:
            os.ftruncate(self.cache, l)

            with self.db:
                self.db.execute("DELETE FROM blocks WHERE node_id = ? AND offset >= ?", (self.node_id, l))
                self.db.execute("UPDATE blocks SET end = ? WHERE node_id = ? AND end > ?", (len, self.node_id, l))
        except Exception, e:
            print "Error truncating: %s" % e
        
        return

    def unlink(self):
        shutil.rmtree(self.full_path)
        with self.db:
            self.db.execute("DELETE FROM paths WHERE path = ?", self.path)
            count = self.db.execute("SELECT COUNT(*) FROM paths WHERE paths.path = ? ", self.path).fetchone()[0]
            if count == 0:
                self.db.execute("DELETE FROM nodes WHERE node_id = ? ", self.node_id)


    @staticmethod
    def rmdir(cache_base, path):
        d = os.path.join(cache_base, "file_data") + path
        try:
            shutil.rmtree(d)
        except:
            pass

    def rename(self, new_name):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO paths (path) values (?)", (self.path,))

        new_full_path = self.cache_file(new_name)
            
        try:
            os.makedirs(os.path.dirname(new_full_path))
        except OSError:
            pass

        os.rename(self.full_path, new_full_path)


        


def make_file_class(file_system):
    class CacheFile(object):
        direct_io = False
        keep_cache = False
    
        def __init__(self, path, flags, *mode):
            self.path = path
            self.pp = file_system._physical_path(self.path)
            print('>> file<%s>.open(flags=%d, mode=%s)' % (self.pp, flags, mode))

            if len(mode) > 0:
                self.f = os.open(self.pp, flags, mode[0])
            else:
                self.f = os.open(self.pp, flags)

            inode_id = os.stat(self.pp).st_ino
            self.data_cache = FileDataCache(file_system.cache_db, file_system.cache, path, flags, inode_id)

        def read(self, size, offset):
            try:
                buf = self.data_cache.read(size, offset)
            except CacheMiss:
                os.lseek(self.f, offset, os.SEEK_SET)
                buf = os.read(self.f, size)
                
                self.data_cache.update(buf, offset, os.read(self.f, 1)=='')
            return buf
        
        def write(self, buf, offset):
            print('>> file<%s>.write(len(buf)=%d, offset=%s)' % (self.path, len(buf), offset))
            os.lseek(self.f, offset, os.SEEK_SET)
            os.write(self.f, buf)

            end = os.stat(self.pp).st_size
            self.data_cache.update(buf, offset, offset + len(buf) == end)

            return len(buf)


        def release(self, flags):
            print('>> file<%s>.release()' % self.path)
            os.close(self.f)
            self.data_cache.close()
            self.data_cache.report()
            return 0

        def flush(self):
            os.fsync(self.f)

    return CacheFile

class CacheFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.file_class = make_file_class(self)
        self.caches = {}

    def _physical_path(self, path):
        phys_path = os.path.join(self.target, path.lstrip('/'))
        return phys_path
    
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
        print('>> readlink("%s")' % path)
        phys_resolved = os.readlink(self._physical_path(path))
        debug('   resolves to physical "%s"' % phys_resolved)
        return phys_resolved


    def unlink(self, path):
        print('>> unlink("%s")' % path)
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
        print('>> mkdir("%s")' % path)
        path = self._physical_path(path)
        os.mkdir(path, mode)
        
    def rmdir(self, path):
        print('>> rmdir("%s")' % path)
        os.rmdir( self._physical_path(path) )
        FileDataCache.rmdir(self.cache, path)

    def symlink(self, target, name):
        print('>> symlink("%s", "%s")' % (target, name))
        os.symlink(self._physical_path(target), self._physical_path(name))

    def link(self, target, name):
        print('>> link(%s, %s)' % (target, name))
        os.link(self._physical_path(target), self._physical_path(name))
        FileDataCache(self.cache_db, self.cache, name, None, os.stat(self._physical_path(name)).st_ino)

    def rename(self, old_name, new_name):
        print('>> rename(%s, %s)' % (old_name, new_name))
        os.rename(self._physical_path(old_name),
                  self._physical_path(new_name))
        try:
            fdc = FileDataCache(self.cache_db, self.cache, old_name)
            fdc.rename(new_name)
        except :
            pass
        
    def chmod(self, path, mode):
        os.chmod(self._physical_path(path), mode)
        
    def chown(self, path, user, group):
        os.chown(self._physical_path(path), user, group)
	
    def truncate(self, path, len):
        f = open(self._physical_path(path), "a")
        f.truncate(len)
        f.close()
        try:
            cache = FileDataCache(self.cache_db, self.cache, path)
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
  node_id    INTEGER NOT NULL,
  offset     INTEGER,
  end        INTEGER,
  last_block BOOLEAN DEFAULT false,
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
