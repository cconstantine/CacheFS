#!/usr/bin/env python

import shutil
import time
import os
import stat
import errno
import sys

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


STAT_ATTRIBUTES = (
    "st_atime", "st_blksize", "st_blocks", "st_ctime", "st_dev",
    "st_gid", "st_ino", "st_mode", "st_mtime", "st_nlink",
    "st_rdev", "st_size", "st_uid")

class CacheMiss(Exception):
    def __init__(self):
        debug(">> CACHE MISS")
    pass

class FileDataCache:
    def __init__(self, cachebase, path):
        full_path = cachebase + path

        try:
            os.makedirs(os.path.dirname(full_path))
        except OSError:
            pass

        self.path = path
        self.cache = open(full_path, "w+")
        self.known_offsets = {}

        self.misses = 0
        self.hits = 0

    def report(self):
        print ">> %s Hits: %d, Misses: %d, Rate: %f%%" % (
            self.path, self.hits, self.misses, 100*float(self.hits)/self.misses)

    def __del__(self):
        self.close

    def close(self):

        self.cache.close()

    def __overlapping_block__(self, path, offset):
        for addr, size in self.known_offsets.items():
            if offset >= addr and offset < addr + size:
                return (addr, size)
        return (None, None)

    def read(self, size, offset):
        (addr, s) = self.__overlapping_block__(self.path, offset)
        if addr == None or addr + s < offset + size:
            self.misses += 1
            raise CacheMiss
        
        self.hits += 1
        self.cache.seek(offset)
        return self.cache.read(size)

    def update(self, buff, offset):
        self.cache.seek(offset)
        self.cache.write(buff)
        self.known_offsets[offset] = len(buff)

class WritableStat(fuse.Stat):
    def __init__(self, path, readonly_stat):
        self.path = path
        for key in STAT_ATTRIBUTES:
            setattr(self, key, getattr(readonly_stat, key))

    def __str__(self):
        d = dict((key, str(getattr(self, key))) for key in STAT_ATTRIBUTES)
        d['file'] = self.path
        l = ', '.join(('%s=%s' % (k, v)) for k, v in sorted(d.items()))
        return '>>>> %s' % l

    def check_permission(self, uid, gid, flags):
        """
        Checks the permission of a uid:gid with given flags.
        Returns True for allowed, False for denied.
        flags: As described in man 2 access (Linux Programmer's Manual).
            Either os.F_OK (test for existence of file), or ORing of
            os.R_OK, os.W_OK, os.X_OK (test if file is readable, writable and
            executable, respectively. Must pass all tests).
        """
        if flags == os.F_OK:
            return True
        user = (self.st_mode & 0700) >> 6
        group = (self.st_mode & 070) >> 3
        other = self.st_mode & 07
        if uid == self.st_uid:
            # Use "user" permissions
            mode = user | group | other
        elif gid == self.st_gid:
            # Use "group" permissions
            # XXX This will only check the user's primary group. Don't we need
            # to check all the groups this user is in?
            mode = group | other
        else:
            # Use "other" permissions
            mode = other
        if flags & os.R_OK:
            if mode & os.R_OK == 0:
                return False
        if flags & os.W_OK:
            if mode & os.W_OK == 0:
                return False
        if flags & os.X_OK:
            if uid == 0:
                # Root has special privileges. May execute if anyone can.
                if mode & 0111 == 0:
                    return False
            else:
                if mode & os.X_OK == 0:
                    return False
        return True

def make_file_class(file_system):
    class CacheFile(object):
        direct_io = False
        keep_cache = True

        def __init__(self, path, flags, *mode):
            self.path = path
            m = flag2mode(flags)
            pp = file_system._physical_path(self.path)
            debug('>> file<%s>.open(flags=%d, mode=%s)' % (pp, flags, m))
            self.f = open(pp, m)
            self.cache = file_system._file_cache(path)

        def read(self, size, offset):
            try:
                return self.cache.read(size, offset)
            except CacheMiss:
                self.f.seek(offset)
                buf = self.f.read(size)
                self.cache.update(buf, offset)
                return buf
        
        def write(self, buf, offset):
            block_name = self.cache + '/' + str(offset)
            cf = open(block_name, 'wb')
            cf.write(buf)
            cf.close()

            debug('>> file<%s>.write(size=%s, offset=%s)' % (
                    self.path, len(buf), str(offset)))
            self.f.seek(offset)
            self.f.write(buf)
            return True

        def release(self, flags):
            debug('>> file<%s>.release()' % self.path)
            self.f.close()
            self.cache.report()
            return 0

    return CacheFile


class CacheFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.file_class = make_file_class(self)
        self.caches = {}

    def _physical_path(self, path):
        phys_path = os.path.join(self.target, path.lstrip('/'))
        return phys_path

    def _file_cache(self, path):
        try:
            return self.caches[path]
        except:
            self.caches[path] = c = FileDataCache(self.cache, 
                                                 path)
            return c
            

    def getattr(self, path):
        try:
           debug('>> getattr("' + path + '")')
           pp = self._physical_path(path)
           # Hide non-public files (except root)

           st = WritableStat(path, os.lstat(pp))  # lstat to not follow symlinks
           st.st_atime = int(time.time())

           return st
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
        path = self._physical_path(path)
        debug('>> unlink("%s")' % path)
        os.remove(path)
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
        path = self._physical_path(path)
        os.rmdir(path)

    def symlink(self, target, name):
        name = self._physical_path(name)
        os.symlink(target, name)

    def link(self, target, name):
        name = self._physical_path(name)
        target = self._physical_path(target)
        os.link(target, name)
        
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
    try:
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

    except AttributeError as e:
        print e
        server.parser.print_help()
        sys.exit(1)

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
