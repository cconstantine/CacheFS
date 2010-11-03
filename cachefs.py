#!/usr/bin/env python


import time
import os
import stat
import errno
import sys

CACHE_FS_VERSION = '0.0.1'
import fuse
fuse.fuse_python_api = (0, 2)

def debug(text):
    """
    log_file.write(text)
    log_file.write('\n')
    log_file.flush()
    """
    pass



STAT_ATTRIBUTES = (
    "st_atime", "st_blksize", "st_blocks", "st_ctime", "st_dev",
    "st_gid", "st_ino", "st_mode", "st_mtime", "st_nlink",
    "st_rdev", "st_size", "st_uid")

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


def make_file_class(file_system):
    class RevealFile(object):
        def __init__(self, path, flags, *mode):
            self.path = path
            try:
                debug('>> file<%s>.open(flags=%d)' % (self.path, flags))
                pp = file_system._physical_path(self.path)
                if not os.path.exists(pp):
                    e = OSError()
                    e.errno = ENOENT
                    raise e
                self.f = open(pp, 'rb')
            except Exception, e:
                debug(str(e))
                raise e

        def read(self, size, offset):
            try:
                debug('>> file<%s>.read(size=%d, offset=%d)' % (self.path, size, offset))
                self.f.seek(offset)
                buf = self.f.read(size)
                return buf
            except Exception, e:
                debug(str(e))
                raise e

        def release(self, flags):
            debug('>> file<%s>.release()' % self.path)
            self.f.close()
            return 0
    return RevealFile


class CacheFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.file_class = make_file_class(self)

    def _physical_path(self, path):
        phys_path = os.path.join(self.source, path.lstrip('/'))
        return phys_path

    def getattr(self, path):
        try:
           debug('>> getattr("' + path + '")')
           pp = self._physical_path(path)
           # Hide non-public files (except root)

           st = WritableStat(path, os.lstat(pp))  # lstat to not follow symlinks
           st.st_atime = int(time.time())
           # Make read-only (http://docs.python.org/library/stat.html)
           st.st_mode = st.st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
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


def main():
    usage='%prog MOUNTPOINT -o source=SOURCE [options]'
    server = CacheFS(version='CacheFS %s' % CACHE_FS_VERSION,
                     usage=usage,
                     dash_s_do='setsingle')

    # Wire server.source to command line options
    server.parser.add_option(
        mountopt="source", metavar="PATH",
        default=None,
        help="source path to reveal subsets from")

    server.parse(values=server, errex=1)
    try:
        server.source = os.path.abspath(server.source)
    except AttributeError:
        server.parser.print_help()
        sys.exit(1)

    print 'Setting up RevealFS %s ...' % CACHE_FS_VERSION
    print '  Source      : %s' % server.source
    print '  Destination : %s' % os.path.abspath(server.fuse_args.mountpoint)
    print
    print 'Unmount through:'
    print '  fusermount -u %s' % server.fuse_args.mountpoint
    print
    print 'Done.'
    server.main()

if __name__ == '__main__':
    main()
