CacheFS
=======
Caching your files so you don't have to.

Quick Start (Ubuntu)
-----------
Install libfuse2 and python bindings

    sudo apt-get install libfuse2 python-fuse

Grab the latest CacheFS

    git clone git@github.com:cconstantine/CacheFS.git


Start caching

    ./CacheFS/cachefs.py <mount point> -o target=<slow drive>

Thats it!

