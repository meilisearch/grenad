# grenad

This is like a an SSTable or MTBL but it is way simpler and thereby doesn't expose any way to get a single or ranges.
The main feature is that it support creating, reading, merging and sorting ordered lists of entries and most importantly is able to shrink file while reading it, this highly reduce the amount of memory used.

This is a feature that is only be available on linux 3.14 or higher and on some specific (but common) filesystems (i.e. ext4, XFS) as it is based on [the fallocate syscall](http://manpages.ubuntu.com/manpages/disco/en/man2/fallocate.2.html), to enable it, compile it with the `file-fuse` feature.
