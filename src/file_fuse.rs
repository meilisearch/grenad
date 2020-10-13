use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;

use nix::errno::Errno;
use nix::fcntl::{fallocate, FallocateFlags};

const BLOCK_SIZE: u64 = 4096;

pub struct FileFuse {
    file: File,
    consumed: u64,
    shrink_size: u64,
}

impl FileFuse {
    pub fn new(file: File) -> FileFuse {
        FileFuse::with_shrink_size(file, BLOCK_SIZE * 1024) // 4 KB
    }

    pub fn with_shrink_size(file: File, size: u64) -> FileFuse {
        FileFuse { file, consumed: 0, shrink_size: size }
    }

    pub fn into_inner(self) -> File {
        self.file
    }
}

impl Read for FileFuse {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.file.read(buf)?;
        self.consumed += count as u64;

        if count == 0 {
            self.file.set_len(0)?;
        } else if self.consumed >= self.shrink_size {
            let fd = self.file.as_raw_fd();
            if let Err(e) = fallocate(fd, FallocateFlags::FALLOC_FL_COLLAPSE_RANGE, 0, self.shrink_size as i64) {
                if e.as_errno().map_or(false, |e| e == Errno::EINVAL) {
                    self.file.set_len(0)?;
                } else {
                    return Err(io::Error::new(ErrorKind::Other, e));
                }
            }
            self.consumed -= self.shrink_size;
            self.file.seek(SeekFrom::Start(self.consumed))?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::fs::OpenOptions;

    #[test]
    fn it_works() {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open("target/test-file")
            .unwrap();

        let bytes = &[5; 4096];
        for _ in 0..2048 {
            file.write_all(bytes).unwrap();
        }

        // Reset the seek cursor
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut fuse = FileFuse::new(file);

        let buf = &mut [0; 4096];
        loop {
            let count = fuse.read(buf).unwrap();
            if count == 0 { break }
            assert!(buf[..count].iter().all(|x| *x == 5));
        }

        let file = fuse.into_inner();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }
}
