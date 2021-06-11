use std::fs::File;
use std::io::{self, Read};

pub const BLOCK_SIZE: u64 = 4096;
pub const DEFAULT_SHRINK_SIZE: u64 = BLOCK_SIZE * 1024; // 4 KB

#[derive(Debug, Clone, Copy)]
pub struct FileFuseBuilder {
    shrink_size: u64,
    enable_fusing: bool,
}

impl FileFuseBuilder {
    pub fn new() -> FileFuseBuilder {
        FileFuseBuilder { shrink_size: DEFAULT_SHRINK_SIZE, enable_fusing: false }
    }

    pub fn shrink_size(&mut self, shrink_size: u64) -> &mut Self {
        self.shrink_size = shrink_size;
        self.enable_fusing = true;
        self
    }

    pub fn enable_fusing(&mut self) -> &mut Self {
        self.enable_fusing = true;
        self
    }

    pub fn disable_fusing(&mut self) -> &mut Self {
        self.enable_fusing = false;
        self
    }

    pub fn build(&self, file: File) -> FileFuse {
        let shrink_size = if self.enable_fusing { Some(self.shrink_size) } else { None };

        FileFuse { file, consumed: 0, shrink_size }
    }
}

impl Default for FileFuseBuilder {
    fn default() -> FileFuseBuilder {
        FileFuseBuilder::new()
    }
}

pub struct FileFuse {
    file: File,
    consumed: u64,
    shrink_size: Option<u64>,
}

impl FileFuse {
    pub fn builder() -> FileFuseBuilder {
        FileFuseBuilder::new()
    }

    pub fn new(file: File) -> FileFuse {
        FileFuseBuilder::new().build(file)
    }

    pub fn into_inner(self) -> File {
        self.file
    }
}

#[cfg(target_os = "linux")]
impl Read for FileFuse {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use std::io::{ErrorKind, Seek, SeekFrom};
        use std::os::unix::io::AsRawFd;

        use nix::errno::Errno;
        use nix::fcntl::{fallocate, FallocateFlags};

        if let Some(shrink_size) = self.shrink_size {
            let count = self.file.read(buf)?;
            self.consumed += count as u64;

            if count == 0 {
                self.file.set_len(0)?;
            } else if self.consumed >= shrink_size {
                let fd = self.file.as_raw_fd();
                if let Err(e) =
                    fallocate(fd, FallocateFlags::FALLOC_FL_COLLAPSE_RANGE, 0, shrink_size as i64)
                {
                    if e.as_errno().map_or(false, |e| e == Errno::EINVAL) {
                        self.file.set_len(0)?;
                    } else {
                        return Err(io::Error::new(ErrorKind::Other, e));
                    }
                }
                self.consumed -= shrink_size;
                self.file.seek(SeekFrom::Current(-(shrink_size as i64)))?;
            }

            Ok(count)
        } else {
            self.file.read(buf)
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl Read for FileFuse {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let _ = self.consumed;
        let _ = self.shrink_size;
        self.file.read(buf)
    }
}

#[cfg(feature = "file-fuse")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

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

        let mut fuse = FileFuse::builder().enable_fusing().build(file);

        let buf = &mut [0; 4096];
        loop {
            let count = fuse.read(buf).unwrap();
            if count == 0 {
                break;
            }
            assert!(buf[..count].iter().all(|x| *x == 5));
        }

        let file = fuse.into_inner();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }
}
