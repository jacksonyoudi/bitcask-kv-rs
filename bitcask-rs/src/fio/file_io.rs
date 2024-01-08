use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs;
use std::sync::{Arc, RwLock};
use crate::errors::{Result, Errors};
use crate::fio::IOManager;
use log::error;
use fs::FileExt;

pub struct FileIO {
    //  系统文件的文件描述符
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: std::path::PathBuf) -> Result<Self> {
        match
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => {
                return Ok(
                    FileIO {
                        fd: Arc::new(RwLock::new(file))
                    }
                );
            }
            Err(e) => {
                error!("failed to open data file: {}", e);
                return Err(Errors::FailedToOpenDataFile);
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read()?;
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                return Err(Errors::FailedReadFromDataFile);
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write()?;
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write to data file err: {}", e);
                return Err(Errors::FailedWriteToDataFile);
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read()?;
        if let Err(e) = read_guard.sync_all() {
            error!("failed to sync data file: {}", e);
            return Err(Errors::FailedSyncDataFile);
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read()?;
        let metadata = read_guard.metadata()?;
        metadata.len()
    }
}


