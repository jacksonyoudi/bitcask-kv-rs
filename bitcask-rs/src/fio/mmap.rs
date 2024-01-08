use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use memmap2::Mmap;
use log::error;
use crate::errors::{Result, Errors};
use crate::fio::IOManager;

pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
        {
            Ok(file) => {
                let map = unsafe { Mmap::map(&file).expect("failed to map the file") };
                return Ok(MMapIO {
                    map: Arc::new(Mutex::new(map)),
                });
            }
            Err(e) => {
                error!("failed to open data file: {}", e);
                return Err(Errors::FailedToOpenDataFile);
            }
        }
    }
}


impl IOManager for MMapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let map_arr = self.map.lock();
        let end = offset + buf.len() as u64;
        if end > map_arr.len() as u64 {
            return Err(Errors::ReadDataFileEOF);
        }
        let val = &map_arr[offset as usize..end as usize];
        buf.copy_from_slice(val);

        Ok(val.len())
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn size(&self) -> u64 {
        unimplemented!()
    }
}