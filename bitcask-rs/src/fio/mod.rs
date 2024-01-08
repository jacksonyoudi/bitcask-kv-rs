pub mod file_io;
pub mod mmap;

use std::path::PathBuf;
use crate::errors::Result;
use crate::fio::file_io::FileIO;
use crate::fio::mmap::MMapIO;
use crate::options::IOType;


pub trait IOManager: Sync + Send {
    // 从指定位置读取数据, 将读取的数据保存在buf中, 返回读取的size
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    //  将 buf数据写入
    fn write(&self, buf: &[u8]) -> Result<usize>;

    fn sync(&self) -> Result<()>;

    fn size(&self) -> u64;
}


pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Box<dyn IOManager> {
    match io_type {
        IOType::StandardFIO => Box::new(FileIO::new(file_name)?),
        IOType::MemoryMap => Box::new(MMapIO::new(file_name)?),
    }
}