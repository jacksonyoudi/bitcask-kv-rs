use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;
use prost::bytes::{Buf, BytesMut};
use prost::{decode_length_delimiter, length_delimiter_len};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType, max_log_record_header_size, ReadLogRecord};
use crate::fio::{IOManager, new_io_manager};
use crate::options::IOType;
use crate::errors::{Result, Errors};


pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const HINT_FILE_NAME: &str = "hint-index";
pub const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";


pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>,
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32, io_type: IOType) -> Result<DataFile> {
        let file_name = get_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(file_name, io_type);
        Ok(
            DataFile {
                file_id: Arc::new(RwLock::new(file_id)),
                write_off: Arc::new(RwLock::new(0)),
                io_manager: io_manager,
            }
        )
    }

    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(HINT_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardFIO);

        Ok(
            DataFile {
                file_id: Arc::new(RwLock::new(0)),
                write_off: Arc::new(RwLock::new(0)),
                io_manager: io_manager,
            }
        )
    }


    pub fn new_merge_fin_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardFIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_seq_no_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(SEQ_NO_FILE_NAME);
        let io_manager = new_io_manager(file_name, IOType::StandardFIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.io_manager.size()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard?
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // let mut buf = BytesMut::new();
        // buf.reserve(max_log_record_header_size());
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;

        let rec_type = header_buf.get_u8();
        let key_size = decode_length_delimiter(&mut header_buf)?;
        let value_size = decode_length_delimiter(&mut header_buf)?;

        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }


        let header_size = length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
        let mut body_buf = BytesMut::zeroed(
            key_size + value_size + 4
        );

        self.io_manager.read(&mut body_buf, offset + header_size as u64)?;

        let log_record = LogRecord {
            key: body_buf.get(..key_size)?.to_vec(),
            value: body_buf.get(key_size..key_size + value_size)?.to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        body_buf.advance(key_size + value_size);

        // crc不通过
        if body_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        Ok(ReadLogRecord {
            record: log_record,
            size: header_size + key_size + value_size + 4,
        })
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // lock
        let write_guard = self.write_off.write();
        *write_guard += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn write_hint_record(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let hint_record = LogRecord {
            key,
            value: pos.encode(),
            rec_type: LogRecordType::NORMAL,
        };

        let enc_record = hint_record.encode();
        self.write(&enc_record)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) {
        self.io_manager = new_io_manager(get_data_file_name(dir_path, self.get_file_id()), io_type);
    }
}


pub fn get_data_file_name(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
}
