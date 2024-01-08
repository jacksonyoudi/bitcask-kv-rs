use prost::bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};
use prost::encoding::{decode_varint, encode_varint};

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum LogRecordType {
    NORMAL = 1,

    DELETED = 2,

    TXNFINISHED = 3,
}

#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}


impl LogRecord {

    // encode 对 LogRecord 进行编码，返回字节数组及长度
    //
    //	+-------------+--------------+-------------+--------------+-------------+-------------+
    //	|  type 类型   |    key size |   value size |      key    |      value   |  crc 校验值  |
    //	+-------------+-------------+--------------+--------------+-------------+-------------+
    //	    1字节        变长（最大5）   变长（最大5）        变长           变长           4字节

    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }


    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        // 分配大小
        buf.reserve(self.encoded_length());

        buf.put_u8(self.rec_type as u8);

        encode_length_delimiter(self.key.len(), &mut buf)?;
        encode_length_delimiter(self.value.len(), &mut buf)?;

        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);


        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            _ => panic!("unknown log record type"),
        }
    }
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec();
    }
}


pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);

    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log record pos err: {}", e),
    };

    let offset = match decode_varint(&mut buf) {
        Ok(offset) => offset,
        Err(e) => panic!("decode log record pos err: {}", e),
    };

    let size = match decode_varint(&mut buf) {
        Ok(size) => size,
        Err(e) => panic!("decode log record pos err: {}", e),
    };

    LogRecordPos{
        file_id: fid as u32,
        offset: offset,
        size: size as u32,
    }
}


pub fn max_log_record_header_size() -> usize {
    // type, key len, value len
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}



