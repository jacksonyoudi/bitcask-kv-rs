use crate::data::log_record::LogRecordPos;
use crate::errors::Result;
use bytes::Bytes;
use crate::options::IteratorOptions;

pub mod bptree;
pub mod btree;

pub mod skiplist;


pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn list_keys(&self) -> Result<Vec<Bytes>>;

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}


pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

