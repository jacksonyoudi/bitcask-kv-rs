use std::ops::Index;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use jammdb::DB;
use crate::data::log_record::LogRecordPos;
use crate::index::{Indexer, IndexIterator};
use crate::options::IteratorOptions;

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

pub struct BPlusTree {
    tree: Arc<DB>,
}



impl BPlusTree {
    pub fn new(dir_path: PathBuf) -> Self {
        let bptree = DB::open(dir_path.join(BPTREE_BUCKET_NAME)).expect("failed to open bptree");
        let tree = Arc::new(bptree);
        let tx = tree.tx(true)
            .expect("failed to begin tx");

        tx.get_or_create_bucket(BPTREE_INDEX_FILE_NAME)?;
        tx.commit()?;

        Self {
            tree: tree.clone()
        }
    }
}


impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        todo!()
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        todo!()
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        todo!()
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<Bytes>> {
        todo!()
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        todo!()
    }
}