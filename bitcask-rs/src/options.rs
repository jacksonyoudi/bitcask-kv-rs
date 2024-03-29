#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    // 标准文件 IO
    StandardFIO,

    // 内存文件映射
    MemoryMap,
}


pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}


#[derive(Clone, PartialEq)]
pub enum IndexType {
    /// BTree 索引
    BTree,

    /// 跳表索引
    SkipList,

    /// B+树索引，将索引存储到磁盘上
    BPlusTree,
}