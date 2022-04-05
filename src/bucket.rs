use std::fs::File;
use anyhow::{Result, anyhow};
use std::io::{Read, Seek, SeekFrom, Write};
use std::borrow::{BorrowMut, Borrow};
use crate::items::{Encodable, Decodable, BinSizer};
use std::marker::PhantomData;
use thiserror::Error;
use std::fmt::{Debug, Formatter};
use std::cell::RefCell;
use std::rc::Rc;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_KEY_SIZE: usize = 128;
pub const MAX_VALUE_SIZE: usize = 1024;
const PTR_SIZE: usize = 4;

#[derive(Error, Debug)]
pub enum BucketError {
    #[error("bucket is full, need split")]
    Full
}

pub(crate) struct Bucket<K, V>
{
    pub index: u32,
    buf: [u8; PAGE_SIZE],
    pub bucket_type: BucketType,
    keys_pos: usize,
    values_pos: usize,
    ptrs_pos: usize,
    max_item_count: usize,
    dirty: bool,
    fd: Option<Rc<RefCell<File>>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub(crate) enum BucketType {
    META,
    INTERNAL,
    LEAF,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub(crate) enum Pos {
    Current,
    Left,
    Right
}

impl<K, V> Default for Bucket<K, V> {
    fn default() -> Self {
        Bucket::<K, V> {
            index: 0,
            buf: [0; PAGE_SIZE],
            bucket_type: BucketType::LEAF,
            keys_pos: 0,
            values_pos: 0,
            ptrs_pos: 0,
            max_item_count: 0,
            dirty: false,
            fd: None,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

impl<K, V> Bucket<K, V> where
    K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
    V: Encodable + Decodable + BinSizer + Debug + Clone
{
    pub fn new(fd: Rc<RefCell<File>>, index: u32, pt: BucketType) -> Result<Self> {
        let mut bucket = Self::default();
        bucket.bucket_type = pt;
        bucket.index = index;
        bucket.fd = Some(fd);
        match bucket.bucket_type{
            BucketType::META => {
                bucket.buf[0] = 0x01;
                bucket.set_root_index(0);
                bucket.set_total_bucket(0);
            }
            BucketType::INTERNAL => {
                bucket.buf[0] = 0x02;
                bucket.set_item_count(0).unwrap();
            }
            BucketType::LEAF => {
                bucket.buf[0] = 0;
                bucket.set_item_count(0).unwrap();
            }
        }
        bucket.init_layout();
        Ok(bucket)
    }

    fn init_layout(&mut self) {
        match self.bucket_type{
            BucketType::META => {
            }
            BucketType::INTERNAL => {
                self.max_item_count = (PAGE_SIZE - 8 - PTR_SIZE) / (K::bin_size() + PTR_SIZE);
                self.keys_pos = 8;
                self.ptrs_pos = self.keys_pos + self.max_item_count * K::bin_size()
            }
            BucketType::LEAF => {
                self.max_item_count = (PAGE_SIZE - 8) / (K::bin_size() + V::bin_size());
                self.keys_pos = 8;
                self.values_pos = self.keys_pos + self.max_item_count * K::bin_size();
            }
        };
        assert!(self.bucket_type == BucketType::META || self.max_item_count >= 2)
    }

    pub fn load(fd: Rc<RefCell<File>>, index: u32) -> Result<Self> {
        let mut bucket = Self::default();

        {
            let mut _fd = fd.as_ref().borrow_mut();
            bucket.index = index;
            _fd.seek(SeekFrom::Start((index as usize * PAGE_SIZE) as u64))?;
            _fd.read_exact(bucket.buf.borrow_mut())?;
        }

        bucket.bucket_type = bucket.get_bucket_type();
        bucket.fd = Some(fd);
        bucket.init_layout();
        Ok(bucket)
    }

    fn mark_dirty(&mut self) {
        self.dirty = true
    }

    fn get_bucket_type(&self) -> BucketType {
        let u = self.buf[0];
        if u & 0x01 == 1 {
            BucketType::META
        } else {
            if u & 0x02 > 0 {
                BucketType::INTERNAL
            } else {
                BucketType::LEAF
            }
        }
    }

    pub fn root_index(&self) -> u32 {
        match self.bucket_type {
            BucketType::META => u32::decode(&self.buf[4..]).unwrap().0,
            _ => panic!("not a meta bucket")
        }
    }

    pub fn total_buckets(&self) -> u32 {
        match self.bucket_type {
            BucketType::META => u32::decode(&self.buf[8..]).unwrap().0,
            _ => panic!("not a meta bucket")
        }
    }

    pub fn set_root_index(&mut self, root_index: u32) {
        match self.bucket_type {
            BucketType::META => {
                root_index.encode(&mut self.buf[4..]).unwrap();
                self.mark_dirty();
            }
            _ => panic!("not a meta bucket")
        }
    }

    pub fn set_total_bucket(&mut self, total_bucket: u32) {
        match self.bucket_type {
            BucketType::META => {
                total_bucket.encode(&mut self.buf[8..]).unwrap();
                self.mark_dirty();
            },
            _ => panic!("not a meta bucket")
        }
    }

    pub fn item_count(&self) -> usize {
        match self.bucket_type {
            BucketType::INTERNAL | BucketType::LEAF => u32::decode(&self.buf[4..]).unwrap().0 as usize,
            _ => panic!("not a meta bucket")
        }
    }

    pub fn is_full(&self) -> bool {
        assert_ne!(self.bucket_type, BucketType::META);
        self.item_count() >= self.max_item_count
    }

    pub fn set_item_count(&mut self, item_count: usize) -> Result<()>{
        match self.bucket_type {
            BucketType::INTERNAL | BucketType::LEAF=> {
                if item_count > self.max_item_count {
                    Err(BucketError::Full.into())
                } else {
                    (item_count as u32).encode(&mut self.buf[4..]).unwrap();
                    self.mark_dirty();
                    Ok(())
                }
            },
            _ => panic!("not a meta bucket")
        }
    }

    pub fn key_at(&self, i: usize) -> Option<K> {
        match self.bucket_type {
            BucketType::INTERNAL | BucketType::LEAF=> {
                if i >= self.item_count() {
                    None
                } else {
                    K::decode(&self.buf[(self.keys_pos + i * K::bin_size())..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a internal / leaf bucket")
        }
    }

    pub fn value_at(&self, i: usize) -> Option<V> {
        match self.bucket_type {
            BucketType::LEAF => {
                if i >= self.item_count() {
                    None
                } else {
                    V::decode(&self.buf[(self.values_pos + i * V::bin_size())..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a leaf bucket")
        }
    }

    pub fn ptr_at(&self, i: usize) -> Option<u32> {
        match self.bucket_type {
            BucketType::INTERNAL=> {
                if i >= self.item_count() + 1 {
                    None
                } else {
                    u32::decode(&self.buf[(self.ptrs_pos + i * PTR_SIZE)..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a internal bucket")
        }
    }

    pub fn set_key_at(&mut self, i: usize, key: &K) -> Result<()> {
        match self.bucket_type {
            BucketType::INTERNAL | BucketType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                key.encode(&mut self.buf[(self.keys_pos + i * K::bin_size())..])?;
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a internal / leaf bucket")
        }
    }

    pub fn set_value_at(&mut self, i: usize, value: &V) -> Result<()> {
        match self.bucket_type {
            BucketType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                value.encode(&mut self.buf[(self.values_pos + i * V::bin_size())..])?;
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a leaf bucket")
        }
    }

    pub fn set_ptr_at(&mut self, i: usize, ptr: u32) -> Result<()> {
        match self.bucket_type {
            BucketType::INTERNAL => {
                if i >= self.item_count() + 1 {
                    return Err(anyhow!("over size"))
                }
                ptr.encode(&mut self.buf[(self.ptrs_pos + i * PTR_SIZE)..])?;
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a internal bucket")
        }
    }

    pub fn find(&self, k: &K) -> Option<(usize, Pos)> {
        let item_count = self.item_count();
        if item_count == 0 {
            return None;
        }
        let mut min = 0;
        let mut max = item_count - 1;
        let mut mid;
        while min <= max {
            mid = (min + max) / 2;
            let mid_key = self.key_at(mid).unwrap();
            if mid_key == *k {
                return Some((mid, Pos::Current));
            } else if *k > mid_key {
                if mid == item_count - 1 || self.key_at(mid + 1).unwrap() > *k {
                    return Some((mid, Pos::Right));
                }
                min = mid + 1
            } else if *k < mid_key {
                if mid == 0 {
                    return Some((mid, Pos::Left));
                }
                max = mid - 1
            }
        }

        None
    }

    pub fn insert(&mut self, k: &K, v: &V) -> Result<()> {
        assert_eq!(self.bucket_type, BucketType::LEAF);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_value_at(0, v)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(self.keys_pos);
                            let value_ptr = buf_ptr.add(self.values_pos);
                            std::ptr::copy(key_ptr.add(i * K::bin_size()), key_ptr.add((i + 1) * K::bin_size()), (old_item_count - i) * K::bin_size());
                            std::ptr::copy(value_ptr.add(i * V::bin_size()), value_ptr.add((i + 1) * V::bin_size()), (old_item_count - i) * V::bin_size());
                        }
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(self.keys_pos);
                            let value_ptr = buf_ptr.add(self.values_pos);
                            std::ptr::copy(key_ptr.add((i + 1) * K::bin_size()), key_ptr.add((i + 2) * K::bin_size()), (old_item_count - i - 1) * K::bin_size());
                            std::ptr::copy(value_ptr.add((i + 1) * V::bin_size()), value_ptr.add((i + 2) * V::bin_size()), (old_item_count - i - 1) * V::bin_size());
                        }

                        self.set_key_at(i + 1, k)?;
                        self.set_value_at(i + 1, v)?;
                    }
                }
            }
        }
        self.mark_dirty();
        Ok(())
    }

    pub fn insert_ptr(&mut self, k: &K, ptr: u32) -> Result<()> {
        assert_eq!(self.bucket_type, BucketType::INTERNAL);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                assert!(self.ptr_at(0).unwrap() > 0);
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_ptr_at(1, ptr)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(self.keys_pos);
                            let ptr_ptr = buf_ptr.add(self.ptrs_pos);
                            std::ptr::copy(key_ptr.add(i * K::bin_size()), key_ptr.add((i + 1) * K::bin_size()), (old_item_count - i) * K::bin_size());
                            std::ptr::copy(ptr_ptr.add((i + 1) * PTR_SIZE), ptr_ptr.add((i + 2) * PTR_SIZE), (old_item_count - i) * PTR_SIZE);
                        }

                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(self.keys_pos);
                            let ptr_ptr = buf_ptr.add(self.ptrs_pos);
                            std::ptr::copy(key_ptr.add((i + 1) * K::bin_size()), key_ptr.add((i + 2) * K::bin_size()), (old_item_count - i -1) * K::bin_size());
                            std::ptr::copy(ptr_ptr.add((i + 2) * PTR_SIZE), ptr_ptr.add((i + 3) * PTR_SIZE), (old_item_count - i - 1) * PTR_SIZE);
                        }

                        self.set_key_at(i + 1, k)?;
                        self.set_ptr_at(i + 2, ptr)?;
                    }
                }
            }
        }
        self.mark_dirty();
        Ok(())
    }
}

impl<K,V> Debug for Bucket<K, V> where
    K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
    V: Encodable + Decodable + BinSizer + Debug + Clone
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.bucket_type {
            BucketType::META => {
                f.write_fmt(format_args!("{:?}; root index:{}; total buckets: {}", self.bucket_type, self.root_index(), self.total_buckets()))?;
            }
            BucketType::LEAF => {
                f.write_fmt(format_args!("{:?}; item count:{};\n", self.bucket_type, self.item_count()))?;
                for i in 0..self.item_count() {
                    f.write_fmt(format_args!("#{} {:?}: {:?}\n", i, self.key_at(i).unwrap(), self.value_at(i).unwrap()))?;
                }
            }
            BucketType::INTERNAL => {
                f.write_fmt(format_args!("{:?}; item count:{};\n", self.bucket_type, self.item_count()))?;
                f.write_fmt(format_args!("#_ _: {}\n", self.ptr_at(0).unwrap()))?;
                for i in 0..self.item_count() {
                    f.write_fmt(format_args!("#{} {:?}: {}\n", i, self.key_at(i).unwrap(), self.ptr_at(i + 1).unwrap()))?;
                }
            }
        }
        Ok(())
    }
}

impl<K, V> Bucket<K, V> {
    pub fn sync(&mut self) -> Result<()> {
        if self.dirty {
            let mut fd = self.fd.as_ref().unwrap().as_ref().borrow_mut();
            fd.seek(SeekFrom::Start((self.index as usize * PAGE_SIZE) as u64))?;
            fd.write_all(self.buf.borrow())?;
            self.dirty = false;
        }
        Ok(())
    }
}

impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        self.sync().unwrap();
    }
}