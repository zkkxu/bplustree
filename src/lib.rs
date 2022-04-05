use std::fs::{File, OpenOptions};
use crate::bucket::{Bucket, BucketType, Pos, BucketError};
pub use crate::items::*;
use anyhow::Result;
use std::fmt::Debug;
use std::rc::Rc;
use std::cell::RefCell;

mod bucket;
mod items;

pub struct BPlustree<K, V>
{
    path: &'static str,
    fd: Rc<RefCell<File>>,
    meta_bucket: Option<Bucket<K, V>>,
    root_bucket: Option<Bucket<K, V>>
}

impl<K, V> BPlustree<K, V>
    where
        K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
        V: Encodable + Decodable + BinSizer + Debug + Clone
{
    pub fn new(path: &'static str) -> Self {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path).expect("could not open bplustree file");
        let mut bplustree = BPlustree::<K, V> {
            path,
            fd: Rc::new(RefCell::new(fd)),
            meta_bucket: None,
            root_bucket: None,
        };
        let file_len = bplustree.fd.as_ref().borrow().metadata().unwrap().len();
        if file_len == 0 {
            bplustree.init(true)
        } else {
            bplustree.init(false)
        }
        bplustree
    }

    fn sync(&mut self) -> Result<()>{
        if let Some(p) = self.meta_bucket.as_mut() {
            p.sync()?;
        }
        if let Some(p) = self.root_bucket.as_mut() {
            p.sync()?;
        }
        Ok(())
    }

    fn init(&mut self, empty: bool){
        if empty {
            println!("init empty bplustree");
            let mut meta_bucket = Bucket::<K, V>::new(self.fd.clone(), 0, BucketType::META).unwrap();
            meta_bucket.set_total_bucket(2);
            meta_bucket.set_root_index(1);
            let mut root_bucket = Bucket::<K, V>::new(self.fd.clone(), 1, BucketType::LEAF).unwrap();
            root_bucket.set_item_count(0).unwrap();

            self.meta_bucket = Some(meta_bucket);
            self.root_bucket = Some(root_bucket);
            self.sync().unwrap();
        } else {
            let meta_bucket = Bucket::<K, V>::load(self.fd.clone(), 0).unwrap();
            assert_eq!(meta_bucket.bucket_type, BucketType::META);

            let root_bucket = Bucket::<K, V>::load(self.fd.clone(), meta_bucket.root_index()).unwrap();
            println!("root bucket index: {}; total buckets:{}; root bucket keys: {};", meta_bucket.root_index(), meta_bucket.total_buckets(), root_bucket.item_count());
            self.meta_bucket = Some(meta_bucket);
            self.root_bucket = Some(root_bucket);
        }
    }

    pub fn set(&mut self, key: &K, value: &V) -> Result<()> {
        let mut p = self.root_bucket.as_mut().unwrap();
        let mut buckets = Vec::new();
        loop {
            match p.bucket_type {
                BucketType::INTERNAL => {
                    match p.find(key) {
                        Some((i, pos)) => {
                            let ptr_index = match pos {
                                Pos::Left => {
                                    i
                                }
                                _ => {
                                   i + 1
                                }
                            };
                            let child_bucket_index = p.ptr_at(ptr_index).unwrap();
                            buckets.push(Bucket::<K, V>::load(self.fd.clone(), child_bucket_index).unwrap());
                            let len = buckets.len();
                            p = &mut buckets[len - 1];
                        }
                        None => {
                            panic!("impossible for an empty internal bucket")
                        }
                    }
                }
                BucketType::LEAF => {
                    match p.insert(key, value) {
                        Ok(_) => {
                            return Ok(());
                        },
                        Err(err) => {
                            match err.downcast_ref::<BucketError>() {
                                Some(BucketError::Full) => {
                                    // eh..., the bucket is full, we need to split it
                                    break;
                                }
                                _ => {
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
                _ => {
                    panic!("impossible a meta bucket")
                }
            }
        }

        let mut kp = None;
        for p in buckets.iter_mut().rev() {
            match p.bucket_type {
                BucketType::LEAF => {
                    kp = Some(self.bucket_split(p, key, value)?);
                }
                BucketType::INTERNAL => {
                    let (k, ptr) = kp.unwrap();
                    if p.is_full() {
                        kp = Some(self.split_internal_bucket(p, &k, ptr)?);
                    } else {
                        p.insert_ptr(&k, ptr)?;
                        return Ok(());
                    }
                }
                _ => {
                    panic!("impossible a meta bucket")
                }
            }
        }

        match kp {
            Some((k, ptr)) => {
                let is_root_full;
                {
                    let root_bucket = self.root_bucket.as_mut().unwrap();
                    assert_eq!(root_bucket.bucket_type, BucketType::INTERNAL);
                    is_root_full = root_bucket.is_full();
                }

                if is_root_full {
                    let mut root_bucket = self.root_bucket.take().unwrap();
                    let (k2, ptr2) = self.split_internal_bucket(&mut root_bucket, &k, ptr)?;
                    let mut new_root_bucket = self.make_bucket(BucketType::INTERNAL)?;
                    new_root_bucket.set_item_count(1)?;
                    new_root_bucket.set_ptr_at(0, root_bucket.index)?;
                    new_root_bucket.set_key_at(0, &k2)?;
                    new_root_bucket.set_ptr_at(1, ptr2)?;

                    let meta_bucket = self.meta_bucket.as_mut().unwrap();
                    meta_bucket.set_root_index(new_root_bucket.index);
                    self.root_bucket = Some(new_root_bucket);
                } else {
                    let root_bucket = self.root_bucket.as_mut().unwrap();
                    root_bucket.insert_ptr(&k, ptr)?;
                }
            }
            None => {
                // root bucket is full, do split !!!
                let mut root_bucket = self.root_bucket.take().unwrap();
                assert!(root_bucket.is_full() && root_bucket.bucket_type == BucketType::LEAF);
                let (k, ptr) = self.bucket_split(&mut root_bucket, key, value)?;
                let mut new_root_bucket = self.make_bucket(BucketType::INTERNAL)?;
                new_root_bucket.set_item_count(1)?;
                new_root_bucket.set_ptr_at(0, root_bucket.index)?;
                new_root_bucket.set_key_at(0, &k)?;
                new_root_bucket.set_ptr_at(1, ptr)?;

                let meta_bucket = self.meta_bucket.as_mut().unwrap();
                meta_bucket.set_root_index(new_root_bucket.index);

                self.root_bucket = Some(new_root_bucket);
            }
        }
        self.sync()?;
        Ok(())
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        let mut p = self.root_bucket.as_ref().unwrap();
        let mut buckets = Vec::new();
        loop {
            // println!("{:?} {}", p.bucket_type, p.item_count());
            match p.find(key) {
                Some((i, pos)) => {
                    match p.bucket_type {
                        BucketType::LEAF => {
                            
                            return if pos == Pos::Current {
                                p.value_at(i)
                            } else {
                                None
                            }
                        }
                        BucketType::INTERNAL => {
                            match pos {
                                Pos::Left => {
                                    buckets.push(Bucket::<K, V>::load(self.fd.clone(), p.ptr_at(i).unwrap()).unwrap());
                                    p = &buckets[buckets.len() - 1];
                                }
                                _ => {
                                    buckets.push(Bucket::<K, V>::load(self.fd.clone(), p.ptr_at(i + 1).unwrap()).unwrap());
                                    p = &buckets[buckets.len() - 1];
                                }
                            }
                        }
                        _ => {
                            // impossible
                            return None
                        }
                    }
                },
                None => {
                    return None;
                }
            }
        }
    }

    fn make_bucket(&mut self, pt: BucketType) -> Result<Bucket<K, V>> {
        let meta_bucket = self.meta_bucket.as_mut().unwrap();
        let max_index = meta_bucket.total_buckets();
        meta_bucket.set_total_bucket(max_index + 1);
        Ok(Bucket::<K, V>::new(self.fd.clone(), max_index, pt)?)
    }

    fn bucket_split(&mut self, p: &mut Bucket<K, V>, key: &K, value: &V) -> Result<(K, u32)> {
        assert_eq!(p.bucket_type, BucketType::LEAF);
        let mut make_bucket = self.make_bucket(BucketType::LEAF)?;
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut inserted = false;
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                values.push(value.clone());
                inserted = true;
            }
            keys.push(k);
            values.push(p.value_at(i).unwrap())
        }
        if !inserted {
            keys.push(key.clone());
            values.push(value.clone());
            // inserted = true;
        }
        let cut_i =  (keys.len() + 1) / 2;
        p.set_item_count(cut_i)?;
        make_bucket.set_item_count(keys.len() - cut_i)?;

        for i in 0..cut_i {
            p.set_key_at(i, &keys[i])?;
            p.set_value_at(i, &values[i])?;
        }

        for i in cut_i..keys.len() {
            make_bucket.set_key_at(i - cut_i, &keys[i])?;
            make_bucket.set_value_at(i - cut_i, &values[i])?;
        }

        Ok((keys[cut_i].clone(), make_bucket.index))
    }

    fn split_internal_bucket(&mut self, p: &mut Bucket<K, V>, key: &K, ptr: u32) -> Result<(K, u32)> {
        assert_eq!(p.bucket_type, BucketType::INTERNAL);
        let mut make_bucket = self.make_bucket(BucketType::INTERNAL)?;
        let mut keys = Vec::new();
        let mut ptrs = Vec::new();
        let mut inserted = false;
        ptrs.push(p.ptr_at(0).unwrap());
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                ptrs.push(ptr);
                inserted = true;
            }
            keys.push(k);
            ptrs.push(p.ptr_at(i + 1).unwrap());
        }

        if !inserted {
            keys.push(key.clone());
            ptrs.push(ptr);
        }

        let up_i =  (keys.len() - 1) / 2;
        p.set_item_count(up_i)?;
        make_bucket.set_item_count(keys.len() - up_i - 1)?;

        for i in 0..up_i {
            p.set_key_at(i, &keys[i])?;
            p.set_ptr_at(i + 1, ptrs[i + 1])?;
        }

        make_bucket.set_ptr_at(0, ptrs[up_i + 1])?;
        for i in (up_i + 1)..keys.len() {
            make_bucket.set_key_at(i - up_i - 1, &keys[i])?;
            make_bucket.set_ptr_at(i - up_i, ptrs[i + 1])?;
        }
        Ok((keys[up_i].clone(), make_bucket.index))
    }
}
