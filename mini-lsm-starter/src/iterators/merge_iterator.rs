// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::{Ok, Result};
use nom::Err;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        let _ = iters.into_iter().enumerate().for_each(|(idx, iter)| {
            let wrapper = HeapWrapper(idx, iter);
            if wrapper.1.is_valid() {
                heap.push(wrapper);
            }
        });

        let mut current = None;

        if let Some(inner) = heap.peek_mut() {
            current = Some(PeekMut::pop(inner));
        }

        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let iter = self.current.as_ref().unwrap();
        iter.1.key()
    }

    fn value(&self) -> &[u8] {
        let iter = self.current.as_ref().unwrap();
        iter.1.value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(HeapWrapper(size, iter)) => {
                // println!("size: {}", size);
                iter.is_valid()
            }
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let mut key = KeySlice::from_slice(&[]);
        let mut buf = Vec::new();
        if let Some(mut iter) = self.current.take() {
            buf.extend_from_slice(iter.1.key().raw_ref());
            key = KeySlice::from_slice(&buf);
            iter.1.next()?;
            if iter.1.is_valid() {
                self.iters.push(iter);
            }
        }

        while let Some(mut inner) = self.iters.peek_mut() {
            if inner.1.key().le(&key) {
                while inner.1.key().le(&key) {
                    match inner.1.next() {
                        Err(e) => {
                            PeekMut::pop(inner);
                            return Err(e);
                        }
                        _ => {
                            if !inner.1.is_valid() {
                                PeekMut::pop(inner);
                                break;
                            }
                        }
                    }
                }
            } else {
                // println!("current assign");
                self.current = Some(PeekMut::pop(inner));
                break;
            }
        }
        Ok(())
    }
}
