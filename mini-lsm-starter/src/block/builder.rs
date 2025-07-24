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

use bytes::BufMut;

use super::Block;
use crate::key::{KeySlice, KeyVec};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let mut size = self.offsets.len() + self.data.len() + 2;
        size += 6 + key.len() + value.len();

        if size >= self.block_size && !self.is_empty() {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        let key_len = key.len() as u16;
        let val_len = value.len() as u16;
        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&key_len.to_ne_bytes());
        self.data.extend_from_slice(key.into_inner());
        self.data.extend_from_slice(&val_len.to_ne_bytes());
        self.data.extend_from_slice(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut data = Vec::new();
        data.extend_from_slice(&self.data);
        data.extend_from_slice(
            &self
                .offsets
                .iter()
                .flat_map(|&v| v.to_ne_bytes())
                .collect::<Vec<u8>>(),
        );
        data.put_u16_ne(self.offsets.len() as u16);
        Block::decode(&data)
    }
}
