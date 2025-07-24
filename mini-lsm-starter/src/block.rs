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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut ans = Vec::new();
        ans.extend_from_slice(&self.data[..]);
        let ptr = self.offsets.as_ptr().cast::<u8>();
        ans.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, self.offsets.len() * 2) });
        Bytes::from(ans)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_vec: Vec<u8>;
        let mut block = Self {
            data: [].to_vec(),
            offsets: [].to_vec(),
        };
        if data.is_empty() {
            block
        } else {
            let cnt: usize = u16::from_ne_bytes(*data.last_chunk::<2>().unwrap()) as usize;
            // println!("cnt {}", cnt);
            // cnt = (cnt << 8) + data[data.len() - 1] as usize;
            // println!("data len {}, cnt {}", data.len(), cnt);
            let upper_bound = data.len() - cnt * 2 - 2;
            // println!("upper bound {}", upper_bound);
            block.data.extend_from_slice(&data[..upper_bound]);
            // let ptr = data.as_ptr();
            // unsafe {
            //     let aling_size = std::mem::align_of::<u8>();
            //     println!("align_size {}",aling_size);
            //     println!("data {}", ptr.add(upper_bound) as usize);
            //     println!("ans {}", ptr.add(upper_bound) as usize % aling_size);
            //     println!("Value: ptr.add(upper_bound) {}", *ptr.add(upper_bound+1));
            // }
            let tail = &data[upper_bound..];
            let offsets: Vec<u16> = tail
                .chunks_exact(2)
                .take(cnt)
                .map(|s| u16::from_ne_bytes([s[0], s[1]]))
                .collect();
            block.offsets.extend_from_slice(&offsets);
            block
        }
    }
}
