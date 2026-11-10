// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The native operations of a source instance.

use std::fmt;
use std::ops::BitOr;

use crate::abi;

/// The native operations that a source instance supports with its effective options.
///
/// A source implements the operations whose types in [`DataSource`](crate::DataSource)
/// are not [`Unsupported`](crate::Unsupported). The SDK keeps only those of
/// [`DataSource::capabilities`](crate::DataSource::capabilities), which can therefore
/// remove some, but cannot enable an operation that the source does not implement.
///
/// These are native capabilities, not Spark's Data Source V2 routing flags or per-scan
/// pushdown results.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Capabilities(u64);

impl Capabilities {
    pub const BATCH_READ: Self = Self(abi::BATCH_READ);
    pub const MICRO_BATCH_READ: Self = Self(abi::MICRO_BATCH_READ);
    pub const BATCH_WRITE: Self = Self(abi::BATCH_WRITE);
    pub const STREAMING_WRITE: Self = Self(abi::STREAMING_WRITE);
    /// Batch writes with the save mode "overwrite", which replace the existing data. Requires
    /// `BATCH_WRITE`: the SDK does not declare it without it.
    pub const BATCH_TRUNCATE: Self = Self(abi::BATCH_TRUNCATE);
    /// Micro-batches with the output mode "complete", which replace the output of the previous
    /// ones. Requires `STREAMING_WRITE`: the SDK does not declare it without it.
    pub const STREAMING_TRUNCATE: Self = Self(abi::STREAMING_TRUNCATE);

    const NAMES: [(Self, &'static str); 6] = [
        (Self::BATCH_READ, "BATCH_READ"),
        (Self::MICRO_BATCH_READ, "MICRO_BATCH_READ"),
        (Self::BATCH_WRITE, "BATCH_WRITE"),
        (Self::STREAMING_WRITE, "STREAMING_WRITE"),
        (Self::BATCH_TRUNCATE, "BATCH_TRUNCATE"),
        (Self::STREAMING_TRUNCATE, "STREAMING_TRUNCATE"),
    ];

    /// No operations. A source without capabilities still supports schema discovery.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Every operation of this SDK, which
    /// [`DataSource::capabilities`](crate::DataSource::capabilities) returns by default.
    /// The SDK keeps only the operations that the source implements.
    pub const fn all() -> Self {
        Self(
            abi::BATCH_READ
                | abi::MICRO_BATCH_READ
                | abi::BATCH_WRITE
                | abi::STREAMING_WRITE
                | abi::BATCH_TRUNCATE
                | abi::STREAMING_TRUNCATE,
        )
    }

    /// Whether this set contains every operation of `other`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Removes the operations of `other`.
    #[must_use]
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// The operations that a source implements: those paired with true.
    pub(crate) const fn implemented(operations: &[(Self, bool)]) -> Self {
        let mut bits = 0;
        let mut index = 0;
        while index < operations.len() {
            if operations[index].1 {
                bits |= operations[index].0 .0;
            }
            index += 1;
        }
        Self(bits)
    }

    /// The operations in both sets.
    pub(crate) const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// The native capability mask, a combination of the SPARK_DS_* flags.
    pub(crate) const fn bits(self) -> u64 {
        self.0
    }
}

/// The operations of both sets, as in `Capabilities::BATCH_WRITE | Capabilities::STREAMING_WRITE`.
impl BitOr for Capabilities {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut set = formatter.debug_set();
        for (capability, name) in Self::NAMES {
            if self.contains(capability) {
                set.entry(&format_args!("{name}"));
            }
        }
        set.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Capabilities;

    #[test]
    fn sets_combine_and_remove_operations() {
        let all = Capabilities::all();
        let truncations = Capabilities::BATCH_TRUNCATE | Capabilities::STREAMING_TRUNCATE;
        let append_only = all.without(truncations);
        assert!(append_only.contains(Capabilities::BATCH_WRITE | Capabilities::STREAMING_WRITE));
        assert!(!append_only.contains(Capabilities::BATCH_TRUNCATE));
        assert!(!append_only.contains(Capabilities::STREAMING_TRUNCATE));
        let reads = Capabilities::BATCH_READ | Capabilities::MICRO_BATCH_READ;
        assert_eq!(
            all.without(reads).without(truncations),
            all.without(reads | truncations)
        );
        assert_eq!(
            Capabilities::implemented(&[
                (Capabilities::BATCH_READ, true),
                (Capabilities::BATCH_WRITE, false),
                (Capabilities::BATCH_TRUNCATE, true),
            ]),
            Capabilities::BATCH_READ | Capabilities::BATCH_TRUNCATE
        );
        assert_eq!(
            truncations.intersection(Capabilities::BATCH_TRUNCATE),
            Capabilities::BATCH_TRUNCATE
        );
        assert!(all.contains(Capabilities::empty()));
    }

    #[test]
    fn debug_lists_operation_names() {
        assert_eq!(
            format!("{:?}", Capabilities::all()),
            "{BATCH_READ, MICRO_BATCH_READ, BATCH_WRITE, STREAMING_WRITE, BATCH_TRUNCATE, \
             STREAMING_TRUNCATE}"
        );
        assert_eq!(format!("{:?}", Capabilities::empty()), "{}");
    }
}
