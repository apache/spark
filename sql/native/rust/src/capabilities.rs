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
    /// Writes that replace the existing data: batch writes with the save mode "overwrite",
    /// and micro-batches with the output mode "complete". Requires `BATCH_WRITE` or
    /// `STREAMING_WRITE`: the SDK removes it from a set without them.
    pub const TRUNCATE: Self = Self(abi::TRUNCATE);

    const NAMES: [(Self, &'static str); 5] = [
        (Self::BATCH_READ, "BATCH_READ"),
        (Self::MICRO_BATCH_READ, "MICRO_BATCH_READ"),
        (Self::BATCH_WRITE, "BATCH_WRITE"),
        (Self::STREAMING_WRITE, "STREAMING_WRITE"),
        (Self::TRUNCATE, "TRUNCATE"),
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
                | abi::TRUNCATE,
        )
    }

    /// Whether this set contains every operation of `other`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Removes the operations of `other`, and also `TRUNCATE` when no write remains.
    #[must_use]
    pub const fn without(self, other: Self) -> Self {
        Self::from_bits(self.0 & !other.0)
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
        Self::from_bits(bits)
    }

    /// The operations in both sets.
    pub(crate) const fn intersection(self, other: Self) -> Self {
        Self::from_bits(self.0 & other.0)
    }

    /// The native capability mask, a combination of the SPARK_DS_* flags.
    pub(crate) const fn bits(self) -> u64 {
        self.0
    }

    // Truncation requires a write, so a set never keeps it without one.
    const fn from_bits(bits: u64) -> Self {
        if bits & (abi::BATCH_WRITE | abi::STREAMING_WRITE) == 0 {
            Self(bits & !abi::TRUNCATE)
        } else {
            Self(bits)
        }
    }
}

/// The operations of both sets, as in `Capabilities::BATCH_WRITE | Capabilities::STREAMING_WRITE`.
impl BitOr for Capabilities {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self::from_bits(self.0 | other.0)
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
    fn truncate_never_remains_without_writes() {
        let all = Capabilities::all();
        // Truncation remains while either write does.
        assert!(all
            .without(Capabilities::BATCH_WRITE)
            .contains(Capabilities::TRUNCATE));
        let reads = all
            .without(Capabilities::BATCH_WRITE)
            .without(Capabilities::STREAMING_WRITE);
        assert!(reads.contains(Capabilities::BATCH_READ));
        assert!(reads.contains(Capabilities::MICRO_BATCH_READ));
        assert!(!reads.contains(Capabilities::TRUNCATE));
        assert_eq!(
            all.without(Capabilities::BATCH_WRITE | Capabilities::STREAMING_WRITE),
            reads
        );
        // A set of truncation alone has no write to truncate.
        assert_eq!(
            Capabilities::TRUNCATE | Capabilities::empty(),
            Capabilities::empty()
        );
        let append_only = all.without(Capabilities::TRUNCATE);
        assert!(append_only.contains(Capabilities::BATCH_WRITE));
        assert!(!append_only.contains(Capabilities::TRUNCATE));
        assert_eq!(
            Capabilities::implemented(&[
                (Capabilities::BATCH_READ, true),
                (Capabilities::BATCH_WRITE, false),
                (Capabilities::TRUNCATE, true),
            ]),
            Capabilities::BATCH_READ
        );
        assert_eq!(
            Capabilities::TRUNCATE.intersection(all),
            Capabilities::empty()
        );
        assert!(all.contains(Capabilities::empty()));
    }

    #[test]
    fn debug_lists_operation_names() {
        assert_eq!(
            format!("{:?}", Capabilities::all()),
            "{BATCH_READ, MICRO_BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}"
        );
        assert_eq!(format!("{:?}", Capabilities::empty()), "{}");
    }
}
