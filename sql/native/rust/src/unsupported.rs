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

//! The type of the operations that a source does not implement.

use serde::{Deserialize, Serialize};

use crate::{
    BatchWrite, DataWriterFactory, MicroBatchStream, PartitionReader, PartitionReaderFactory,
    PartitionWriter, ReaderContext, Result, ScanBuilder, ScanPlan, StreamingWrite, WriterContext,
};

/// The type of an operation that a source does not implement, as in
/// `type BatchWrite = Unsupported;`. It has no values, so none of the methods of its
/// trait implementations can be called.
#[derive(Debug, Serialize, Deserialize)]
pub enum Unsupported {}

impl ScanBuilder for Unsupported {
    type Factory = Self;

    const SUPPORTED: bool = false;

    fn plan(self) -> Result<ScanPlan<Self>> {
        match self {}
    }
}

impl PartitionReaderFactory for Unsupported {
    type Partition = Self;

    fn open<'task>(&self, _: Self, _: ReaderContext<'task>) -> Result<PartitionReader<'task>> {
        match *self {}
    }
}

impl MicroBatchStream for Unsupported {
    type Offset = Self;
    type Factory = Self;

    const SUPPORTED: bool = false;

    fn initial_offset(&mut self) -> Result<Self> {
        match *self {}
    }

    fn latest_offset(&mut self) -> Result<Self> {
        match *self {}
    }

    fn plan(&mut self, _: Self, _: Self) -> Result<ScanPlan<Self>> {
        match *self {}
    }
}

impl BatchWrite for Unsupported {
    type Factory = Self;
    type Message = Self;

    const SUPPORTED: bool = false;

    fn writer_factory(&self) -> Result<Self> {
        match *self {}
    }

    fn commit(&mut self, _: Vec<Self>) -> Result<()> {
        match *self {}
    }

    fn abort(&mut self, _: Vec<Option<Self>>) -> Result<()> {
        match *self {}
    }
}

impl DataWriterFactory for Unsupported {
    type Message = Self;

    fn open<'task>(&self, _: WriterContext<'task>) -> Result<PartitionWriter<'task, Self>> {
        match *self {}
    }
}

impl StreamingWrite for Unsupported {
    type Factory = Self;
    type Message = Self;

    const SUPPORTED: bool = false;

    fn writer_factory(&self) -> Result<Self> {
        match *self {}
    }

    fn commit(&mut self, _: i64, _: Vec<Self>) -> Result<()> {
        match *self {}
    }

    fn abort(&mut self, _: i64, _: Vec<Option<Self>>) -> Result<()> {
        match *self {}
    }
}
