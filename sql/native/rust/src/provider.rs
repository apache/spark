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

//! Compile-time registration of supported callback families.

use std::marker::PhantomData;

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    BatchReadSource, BatchWrite, BatchWriteSource, Capabilities, DataSource, DataWriterFactory,
    PartitionReader, PartitionReaderFactory, PartitionWriter, ReaderContext, Result, ScanBuilder,
    ScanPlan, WriteInfo, WritePlan, WriterContext,
};

pub trait Provider: Send + 'static {
    type Source: DataSource;
    type ScanBuilder: ScanBuilder;
    type BatchWrite: BatchWrite;
    const READ: bool;
    const WRITE: bool;
    const CAPABILITIES: Capabilities;

    fn new_scan(source: &Self::Source, schema: SchemaRef) -> Result<Self::ScanBuilder>;
    fn new_write(source: &Self::Source, info: WriteInfo) -> Result<WritePlan<Self::BatchWrite>>;
}

pub struct ReadOnly<S>(PhantomData<S>);
pub struct WriteOnly<S>(PhantomData<S>);
pub struct ReadWrite<S>(PhantomData<S>);

impl<S: BatchReadSource> Provider for ReadOnly<S> {
    type Source = S;
    type ScanBuilder = S::ScanBuilder;
    type BatchWrite = Unavailable;
    const READ: bool = true;
    const WRITE: bool = false;
    const CAPABILITIES: Capabilities = Capabilities::registered(true, false, false);

    fn new_scan(source: &S, schema: SchemaRef) -> Result<Self::ScanBuilder> {
        source.new_scan(schema)
    }

    fn new_write(_: &S, _: WriteInfo) -> Result<WritePlan<Self::BatchWrite>> {
        Err("batch writes are not registered".into())
    }
}

impl<S: BatchWriteSource> Provider for WriteOnly<S> {
    type Source = S;
    type ScanBuilder = Unavailable;
    type BatchWrite = S::BatchWrite;
    const READ: bool = false;
    const WRITE: bool = true;
    const CAPABILITIES: Capabilities = Capabilities::registered(false, true, S::SUPPORTS_TRUNCATE);

    fn new_scan(_: &S, _: SchemaRef) -> Result<Self::ScanBuilder> {
        Err("batch reads are not registered".into())
    }

    fn new_write(source: &S, info: WriteInfo) -> Result<WritePlan<Self::BatchWrite>> {
        source.new_write(info)
    }
}

impl<S: BatchReadSource + BatchWriteSource> Provider for ReadWrite<S> {
    type Source = S;
    type ScanBuilder = <S as BatchReadSource>::ScanBuilder;
    type BatchWrite = <S as BatchWriteSource>::BatchWrite;
    const READ: bool = true;
    const WRITE: bool = true;
    const CAPABILITIES: Capabilities = Capabilities::registered(true, true, S::SUPPORTS_TRUNCATE);

    fn new_scan(source: &S, schema: SchemaRef) -> Result<Self::ScanBuilder> {
        source.new_scan(schema)
    }

    fn new_write(source: &S, info: WriteInfo) -> Result<WritePlan<Self::BatchWrite>> {
        source.new_write(info)
    }
}

// Keeps the adapter generic without requiring connectors to implement operations
// they do not support. Corresponding C function slots are always null.
#[derive(Serialize, Deserialize)]
pub struct Unavailable;

impl ScanBuilder for Unavailable {
    type Factory = Self;

    fn plan(self) -> Result<ScanPlan<Self>> {
        Err("batch reads are not registered".into())
    }
}

impl PartitionReaderFactory for Unavailable {
    type Partition = ();

    fn open<'task>(&self, _: (), _: ReaderContext<'task>) -> Result<PartitionReader<'task>> {
        Err("batch reads are not registered".into())
    }
}

impl BatchWrite for Unavailable {
    type Factory = Self;

    fn commit(&mut self, _: Vec<()>) -> Result<()> {
        Err("batch writes are not registered".into())
    }

    fn abort(&mut self, _: Vec<Option<()>>) -> Result<()> {
        Err("batch writes are not registered".into())
    }
}

impl DataWriterFactory for Unavailable {
    type Message = ();

    fn open<'task>(&self, _: WriterContext<'task>) -> Result<PartitionWriter<'task, ()>> {
        Err("batch writes are not registered".into())
    }
}
