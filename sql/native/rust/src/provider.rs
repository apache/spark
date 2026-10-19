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
    BatchReader, BatchReaderFactory, BatchSource, BatchWrite, BatchWriteSource, DataSource,
    DataWriterFactory, Result, ScanBuilder, ScanPlan, SourceCapability, TaskContext, TaskWriter,
    WriteMode,
};

pub trait Provider: Send + 'static {
    type Source: DataSource;
    type Scan: ScanBuilder;
    type Write: BatchWrite;
    const READ: bool;
    const WRITE: bool;
    const CAPABILITIES: &'static [SourceCapability];

    fn scan(source: &Self::Source, schema: SchemaRef) -> Result<Self::Scan>;
    fn write(source: &Self::Source, schema: SchemaRef, mode: WriteMode) -> Result<Self::Write>;
}

pub struct ReadOnly<S>(PhantomData<S>);
pub struct WriteOnly<S, const TRUNCATE: bool>(PhantomData<S>);
pub struct ReadWrite<S, const TRUNCATE: bool>(PhantomData<S>);

impl<S: BatchSource> Provider for ReadOnly<S> {
    type Source = S;
    type Scan = S::Scan;
    type Write = Unavailable;
    const READ: bool = true;
    const WRITE: bool = false;
    const CAPABILITIES: &'static [SourceCapability] = &[SourceCapability::BatchRead];

    fn scan(source: &S, schema: SchemaRef) -> Result<Self::Scan> {
        source.scan(schema)
    }

    fn write(_: &S, _: SchemaRef, _: WriteMode) -> Result<Self::Write> {
        Err("batch writes are not registered".into())
    }
}

impl<S: BatchWriteSource, const TRUNCATE: bool> Provider for WriteOnly<S, TRUNCATE> {
    type Source = S;
    type Scan = Unavailable;
    type Write = S::Write;
    const READ: bool = false;
    const WRITE: bool = true;
    const CAPABILITIES: &'static [SourceCapability] = if TRUNCATE {
        &[SourceCapability::BatchWrite, SourceCapability::Truncate]
    } else {
        &[SourceCapability::BatchWrite]
    };

    fn scan(_: &S, _: SchemaRef) -> Result<Self::Scan> {
        Err("batch reads are not registered".into())
    }

    fn write(source: &S, schema: SchemaRef, mode: WriteMode) -> Result<Self::Write> {
        source.write(schema, mode)
    }
}

impl<S: BatchSource + BatchWriteSource, const TRUNCATE: bool> Provider for ReadWrite<S, TRUNCATE> {
    type Source = S;
    type Scan = S::Scan;
    type Write = S::Write;
    const READ: bool = true;
    const WRITE: bool = true;
    const CAPABILITIES: &'static [SourceCapability] = if TRUNCATE {
        &[
            SourceCapability::BatchRead,
            SourceCapability::BatchWrite,
            SourceCapability::Truncate,
        ]
    } else {
        &[SourceCapability::BatchRead, SourceCapability::BatchWrite]
    };

    fn scan(source: &S, schema: SchemaRef) -> Result<Self::Scan> {
        source.scan(schema)
    }

    fn write(source: &S, schema: SchemaRef, mode: WriteMode) -> Result<Self::Write> {
        source.write(schema, mode)
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

impl BatchReaderFactory for Unavailable {
    type Partition = ();

    fn open<'task>(&self, _: (), _: TaskContext<'task>) -> Result<BatchReader<'task>> {
        Err("batch reads are not registered".into())
    }
}

impl BatchWrite for Unavailable {
    type Factory = Self;

    fn factory(&mut self) -> Result<Self> {
        Err("batch writes are not registered".into())
    }

    fn commit(&mut self, _: Vec<()>) -> Result<()> {
        Err("batch writes are not registered".into())
    }

    fn abort(&mut self, _: Vec<Option<()>>) -> Result<()> {
        Err("batch writes are not registered".into())
    }
}

impl DataWriterFactory for Unavailable {
    type Message = ();

    fn open<'task>(&self, _: TaskContext<'task>) -> Result<TaskWriter<'task, ()>> {
        Err("batch writes are not registered".into())
    }
}
