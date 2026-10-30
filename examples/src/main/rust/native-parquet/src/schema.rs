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

//! The Arrow types that the source reads from a Parquet file, and the schema that Spark gets.

use std::fs::File;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::PageIndexPolicy;
use parquet::schema::types::SchemaDescriptor;
use spark_native_source::Result;

/// Loads the metadata of a file with the Arrow types that the source reads: the default
/// conversion of its Parquet schema, ignoring any Arrow schema that a writer embedded, except
/// for INT96 timestamps, strings, and binary values. Spark writes timestamps as INT96 by default,
/// which arrow-rs reads as nanoseconds without a time zone; the source reads them as the
/// microseconds since the epoch that they are to Spark, which also covers the years that
/// nanoseconds overflow. It reads strings and binary values as views, which refer to the values in
/// the pages that arrow-rs decompresses, or in their dictionaries, rather than copying each value
/// into the buffer of its batch. With `page_index`, also loads the column and offset indexes of the
/// pages, if the file has them.
pub(crate) fn load(file: &File, page_index: bool) -> Result<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new()
        .with_skip_arrow_metadata(true)
        .with_page_index_policy(PageIndexPolicy::from(page_index));
    let metadata = ArrowReaderMetadata::load(file, options)?;
    let mut leaf = 0;
    let fields: Fields = metadata
        .schema()
        .fields()
        .iter()
        .map(|field| read_field(field, metadata.parquet_schema(), &mut leaf))
        .collect();
    if fields == *metadata.schema().fields() {
        return Ok(metadata);
    }
    let schema = Schema::new_with_metadata(fields, metadata.schema().metadata().clone());
    let options = ArrowReaderOptions::new().with_schema(Arc::new(schema));
    Ok(ArrowReaderMetadata::try_new(
        metadata.metadata().clone(),
        options,
    )?)
}

/// Reads the INT96 columns in a field as microsecond timestamps in UTC, and its strings and
/// binary values as views. Arrow types have their leaves in the order of the Parquet columns,
/// which `leaf` counts.
fn read_field(field: &FieldRef, parquet: &SchemaDescriptor, leaf: &mut usize) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| read_field(field, parquet, leaf))
                .collect(),
        ),
        DataType::List(element) => DataType::List(read_field(element, parquet, leaf)),
        DataType::Map(entries, sorted) => {
            DataType::Map(read_field(entries, parquet, leaf), *sorted)
        }
        data_type => {
            let column = parquet.columns().get(*leaf);
            *leaf += 1;
            match (data_type, column) {
                (_, Some(column)) if column.physical_type() == PhysicalType::INT96 => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                }
                (DataType::Utf8, _) => DataType::Utf8View,
                (DataType::Binary, _) => DataType::BinaryView,
                (data_type, _) => data_type.clone(),
            }
        }
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

/// The schema that Spark reads from a file: its columns, nullable, as Spark reads the columns
/// of files, with strings and binary values, which the reader returns as views, in the Arrow
/// types that Spark gives them. Fails for a type that version 1 of the native ABI does not carry,
/// or that Spark reads differently, such as timestamps in other units than microseconds.
pub(crate) fn spark_schema(file: &Schema) -> Result<SchemaRef> {
    let fields: Result<Vec<FieldRef>> = file
        .fields()
        .iter()
        .map(|field| {
            check_type(field.data_type(), field.name())?;
            Ok(spark_field(field, true))
        })
        .collect();
    Ok(Arc::new(Schema::new(fields?)))
}

fn check_type(data_type: &DataType, column: &str) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::BinaryView
        | DataType::Date32
        | DataType::Decimal128(_, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(()),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| check_type(field.data_type(), column)),
        DataType::List(element) => check_type(element.data_type(), column),
        DataType::Map(entries, _) => check_type(entries.data_type(), column),
        other => Err(format!(
            "The column {column} contains the Arrow type {other}, which this source does not read"
        )
        .into()),
    }
}

/// The field that Spark gets for a field that the reader reads: with strings and binary values
/// instead of their views, and, if `nullable`, nullable at every level, except for the entries
/// of maps and their keys, which Arrow requires to be non-null.
fn spark_field(field: &FieldRef, nullable: bool) -> FieldRef {
    let child = |field| spark_field(field, nullable);
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::List(element) => DataType::List(child(element)),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(key_value) = entries.data_type() else {
                unreachable!("the entries of a map are a struct");
            };
            let key_value: Fields =
                vec![spark_field(&key_value[0], false), child(&key_value[1])].into();
            let entries = Field::new(entries.name(), DataType::Struct(key_value), false);
            DataType::Map(Arc::new(entries), *sorted)
        }
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        data_type => data_type.clone(),
    };
    let spark = field.as_ref().clone().with_data_type(data_type);
    Arc::new(if nullable {
        spark.with_nullable(true)
    } else {
        spark
    })
}
