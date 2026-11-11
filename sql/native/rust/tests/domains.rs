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

//! The domains that Spark pushes down to a scan through `push_domains`.

use std::ffi::{c_int, c_void, CStr, CString};
use std::mem::size_of;
use std::ops::Bound;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use serde::{Deserialize, Serialize};
use spark_datasource::__private::abi::{
    ArrowSchema, SparkDsCreateInfo, SparkDsDomain, SparkDsHost, SparkDsLibrary, SparkDsReaderInfo,
    ABI_VERSION,
};
use spark_datasource::{
    CreateInfo, DataSource, Domain, PartitionReader, PartitionReaderFactory, ReaderContext, Result,
    ScanBuilder, ScanInfo, ScanPlan, Unsupported, Value, ValueRange,
};

// The tests share the domains that the last scan got.
static TEST_LOCK: Mutex<()> = Mutex::new(());
static PUSHED: Mutex<Vec<Domain>> = Mutex::new(Vec::new());

struct Source {
    mode: String,
}

impl DataSource for Source {
    type ScanBuilder = Builder;
    type MicroBatchStream = Unsupported;
    type BatchWrite = Unsupported;
    type StreamingWrite = Unsupported;

    const INFERS_SCHEMA: bool = false;

    fn new(info: CreateInfo) -> Result<Self> {
        let mode = info.options.get("mode").cloned().unwrap_or_default();
        Ok(Self { mode })
    }

    fn new_scan(&self, _: ScanInfo) -> Result<Builder> {
        Ok(Builder {
            mode: self.mode.clone(),
        })
    }
}

struct Builder {
    mode: String,
}

impl ScanBuilder for Builder {
    type Factory = Factory;

    fn push_domains(&mut self, domains: &[Domain]) -> Result<Vec<bool>> {
        *PUSHED.lock().unwrap() = domains.to_vec();
        match self.mode.as_str() {
            "one_result" => Ok(vec![true]),
            // Uses the domains that constrain the values, not only their nulls.
            _ => Ok(domains
                .iter()
                .map(|domain| domain.ranges.is_some())
                .collect()),
        }
    }

    fn plan(self) -> Result<ScanPlan<Factory>> {
        Ok(ScanPlan::new(Factory, vec![]))
    }
}

#[derive(Serialize, Deserialize)]
struct Factory;

impl PartitionReaderFactory for Factory {
    type Partition = ();

    fn open<'task>(&self, _: (), _: ReaderContext<'task>) -> Result<PartitionReader<'task>> {
        Err("domain tests do not read partitions".into())
    }
}

spark_datasource::export_data_source!(Source);

extern "C" {
    fn spark_ds_init(host: *const SparkDsHost, library: *mut *const SparkDsLibrary) -> c_int;
}

fn library() -> &'static SparkDsLibrary {
    // Spark's host structure stays valid until the JVM exits.
    let host = Box::leak(Box::new(SparkDsHost {
        size: size_of::<SparkDsHost>() as i64,
        abi_version: ABI_VERSION,
        spark_version: c"test".as_ptr(),
        log: None,
        is_log_enabled: None,
    }));
    let mut table = ptr::null();
    assert_eq!(unsafe { spark_ds_init(host, &mut table) }, 0);
    unsafe { &*table }
}

fn error(library: &SparkDsLibrary) -> String {
    let message = unsafe { library.last_error.unwrap()() };
    assert!(!message.is_null());
    unsafe { CStr::from_ptr(message) }.to_str().unwrap().into()
}

/// The columns `id BIGINT, label STRING, point STRUCT<x: INT, y: STRING>, score DOUBLE`, and
/// `tags ARRAY<BIGINT>`, which a scan can read but whose values have no `Value` in the SDK.
fn schema() -> Schema {
    let point = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("point", DataType::Struct(point), true),
        Field::new("score", DataType::Float64, true),
        Field::new("tags", DataType::new_list(DataType::Int64, true), true),
    ])
}

/// The ranges of a domain, as Spark exports them: a struct array with the bounds and whether
/// each is included.
fn ranges(low: ArrayRef, high: ArrayRef, inclusive: &[(bool, bool)]) -> StructArray {
    let flags = |index: fn(&(bool, bool)) -> bool| {
        let flags: Vec<bool> = inclusive.iter().map(index).collect();
        Arc::new(BooleanArray::from(flags)) as ArrayRef
    };
    StructArray::from(vec![
        (
            Arc::new(Field::new("low", low.data_type().clone(), true)),
            low,
        ),
        (
            Arc::new(Field::new("high", high.data_type().clone(), true)),
            high,
        ),
        (
            Arc::new(Field::new("low_inclusive", DataType::Boolean, false)),
            flags(|flags| flags.0),
        ),
        (
            Arc::new(Field::new("high_inclusive", DataType::Boolean, false)),
            flags(|flags| flags.1),
        ),
    ])
}

/// A domain that Spark passes, with the Arrow structs of its ranges, which Spark releases after
/// the call unless the library moved them.
struct Exported {
    path: Vec<i32>,
    null_allowed: bool,
    nan_allowed: bool,
    ranges: Option<(Box<FFI_ArrowArray>, Box<ArrowSchema>)>,
}

impl Exported {
    fn new(path: &[i32], null_allowed: bool, ranges: Option<StructArray>) -> Self {
        let ranges = ranges.map(|ranges| {
            let data = ranges.into_data();
            let schema = ArrowSchema::try_from(data.data_type()).unwrap();
            (Box::new(FFI_ArrowArray::new(&data)), Box::new(schema))
        });
        Self {
            path: path.to_vec(),
            null_allowed,
            nan_allowed: false,
            ranges,
        }
    }

    fn domain(&mut self) -> SparkDsDomain {
        let (ranges, ranges_schema) = match &mut self.ranges {
            Some((array, schema)) => (&mut **array as *mut _, &mut **schema as *mut _),
            None => (ptr::null_mut(), ptr::null_mut()),
        };
        SparkDsDomain {
            size: size_of::<SparkDsDomain>() as i64,
            path: self.path.as_ptr(),
            path_length: self.path.len() as i64,
            null_allowed: self.null_allowed,
            nan_allowed: self.nan_allowed,
            ranges,
            ranges_schema,
        }
    }
}

/// Creates a scan of [`schema`] by a source of `mode`, and pushes the domains down to it.
/// Returns whether the scan uses each domain, and the error of push_domains, if it fails.
fn push(
    library: &SparkDsLibrary,
    mode: &str,
    exported: &mut [Exported],
) -> (Vec<bool>, Option<String>) {
    let mode = CString::new(mode).unwrap();
    let (keys, values) = ([c"mode".as_ptr()], [mode.as_ptr()]);
    let info = SparkDsCreateInfo {
        size: size_of::<SparkDsCreateInfo>() as i64,
        name: c"domain_test".as_ptr(),
        option_keys: keys.as_ptr(),
        option_values: values.as_ptr(),
        num_options: 1,
    };
    let (mut mask, mut source) = (0, ptr::null_mut());
    assert_eq!(
        unsafe { library.create.unwrap()(&info, &mut mask, &mut source) },
        0
    );
    let mut schema = ArrowSchema::try_from(&schema()).unwrap();
    let reader_info = SparkDsReaderInfo {
        size: size_of::<SparkDsReaderInfo>() as i64,
    };
    let mut reader: *mut c_void = ptr::null_mut();
    let status =
        unsafe { library.create_reader.unwrap()(source, &mut schema, &reader_info, &mut reader) };
    assert_eq!(status, 0, "{}", error(library));

    let domains: Vec<SparkDsDomain> = exported.iter_mut().map(Exported::domain).collect();
    let pointers: Vec<*const SparkDsDomain> = domains.iter().map(ptr::from_ref).collect();
    let mut used = vec![false; domains.len()];
    let push_domains = library.push_domains.unwrap();
    let status = unsafe {
        push_domains(
            reader,
            pointers.as_ptr(),
            pointers.len() as i64,
            used.as_mut_ptr(),
        )
    };
    // Read the error before the next call of the library replaces it.
    let failure = (status != 0).then(|| error(library));
    unsafe {
        library.close_reader.unwrap()(reader);
        library.close.unwrap()(source);
    }
    (used, failure)
}

#[test]
fn passes_the_domains_by_the_names_of_their_columns_with_their_values() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    // id < 5 OR id >= 10, the field x of point, which may not be null, and label LIKE 'ab%'.
    let ids = ranges(
        Arc::new(Int64Array::from(vec![None, Some(10)])),
        Arc::new(Int64Array::from(vec![Some(5), None])),
        &[(false, false), (true, false)],
    );
    let labels = ranges(
        Arc::new(StringArray::from(vec!["ab"])),
        Arc::new(StringArray::from(vec!["ac"])),
        &[(true, false)],
    );
    let mut exported = [
        Exported::new(&[0], false, Some(ids)),
        Exported::new(&[2, 0], false, None),
        Exported::new(&[1], true, Some(labels)),
    ];
    let (used, failure) = push(library, "", &mut exported);
    assert_eq!(failure, None);
    assert_eq!(used, [true, false, true]);

    let string = |value: &str| Value::String(value.into());
    let mut label = Domain::new(
        vec!["label".into()],
        Some(vec![ValueRange {
            low: Bound::Included(string("ab")),
            high: Bound::Excluded(string("ac")),
        }]),
    );
    label.null_allowed = true;
    let expected = [
        Domain::new(
            vec!["id".into()],
            Some(vec![
                ValueRange {
                    low: Bound::Unbounded,
                    high: Bound::Excluded(Value::Integer(5)),
                },
                ValueRange {
                    low: Bound::Included(Value::Integer(10)),
                    high: Bound::Unbounded,
                },
            ]),
        ),
        Domain::new(vec!["point".into(), "x".into()], None),
        label,
    ];
    assert_eq!(*PUSHED.lock().unwrap(), expected);
    // The SDK moved the ranges to read them, so Spark's release of the structs does nothing.
    for domain in [&exported[0], &exported[2]] {
        assert!(domain.ranges.as_ref().unwrap().0.is_released());
    }
}

#[test]
fn passes_nan_and_skips_the_domains_of_columns_without_values() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    // id = 3, tags > array(1), and score > 10.0, which NaN satisfies.
    let ids = ranges(
        Arc::new(Int64Array::from(vec![3])),
        Arc::new(Int64Array::from(vec![3])),
        &[(true, true)],
    );
    let list = |values: Option<Vec<Option<i64>>>| {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([values])) as ArrayRef
    };
    let tags = ranges(list(Some(vec![Some(1)])), list(None), &[(false, false)]);
    let scores = ranges(
        Arc::new(Float64Array::from(vec![Some(10.0)])),
        Arc::new(Float64Array::from(vec![None::<f64>])),
        &[(false, false)],
    );
    let mut score = Exported::new(&[3], false, Some(scores));
    score.nan_allowed = true;
    let mut exported = [
        Exported::new(&[0], false, Some(ids)),
        Exported::new(&[4], false, Some(tags)),
        score,
    ];
    let (used, failure) = push(library, "", &mut exported);
    assert_eq!(failure, None);
    assert_eq!(used, [true, false, true]);

    let mut score = Domain::new(
        vec!["score".into()],
        Some(vec![ValueRange {
            low: Bound::Excluded(Value::Float(10.0)),
            high: Bound::Unbounded,
        }]),
    );
    score.nan_allowed = true;
    let expected = [
        Domain::new(
            vec!["id".into()],
            Some(vec![ValueRange::single(Value::Integer(3))]),
        ),
        score,
    ];
    assert_eq!(*PUSHED.lock().unwrap(), expected);

    // The scan is not called without a domain of a column that has values, such as that of
    // `tags IS NOT NULL`.
    let unchanged = vec![Domain::new(vec!["unchanged".into()], None)];
    *PUSHED.lock().unwrap() = unchanged.clone();
    let (used, failure) = push(library, "", &mut [Exported::new(&[4], false, None)]);
    assert_eq!((used, failure), (vec![false], None));
    assert_eq!(*PUSHED.lock().unwrap(), unchanged);
}

#[test]
fn the_default_push_domains_uses_no_domain() {
    struct Ignoring;

    impl ScanBuilder for Ignoring {
        type Factory = Factory;

        fn plan(self) -> Result<ScanPlan<Factory>> {
            Ok(ScanPlan::new(Factory, vec![]))
        }
    }

    let domains = [
        Domain::new(vec!["id".into()], None),
        Domain::new(vec!["label".into()], Some(vec![])),
    ];
    assert_eq!(Ignoring.push_domains(&domains).unwrap(), [false, false]);
}

#[test]
fn rejects_invalid_paths_and_a_wrong_number_of_results() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    for (path, message) in [
        (&[-1][..], "the path of a domain has a negative index -1"),
        (&[5], "the path [5] of a domain is not a column of the scan"),
        (
            &[0, 0],
            "the path [0, 0] of a domain is not a column of the scan",
        ),
    ] {
        let (_, failure) = push(library, "", &mut [Exported::new(path, true, None)]);
        assert_eq!(failure.as_deref(), Some(message));
    }
    let mut exported = [
        Exported::new(&[0], true, None),
        Exported::new(&[1], true, None),
    ];
    let (used, failure) = push(library, "one_result", &mut exported);
    assert_eq!(
        failure.as_deref(),
        Some("ScanBuilder::push_domains returned 1 results for 2 domains")
    );
    assert_eq!(used, [false, false]);
}
