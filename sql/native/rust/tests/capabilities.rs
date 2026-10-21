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

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::__private::abi::{
    ArrowSchema, SparkDsCreateInfo, SparkDsHost, SparkDsLibrary, ABI_VERSION,
};
use spark_native_source::log::{self, LevelFilter};
use spark_native_source::{
    Capabilities, CreateInfo, DataSource, HostInfo, PartitionReader, PartitionReaderFactory,
    ReaderContext, Result, ScanBuilder, ScanInfo, ScanPlan, Unsupported,
};

static DECLARATIONS: AtomicUsize = AtomicUsize::new(0);
static SCHEMAS: AtomicUsize = AtomicUsize::new(0);
static SCANS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);
static INITIALIZED: Mutex<Vec<Option<String>>> = Mutex::new(Vec::new());
static LOGS: Mutex<Vec<(i32, String)>> = Mutex::new(Vec::new());

struct Source {
    mode: String,
}

impl DataSource for Source {
    type ScanBuilder = Builder;
    type BatchWrite = Unsupported;

    fn init(host: &HostInfo) -> Result<()> {
        INITIALIZED.lock().unwrap().push(host.spark_version.clone());
        if host.spark_version.as_deref() == Some("unsupported") {
            return Err("this library does not support the Spark version".into());
        }
        log::info!("capability test library initialized");
        Ok(())
    }

    fn new(info: CreateInfo) -> Result<Self> {
        if info.name != "capability_test" {
            return Err(format!("unexpected source name {}", info.name).into());
        }
        Ok(Self {
            mode: info.options.get("capabilities").cloned().unwrap(),
        })
    }

    fn capabilities(&self) -> Capabilities {
        DECLARATIONS.fetch_add(1, Ordering::SeqCst);
        match self.mode.as_str() {
            "empty" => Capabilities::empty(),
            "read" => Capabilities::BATCH_READ,
            "without_read" => Capabilities::all().without(Capabilities::BATCH_READ),
            // Operations that the source does not implement are dropped.
            "all" => Capabilities::all(),
            "panic" => panic!("capability discovery failed"),
            other => panic!("unexpected test mode: {other}"),
        }
    }

    fn schema(&self) -> Result<SchemaRef> {
        SCHEMAS.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Schema::empty()))
    }

    fn new_scan(&self, _: ScanInfo) -> Result<Builder> {
        SCANS.fetch_add(1, Ordering::SeqCst);
        Ok(Builder)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

struct Builder;

impl ScanBuilder for Builder {
    type Factory = Factory;

    fn plan(self) -> Result<ScanPlan<Factory>> {
        Ok(ScanPlan::new(Factory, vec![]))
    }
}

#[derive(Serialize, Deserialize)]
struct Factory;

impl PartitionReaderFactory for Factory {
    type Partition = ();

    fn open<'task>(&self, _: (), _: ReaderContext<'task>) -> Result<PartitionReader<'task>> {
        Err("capability tests do not open partition readers".into())
    }
}

spark_native_source::export_data_source!(Source);

extern "C" {
    fn spark_ds_init(host: *const SparkDsHost, library: *mut *const SparkDsLibrary) -> c_int;
}

unsafe extern "C" fn record_log(level: i32, message: *const c_char) {
    let message = unsafe { CStr::from_ptr(message) }
        .to_str()
        .unwrap()
        .to_owned();
    LOGS.lock().unwrap().push((level, message));
}

/// A host that, like Spark's, stays valid until the process exits.
fn host(spark_version: &'static CStr) -> &'static SparkDsHost {
    Box::leak(Box::new(SparkDsHost {
        size: size_of::<SparkDsHost>() as i64,
        abi_version: ABI_VERSION,
        spark_version: spark_version.as_ptr(),
        log: Some(record_log),
    }))
}

struct SourceHandle<'a> {
    library: &'a SparkDsLibrary,
    pointer: *mut c_void,
}

impl Drop for SourceHandle<'_> {
    fn drop(&mut self) {
        unsafe { self.library.close.unwrap()(self.pointer) };
    }
}

fn create(library: &SparkDsLibrary, name: &str, mode: &str) -> (c_int, u64, *mut c_void) {
    let name = CString::new(name).unwrap();
    let key = CString::new("capabilities").unwrap();
    let value = CString::new(mode).unwrap();
    let keys = [key.as_ptr()];
    let values = [value.as_ptr()];
    let info = SparkDsCreateInfo {
        size: size_of::<SparkDsCreateInfo>() as i64,
        name: name.as_ptr(),
        option_keys: keys.as_ptr(),
        option_values: values.as_ptr(),
        num_options: 1,
    };
    let mut mask = 0;
    let mut source = ptr::null_mut();
    let status = unsafe { library.create.unwrap()(&info, &mut mask, &mut source) };
    (status, mask, source)
}

fn error(library: &SparkDsLibrary) -> String {
    let message = unsafe { library.last_error.unwrap()() };
    assert!(!message.is_null(), "failed C call must set last_error");
    unsafe { CStr::from_ptr(message) }.to_str().unwrap().into()
}

#[test]
fn initialization_logging_and_capabilities_follow_the_source_lifecycle() {
    // One lifecycle test owns these counters; no parallel test resets shared state.
    let mut table = ptr::null();
    assert_ne!(
        unsafe { spark_ds_init(host(c"unsupported"), &mut table) },
        0
    );
    assert!(
        table.is_null(),
        "a library whose init fails returns no table"
    );
    assert_eq!(
        LOGS.lock().unwrap().as_slice(),
        [(
            1,
            "this library does not support the Spark version".to_owned()
        )],
    );
    assert_eq!(unsafe { spark_ds_init(host(c"4.4.0-test"), &mut table) }, 0);
    assert!(!table.is_null());
    let library = unsafe { &*table };
    assert_eq!(
        INITIALIZED.lock().unwrap().as_slice(),
        [
            Some("unsupported".to_owned()),
            Some("4.4.0-test".to_owned())
        ],
    );
    log::warn!("a message with a \0 byte");
    // The default level drops debug messages, until the library raises it.
    log::debug!("a dropped debug message");
    log::set_max_level(LevelFilter::Debug);
    log::debug!("a debug message");
    assert_eq!(
        LOGS.lock().unwrap()[1..],
        [
            (3, "capability test library initialized".to_owned()),
            (2, "a message with a \\0 byte".to_owned()),
            (4, "a debug message".to_owned()),
        ],
    );

    // Analysis can ask only for capabilities, then close without schema or scan discovery.
    let (status, mask, pointer) = create(library, "capability_test", "all");
    assert_eq!(status, 0);
    assert_eq!(mask, 1);
    assert!(!pointer.is_null());
    drop(SourceHandle { library, pointer });
    assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 0);
    assert_eq!(SCANS.load(Ordering::SeqCst), 0);
    assert_eq!(DROPS.load(Ordering::SeqCst), 1);

    // Keep all instances alive together: the last declaration must not affect earlier sources.
    let sources: Vec<_> = [("empty", 0), ("read", 1), ("all", 1), ("without_read", 0)]
        .into_iter()
        .map(|(mode, expected)| {
            let (status, mask, pointer) = create(library, "capability_test", mode);
            assert_eq!(status, 0);
            assert_eq!(mask, expected, "{mode}");
            assert!(!pointer.is_null());
            (expected, SourceHandle { library, pointer })
        })
        .collect();
    assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 5);

    for (mask, source) in sources {
        let mut schema = ArrowSchema::empty();
        assert_eq!(
            unsafe { library.schema.unwrap()(source.pointer, &mut schema) },
            0
        );
        let scans_before = SCANS.load(Ordering::SeqCst);
        let mut reader = ptr::null_mut();
        let status =
            unsafe { library.create_reader.unwrap()(source.pointer, &mut schema, &mut reader) };
        if mask == 0 {
            assert_ne!(status, 0);
            assert_eq!(error(library), "source does not support batch reads");
            assert!(reader.is_null());
            assert_eq!(SCANS.load(Ordering::SeqCst), scans_before);
        } else {
            assert_eq!(status, 0);
            assert!(!reader.is_null());
            assert_eq!(SCANS.load(Ordering::SeqCst), scans_before + 1);
            unsafe { library.close_reader.unwrap()(reader) };
        }
        assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 5);
    }
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 4);
    assert_eq!(SCANS.load(Ordering::SeqCst), 2);
    assert_eq!(DROPS.load(Ordering::SeqCst), 5);

    // The source receives the name that Spark passes, so it can reject other names.
    let (status, mask, source) = create(library, "other_source", "all");
    assert_ne!(status, 0);
    assert_eq!(error(library), "unexpected source name other_source");
    assert_eq!(mask, 0);
    assert!(source.is_null());

    let drops_before = DROPS.load(Ordering::SeqCst);
    let (status, mask, source) = create(library, "capability_test", "panic");
    assert_ne!(status, 0);
    assert_eq!(
        error(library),
        "native provider panicked: capability discovery failed"
    );
    assert_eq!(mask, 0);
    assert!(source.is_null());
    assert_eq!(DROPS.load(Ordering::SeqCst), drops_before + 1);
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 4);
    assert_eq!(SCANS.load(Ordering::SeqCst), 2);
}
