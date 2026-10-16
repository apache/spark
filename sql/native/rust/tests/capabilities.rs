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

use std::ffi::{c_int, c_void, CStr, CString};
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::abi::{
    ArrowSchema, SparkDsCreateInfo, SparkDsHost, SparkDsLibrary, ABI_VERSION,
};
use spark_native_source::{
    BatchReader, BatchReaderFactory, BatchSource, DataSource, Options, Result, ScanBuilder,
    ScanPlan, SourceCapability, TaskContext,
};

static DECLARATIONS: AtomicUsize = AtomicUsize::new(0);
static SCHEMAS: AtomicUsize = AtomicUsize::new(0);
static SCANS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);

struct Source {
    mode: String,
}

impl DataSource for Source {
    fn new(options: Options) -> Result<Self> {
        Ok(Self {
            mode: options.get("capabilities").cloned().unwrap(),
        })
    }

    fn capabilities(&self, _: &[SourceCapability]) -> Vec<SourceCapability> {
        DECLARATIONS.fetch_add(1, Ordering::SeqCst);
        match self.mode.as_str() {
            "empty" => vec![],
            "batch" => vec![SourceCapability::BatchRead],
            "duplicates" => vec![SourceCapability::BatchRead, SourceCapability::BatchRead],
            "micro_batch" => vec![SourceCapability::MicroBatchRead],
            "batch_write" => vec![SourceCapability::BatchWrite],
            "read_write" => vec![SourceCapability::BatchRead, SourceCapability::BatchWrite],
            "streaming_write" => vec![SourceCapability::StreamingWrite],
            "truncate" => vec![SourceCapability::Truncate],
            "panic" => panic!("capability discovery failed"),
            other => panic!("unexpected test mode: {other}"),
        }
    }

    fn schema(&self) -> Result<SchemaRef> {
        SCHEMAS.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Schema::empty()))
    }
}

impl BatchSource for Source {
    type Scan = Builder;

    fn scan(&self, _: SchemaRef) -> Result<Builder> {
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
        Ok(ScanPlan {
            factory: Factory,
            partitions: vec![],
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Factory;

impl BatchReaderFactory for Factory {
    type Partition = ();

    fn open<'task>(&self, _: (), _: TaskContext<'task>) -> Result<BatchReader<'task>> {
        Err("capability tests do not open partition readers".into())
    }
}

spark_native_source::export_data_source!(Source);

extern "C" {
    fn spark_ds_init(host: *const SparkDsHost, library: *mut *const SparkDsLibrary) -> c_int;
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

fn create(library: &SparkDsLibrary, mode: &str) -> (c_int, u64, *mut c_void) {
    let name = CString::new("capability_test").unwrap();
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
fn source_capabilities_control_the_exported_abi_and_source_lifecycle() {
    // One lifecycle test owns these counters; no parallel test resets shared state.
    let spark_version = CString::new("test").unwrap();
    let host = SparkDsHost {
        size: size_of::<SparkDsHost>() as i64,
        abi_version: ABI_VERSION,
        spark_version: spark_version.as_ptr(),
        log: None,
    };
    let mut table = ptr::null();
    assert_eq!(unsafe { spark_ds_init(&host, &mut table) }, 0);
    assert!(!table.is_null());
    let library = unsafe { &*table };

    // Analysis can ask only for capabilities, then close without schema or scan discovery.
    let (status, mask, pointer) = create(library, "batch");
    assert_eq!(status, 0);
    assert_eq!(mask, 1);
    assert!(!pointer.is_null());
    drop(SourceHandle { library, pointer });
    assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 0);
    assert_eq!(SCANS.load(Ordering::SeqCst), 0);
    assert_eq!(DROPS.load(Ordering::SeqCst), 1);

    // Keep all instances alive together: the last declaration must not affect earlier sources.
    let sources: Vec<_> = [("empty", 0), ("batch", 1), ("duplicates", 1)]
        .into_iter()
        .map(|(mode, expected)| {
            let (status, mask, pointer) = create(library, mode);
            assert_eq!(status, 0);
            assert_eq!(mask, expected, "{mode}");
            assert!(!pointer.is_null());
            (expected, SourceHandle { library, pointer })
        })
        .collect();
    assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 4);

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
        assert_eq!(DECLARATIONS.load(Ordering::SeqCst), 4);
    }
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 3);
    assert_eq!(SCANS.load(Ordering::SeqCst), 2);
    assert_eq!(DROPS.load(Ordering::SeqCst), 4);

    for (mode, capability) in [
        ("micro_batch", "MicroBatchRead"),
        ("batch_write", "BatchWrite"),
        ("read_write", "BatchWrite"),
        ("streaming_write", "StreamingWrite"),
        ("truncate", "Truncate"),
    ] {
        let drops_before = DROPS.load(Ordering::SeqCst);
        let declarations_before = DECLARATIONS.load(Ordering::SeqCst);
        let (status, mask, source) = create(library, mode);
        assert_ne!(status, 0, "{mode}");
        assert_eq!(
            error(library),
            format!("capability {capability} is not registered")
        );
        assert_eq!(mask, 0);
        assert!(source.is_null());
        assert_eq!(DROPS.load(Ordering::SeqCst), drops_before + 1);
        assert_eq!(DECLARATIONS.load(Ordering::SeqCst), declarations_before + 1);
    }

    let drops_before = DROPS.load(Ordering::SeqCst);
    let (status, mask, source) = create(library, "panic");
    assert_ne!(status, 0);
    assert_eq!(
        error(library),
        "native provider panicked: capability discovery failed"
    );
    assert_eq!(mask, 0);
    assert!(source.is_null());
    assert_eq!(DROPS.load(Ordering::SeqCst), drops_before + 1);
    assert_eq!(SCHEMAS.load(Ordering::SeqCst), 3);
    assert_eq!(SCANS.load(Ordering::SeqCst), 2);
}
