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

//! Implementation details used by the exported JNI functions.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::{ffi::FFI_ArrowSchema, Schema, SchemaRef};
pub use jni;
use jni::objects::{JObject, JObjectArray, JString};
use jni::sys::{jbooleanArray, jlong, jobjectArray};
use jni::JNIEnv;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{BatchSource, FilterSupport, LimitSupport, Options, Predicate, Result, ScanBuilder};

#[path = "stream.rs"]
mod stream;

const STATE_VERSION: u32 = 1;

type HandleMap<T> = Mutex<HashMap<jlong, Arc<Mutex<T>>>>;

/// One registry per loaded plugin. Values have independent locks, so calls on
/// unrelated handles do not hold a shared lock while provider code runs.
pub struct Bridge<S: BatchSource> {
    next_handle: AtomicI64,
    sources: HandleMap<S>,
    readers: HandleMap<Reader<S::Scan>>,
}

impl<S: BatchSource> Default for Bridge<S> {
    fn default() -> Self {
        Self {
            next_handle: AtomicI64::new(1),
            sources: Mutex::new(HashMap::new()),
            readers: Mutex::new(HashMap::new()),
        }
    }
}

impl<S: BatchSource> Bridge<S> {
    fn insert<T>(&self, handles: &HandleMap<T>, value: T) -> Result<jlong> {
        let handle = self
            .next_handle
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| "native handle IDs exhausted")?;
        lock(handles).insert(handle, Arc::new(Mutex::new(value)));
        Ok(handle)
    }

    pub fn create(&self, options: Options) -> Result<jlong> {
        self.insert(&self.sources, S::new(options)?)
    }

    pub fn schema(&self, handle: jlong) -> Result<SchemaRef> {
        let source = get(&self.sources, handle)?;
        let result = lock(&source).schema();
        result
    }

    pub fn create_reader(&self, handle: jlong, schema: SchemaRef) -> Result<jlong> {
        let source = get(&self.sources, handle)?;
        let builder = lock(&source).scan(schema)?;
        self.insert(&self.readers, Reader::new(builder))
    }

    pub fn push_predicates(&self, handle: jlong, predicates: &[Predicate]) -> Result<Vec<bool>> {
        let reader = get(&self.readers, handle)?;
        let mut reader = lock(&reader);
        let support = reader.builder()?.push_filters(predicates)?;
        if support.len() != predicates.len() {
            // Never permit a later limit after an invalid filter negotiation.
            reader.has_residual = true;
            return Err("push_filters must return one result per predicate".into());
        }
        let exact: Vec<bool> = support.iter().map(|s| *s == FilterSupport::Exact).collect();
        reader.has_residual |= exact.iter().any(|s| !s);
        Ok(exact)
    }

    pub fn push_limit(&self, handle: jlong, limit: usize) -> Result<bool> {
        let reader = get(&self.readers, handle)?;
        let mut reader = lock(&reader);
        if reader.has_residual {
            return Ok(false);
        }
        Ok(reader.builder()?.push_limit(limit)? == LimitSupport::Global)
    }

    pub fn prune_columns(&self, handle: jlong, columns: &[String]) -> Result<bool> {
        let reader = get(&self.readers, handle)?;
        let result = lock(&reader).builder()?.prune_columns(columns);
        result
    }

    pub fn partitions(&self, handle: jlong) -> Result<Vec<Vec<u8>>> {
        let reader = get(&self.readers, handle)?;
        let result = lock(&reader).plan().map(|plan| plan.partitions.clone());
        result
    }

    pub fn serialize_reader(&self, handle: jlong) -> Result<Vec<u8>> {
        let reader = get(&self.readers, handle)?;
        let result = lock(&reader).plan().map(|plan| plan.state.clone());
        result
    }

    pub fn close_source(&self, handle: jlong) -> Result<()> {
        remove(&self.sources, handle)
    }

    pub fn close_reader(&self, handle: jlong) -> Result<()> {
        remove(&self.readers, handle)
    }

    pub fn open_reader(&self, state: &[u8], partition: &[u8]) -> Result<FFI_ArrowArrayStream> {
        let reader = S::open(decode(state)?, decode(partition)?)?;
        stream::export(reader)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A provider panic is reported through JNI. Recover the lock so close can
    // still remove and release its handle after that failed call.
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn get<T>(handles: &HandleMap<T>, handle: jlong) -> Result<Arc<Mutex<T>>> {
    lock(handles)
        .get(&handle)
        .cloned()
        .ok_or_else(|| format!("unknown or closed native handle: {handle}").into())
}

fn remove<T>(handles: &HandleMap<T>, handle: jlong) -> Result<()> {
    let value = lock(handles).remove(&handle);
    // Provider destructors run after releasing the registry lock. Repeated
    // closes are harmless, and IDs are never reused.
    drop(value);
    Ok(())
}

struct Reader<B: ScanBuilder> {
    builder: Option<B>,
    plan: Option<EncodedPlan>,
    has_residual: bool,
}

struct EncodedPlan {
    state: Vec<u8>,
    partitions: Vec<Vec<u8>>,
}

impl<B: ScanBuilder> Reader<B> {
    fn new(builder: B) -> Self {
        Self {
            builder: Some(builder),
            plan: None,
            has_residual: false,
        }
    }

    fn builder(&mut self) -> Result<&mut B> {
        self.builder
            .as_mut()
            .ok_or_else(|| "scan has already been planned".into())
    }

    fn plan(&mut self) -> Result<&EncodedPlan> {
        if self.plan.is_none() {
            let builder = self
                .builder
                .take()
                .ok_or("scan planning previously failed")?;
            let plan = builder.build()?;
            self.plan = Some(EncodedPlan {
                state: encode(&plan.state)?,
                partitions: plan.partitions.iter().map(encode).collect::<Result<_>>()?,
            });
        }
        Ok(self.plan.as_ref().expect("plan was initialized"))
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: u32,
    value: T,
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&Envelope {
        version: STATE_VERSION,
        value,
    })?)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let envelope: Envelope<T> = serde_json::from_slice(bytes)?;
    if envelope.version != STATE_VERSION {
        return Err(format!("unsupported native state version: {}", envelope.version).into());
    }
    Ok(envelope.value)
}

pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> String {
    let message = if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        // An arbitrary panic payload can itself panic when dropped.
        std::mem::forget(payload);
        return "native provider panicked with a non-string payload".to_owned();
    };
    format!("native provider panicked: {message}")
}

/// Converts all errors and unwinding panics into Java exceptions. This does
/// not recover from aborting panics, allocation failure, or process faults.
pub fn jni_call<'local, T: Default>(
    env: &mut JNIEnv<'local>,
    action: impl FnOnce(&mut JNIEnv<'local>) -> Result<T>,
) -> T {
    let result = catch_unwind(AssertUnwindSafe(|| action(env).map_err(|e| e.to_string())));
    let message = match result {
        Ok(Ok(value)) => return value,
        Ok(Err(message)) => message,
        Err(payload) => panic_message(payload),
    };
    // Keep a pending JVM exception (for example an allocation failure).
    if !env.exception_check().unwrap_or(true) {
        let _ = env.throw_new("java/lang/RuntimeException", message);
    }
    T::default()
}

pub fn strings(env: &mut JNIEnv<'_>, array: &JObjectArray<'_>) -> Result<Vec<String>> {
    let len = env.get_array_length(array)?;
    let mut strings = Vec::with_capacity(len as usize);
    for index in 0..len {
        let string = JString::from(env.get_object_array_element(array, index)?);
        strings.push(env.get_string(&string)?.into());
        env.delete_local_ref(string)?;
    }
    Ok(strings)
}

pub fn options(
    env: &mut JNIEnv<'_>,
    keys: &JObjectArray<'_>,
    values: &JObjectArray<'_>,
) -> Result<Options> {
    let keys = strings(env, keys)?;
    let values = strings(env, values)?;
    if keys.len() != values.len() {
        return Err("option keys and values have different lengths".into());
    }
    Ok(keys.into_iter().zip(values).collect())
}

pub fn booleans(env: &mut JNIEnv<'_>, values: &[bool]) -> Result<jbooleanArray> {
    let array = env.new_boolean_array(values.len().try_into()?)?;
    let values: Vec<_> = values.iter().map(|v| u8::from(*v)).collect();
    env.set_boolean_array_region(&array, 0, &values)?;
    Ok(array.into_raw())
}

pub fn parse_predicates(predicates: &[String]) -> Result<Vec<Predicate>> {
    predicates
        .iter()
        .map(|text| Ok(serde_json::from_str(text)?))
        .collect()
}

pub fn byte_arrays(env: &mut JNIEnv<'_>, values: &[Vec<u8>]) -> Result<jobjectArray> {
    let array = env.new_object_array(values.len().try_into()?, "[B", JObject::null())?;
    for (index, value) in values.iter().enumerate() {
        let bytes = env.byte_array_from_slice(value)?;
        env.set_object_array_element(&array, index.try_into()?, &bytes)?;
        env.delete_local_ref(bytes)?;
    }
    Ok(array.into_raw())
}

fn check_address<T>(address: jlong) -> Result<*mut T> {
    let pointer = address as usize as *mut T;
    if pointer.is_null() || !pointer.is_aligned() {
        return Err("null or unaligned Arrow structure address".into());
    }
    Ok(pointer)
}

/// Moves a schema from Spark's C structure and releases it after conversion.
///
/// # Safety
/// The address must point to a live, initialized ArrowSchema that the caller
/// permits this function to move, as required by NativeBridge ABI v1.
pub unsafe fn import_schema(address: jlong) -> Result<SchemaRef> {
    let address = check_address::<FFI_ArrowSchema>(address)?;
    let schema = unsafe { FFI_ArrowSchema::from_raw(address) };
    Ok(Arc::new(Schema::try_from(&schema)?))
}

/// Moves an exported schema into Spark's output ArrowSchema structure.
///
/// # Safety
/// The address must point to writable, aligned storage for an ArrowSchema
/// whose release callback is null. Spark retains ownership of that storage.
pub unsafe fn export_schema(schema: &SchemaRef, address: jlong) -> Result<()> {
    let address = check_address::<FFI_ArrowSchema>(address)?;
    let schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
    unsafe { address.write(schema) };
    Ok(())
}

/// Initializes Spark's output stream, transferring ownership of the reader.
///
/// # Safety
/// The address must point to writable, aligned storage for an ArrowArrayStream
/// whose release callback is null. Spark retains ownership of that storage.
pub unsafe fn export_stream(stream: FFI_ArrowArrayStream, address: jlong) -> Result<()> {
    let address = check_address::<FFI_ArrowArrayStream>(address)?;
    unsafe { address.write(stream) };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::{BatchReader, ScanPlan};

    struct Source {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        filter_support: FilterSupport,
        limit_support: LimitSupport,
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Builder {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        filter_support: FilterSupport,
        limit_support: LimitSupport,
    }

    impl BatchSource for Source {
        type Scan = Builder;

        fn new(_: Options) -> Result<Self> {
            Ok(Self {
                builds: Arc::default(),
                limits: Arc::default(),
                drops: Arc::default(),
                filter_support: FilterSupport::Unsupported,
                limit_support: LimitSupport::Unsupported,
            })
        }

        fn schema(&self) -> Result<SchemaRef> {
            Ok(Arc::new(Schema::empty()))
        }

        fn scan(&self, _: SchemaRef) -> Result<Builder> {
            Ok(Builder {
                builds: self.builds.clone(),
                limits: self.limits.clone(),
                filter_support: self.filter_support,
                limit_support: self.limit_support,
            })
        }

        fn open(_: u64, _: u64) -> Result<BatchReader> {
            unreachable!("these tests inspect driver-side planning")
        }
    }

    impl ScanBuilder for Builder {
        type State = u64;
        type Partition = u64;

        fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
            Ok(vec![self.filter_support; filters.len()])
        }

        fn push_limit(&mut self, _: usize) -> Result<LimitSupport> {
            self.limits.fetch_add(1, Ordering::SeqCst);
            Ok(self.limit_support)
        }

        fn build(self) -> Result<ScanPlan<u64, u64>> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Ok(ScanPlan {
                state: 42,
                partitions: vec![2, 7],
            })
        }
    }

    #[test]
    fn source_can_close_before_planning_and_plan_is_built_once() {
        let bridge = Bridge::<Source>::default();
        let source = Source::new(Options::new()).unwrap();
        let builds = source.builds.clone();
        let drops = source.drops.clone();
        let handle = bridge.insert(&bridge.sources, source).unwrap();
        let schema = bridge.schema(handle).unwrap();
        let reader = bridge.create_reader(handle, schema).unwrap();
        bridge.close_source(handle).unwrap();
        bridge.close_source(handle).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(bridge.schema(handle).is_err());

        let partitions = bridge.partitions(reader).unwrap();
        assert_eq!(bridge.partitions(reader).unwrap(), partitions);
        assert_eq!(decode::<u64>(&partitions[0]).unwrap(), 2);
        assert_eq!(decode::<u64>(&partitions[1]).unwrap(), 7);
        assert_eq!(
            decode::<u64>(&bridge.serialize_reader(reader).unwrap()).unwrap(),
            42
        );
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(bridge.prune_columns(reader, &[]).is_err());
        bridge.close_reader(reader).unwrap();
        assert!(bridge.partitions(reader).is_err());
    }

    #[test]
    fn residual_filters_prevent_calling_the_providers_limit_method() {
        for support in [FilterSupport::PruningOnly, FilterSupport::Unsupported] {
            let bridge = Bridge::<Source>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.filter_support = support;
            source.limit_support = LimitSupport::Global;
            let limits = source.limits.clone();
            let handle = bridge.insert(&bridge.sources, source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();
            assert_eq!(
                bridge
                    .push_predicates(reader, &[Predicate::Unknown])
                    .unwrap(),
                vec![false]
            );
            assert!(!bridge.push_limit(reader, 1).unwrap());
            assert_eq!(limits.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn only_global_limits_return_true_to_jni() {
        for support in [LimitSupport::PerPartition, LimitSupport::Global] {
            let bridge = Bridge::<Source>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.limit_support = support;
            let limits = source.limits.clone();
            let handle = bridge.insert(&bridge.sources, source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();
            assert_eq!(
                bridge.push_limit(reader, 4).unwrap(),
                support == LimitSupport::Global
            );
            assert_eq!(limits.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn rejects_a_different_wire_version() {
        let error = decode::<u64>(br#"{"version":2,"value":42}"#).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported native state version"));
    }
}
