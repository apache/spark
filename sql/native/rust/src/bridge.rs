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

//! Driver-only handles and planners, with stateless executor plan restoration.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::{Schema, SchemaRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    BatchReaderFactory, DataSource, FilterSupport, LimitSupport, Options, Predicate, Result,
    ScanBuilder, SourceCapability, TaskContext, WriteMode,
};

use crate::provider::Provider;
use crate::write::DriverWriter;

#[path = "stream.rs"]
pub(crate) mod stream;

const STATE_VERSION: u32 = 1;

type HandleMap<T> = Mutex<HashMap<i64, Arc<Mutex<T>>>>;

/// One registry per loaded plugin. Provider code runs under a per-handle lock,
/// so independent sources and scans do not serialize behind a global lock.
pub struct Bridge<P: Provider> {
    next_handle: AtomicI64,
    sources: HandleMap<SourceState<P::Source>>,
    planners: HandleMap<Planner<P::Scan>>,
    writers: HandleMap<DriverWriter<P::Write>>,
}

impl<P: Provider> Default for Bridge<P> {
    fn default() -> Self {
        Self {
            next_handle: AtomicI64::new(1),
            sources: Mutex::new(HashMap::new()),
            planners: Mutex::new(HashMap::new()),
            writers: Mutex::new(HashMap::new()),
        }
    }
}

impl<P: Provider> Bridge<P> {
    fn insert<T>(&self, handles: &HandleMap<T>, value: T) -> Result<i64> {
        let handle = self
            .next_handle
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| "native handle IDs exhausted")?;
        lock(handles).insert(handle, Arc::new(Mutex::new(value)));
        Ok(handle)
    }

    pub fn create(&self, options: Options) -> Result<(i64, u64)> {
        self.register_source(P::Source::new(options)?)
    }

    fn register_source(&self, source: P::Source) -> Result<(i64, u64)> {
        let mut capabilities = 0;
        for capability in source.capabilities(P::CAPABILITIES) {
            if !P::CAPABILITIES.contains(&capability) {
                return Err(format!("capability {capability:?} is not registered").into());
            }
            capabilities |= match capability {
                SourceCapability::BatchRead => crate::abi::BATCH_READ,
                SourceCapability::BatchWrite => crate::abi::BATCH_WRITE,
                SourceCapability::Truncate => crate::abi::TRUNCATE,
                _ => return Err(format!("capability {capability:?} is not implemented").into()),
            };
        }
        if capabilities & crate::abi::TRUNCATE != 0 && capabilities & crate::abi::BATCH_WRITE == 0 {
            return Err("Truncate requires BatchWrite".into());
        }
        let handle = self.insert(
            &self.sources,
            SourceState {
                source,
                capabilities,
            },
        )?;
        Ok((handle, capabilities))
    }

    pub fn schema(&self, handle: i64) -> Result<SchemaRef> {
        let source = get(&self.sources, handle)?;
        let schema = lock(&source).source.schema()?;
        stream::validate_schema(&schema)?;
        Ok(schema)
    }

    pub fn create_reader(&self, handle: i64, schema: SchemaRef) -> Result<i64> {
        stream::validate_schema(&schema)?;
        let source = get(&self.sources, handle)?;
        let builder = {
            let source = lock(&source);
            if source.capabilities & crate::abi::BATCH_READ == 0 {
                return Err("source does not support batch reads".into());
            }
            P::scan(&source.source, schema.clone())?
        };
        self.insert(&self.planners, Planner::new(builder, schema))
    }

    pub fn push_predicates(
        &self,
        handle: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<FilterSupport>> {
        let reader = get(&self.planners, handle)?;
        let mut reader = lock(&reader);
        let support = reader.negotiate(|builder| {
            let support = builder.push_filters(predicates)?;
            if support.len() != predicates.len() {
                return Err("push_filters must return one result per predicate".into());
            }
            Ok(support)
        })?;
        reader.has_residual |= support.iter().any(|s| *s != FilterSupport::Exact);
        Ok(support)
    }

    pub fn push_limit(&self, handle: i64, limit: usize) -> Result<bool> {
        let reader = get(&self.planners, handle)?;
        let mut reader = lock(&reader);
        let has_residual = reader.has_residual;
        reader.negotiate(|builder| {
            if has_residual {
                Ok(false)
            } else {
                Ok(builder.push_limit(limit)? == LimitSupport::Global)
            }
        })
    }

    pub fn prune_columns(&self, handle: i64, columns: &[String]) -> Result<bool> {
        let reader = get(&self.planners, handle)?;
        let mut reader = lock(&reader);
        let indices: Vec<_> = columns
            .iter()
            .map(|column| reader.schema.index_of(column))
            .collect::<std::result::Result<_, _>>()?;
        let schema = Arc::new(reader.schema.project(&indices)?);
        let pruned = reader.negotiate(|builder| builder.prune_columns(columns))?;
        if pruned {
            reader.schema = schema;
        }
        Ok(pruned)
    }

    pub fn planned(&self, handle: i64) -> Result<Arc<EncodedPlan>> {
        let reader = get(&self.planners, handle)?;
        let result = lock(&reader).plan();
        result
    }

    pub fn create_writer(&self, handle: i64, schema: SchemaRef, mode: WriteMode) -> Result<i64> {
        stream::validate_schema(&schema)?;
        let source = get(&self.sources, handle)?;
        let writer = {
            let source = lock(&source);
            if source.capabilities & crate::abi::BATCH_WRITE == 0 {
                return Err("source does not support batch writes".into());
            }
            if mode == WriteMode::Truncate && source.capabilities & crate::abi::TRUNCATE == 0 {
                return Err("source does not support truncate".into());
            }
            P::write(&source.source, schema.clone(), mode)?
        };
        self.insert(&self.writers, DriverWriter::new(writer, schema))
    }

    pub fn serialized_writer(&self, handle: i64) -> Result<Arc<Vec<u8>>> {
        let writer = get(&self.writers, handle)?;
        let result = lock(&writer).serialized();
        result
    }

    pub fn commit(&self, handle: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let writer = get(&self.writers, handle)?;
        let result = lock(&writer).commit(messages);
        result
    }

    pub fn abort(&self, handle: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let writer = get(&self.writers, handle)?;
        let result = lock(&writer).abort(messages);
        result
    }

    pub fn close_writer(&self, handle: i64) -> Result<()> {
        remove(&self.writers, handle);
        Ok(())
    }

    pub fn close_source(&self, handle: i64) -> Result<()> {
        remove(&self.sources, handle);
        Ok(())
    }

    pub fn close_reader(&self, handle: i64) -> Result<()> {
        remove(&self.planners, handle);
        Ok(())
    }
}

/// Restores an executor reader using only its factory type and serialized values.
/// No source, planner, or driver handle registry is created or consulted.
///
/// # Safety
/// `task` must satisfy TaskContext::from_raw and remain valid until the
/// returned stream is released. The stream's consumer must follow the
/// Arrow C Stream ownership protocol and keep the plugin loaded.
pub unsafe fn open_reader<F: BatchReaderFactory>(
    task: *const crate::abi::SparkDsTaskContext,
    state: &[u8],
    partition: &[u8],
) -> Result<FFI_ArrowArrayStream> {
    let state: ReaderState<F> = decode(state)?;
    let task = unsafe { TaskContext::from_raw(task)? };
    let reader = state.factory.open(decode(partition)?, task)?;
    unsafe { stream::export(reader, &Arc::new(state.schema)) }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A provider panic becomes an ABI error. Recover the lock so close can
    // still remove and release the handle after that failed call.
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn get<T>(handles: &HandleMap<T>, handle: i64) -> Result<Arc<Mutex<T>>> {
    lock(handles)
        .get(&handle)
        .cloned()
        .ok_or_else(|| format!("unknown or closed native handle: {handle}").into())
}

fn remove<T>(handles: &HandleMap<T>, handle: i64) {
    let value = lock(handles).remove(&handle);
    // Provider destructors run after releasing the registry lock. Repeated
    // closes are harmless, and IDs are never reused.
    drop(value);
}

struct SourceState<S> {
    source: S,
    capabilities: u64,
}

struct Planner<B: ScanBuilder> {
    phase: Phase<B>,
    schema: SchemaRef,
    has_residual: bool,
}

enum Phase<B: ScanBuilder> {
    Negotiating(B),
    Planned(Arc<EncodedPlan>),
    Failed(String),
}

pub struct EncodedPlan {
    pub state: Vec<u8>,
    pub partitions: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct ReaderState<F> {
    factory: F,
    schema: Schema,
}

impl<B: ScanBuilder> Planner<B> {
    fn new(builder: B, schema: SchemaRef) -> Self {
        Self {
            phase: Phase::Negotiating(builder),
            schema,
            has_residual: false,
        }
    }

    fn negotiate<T>(&mut self, action: impl FnOnce(&mut B) -> Result<T>) -> Result<T> {
        let builder = match &mut self.phase {
            Phase::Negotiating(builder) => builder,
            Phase::Planned(_) => return Err("scan has already been planned".into()),
            Phase::Failed(error) => return Err(error.clone().into()),
        };
        match provider_call(|| action(builder)) {
            Ok(value) => Ok(value),
            Err(error) => {
                // A partially mutated builder must never be reused after failure.
                let previous = std::mem::replace(&mut self.phase, Phase::Failed(error.clone()));
                // Install the failure before dropping provider state. Preserve
                // the original cause even if its destructor also panics.
                let _ = provider_call(|| {
                    drop(previous);
                    Ok(())
                });
                Err(error.into())
            }
        }
    }

    fn plan(&mut self) -> Result<Arc<EncodedPlan>> {
        match &self.phase {
            Phase::Planned(plan) => return Ok(plan.clone()),
            Phase::Failed(error) => return Err(error.clone().into()),
            Phase::Negotiating(_) => {}
        }
        let phase = std::mem::replace(
            &mut self.phase,
            Phase::Failed("scan planning did not complete".into()),
        );
        let Phase::Negotiating(builder) = phase else {
            unreachable!("only a negotiating scan reaches this transition")
        };
        let result = provider_call(|| {
            let plan = builder.plan()?;
            Ok(Arc::new(EncodedPlan {
                state: encode(&ReaderState {
                    factory: plan.factory,
                    schema: self.schema.as_ref().clone(),
                })?,
                partitions: plan.partitions.iter().map(encode).collect::<Result<_>>()?,
            }))
        });
        match result {
            Ok(plan) => {
                self.phase = Phase::Planned(plan.clone());
                Ok(plan)
            }
            Err(error) => {
                self.phase = Phase::Failed(error.clone());
                Err(error.into())
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: u32,
    value: T,
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&Envelope {
        version: STATE_VERSION,
        value,
    })?)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let envelope: Envelope<T> = serde_json::from_slice(bytes)?;
    if envelope.version != STATE_VERSION {
        return Err(format!("unsupported native state version: {}", envelope.version).into());
    }
    Ok(envelope.value)
}

pub(crate) fn provider_call<T>(
    action: impl FnOnce() -> Result<T>,
) -> std::result::Result<T, String> {
    catch_unwind(AssertUnwindSafe(|| {
        action().map_err(|error| error.to_string())
    }))
    .unwrap_or_else(|payload| Err(panic_message(payload)))
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

pub fn parse_predicates(predicates: &[String]) -> Result<Vec<Predicate>> {
    predicates
        .iter()
        .map(|text| Ok(serde_json::from_str(text)?))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::provider::ReadOnly;
    use crate::{BatchReader, BatchSource, ScanPlan};

    #[derive(Clone, Copy)]
    enum FailureMode {
        None,
        Error,
        Panic,
    }

    impl FailureMode {
        fn fail(self, message: &str) -> Result<()> {
            match self {
                Self::None => Ok(()),
                Self::Error => Err(message.into()),
                Self::Panic => panic!("{message}"),
            }
        }
    }

    struct Source {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        builder_drops: Arc<AtomicUsize>,
        filter_support: FilterSupport,
        limit_support: LimitSupport,
        failure: FailureMode,
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Builder {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        filter_support: FilterSupport,
        limit_support: LimitSupport,
        failure: FailureMode,
        value: u64,
    }

    impl Drop for Builder {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl DataSource for Source {
        fn new(_: Options) -> Result<Self> {
            Ok(Self {
                builds: Arc::default(),
                limits: Arc::default(),
                drops: Arc::default(),
                builder_drops: Arc::default(),
                filter_support: FilterSupport::Unsupported,
                limit_support: LimitSupport::Unsupported,
                failure: FailureMode::None,
            })
        }

        fn schema(&self) -> Result<SchemaRef> {
            Ok(Arc::new(Schema::empty()))
        }
    }

    impl BatchSource for Source {
        type Scan = Builder;

        fn scan(&self, _: SchemaRef) -> Result<Builder> {
            Ok(Builder {
                builds: self.builds.clone(),
                limits: self.limits.clone(),
                drops: self.builder_drops.clone(),
                filter_support: self.filter_support,
                limit_support: self.limit_support,
                failure: self.failure,
                value: 42,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Factory(u64);

    impl BatchReaderFactory for Factory {
        type Partition = u64;

        fn open<'task>(&self, partition: u64, _: TaskContext<'task>) -> Result<BatchReader<'task>> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let values = Arc::new(Int64Array::from(vec![(self.0 + partition) as i64]));
            let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
            Ok(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
        }
    }

    impl ScanBuilder for Builder {
        type Factory = Factory;

        fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
            self.failure.fail("negotiation failed")?;
            Ok(vec![self.filter_support; filters.len()])
        }

        fn push_limit(&mut self, limit: usize) -> Result<LimitSupport> {
            self.limits.fetch_add(1, Ordering::SeqCst);
            self.value = limit as u64;
            self.failure.fail("negotiation failed")?;
            Ok(self.limit_support)
        }

        fn plan(self) -> Result<ScanPlan<Factory>> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            self.failure.fail("planning failed")?;
            Ok(ScanPlan {
                factory: Factory(self.value),
                partitions: vec![2, 7],
            })
        }
    }

    fn error_message<T>(result: Result<T>) -> String {
        result
            .err()
            .expect("expected a provider failure")
            .to_string()
    }

    #[test]
    fn source_can_close_before_planning_and_plan_is_built_once() {
        let bridge = Bridge::<ReadOnly<Source>>::default();
        let source = Source::new(Options::new()).unwrap();
        let builds = source.builds.clone();
        let drops = source.drops.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let schema = bridge.schema(handle).unwrap();
        let reader = bridge.create_reader(handle, schema).unwrap();
        bridge.close_source(handle).unwrap();
        bridge.close_source(handle).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(bridge.schema(handle).is_err());

        let plan = bridge.planned(reader).unwrap();
        assert!(Arc::ptr_eq(&bridge.planned(reader).unwrap(), &plan));
        assert_eq!(decode::<u64>(&plan.partitions[0]).unwrap(), 2);
        assert_eq!(decode::<u64>(&plan.partitions[1]).unwrap(), 7);
        assert_eq!(
            decode::<ReaderState<Factory>>(&plan.state)
                .unwrap()
                .factory
                .0,
            42
        );
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(bridge.prune_columns(reader, &[]).is_err());
        bridge.close_reader(reader).unwrap();
        assert!(bridge.planned(reader).is_err());
    }

    #[test]
    fn residual_filters_prevent_calling_the_providers_limit_method() {
        for support in [FilterSupport::PruningOnly, FilterSupport::Unsupported] {
            let bridge = Bridge::<ReadOnly<Source>>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.filter_support = support;
            source.limit_support = LimitSupport::Global;
            let limits = source.limits.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();
            assert_eq!(
                bridge
                    .push_predicates(reader, &[Predicate::Unknown])
                    .unwrap(),
                vec![support]
            );
            assert!(!bridge.push_limit(reader, 1).unwrap());
            assert_eq!(limits.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn only_global_limits_return_true_to_the_abi() {
        for support in [LimitSupport::PerPartition, LimitSupport::Global] {
            let bridge = Bridge::<ReadOnly<Source>>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.limit_support = support;
            let limits = source.limits.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
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
    fn planning_errors_and_panics_preserve_the_first_failure_without_retrying() {
        for (failure, expected) in [
            (FailureMode::Error, "planning failed"),
            (
                FailureMode::Panic,
                "native provider panicked: planning failed",
            ),
        ] {
            let bridge = Bridge::<ReadOnly<Source>>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.failure = failure;
            let builds = source.builds.clone();
            let drops = source.builder_drops.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();
            bridge.close_source(handle).unwrap();

            assert_eq!(error_message(bridge.planned(reader)), expected);
            assert_eq!(error_message(bridge.planned(reader)), expected);
            assert_eq!(error_message(bridge.push_limit(reader, 1)), expected);
            assert_eq!(builds.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            bridge.close_reader(reader).unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn failed_negotiation_discards_the_partially_mutated_builder() {
        for (failure, expected) in [
            (FailureMode::Error, "negotiation failed"),
            (
                FailureMode::Panic,
                "native provider panicked: negotiation failed",
            ),
        ] {
            let bridge = Bridge::<ReadOnly<Source>>::default();
            let mut source = Source::new(Options::new()).unwrap();
            source.failure = failure;
            let builds = source.builds.clone();
            let limits = source.limits.clone();
            let drops = source.builder_drops.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();

            assert_eq!(error_message(bridge.push_limit(reader, 5)), expected);
            assert_eq!(error_message(bridge.push_limit(reader, 1)), expected);
            assert_eq!(error_message(bridge.push_predicates(reader, &[])), expected);
            assert_eq!(error_message(bridge.planned(reader)), expected);
            assert_eq!(limits.load(Ordering::SeqCst), 1);
            assert_eq!(builds.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            bridge.close_reader(reader).unwrap();
            bridge.close_source(handle).unwrap();
        }
    }

    #[test]
    fn scans_of_one_source_keep_pushdown_and_cached_plans_independent() {
        let bridge = Bridge::<ReadOnly<Source>>::default();
        let mut source = Source::new(Options::new()).unwrap();
        source.filter_support = FilterSupport::PruningOnly;
        source.limit_support = LimitSupport::Global;
        let builds = source.builds.clone();
        let limits = source.limits.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let first = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        let second = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        bridge.close_source(handle).unwrap();

        bridge
            .push_predicates(second, &[Predicate::Unknown])
            .unwrap();
        assert!(bridge.push_limit(first, 5).unwrap());
        assert!(!bridge.push_limit(second, 1).unwrap());
        let first_plan = bridge.planned(first).unwrap();
        let second_plan = bridge.planned(second).unwrap();
        assert_eq!(
            decode::<ReaderState<Factory>>(&first_plan.state)
                .unwrap()
                .factory
                .0,
            5
        );
        assert_eq!(
            decode::<ReaderState<Factory>>(&second_plan.state)
                .unwrap()
                .factory
                .0,
            42
        );
        assert!(!Arc::ptr_eq(&first_plan, &second_plan));
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(limits.load(Ordering::SeqCst), 1);
        bridge.close_reader(first).unwrap();
        assert!(Arc::ptr_eq(&second_plan, &bridge.planned(second).unwrap()));
        bridge.close_reader(second).unwrap();
    }

    #[test]
    fn executor_retries_use_only_serialized_values_after_the_driver_is_dropped() {
        let bridge = Bridge::<ReadOnly<Source>>::default();
        let source = Source::new(Options::new()).unwrap();
        let builds = source.builds.clone();
        let source_drops = source.drops.clone();
        let builder_drops = source.builder_drops.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let planner = bridge.create_reader(handle, schema).unwrap();
        bridge.close_source(handle).unwrap();
        let plan = bridge.planned(planner).unwrap();
        // Model Spark copying the factory and partition descriptors out of the plugin.
        let state = plan.state.clone();
        let partitions = plan.partitions.clone();
        drop(plan);
        bridge.close_reader(planner).unwrap();
        drop(bridge);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(builder_drops.load(Ordering::SeqCst), 1);

        for attempt in 0..2 {
            for (index, partition) in partitions.iter().enumerate() {
                let task = crate::abi::SparkDsTaskContext {
                    size: std::mem::size_of::<crate::abi::SparkDsTaskContext>() as i64,
                    stage_id: 1,
                    stage_attempt_number: 0,
                    partition_id: index as i32,
                    attempt_number: attempt,
                    task_attempt_id: i64::from(attempt) * 2 + index as i64,
                    is_interrupted: None,
                };
                // The executor entry point depends only on Factory, not Source or Builder.
                let stream = unsafe { open_reader::<Factory>(&task, &state, partition) }.unwrap();
                let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
                let batch = reader.next().unwrap().unwrap();
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ids.values(), &[42 + [2, 7][index]]);
                assert!(reader.next().is_none());
            }
        }
    }

    #[test]
    fn executor_rejects_a_factory_schema_different_from_the_negotiated_schema() {
        let bridge = Bridge::<ReadOnly<Source>>::default();
        let (handle, _) = bridge.create(Options::new()).unwrap();
        let reader = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        let plan = bridge.planned(reader).unwrap();
        bridge.close_reader(reader).unwrap();
        bridge.close_source(handle).unwrap();

        let task = crate::abi::SparkDsTaskContext {
            size: std::mem::size_of::<crate::abi::SparkDsTaskContext>() as i64,
            stage_id: 1,
            stage_attempt_number: 0,
            partition_id: 0,
            attempt_number: 0,
            task_attempt_id: 1,
            is_interrupted: None,
        };
        // The factory returns one id column, but the driver negotiated an empty schema.
        // The expected schema must survive serialization and the removal of driver handles.
        drop(bridge);
        let error = error_message(unsafe {
            open_reader::<Factory>(&task, &plan.state, &plan.partitions[0])
        });
        assert!(error.contains("schema"), "{error}");
    }

    #[test]
    fn rejects_a_different_wire_version() {
        let error = decode::<u64>(br#"{"version":2,"value":42}"#).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported native state version"));
    }
}
