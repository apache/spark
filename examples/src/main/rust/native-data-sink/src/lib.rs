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

//! A write-only JSONL sink demonstrating task staging and atomic manifest commits.
//!
//! This example requires one local or shared POSIX filesystem. It is not an
//! object-store connector, a concurrent writer protocol, or a durable table format.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::{
    BatchWrite, BatchWriteSource, BatchWriter, BatchWriterFactory, DataSource, Options, Result,
    SourceCapability, TaskContext, TaskWriter, WriteMode,
};

spark_native_source::export_data_source!(JsonSink, batch_write, truncate);

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// Construction and capability discovery do not access the filesystem.
pub struct JsonSink {
    path: PathBuf,
    readonly: bool,
}

impl DataSource for JsonSink {
    fn new(mut options: Options) -> Result<Self> {
        let path = PathBuf::from(
            options
                .remove("path")
                .ok_or("The path option is required")?,
        );
        if !path.is_absolute() {
            return Err("The path must be absolute and shared by the driver and executors".into());
        }
        let readonly = match options.remove("readonly").as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err("The readonly option must be true or false".into()),
        };
        if !options.is_empty() {
            return Err(
                format!("Unknown options: {:?}", options.keys().collect::<Vec<_>>()).into(),
            );
        }
        Ok(Self { path, readonly })
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(sink_schema())
    }

    fn capabilities(&self, supported: &[SourceCapability]) -> Vec<SourceCapability> {
        if self.readonly {
            Vec::new()
        } else {
            supported.to_vec()
        }
    }
}

impl BatchWriteSource for JsonSink {
    type Write = JsonWrite;

    fn write(&self, schema: SchemaRef, mode: WriteMode) -> Result<Self::Write> {
        if self.readonly {
            return Err("This sink is readonly".into());
        }
        validate_schema(&schema)?;
        JsonWrite::new(self.path.clone(), mode)
    }
}

fn sink_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn validate_schema(schema: &Schema) -> Result<()> {
    let fields = schema.fields();
    if fields.len() != 2
        || fields[0].name() != "id"
        || fields[0].data_type() != &DataType::Int64
        || fields[1].name() != "label"
        || fields[1].data_type() != &DataType::Utf8
    {
        return Err("Expected columns id BIGINT, label STRING, in that order".into());
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DataFile {
    path: String,
    rows: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Manifest {
    version: u32,
    files: Vec<DataFile>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: 1,
            files: Vec::new(),
        }
    }
}

fn read_manifest(root: &Path) -> Result<Manifest> {
    let file = match File::open(root.join("manifest.json")) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Manifest::default()),
        Err(error) => return Err(error.into()),
    };
    let manifest: Manifest = serde_json::from_reader(file)?;
    if manifest.version != 1 {
        return Err("Unsupported sink manifest version".into());
    }
    for file in &manifest.files {
        let components = Path::new(&file.path).components().collect::<Vec<_>>();
        if components.len() != 3
            || components[0] != Component::Normal("objects".as_ref())
            || !components.iter().all(|c| matches!(c, Component::Normal(_)))
        {
            return Err("Invalid sink manifest data path".into());
        }
    }
    Ok(manifest)
}

/// This lock is held for the whole write operation, including all task attempts.
struct WriteLock(PathBuf);

impl WriteLock {
    fn acquire(root: &Path) -> Result<Self> {
        let path = root.join(".write-lock");
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self(path)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                Err("Another write owns .write-lock; concurrent writes are unsupported".into())
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.0);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteState {
    Open,
    Committed,
    Aborted,
}

pub struct JsonWrite {
    root: PathBuf,
    operation: String,
    staging: PathBuf,
    objects: PathBuf,
    temporary_manifest: PathBuf,
    previous: Manifest,
    mode: WriteMode,
    state: WriteState,
    sealed: bool,
    lock: Option<WriteLock>,
}

impl JsonWrite {
    fn new(root: PathBuf, mode: WriteMode) -> Result<Self> {
        if !cfg!(unix) {
            return Err("This demo requires a POSIX filesystem".into());
        }
        fs::create_dir_all(&root)?;
        let lock = WriteLock::acquire(&root)?;
        let previous = read_manifest(&root)?;
        fs::create_dir_all(root.join(".staging"))?;
        fs::create_dir_all(root.join("objects"))?;
        let staging = unique_directory(&root.join(".staging"))?;
        let operation = staging.file_name().unwrap().to_str().unwrap().to_owned();
        Ok(Self {
            objects: root.join("objects").join(&operation),
            temporary_manifest: root.join(format!(".manifest-{operation}.json")),
            root,
            operation,
            staging,
            previous,
            mode,
            state: WriteState::Open,
            sealed: false,
            lock: Some(lock),
        })
    }

    fn publish(&mut self, messages: Vec<CommitMessage>) -> Result<()> {
        let mut filenames = BTreeSet::new();
        let mut partitions = BTreeSet::new();
        for message in &messages {
            if message.operation != self.operation
                || !is_filename(&message.filename)
                || !filenames.insert(message.filename.clone())
            {
                return Err("Invalid or duplicate task commit message".into());
            }
            if let Some(partition) = message.partition {
                if !partitions.insert(partition) {
                    return Err("More than one winning attempt for the same partition".into());
                }
            }
            if !self.staging.join(&message.filename).is_file() {
                return Err("A winning task's staged file is missing".into());
            }
        }

        // Seal the entire operation directory. New attempts can no longer open
        // a staged file, and existing losing files remain invisible to readers.
        fs::rename(&self.staging, &self.objects)?;
        self.sealed = true;
        for entry in fs::read_dir(&self.objects)? {
            let entry = entry?;
            if !filenames.contains(&entry.file_name().to_string_lossy().into_owned()) {
                fs::remove_file(entry.path())?;
            }
        }

        let mut files = match self.mode {
            WriteMode::Append => self.previous.files.clone(),
            WriteMode::Truncate => Vec::new(),
        };
        files.extend(messages.into_iter().map(|message| DataFile {
            path: format!("objects/{}/{}", self.operation, message.filename),
            rows: message.rows,
        }));
        let manifest = Manifest { version: 1, files };
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.temporary_manifest)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &manifest)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);

        // This single rename is the commit point. No existing data is deleted,
        // including during truncate. No fallible work follows publication.
        fs::rename(&self.temporary_manifest, self.root.join("manifest.json"))?;
        self.state = WriteState::Committed;
        self.lock.take();
        Ok(())
    }

    fn cleanup(&self) -> Result<()> {
        // Do not rely on commit messages: failed tasks and losing attempts may
        // have staged files without returning a message to the driver.
        let mut first_error = None;
        for result in [
            fs::remove_dir_all(&self.staging),
            if self.sealed {
                fs::remove_dir_all(&self.objects)
            } else {
                Ok(())
            },
            fs::remove_file(&self.temporary_manifest),
        ] {
            if let Err(error) = result {
                if error.kind() != ErrorKind::NotFound && first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

impl BatchWrite for JsonWrite {
    type Factory = JsonWriterFactory;

    fn factory(&mut self) -> Result<Self::Factory> {
        if self.state != WriteState::Open {
            return Err("The write operation is no longer open".into());
        }
        Ok(JsonWriterFactory {
            staging: self.staging.clone(),
            operation: self.operation.clone(),
        })
    }

    fn commit(&mut self, messages: Vec<CommitMessage>) -> Result<()> {
        match self.state {
            WriteState::Committed => Ok(()),
            WriteState::Aborted => Err("The write operation was aborted".into()),
            WriteState::Open => self.publish(messages),
        }
    }

    fn abort(&mut self, _messages: Vec<Option<CommitMessage>>) -> Result<()> {
        if self.state == WriteState::Committed {
            return Ok(());
        }
        let result = self.cleanup();
        self.state = WriteState::Aborted;
        self.lock.take();
        result
    }
}

impl Drop for JsonWrite {
    fn drop(&mut self) {
        if self.state != WriteState::Committed {
            let _ = self.cleanup();
        }
    }
}

/// Serializable values only: the driver lock and open files stay on their hosts.
#[derive(Deserialize, Serialize)]
pub struct JsonWriterFactory {
    staging: PathBuf,
    operation: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitMessage {
    operation: String,
    filename: String,
    partition: Option<i32>,
    rows: u64,
}

impl BatchWriterFactory for JsonWriterFactory {
    type Message = CommitMessage;

    fn open<'task>(&self, task: TaskContext<'task>) -> Result<TaskWriter<'task, Self::Message>> {
        if task.is_interrupted() {
            return Err("Task cancelled before opening the sink".into());
        }
        // Do not recreate staging: its absence means the driver sealed or
        // aborted this operation. create_new also separates speculative tasks.
        for _ in 0..16 {
            let filename = format!(
                "part-{}-attempt-{}-{}.jsonl",
                task.partition_id().unwrap_or(-1),
                task.task_attempt_id().unwrap_or(-1),
                unique_id()?
            );
            let path = self.staging.join(&filename);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(Box::new(JsonTaskWriter {
                        task,
                        path,
                        message: CommitMessage {
                            operation: self.operation.clone(),
                            filename,
                            partition: task.partition_id(),
                            rows: 0,
                        },
                        writer: Some(BufWriter::new(file)),
                        prepared: false,
                        failed: false,
                    }));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err("Could not create a unique task attempt file".into())
    }
}

#[derive(Serialize)]
struct Row<'a> {
    id: Option<i64>,
    label: Option<&'a str>,
}

struct JsonTaskWriter<'task> {
    task: TaskContext<'task>,
    path: PathBuf,
    message: CommitMessage,
    writer: Option<BufWriter<File>>,
    prepared: bool,
    failed: bool,
}

impl JsonTaskWriter<'_> {
    fn discard_buffer(&mut self) {
        if let Some(writer) = self.writer.take() {
            // BufWriter normally flushes on drop; abort discards buffered rows.
            let (file, _) = writer.into_parts();
            drop(file);
        }
    }

    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.prepared || self.failed {
            return Err("The task writer is no longer accepting batches".into());
        }
        validate_schema(batch.schema().as_ref())?;
        let writer = self.writer.as_mut().ok_or("The task writer is closed")?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            if index % 1024 == 0 && self.task.is_interrupted() {
                return Err("Task cancelled while writing the sink".into());
            }
            let row = Row {
                id: (!ids.is_null(index)).then(|| ids.value(index)),
                label: (!labels.is_null(index)).then(|| labels.value(index)),
            };
            serde_json::to_writer(&mut *writer, &row)?;
            writer.write_all(b"\n")?;
        }
        self.message.rows = self
            .message
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or("Task row count overflow")?;
        Ok(())
    }

    fn prepare(&mut self) -> Result<CommitMessage> {
        if self.failed {
            return Err("Cannot commit a failed task writer".into());
        }
        if self.prepared {
            return Ok(self.message.clone());
        }
        if self.task.is_interrupted() {
            return Err("Task cancelled before committing the sink".into());
        }
        let mut writer = self.writer.take().ok_or("The task writer is closed")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        self.prepared = true;
        Ok(self.message.clone())
    }
}

impl BatchWriter for JsonTaskWriter<'_> {
    type Message = CommitMessage;

    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let result = self.write_batch(batch);
        self.failed |= result.is_err();
        result
    }

    fn commit(&mut self) -> Result<Self::Message> {
        let result = self.prepare();
        self.failed |= result.is_err();
        result
    }

    fn abort(&mut self) -> Result<()> {
        self.discard_buffer();
        self.prepared = false;
        self.failed = true;
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for JsonTaskWriter<'_> {
    fn drop(&mut self) {
        if !self.prepared {
            self.discard_buffer();
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn is_filename(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn unique_id() -> Result<String> {
    Ok(format!(
        "{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn unique_directory(parent: &Path) -> Result<PathBuf> {
    for _ in 0..16 {
        let path = parent.join(unique_id()?);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err("Could not create a unique write operation directory".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            Self(unique_directory(&std::env::temp_dir()).unwrap())
        }

        fn source(&self) -> JsonSink {
            JsonSink::new(Options::from([(
                "path".to_owned(),
                self.0.to_str().unwrap().to_owned(),
            )]))
            .unwrap()
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn batch(ids: Vec<Option<i64>>) -> RecordBatch {
        let labels = ids
            .iter()
            .map(|id| id.map(|id| format!("row-{id}")))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            sink_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap()
    }

    fn stage(factory: &JsonWriterFactory, ids: Vec<Option<i64>>) -> CommitMessage {
        // Executors reconstruct the factory without the source or driver write.
        let factory: JsonWriterFactory =
            serde_json::from_slice(&serde_json::to_vec(factory).unwrap()).unwrap();
        let mut writer = factory.open(TaskContext::detached()).unwrap();
        writer.write(batch(ids)).unwrap();
        let message = writer.commit().unwrap();
        serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap()
    }

    fn committed_rows(path: &Path) -> Vec<serde_json::Value> {
        read_manifest(path)
            .unwrap()
            .files
            .iter()
            .flat_map(|file| {
                let text = fs::read_to_string(path.join(&file.path)).unwrap();
                let rows = text
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(rows.len() as u64, file.rows);
                rows
            })
            .collect()
    }

    fn commit_ids(source: &JsonSink, mode: WriteMode, ids: Vec<Option<i64>>) {
        let mut write = source.write(sink_schema(), mode).unwrap();
        let message = stage(&write.factory().unwrap(), ids);
        write.commit(vec![message]).unwrap();
    }

    #[test]
    fn append_publishes_all_committed_rows_and_preserves_nulls() {
        let directory = TempDirectory::new();
        let source = directory.source();
        commit_ids(&source, WriteMode::Append, vec![Some(1), None]);
        commit_ids(&source, WriteMode::Append, vec![Some(2)]);
        assert_eq!(
            committed_rows(&directory.0),
            vec![
                serde_json::json!({"id": 1, "label": "row-1"}),
                serde_json::json!({"id": null, "label": null}),
                serde_json::json!({"id": 2, "label": "row-2"}),
            ]
        );
        assert_eq!(read_manifest(&directory.0).unwrap().files.len(), 2);
        assert!(!directory.0.join(".write-lock").exists());
    }

    #[test]
    fn truncate_replaces_manifest_but_retains_previously_published_files() {
        let directory = TempDirectory::new();
        let source = directory.source();
        commit_ids(&source, WriteMode::Append, vec![Some(1)]);
        let previous = read_manifest(&directory.0).unwrap();
        commit_ids(&source, WriteMode::Truncate, vec![Some(2)]);
        assert_eq!(
            committed_rows(&directory.0),
            vec![serde_json::json!({"id": 2, "label": "row-2"})]
        );
        assert!(directory.0.join(&previous.files[0].path).is_file());
        commit_ids(&source, WriteMode::Truncate, Vec::new());
        assert!(committed_rows(&directory.0).is_empty());
    }

    #[test]
    fn driver_publishes_only_winning_attempts_and_removes_unreported_files() {
        let directory = TempDirectory::new();
        let mut write = directory
            .source()
            .write(sink_schema(), WriteMode::Append)
            .unwrap();
        let factory = write.factory().unwrap();
        let winning = stage(&factory, vec![Some(1)]);
        let _losing = stage(&factory, vec![Some(2)]);
        let _missing_message = stage(&factory, vec![Some(3)]);
        assert_eq!(fs::read_dir(&write.staging).unwrap().count(), 3);
        write.commit(vec![winning]).unwrap();
        assert!(!write.staging.exists());
        assert_eq!(fs::read_dir(&write.objects).unwrap().count(), 1);
        assert_eq!(
            committed_rows(&directory.0),
            vec![serde_json::json!({"id": 1, "label": "row-1"})]
        );
        assert!(factory.open(TaskContext::detached()).is_err());
    }

    #[test]
    fn abort_cleans_the_operation_even_with_missing_messages() {
        let directory = TempDirectory::new();
        let source = directory.source();
        commit_ids(&source, WriteMode::Append, vec![Some(1)]);
        let before = fs::read(directory.0.join("manifest.json")).unwrap();
        let mut write = source.write(sink_schema(), WriteMode::Truncate).unwrap();
        let factory = write.factory().unwrap();
        let message = stage(&factory, vec![Some(2)]);
        let _missing = stage(&factory, vec![Some(3)]);
        let mut incomplete = factory.open(TaskContext::detached()).unwrap();
        incomplete.write(batch(vec![Some(4)])).unwrap();
        write.abort(vec![Some(message), None]).unwrap();
        assert!(!write.staging.exists());
        assert!(!write.objects.exists());
        assert!(!directory.0.join(".write-lock").exists());
        assert_eq!(fs::read(directory.0.join("manifest.json")).unwrap(), before);
        assert_eq!(committed_rows(&directory.0).len(), 1);
        assert!(factory.open(TaskContext::detached()).is_err());
    }

    #[test]
    fn failed_publication_keeps_previous_manifest_and_abort_removes_sealed_files() {
        let directory = TempDirectory::new();
        let source = directory.source();
        commit_ids(&source, WriteMode::Append, vec![Some(1)]);
        let before = fs::read(directory.0.join("manifest.json")).unwrap();
        let mut write = source.write(sink_schema(), WriteMode::Truncate).unwrap();
        let message = stage(&write.factory().unwrap(), vec![Some(2)]);
        // Force create_new to fail after the operation directory has been sealed.
        fs::write(&write.temporary_manifest, b"publication failure").unwrap();
        assert!(write.commit(vec![message]).is_err());
        assert!(write.objects.exists());
        assert_eq!(fs::read(directory.0.join("manifest.json")).unwrap(), before);
        write.abort(vec![None]).unwrap();
        assert!(!write.objects.exists());
        assert!(!write.temporary_manifest.exists());
        assert_eq!(committed_rows(&directory.0).len(), 1);
    }

    #[test]
    fn concurrent_write_is_rejected_and_drop_releases_staging_and_lock() {
        let directory = TempDirectory::new();
        let source = directory.source();
        let mut write = source.write(sink_schema(), WriteMode::Append).unwrap();
        let _message = stage(&write.factory().unwrap(), vec![Some(1)]);
        let staging = write.staging.clone();
        assert!(source.write(sink_schema(), WriteMode::Append).is_err());
        drop(write);
        assert!(!staging.exists());
        assert!(!directory.0.join(".write-lock").exists());
        assert!(source.write(sink_schema(), WriteMode::Append).is_ok());
    }

    #[test]
    fn readonly_removes_all_write_capabilities_without_creating_files() {
        let directory = TempDirectory::new();
        let path = directory.0.join("not-created");
        let source = JsonSink::new(Options::from([
            ("path".to_owned(), path.to_str().unwrap().to_owned()),
            ("readonly".to_owned(), "true".to_owned()),
        ]))
        .unwrap();
        assert!(source
            .capabilities(&[SourceCapability::BatchWrite, SourceCapability::Truncate])
            .is_empty());
        assert!(source.write(sink_schema(), WriteMode::Append).is_err());
        assert!(!path.exists());
    }
}
