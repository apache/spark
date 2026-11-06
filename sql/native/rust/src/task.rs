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

use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::abi::{self, Interrupted, SparkDsTaskContext};
use crate::Result;

/// Task identifiers and cooperative cancellation, borrowed until its reader or writer is closed.
///
/// A reader or writer may retain this value and check it during long operations.
/// It cannot retain it beyond the lifetime of its returned reader or writer.
/// Blocking I/O still needs its own timeouts.
///
/// ```compile_fail
/// use spark_native_source::TaskContext;
///
/// fn retain_forever(task: TaskContext<'_>) -> TaskContext<'static> {
///     task
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TaskContext<'task> {
    stage_id: i32,
    stage_attempt_number: i32,
    partition_id: i32,
    attempt_number: i32,
    task_attempt_id: i64,
    interruption: Interruption<'task>,
}

/// Where a task learns that Spark killed it.
#[derive(Debug, Clone, Copy)]
enum Interruption<'task> {
    Never,
    Host {
        context: *const SparkDsTaskContext,
        callback: Interrupted,
        lifetime: PhantomData<&'task SparkDsTaskContext>,
    },
    Flag(&'task AtomicBool),
}

// The C ABI permits task callbacks from any thread. The lifetime prevents use after the
// host releases its context; the remaining fields are copied values or immutable callbacks.
unsafe impl Send for TaskContext<'_> {}
unsafe impl Sync for TaskContext<'_> {}

impl TaskContext<'static> {
    /// A context for testing a reader or writer outside Spark. Its IDs are 0 until set
    /// with the `with_*` methods, and it is never interrupted unless it is given a flag
    /// with [`interrupted_by`](Self::interrupted_by).
    pub fn for_testing() -> Self {
        Self {
            stage_id: 0,
            stage_attempt_number: 0,
            partition_id: 0,
            attempt_number: 0,
            task_attempt_id: 0,
            interruption: Interruption::Never,
        }
    }

    #[must_use]
    pub fn with_stage_id(mut self, stage_id: i32) -> Self {
        self.stage_id = stage_id;
        self
    }

    #[must_use]
    pub fn with_stage_attempt_number(mut self, stage_attempt_number: i32) -> Self {
        self.stage_attempt_number = stage_attempt_number;
        self
    }

    #[must_use]
    pub fn with_partition_id(mut self, partition_id: i32) -> Self {
        self.partition_id = partition_id;
        self
    }

    #[must_use]
    pub fn with_attempt_number(mut self, attempt_number: i32) -> Self {
        self.attempt_number = attempt_number;
        self
    }

    #[must_use]
    pub fn with_task_attempt_id(mut self, task_attempt_id: i64) -> Self {
        self.task_attempt_id = task_attempt_id;
        self
    }

    /// Reports the task as interrupted whenever `flag` is true. The context borrows the
    /// flag, so set its IDs first.
    #[must_use]
    pub fn interrupted_by(self, flag: &AtomicBool) -> TaskContext<'_> {
        TaskContext {
            interruption: Interruption::Flag(flag),
            ..self
        }
    }
}

impl<'task> TaskContext<'task> {
    /// Wraps a host context, which has every field of version 1 of the ABI.
    ///
    /// # Safety
    /// The host must keep the context and its callbacks valid for `'task`, including calls from
    /// other threads. Spark keeps it alive until the Arrow stream is released or
    /// data writer closes.
    pub(crate) unsafe fn from_raw(pointer: *const SparkDsTaskContext) -> Result<Self> {
        let size = unsafe { abi::struct_size(pointer)? };
        // Fields are only appended to the structure, so every host has those of version 1.
        if size < size_of::<SparkDsTaskContext>() as i64 {
            return Err(format!(
                "the task context has {size} bytes, fewer than in version 1 of the native ABI"
            )
            .into());
        }
        let context = unsafe { pointer.read_unaligned() };
        Ok(Self {
            stage_id: context.stage_id,
            stage_attempt_number: context.stage_attempt_number,
            partition_id: context.partition_id,
            attempt_number: context.attempt_number,
            task_attempt_id: context.task_attempt_id,
            interruption: match context.is_interrupted {
                Some(callback) => Interruption::Host {
                    context: pointer,
                    callback,
                    lifetime: PhantomData,
                },
                None => Interruption::Never,
            },
        })
    }

    /// The ID of the stage of the task.
    pub fn stage_id(&self) -> i32 {
        self.stage_id
    }

    /// The attempt number of the stage of the task.
    pub fn stage_attempt_number(&self) -> i32 {
        self.stage_attempt_number
    }

    /// The partition that the task reads or writes.
    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    /// The attempt number of the task.
    pub fn attempt_number(&self) -> i32 {
        self.attempt_number
    }

    /// The ID of the task attempt, unique within the Spark application but not across
    /// applications.
    pub fn task_attempt_id(&self) -> i64 {
        self.task_attempt_id
    }

    /// Whether Spark killed this task, read from Spark each time.
    pub fn is_interrupted(&self) -> bool {
        match self.interruption {
            Interruption::Never => false,
            Interruption::Host {
                context, callback, ..
            } => unsafe { callback(context) },
            Interruption::Flag(flag) => flag.load(Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[repr(C)]
    struct HostTask {
        task: SparkDsTaskContext,
        interrupted: AtomicBool,
    }

    unsafe extern "C" fn interrupted(task: *const SparkDsTaskContext) -> bool {
        unsafe { &*task.cast::<HostTask>() }
            .interrupted
            .load(Ordering::SeqCst)
    }

    #[test]
    fn reads_ids_and_live_cancellation_from_the_host_context() {
        let host = HostTask {
            task: SparkDsTaskContext {
                size: size_of::<SparkDsTaskContext>() as i64,
                stage_id: 3,
                stage_attempt_number: 4,
                partition_id: 5,
                attempt_number: 6,
                task_attempt_id: 7,
                is_interrupted: Some(interrupted),
            },
            interrupted: AtomicBool::new(false),
        };
        let context = unsafe { TaskContext::from_raw(&host.task) }.unwrap();
        assert_eq!(context.stage_id(), 3);
        assert_eq!(context.stage_attempt_number(), 4);
        assert_eq!(context.partition_id(), 5);
        assert_eq!(context.attempt_number(), 6);
        assert_eq!(context.task_attempt_id(), 7);
        assert!(!context.is_interrupted());
        host.interrupted.store(true, Ordering::SeqCst);
        std::thread::scope(|scope| {
            scope.spawn(|| assert!(context.is_interrupted()));
        });
    }

    #[test]
    fn rejects_a_context_without_every_field_of_version_1() {
        let prefix = size_of::<i64>() as i64;
        let pointer = (&prefix as *const i64).cast::<SparkDsTaskContext>();
        let error = unsafe { TaskContext::from_raw(pointer) }.unwrap_err();
        assert!(error.to_string().contains("version 1"), "{error}");
        assert!(unsafe { TaskContext::from_raw(std::ptr::null()) }.is_err());
    }

    #[test]
    fn test_contexts_carry_ids_and_an_optional_interruption_flag() {
        let context = TaskContext::for_testing()
            .with_stage_id(1)
            .with_stage_attempt_number(2)
            .with_partition_id(3)
            .with_attempt_number(4)
            .with_task_attempt_id(5);
        assert_eq!(
            (
                context.stage_id(),
                context.stage_attempt_number(),
                context.partition_id(),
                context.attempt_number(),
                context.task_attempt_id()
            ),
            (1, 2, 3, 4, 5)
        );
        assert!(!context.is_interrupted());
        let flag = AtomicBool::new(false);
        let context = TaskContext::for_testing()
            .with_partition_id(8)
            .interrupted_by(&flag);
        assert_eq!(context.partition_id(), 8);
        assert!(!context.is_interrupted());
        flag.store(true, Ordering::SeqCst);
        assert!(context.is_interrupted());
    }
}
