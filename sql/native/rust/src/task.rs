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
use std::mem::offset_of;

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
#[derive(Clone, Copy)]
pub struct TaskContext<'task> {
    pointer: *const SparkDsTaskContext,
    stage_id: Option<i32>,
    stage_attempt_number: Option<i32>,
    partition_id: Option<i32>,
    attempt_number: Option<i32>,
    task_attempt_id: Option<i64>,
    interrupted: Option<Interrupted>,
    lifetime: PhantomData<&'task SparkDsTaskContext>,
}

// The C ABI permits task callbacks from any thread. The lifetime prevents use after the
// host releases its context; the remaining fields are copied values or immutable callbacks.
unsafe impl Send for TaskContext<'_> {}
unsafe impl Sync for TaskContext<'_> {}

impl TaskContext<'static> {
    /// A context without Spark task metadata, for testing a reader or writer directly.
    pub fn detached() -> Self {
        Self {
            pointer: std::ptr::null(),
            stage_id: None,
            stage_attempt_number: None,
            partition_id: None,
            attempt_number: None,
            task_attempt_id: None,
            interrupted: None,
            lifetime: PhantomData,
        }
    }
}

impl<'task> TaskContext<'task> {
    /// Wraps a host context without reading fields beyond its declared size.
    ///
    /// # Safety
    /// The host must keep the context and its callbacks valid for `'task`, including calls from
    /// other threads. Spark keeps it alive until the Arrow stream is released or
    /// data writer closes.
    #[doc(hidden)]
    pub unsafe fn from_raw(pointer: *const SparkDsTaskContext) -> Result<Self> {
        let size = unsafe { abi::struct_size(pointer)? };
        let bytes = pointer.cast();
        Ok(Self {
            pointer,
            stage_id: unsafe { abi::field(bytes, size, offset_of!(SparkDsTaskContext, stage_id)) },
            stage_attempt_number: unsafe {
                abi::field(
                    bytes,
                    size,
                    offset_of!(SparkDsTaskContext, stage_attempt_number),
                )
            },
            partition_id: unsafe {
                abi::field(bytes, size, offset_of!(SparkDsTaskContext, partition_id))
            },
            attempt_number: unsafe {
                abi::field(bytes, size, offset_of!(SparkDsTaskContext, attempt_number))
            },
            task_attempt_id: unsafe {
                abi::field(bytes, size, offset_of!(SparkDsTaskContext, task_attempt_id))
            },
            interrupted: unsafe {
                abi::field(bytes, size, offset_of!(SparkDsTaskContext, is_interrupted))
            }
            .flatten(),
            lifetime: PhantomData,
        })
    }

    pub fn stage_id(&self) -> Option<i32> {
        self.stage_id
    }

    pub fn stage_attempt_number(&self) -> Option<i32> {
        self.stage_attempt_number
    }

    pub fn partition_id(&self) -> Option<i32> {
        self.partition_id
    }

    pub fn attempt_number(&self) -> Option<i32> {
        self.attempt_number
    }

    pub fn task_attempt_id(&self) -> Option<i64> {
        self.task_attempt_id
    }

    /// Whether Spark killed this task. Older contexts without the callback return false.
    pub fn is_interrupted(&self) -> bool {
        self.interrupted
            .is_some_and(|callback| unsafe { callback(self.pointer) })
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
        assert_eq!(context.stage_id(), Some(3));
        assert_eq!(context.stage_attempt_number(), Some(4));
        assert_eq!(context.partition_id(), Some(5));
        assert_eq!(context.attempt_number(), Some(6));
        assert_eq!(context.task_attempt_id(), Some(7));
        assert!(!context.is_interrupted());
        host.interrupted.store(true, Ordering::SeqCst);
        std::thread::scope(|scope| {
            scope.spawn(|| assert!(context.is_interrupted()));
        });
    }

    #[test]
    fn only_reads_fields_present_in_the_declared_prefix() {
        let prefix = size_of::<i64>() as i64;
        let pointer = (&prefix as *const i64).cast::<SparkDsTaskContext>();
        let context = unsafe { TaskContext::from_raw(pointer) }.unwrap();
        assert_eq!(context.stage_id(), None);
        assert_eq!(context.task_attempt_id(), None);
        assert!(!context.is_interrupted());
        assert!(!TaskContext::detached().is_interrupted());
    }
}
