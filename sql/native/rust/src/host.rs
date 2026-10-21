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

//! The Spark process that loaded the library: its version and its log.

use std::ffi::{c_char, CString};
use std::sync::{Once, PoisonError, RwLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

pub(crate) type LogFunction = unsafe extern "C" fn(i32, *const c_char);

// Copied from the host when Spark initializes the library, so that logging never
// reads the host structure afterwards.
static LOG: RwLock<Option<LogFunction>> = RwLock::new(None);

/// The Spark process that loaded the library, as [`DataSource::init`](crate::DataSource::init)
/// receives it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HostInfo {
    /// The newest version of the native data source ABI that Spark supports.
    pub abi_version: i32,
    /// The version of Spark, such as `4.4.0`, when Spark provides it.
    pub spark_version: Option<String>,
}

impl Default for HostInfo {
    /// A host that supports the ABI version of this SDK, for testing outside Spark.
    fn default() -> Self {
        Self {
            abi_version: crate::abi::ABI_VERSION,
            spark_version: None,
        }
    }
}

/// Passes the records of the `log` crate to the log of the host, from any thread.
/// Records are dropped until Spark initializes the library, for example in unit tests.
struct HostLog;

impl Log for HostLog {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        LOG.read().unwrap_or_else(PoisonError::into_inner).is_some()
    }

    fn log(&self, record: &Record<'_>) {
        let function = *LOG.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(function) = function {
            // The SPARK_DS_LOG_* value of the level.
            let level = match record.level() {
                Level::Error => 1,
                Level::Warn => 2,
                Level::Info => 3,
                Level::Debug => 4,
                Level::Trace => 5,
            };
            let message = record.args().to_string().replace('\0', "\\0");
            let message = CString::new(message).expect("NULs removed");
            unsafe { function(level, message.as_ptr()) };
        }
    }

    fn flush(&self) {}
}

static HOST_LOG: HostLog = HostLog;

/// Installs the log of the host that initializes the library.
pub(crate) fn install(function: Option<LogFunction>) {
    *LOG.write().unwrap_or_else(PoisonError::into_inner) = function;
    // Only the first initialization installs the logger of the `log` crate, unless the
    // library installed its own, and its default level, which the library may change.
    static LOGGER: Once = Once::new();
    LOGGER.call_once(|| {
        if log::set_logger(&HOST_LOG).is_ok() {
            log::set_max_level(LevelFilter::Info);
        }
    });
}
