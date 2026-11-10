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

use std::ffi::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Once, PoisonError, RwLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

use crate::bridge::c_message;

pub(crate) type LogFunction = unsafe extern "C" fn(i32, *const c_char);
pub(crate) type LogEnabledFunction = unsafe extern "C" fn(i32) -> bool;

/// The log of the host, copied from the host when Spark initializes the library, so that
/// logging never reads the host structure afterwards.
#[derive(Clone, Copy)]
struct HostLog {
    log: LogFunction,
    is_log_enabled: Option<LogEnabledFunction>,
}

static HOST: RwLock<Option<HostLog>> = RwLock::new(None);
// Whether the logger of the `log` crate is the SDK's, which follows the level of Spark.
static INSTALLED: AtomicBool = AtomicBool::new(false);

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

/// The SPARK_DS_LOG_* value of a level.
fn native_level(level: Level) -> i32 {
    match level {
        Level::Error => 1,
        Level::Warn => 2,
        Level::Info => 3,
        Level::Debug => 4,
        Level::Trace => 5,
    }
}

fn host() -> Option<HostLog> {
    *HOST.read().unwrap_or_else(PoisonError::into_inner)
}

/// Passes the records of the `log` crate to the log of the host, from any thread.
/// Records are dropped until Spark initializes the library, for example in unit tests.
struct Logger;

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        host().is_some() && metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if let Some(host) = host() {
            let message = c_message(record.args().to_string());
            unsafe { (host.log)(native_level(record.level()), message.as_ptr()) };
        }
    }

    fn flush(&self) {}
}

static LOGGER: Logger = Logger;

/// Installs the log of the host that initializes the library.
pub(crate) fn install(log: Option<LogFunction>, is_log_enabled: Option<LogEnabledFunction>) {
    *HOST.write().unwrap_or_else(PoisonError::into_inner) = log.map(|log| HostLog {
        log,
        is_log_enabled,
    });
    // Only the first initialization installs the logger of the `log` crate, unless the
    // library installed its own.
    static ONCE: Once = Once::new();
    ONCE.call_once(|| INSTALLED.store(log::set_logger(&LOGGER).is_ok(), Ordering::SeqCst));
    follow_level();
}

/// Sets the level of the `log` crate to the most verbose level that the log of Spark writes, or
/// to `Info` with a host that does not tell. The level of Spark can change while it runs, and
/// asking is a call into the JVM, so the SDK asks again only when an operation or a task starts.
pub(crate) fn follow_level() {
    if !INSTALLED.load(Ordering::SeqCst) {
        return;
    }
    let level = match host() {
        None => LevelFilter::Off,
        Some(HostLog {
            is_log_enabled: None,
            ..
        }) => LevelFilter::Info,
        Some(HostLog {
            is_log_enabled: Some(enabled),
            ..
        }) => [
            Level::Trace,
            Level::Debug,
            Level::Info,
            Level::Warn,
            Level::Error,
        ]
        .into_iter()
        .find(|level| unsafe { enabled(native_level(*level)) })
        .map_or(LevelFilter::Off, |level| level.to_level_filter()),
    };
    log::set_max_level(level);
}
