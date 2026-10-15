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

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn c_abi_arrow_round_trip() -> Result<(), Box<dyn Error>> {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = TestDirectory(
        std::env::temp_dir().join(format!("spark-native-c-{}-{unique}", std::process::id())),
    );
    fs::create_dir(&directory.0)?;

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let header = manifest.join("../../../../../sql/native/rust/tests/fixtures");
    let harness = directory.0.join("c_abi_smoke");
    let compiler = std::env::var_os("CC").unwrap_or_else(|| "cc".into());
    let mut command = Command::new(compiler);
    command
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror"])
        .arg("-I")
        .arg(header)
        .arg(manifest.join("tests/c_abi_smoke.c"))
        .arg("-o")
        .arg(&harness);
    if cfg!(target_os = "linux") {
        command.arg("-ldl");
    }
    let compiled = command.output()?;
    assert!(
        compiled.status.success(),
        "C ABI harness did not compile:\n{}",
        String::from_utf8_lossy(&compiled.stderr)
    );

    let executable = std::env::current_exe()?;
    let profile = executable
        .parent()
        .and_then(Path::parent)
        .ok_or("Missing Cargo profile directory")?;
    let library = profile.join(format!(
        "{}spark_datasource_rust_sdk_range{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    assert!(
        library.is_file(),
        "Run cargo build --workspace first: {}",
        library.display()
    );

    let result = Command::new(harness).arg(library).output()?;
    assert!(
        result.status.success(),
        "C ABI harness failed ({}):\n{}\n{}",
        result.status,
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(())
}
