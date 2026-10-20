<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Rust native batch sink

This write-only example implements `DataSource`, `BatchWrite`, `DataWriterFactory`,
and `DataWriter` from the [Rust native data source SDK][sdk]. Its `DataSource` sets
`type ScanBuilder = Unsupported` and declares overwrite support with
`const SUPPORTS_TRUNCATE: bool = true`; it exports `export_data_source!(JsonSink)` and
implements no read traits. The SDK supplies the C ABI entry point, typed factory and commit
message serialization, Arrow batch import, capability checks, and error handling.

The example targets C ABI version 1 from [Spark PR #59424][native-source-pr], pinned
to `74aa0dffc53463034fa6549796c4dd599e9705de`. Running it in Spark requires that
framework and Java 22 or later on the driver and executors. The standalone Rust
tests do not need Spark or a JVM.

## Supported data and options

The sink accepts exactly two columns, in this order: `id BIGINT` and `label STRING`.
Both nullable and non-nullable input fields are accepted. Null values are preserved.
Task writers encode batches incrementally as JSON lines, avoiding an additional
file-format dependency in this small example.

| Option | Default | Meaning |
| --- | --- | --- |
| `path` | Required | Absolute directory shared by the driver and every executor |
| `readonly` | `false` | Remove both batch-write and truncate capabilities when `true` |

Unknown options are rejected. Source creation and capability discovery do not create
files. The default instance advertises only native `BatchWrite` and `Truncate`; it
does not advertise batch reads or streaming operations. Spark performs its native
capability check again with the operation's effective options.

## Commit and abort protocol

Each write operation holds a `.write-lock` directory under `path`, rejecting another
concurrent writer. It assigns an operation ID and stages tasks in `.staging/<operation>`.
Each task attempt exclusively creates its own file, writes Arrow batches, flushes and
syncs the file, then returns a typed message containing its operation, file, partition,
and row count. Task commit alone does not publish data.

The driver receives the winning attempt messages, the message of partition `i` at
position `i`. It validates those messages and renames the entire staging directory
to `objects/<operation>`, preventing new tasks from opening files for that
operation. It removes every file not named by a winning
message, including speculative attempts and attempts whose messages never arrived.
It then writes and syncs a new manifest and atomically renames it to `manifest.json`.
That one manifest replacement is the commit point:

- Append includes the previous manifest's files plus the winning new files.
- Truncate includes only the winning new files, including an empty result.
- Previously published data files are retained, so an existing manifest snapshot
  stays readable and a failed overwrite cannot delete the old data.
- Abort removes the entire operation, including files with no commit message and
  a sealed directory whose manifest publication failed. It leaves the current
  manifest and its data unchanged. Dropping an unfinished coordinator also performs
  best-effort cleanup. Dropping a committed task writer leaves its file for the driver.

To inspect output, read only the files listed in `manifest.json`. Recursively reading
the destination directory would also pick up data from previous truncate operations.
For example, after a successful write:

```python
import json
from pathlib import Path

root = Path("/absolute/shared/path/native-sink")
manifest = json.loads((root / "manifest.json").read_text())
files = [str(root / entry["path"]) for entry in manifest["files"]]
if files:
    spark.read.schema("id LONG, label STRING").json(files).show()
```

## Build, package, and run

From the repository root:

```sh
cargo build --manifest-path sql/native/rust/Cargo.toml --workspace --locked
cargo test --manifest-path sql/native/rust/Cargo.toml -p spark-native-sink-demo --locked

python3 examples/src/main/rust/native-data-source/package.py \
  --source rust_sdk_sink \
  --library sql/native/rust/target/debug/libspark_datasource_rust_sdk_sink.so \
  --output sql/native/rust/target/rust_sdk_sink.sparkpkg

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' \
  examples/src/main/rust/native-data-sink/smoke.py \
  --package sql/native/rust/target/rust_sdk_sink.sparkpkg
```

Use `libspark_datasource_rust_sdk_sink.dylib` for macOS. The smoke script uses a
temporary local directory and requires a local Spark master. It checks append,
overwrite, a failed overwrite that preserves published data and cleans staging,
and write rejection with `readonly=true`. Rust tests additionally exercise losing
attempts, missing commit messages, a driver publication failure, and lock cleanup.

An application can write with:

```python
spark.addArtifact("sql/native/rust/target/rust_sdk_sink.sparkpkg")
data = spark.range(10).selectExpr("id", "concat('row-', id) AS label")
data.write.format("rust_sdk_sink").mode("append").save("/absolute/shared/path/native-sink")
data.write.format("rust_sdk_sink").mode("overwrite").save("/absolute/shared/path/native-sink")
```

## Limits of this example

Use a local POSIX filesystem, or a shared filesystem with atomic directory renames,
atomic file replacement, and coherent exclusive creation. The same absolute path must
refer to the same storage on all hosts. Executor-local paths in a distributed cluster,
object-store URLs, and Windows are unsupported. Only one write operation can own a
destination at a time; the lock is not a distributed lease or fencing protocol.

This example demonstrates atomic visibility during normal execution and abort, not
crash recovery or exactly-once publication across driver retries. It syncs files but
does not implement a crash-durable metadata journal. Process or machine failure can
leave a stale `.write-lock`, staged files, or unreferenced objects. Recover the lock
only after confirming no operation still owns it. Retained files from old manifests
also need garbage collection with an appropriate reader-retention policy; this demo
does not implement that policy.

[sdk]: ../../../../../sql/native/rust/README.md
[native-source-pr]: https://github.com/apache/spark/pull/59424
