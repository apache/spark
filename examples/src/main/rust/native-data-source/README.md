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

# Rust native range data source

This example implements batch reads using the [Rust native data source SDK][sdk].
The connector in [src/lib.rs](src/lib.rs) exports `rust_sdk_range` through the SDK's JNI macro.
See the SDK guide for API contracts, ownership rules, and local test instructions.

The example targets NativeBridge ABI version 1 from [Spark PR #59313][native-source-pr], pinned
to commit `ff7b1ae0150fbcb3b6a7c67bea8accf6aa4ad9bd`. Set `SPARK_HOME` to a Spark distribution
built with that PR before running the Spark commands below.

## Data and options

| Column | Spark type | Value |
| --- | --- | --- |
| `id` | `BIGINT`, non-null | Integers in `[start, end)` |
| `label` | `STRING`, non-null | `row-{id}`, such as `row-13` |

| Option | Default | Meaning |
| --- | --- | --- |
| `start` | `0` | Inclusive start, as a signed 64-bit integer |
| `end` | `100` | Exclusive end; must be at least `start` |
| `numpartitions` | `2` | Maximum number of partitions; must be positive |
| `batchsize` | `1024` | Maximum rows per Arrow batch; must be positive |

Spark normalizes option names to lower case. Unknown options are rejected.

The source evaluates `=`, `<=>`, `<`, `<=`, `>`, and `>=` comparisons between `id` and non-null
integer literals, including conjunctions of supported predicates. Other predicates remain in
Spark. A global limit is applied after exact filters and before splitting the range into partitions;
the SDK declines limit pushdown when a residual predicate remains. Column pruning preserves the
requested order and supports zero-column batches for operations such as `COUNT(*)`.

## Build and run

Run these commands from the repository root. The workspace, lockfile, and build output belong to
`sql/native/rust`; the example is a member of that workspace.

```sh
cargo build --manifest-path sql/native/rust/Cargo.toml --workspace --locked

python3 examples/src/main/rust/native-data-source/package.py \
  --library sql/native/rust/target/debug/libspark_datasource_rust_sdk_range.so \
  --output sql/native/rust/target/rust_sdk_range.sparkpkg

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' \
  examples/src/main/rust/native-data-source/smoke.py \
  --package sql/native/rust/target/rust_sdk_range.sparkpkg
```

The library name above is for Linux. On macOS use `libspark_datasource_rust_sdk_range.dylib`;
on Windows use `spark_datasource_rust_sdk_range.dll`. The packaging script detects the host
platform. For a cross-compiled library, supply `--platform`, for example `linux-aarch64`.

The package can also be used from a Python session backed by the same Spark build:

```python
spark.addArtifact("sql/native/rust/target/rust_sdk_range.sparkpkg")
df = (
    spark.read.format("rust_sdk_range")
    .option("end", 100)
    .option("numpartitions", 4)
    .load()
)
df.where("id >= 10 AND id < 20").select("label", "id").show()
```

[smoke.py](smoke.py) checks the resulting rows, projection, limits, zero-column counts, and
filters that Spark must evaluate. This Spark smoke script has not yet been run against the
required distribution. The local JVM integration test exercises JNI and Arrow C Stream without
launching Spark; it does not establish end-to-end Spark compatibility.

[sdk]: ../../../../../sql/native/rust/README.md
[native-source-pr]: https://github.com/apache/spark/pull/59313
