---
layout: global
title: Compact UI Store Prototype
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

This prototype reduces the object graphs retained for Spark UI history. Enable it with
`spark.ui.store.compact.enabled=true`. It is disabled by default. Retention counts and public
REST responses keep their existing meanings; compaction is lossless.

## Storage and lifecycle

`CompactInMemoryStore` implements `KVStore` independently of `InMemoryStore`. Numeric natural
keys and stage membership use primitive hash tables. Generic records use encoded payloads and
packed index projections; larger protobuf payloads are compressed when that saves space.
Existing protobuf serializers and their JSON fallback remain usable.

Tasks have a specialized codec. Running snapshots remain cheap to replace. Completed tasks
first use signed varints, then become stage-attempt column blocks. Columns use constant values,
implicit zeros or bit-packed offsets from their minimum. Repeated strings use block dictionaries.
Small stages keep varint records when column headers would be more expensive. Pending builders
are bounded, and stage completion and listener flushes seal partial blocks.
Completed-task errors and accumulator details use separate encoded payloads, with compression
for larger values. Ordinary numeric sorting does not decode those payloads.

Records held by an iterator remain readable after replacement, eviction or compaction. Sealing
publishes a new representation of the same values. Late writes create replacement records.
Evicting a stage removes both records and parent membership. Old blocks are reclaimed when the
last store record and iterator reference disappear.

Completed jobs separate list fields from tags, kill reasons and internal detail. SQL executions
separate descriptions, status, timestamps and job/stage relationships from plans, configurations
and metric payloads. SQL job-status updates reuse unchanged detail payloads. The job table loads
full records and formats rows after selecting a page. The SQL table and child-execution lookup
read summaries; detail and public REST requests reconstruct full records.

## Query allocation

Range and parent filters run before sorting. Small finite pages use bounded selection, retaining
at most 16,384 candidate references. Only consumed iterator rows are decoded. Counts do not sort
records. Task-table search counts every matching row while retaining only the requested page.

Large offsets and unbounded views still sort O(N) compact record references. Full-detail REST
responses still materialize their requested data, and exact SQL medians/custom metrics require
temporary value arrays. These paths do not have a hard query-memory limit. The prototype adds
no unbounded decoded-record cache.

## Listener state

Job and stage completion sets use exact compressed bitmaps. Job deduplication still keys on
stage ID and task index, independently of stage-attempt ID.

With the compact option enabled, built-in SQL sums retain completed totals and replaceable
unfinished contributions. Other metrics use exact paged long arrays, compacting completed
pages and stages. Zero, negative and absent values keep their existing meanings. Custom metrics
receive the same values, and medians remain exact. Successful task-ID mappings are released;
AQE metadata is deduplicated by accumulator ID while preserving distinct historical metrics.
Aggregation processes one metric at a time instead of repeatedly concatenating stage arrays.
Unfinished sum contributions still use maps; many simultaneously unfinished tasks can use more
memory than the original dense arrays. Decoding exact metric values holds the stage lock, so
concurrent update latency needs separate measurement.

## Live UI and History Server

The live UI uses the compact backend when no `spark.ui.store.path` is configured. A disk store
continues to take precedence. Compact SQL listener metrics can also reduce live state when the
UI uses disk storage.

The History Server can use the compact backend for memory-only replay and the in-memory portion
of `HybridStore`. Hybrid replay retains the existing SQL record schema so its disk handoff does
not create a new persistent format. Handoff decodes at most 1,024 records per write batch.
Normal disk replay remains unchanged.

Compression does not impose a total memory budget. Active state, retained history, concurrent
readers and temporary compaction buffers all consume memory. A hard total limit with unchanged
history retention would require spill or additional admission control.

## Validation and benchmarking

The prototype includes shared KVStore ordering/range tests, randomized primitive-map tests,
round-trip checks for every task index, concurrent snapshot checks, exact quantile and search
comparisons, SQL metric parity tests, and compact variants of listener and hybrid-store suites.

Run focused checks with SBT:

```sh
build/sbt 'kvstore/testOnly *CompactInMemoryStoreSuite *CompactInMemoryIteratorSuite *CompactRecordMapSuite'
build/sbt 'core/testOnly *CompactTaskDataCodecSuite *CompactJobDataCodecSuite *CompactLongArraySuite *AppStatusStoreSuite *AppStatusListenerWithCompactStoreSuite *LiveEntitySuite *CompactRocksDBHybridStoreSuite'
build/sbt 'sql/testOnly *CompactSQLExecutionDataSuite *SQLAppStatusListenerWithCompactStoreSuite *SQLAppStatusListenerWithInMemoryStoreSuite'
```

Synthetic benchmarks do not require a Spark session:

```sh
# Total tasks, tasks per stage, retained jobs.
build/sbt 'core/Test/runMain org.apache.spark.status.CompactUIStoreBenchmark 100000 10000 1000'
# Tasks per stage, metrics per task, retained executions, plan lines per execution.
build/sbt 'sql/Test/runMain org.apache.spark.sql.execution.ui.CompactSQLStatusBenchmark 100000 30 1000 128'
```

Memory output uses `SizeEstimator.estimateWithoutSampling` to visit every object in the graph.
This avoids extrapolating shared column blocks from sampled rows. Object layouts remain estimates;
the output is not measured post-GC retained heap or process RSS. The benchmarks also compare
population, query and metric aggregation time. Vary stage size, metric density, failure/error
payloads, plan size and active task count independently; sparse successful tasks are only one
workload shape. The SQL aggregation benchmark covers a single completed stage's metric snapshots
and formatting, not concurrent writers or the full multi-stage listener path.

### Local prototype results

The commands above were run on an Intel Xeon 6975P-C with OpenJDK 17.0.15. These synthetic
workloads favor compression: task metrics are mostly zero, executor/host strings repeat, SQL
metric values are small, and plans repeat the same scan line. No task errors or accumulator
details are included in the task workload.

| Full-graph layout estimate | Baseline | Compact | Reduction |
| --- | ---: | ---: | ---: |
| 100,000 completed tasks and 1,000 jobs, above empty-store size | 46.66 MiB | 11.77 MiB | 74.8% |
| Completed stage with 100,000 tasks and 30 SQL metrics | 25.94 MiB | 1.57 MiB | 93.9% |
| 1,000 SQL executions and graphs, 128 plan lines each | 8.18 MiB | 1.01 MiB | 87.7% |

The best warmed timings show the CPU cost of encoding and decoding. Task requests below use a
10,000-task stage and 100-row pages. These local timings are diagnostics, not stable performance
guarantees.

| Operation | Baseline | Compact |
| --- | ---: | ---: |
| Populate 100,000 tasks and 1,000 jobs | 207 ms | 246 ms |
| First task page sorted by runtime | 1.12 ms | 0.52 ms |
| Task page at offset 5,000 | 1.13 ms | 4.90 ms |
| Uncached exact task quantiles | 33.46 ms | 290.18 ms |
| List 1,000 job summaries | 0.028 ms | 0.507 ms |
| Populate SQL metric state for 100,000 tasks | 82 ms | 120 ms |
| Snapshot and format 30 SQL metrics | 29.02 ms | 31.80 ms |
| List 1,000 SQL execution summaries | 0.020 ms | 2.167 ms |
| Read one SQL execution detail | 0.000051 ms | 0.015 ms |

Deep paging, uncached quantiles and decoded summary scans need further performance work.

Before enabling this by default, measure retained heap and peak heap under concurrent UI reads,
allocation rate, GC time, listener queue delay, update throughput and UI latency on representative
event-log replays. The prototype target is at least 50% less retained UI heap at identical
retention; that target is not a claimed result for arbitrary applications.
