---
layout: global
title: "Migration Guide: Structured Streaming"
displayTitle: "Migration Guide: Structured Streaming"
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

Note that this migration guide describes the items specific to Structured Streaming.
Many items of SQL migration can be applied when migrating Structured Streaming to higher versions.
Please refer [Migration Guide: SQL, Datasets and DataFrame](../sql-migration-guide.html).

## Upgrading from Structured Streaming 4.3 to 4.4

- Since Spark 4.4, left outer and left semi stream-stream joins require a watermark on the
  right input and a time condition that limits how late a matching right row can arrive.
  Right outer joins require the corresponding settings on the left; full outer joins require
  them on both sides. The time condition must be an equality on the other input's event time
  or its time window, or a range condition that bounds a timestamp column of the finalized
  input by the other input's event time; a range condition on a numeric, DATE or
  TIMESTAMP_NTZ column, or on a time window, does not qualify. Some previously accepted
  queries now fail if these requirements are not met. Update their watermarks and join
  conditions using the
  [join support table](apis-on-dataframes-and-datasets.html#support-matrix-for-joins-in-streaming-queries).
  A stateful operator after such a join that uses the other input's event time also needs that
  input bounded: for example, a left outer join followed by an append-mode aggregation over the
  right event time also needs a watermark on the left input and a time condition that limits
  how late a matching left row can arrive. Without them, the join holds back its output
  watermark and the aggregation stops emitting results.

- Since Spark 4.4, you can set `spark.sql.streaming.join.checkFinalization.enabled` to `false`
  to run a stream-stream outer or left semi join that fails the requirement above. Spark then
  logs a warning instead of failing the query. The corrected state eviction cannot be turned
  off: such a query keeps the state of the input that lacks a bound for the life of the query,
  and an outer join never outputs that input's unmatched rows. Because the corrected eviction
  changes results and cannot be turned off, these stream-stream join corrections are made in
  Spark 4.4.0 and are not backported to earlier releases.

- Spark 4.4 corrects cases where stream-stream equality joins could discard rows before their
  matches arrived or output unmatched rows too early. Such joins with a watermark on only one
  input now keep the watermarked input's rows for the life of the query, because the other input
  has no watermark that bounds them, and hold back the join's output watermark, so a stateful
  operator after the join stops emitting results. Add a watermark on the other input to bound
  those rows.

- Spark 4.4 holds back the output watermark of a stream-stream join when retained state can
  still emit old event times. Downstream stateful operators may retain more state, take longer
  to finalize results, or stop making progress while the join has no event-time eviction bound.
  The join holds back only for an input whose event time reaches the next stateful operator: in
  a join on `clickTime <= impressionTime + INTERVAL 1 HOUR`, a window aggregation over
  `impressionTime` makes progress, and one over `clickTime` does not. When Spark cannot tell
  which input's event time the operator uses, for example after a union, before a sliding window
  or before `transformWithState`, it holds back for both inputs.

- When restarting a stream-stream join from an older Spark version, Spark may reject the
  existing checkpoint with `STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE`. Spark reuses the checkpoint
  of a join without watermarks, of a join with an equality condition between the two inputs'
  event times, such as `leftTime = rightTime`, and of a join whose time condition bounds each
  input's event time by the other's with constant intervals, such as
  `rightTime BETWEEN leftTime AND leftTime + INTERVAL 1 HOUR`. It rejects other joins with
  watermarks, for example when the time condition uses another column, bounds only one input,
  or adds whole days. Spark cannot verify that their saved rows can be reused safely after the
  correction, even if the query did not previously produce incorrect results.

  Before upgrading, check that the input history needed to rebuild the join is still available.
  If the checkpoint is rejected, start with a new checkpoint and replay that input. Account for
  results already written to the destination so that replay does not create duplicates and any
  incorrect results are corrected. Starting only with new input does not recover past results.
  If the required history is no longer available, a new checkpoint cannot recover those results.
  Do not edit the rejected checkpoint to bypass the error or use a checkpoint created by the
  new version with an older Spark version.

- Since Spark 4.4, streaming queries using `EXCEPT` with a streaming left input are rejected
  because their optimizer rewrites introduce streaming aggregations after the initial
  unsupported-operation checks. Existing queries can still restart from their persistent
  checkpoints using the compatibility value recorded in the offset log.

- Since Spark 4.4, `DataStreamWriter.toTable` writes to an existing V1 table with the partitioning
  of the table. A query whose `partitionBy` does not match the partitioning of the table, including
  the order of the columns, fails to start. A query that already committed files to the table in
  another layout, for example without `partitionBy` to a partitioned table, fails to start with
  `STREAMING_FILE_SINK_PARTITIONING_MISMATCH`. As a result, such existing queries fail to restart.
  To recover, write to a new table location with a new checkpoint location, or rewrite the
  committed files with the partition columns of the table.

## Upgrading from Structured Streaming 4.2 to 4.3

- Since Spark 4.3, in Spark Connect the DataFrame passed to a Python `foreachBatch` function is bound to the streaming query's own session, a clone of the session that started the query, matching classic `foreachBatch`. This runs the batch under the configuration the streaming engine pins on the clone rather than the root session's. As a result, the batch DataFrame can no longer be combined with the root session inside the function: a temporary view created from it is not visible to the root session, referencing it from `spark.sql` on the root session fails with `DATAFRAME_NOT_FOUND`, and joining it with a DataFrame captured from the root session fails with `SESSION_NOT_SAME`. Run such work through the batch DataFrame's own session (`batch_df.sparkSession`) instead, and pass results out through a table or a global temporary view rather than a session-local temporary view. To restore the previous behavior, set `spark.sql.connect.streaming.foreachBatch.useClonedSession` to `false`.

- Since Spark 4.3, a streaming query with a file stream sink validates its partition columns at
  start like batch file writes. The query fails with `INVALID_PARTITION_COLUMN_DATA_TYPE` if a
  partition column has a type such as `ARRAY`, `MAP`, `STRUCT`, or a string with a non-default
  collation like `UTF8_LCASE`, with `COLUMN_ALREADY_EXISTS` if the partition columns are
  duplicated, and with `ALL_PARTITION_COLUMNS_NOT_ALLOWED` if all columns are partition columns.
  As a result, existing queries with such partition columns fail to restart. For the first case,
  change the type of the partition column without renaming it to keep the directory layout. For
  the other cases, change the partition columns and use a new checkpoint location and output path.

## Upgrading from Structured Streaming 4.1 to 4.2

- Since Spark 4.2, restarting a streaming query from a checkpoint whose metadata file is missing while the offset or commit logs contain data fails with `STREAMING_CHECKPOINT_MISSING_METADATA_FILE`, instead of silently generating a new query ID (which can duplicate data in exactly-once sinks). Restore the metadata file or use a new checkpoint location. To restore the previous behavior, set `spark.sql.streaming.checkpoint.verifyMetadataExists.enabled` to `false`. (See [SPARK-55058](https://issues.apache.org/jira/browse/SPARK-55058) for more details.)

## Upgrading from Structured Streaming 4.0 to 4.1

- Since Spark 4.1, AQE is supported for stateless workloads, and it could affect the behavior of the query after upgrade (especially since AQE is turned on by default). In general, it helps to achieve better performance including resolution of skewed partition, but you can turn off AQE via changing `spark.sql.adaptive.streaming.stateless.enabled` to `false` to restore the behavior if you see regression.

## Upgrading from Structured Streaming 3.5 to 4.0

- Since Spark 4.0, Spark falls back to single batch execution if any source in the query does not support `Trigger.AvailableNow`. This is to avoid any possible correctness, duplication, and dataloss issue due to incompatibility between source and wrapper implementation. (See [SPARK-45178](https://issues.apache.org/jira/browse/SPARK-45178) for more details.)
- Since Spark 4.0, new configuration `spark.sql.streaming.ratioExtraSpaceAllowedInCheckpoint` (default: `0.3`) controls the amount of additional space allowed in the checkpoint directory to store stale version files for batch deletion inside maintenance task. This is to amortize the cost of listing in cloud store. Setting this to `0` defaults to the old behavior. (See [SPARK-48931](https://issues.apache.org/jira/browse/SPARK-48931) for more details.)
- Since Spark 4.0, when relative path is used to output data in `DataStreamWriter` the resolution to absolute path is done in the Spark Driver and is not deferred to Spark Executor. This is to make Structured Streaming behavior similar to DataFrame API (`DataFrameWriter`). (See [SPARK-50854](https://issues.apache.org/jira/browse/SPARK-50854) for more details.)

## Upgrading from Structured Streaming 3.3 to 3.4

- Since Spark 3.4, `Trigger.Once` is deprecated, and users are encouraged to migrate from `Trigger.Once` to `Trigger.AvailableNow`. Please refer [SPARK-39805](https://issues.apache.org/jira/browse/SPARK-39805) for more details.

- Since Spark 3.4, the default value of configuration for Kafka offset fetching (`spark.sql.streaming.kafka.useDeprecatedOffsetFetching`) is changed from `true` to `false`. The default no longer relies consumer group based scheduling, which affect the required ACL. For further details please see [Structured Streaming Kafka Integration](structured-streaming-kafka-integration.html#offset-fetching).

## Upgrading from Structured Streaming 3.2 to 3.3

- Since Spark 3.3, all stateful operators require hash partitioning with exact grouping keys. In previous versions, all stateful operators except stream-stream join require loose partitioning criteria which opens the possibility on correctness issue. (See [SPARK-38204](https://issues.apache.org/jira/browse/SPARK-38204) for more details.) To ensure backward compatibility, we retain the old behavior with the checkpoint built from older versions.

## Upgrading from Structured Streaming 3.0 to 3.1

- In Spark 3.0 and before, for the queries that have stateful operation which can emit rows older than the current watermark plus allowed late record delay, which are "late rows" in downstream stateful operations and these rows can be discarded, Spark only prints a warning message. Since Spark 3.1, Spark will check for such queries with possible correctness issue and throw AnalysisException for it by default. For the users who understand the possible risk of correctness issue and still decide to run the query, please disable this check by setting the config `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` to false.

- In Spark 3.0 and before Spark uses `KafkaConsumer` for offset fetching which could cause infinite wait in the driver.
  In Spark 3.1 a new configuration option added `spark.sql.streaming.kafka.useDeprecatedOffsetFetching` (default: `true`)
  which could be set to `false` allowing Spark to use new offset fetching mechanism using `AdminClient`.
  For further details please see [Structured Streaming Kafka Integration](structured-streaming-kafka-integration.html#offset-fetching).

## Upgrading from Structured Streaming 2.4 to 3.0

- In Spark 3.0, Structured Streaming forces the source schema into nullable when file-based datasources such as text, json, csv, parquet and orc are used via `spark.readStream(...)`. Previously, it respected the nullability in source schema; however, it caused issues tricky to debug with NPE. To restore the previous behavior, set `spark.sql.streaming.fileSource.schema.forceNullable` to `false`.

- Spark 3.0 fixes the correctness issue on Stream-stream outer join, which changes the schema of state. (See [SPARK-26154](https://issues.apache.org/jira/browse/SPARK-26154) for more details). If you start your query from checkpoint constructed from Spark 2.x which uses stream-stream outer join, Spark 3.0 fails the query. To recalculate outputs, discard the checkpoint and replay previous inputs.

- In Spark 3.0, the deprecated class `org.apache.spark.sql.streaming.ProcessingTime` has been removed. Use `org.apache.spark.sql.streaming.Trigger.ProcessingTime` instead. Likewise, `org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger` has been removed in favor of `Trigger.Continuous`, and `org.apache.spark.sql.execution.streaming.OneTimeTrigger` has been hidden in favor of `Trigger.Once`.
