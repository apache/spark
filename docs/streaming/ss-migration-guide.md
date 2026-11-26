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

- Since Spark 4.4, a new streaming query whose only stateful operator is a stream-stream join
  can retain watermarked timestamp columns even when they are not selected. This applies when
  the optimizer sees no unexpanded CTEs, the join condition contains only equality keys after
  predicate pushdown, and each input has at most one watermarked column, which must be a
  timestamp. Eligible joins use the timestamps to filter late input rows and store them in join
  state. Checkpoints from earlier Spark versions keep their previous behavior.
  To keep the previous behavior for a new query, set
  `spark.sql.streaming.join.preserveEventTimeColumns.enabled` to `false` before starting it.
  On restart, editing the join condition or a filter that compares the two inputs can change
  whether the join qualifies and which columns it stores. An incompatible schema can fail
  with `STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE` or
  `STREAM_STREAM_JOIN_STATE_TIMESTAMP_COLUMN_CHANGED`.

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

- Since Spark 4.4, a DataFrame as-of join with a streaming left side, such as PySpark
  `DataFrame._joinAsOf` on a streaming DataFrame, runs with the stateless sort-merge as-of join.
  Previously, its rewrite added a streaming aggregation and a stream-stream join. Without a
  watermark, such a query failed at its first micro-batch. With a watermark on the as-of column, it
  ran, but held back rows for the watermark, dropped late rows, and never emitted some rows, such as
  the unmatched rows of a left join. Now it emits each row in the micro-batch that reads the row,
  including late rows. As a result, an existing query with a watermark fails to restart with
  `STREAMING_STATEFUL_OPERATOR_NOT_MATCH_IN_STATE_METADATA`. To recover, use a new checkpoint
  location, or set `spark.sql.legacy.asOfJoin.rewriteStreamingLeftToSubquery` to `true` before
  building the query to restore the previous behavior. The join keys are also checked during
  analysis as for the sort-merge as-of join. For example, a key that does not reference its own
  input fails with `AS_OF_JOIN.KEY_REFERENCE`.

- Since Spark 4.4, a column derived from a typed Scala UDF with a primitive parameter is nullable
  when the input of the parameter can be null, as the UDF returns NULL for a NULL input.
  Previously, some of these columns, for example the output of a generator such as `explode` over
  an array of the UDF results, were non-nullable. As a result, an existing query started before
  Spark 4.3 that keeps such a column in its state, for example as a grouping key of a streaming
  aggregation, fails to restart with `STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE`. To recover, use a new
  checkpoint location, or set `spark.sql.streaming.stateStore.stateSchemaCheck` to `false`. Note
  that the state of such a query can hold wrong keys, such as `-1` or `0`, for NULL inputs. The
  same holds for a query started on Spark 4.3 or later, which restarts and then puts new NULL
  inputs under a separate NULL key. To drop such keys, use a new checkpoint location.

- Since Spark 4.4, a column that `unpivot` (or `melt`) or `groupingSets` of the Dataset API
  computes from a column that an outer join makes nullable, for example the value column of
  `unpivot`, is nullable. Previously, such a column was non-nullable and returned a wrong value,
  such as `-1` or `0`, instead of NULL. As a result, an existing query started before Spark 4.3
  that keeps such a column in its state, for example as a grouping key of a streaming aggregation,
  fails to restart with `STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE`. To recover, use a new checkpoint
  location, or set `spark.sql.streaming.stateStore.stateSchemaCheck` to `false`. Note that the
  state of such a query can hold the wrong keys. The same holds for a query started on Spark 4.3 or
  later, which restarts and then puts new NULL values under a separate NULL key. To drop such keys,
  use a new checkpoint location.

- Since Spark 4.4, a struct field, an array element, or a map value that `unpivot` (or `melt`),
  `rollup`, `cube`, or `groupingSets` of the Dataset API computes from a column that an outer join
  makes nullable, for example the field of a struct value of `unpivot`, is nullable. Previously,
  it was non-nullable and returned a wrong value, such as `0`, instead of NULL. As a result, an
  existing query started before Spark 4.3 that keeps such a struct, array, or map in its state, for
  example as a grouping key of a streaming aggregation, fails to restart with
  `STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE`. To recover, use a new checkpoint location, or set
  `spark.sql.streaming.stateStore.stateSchemaCheck` to `false`. Note that the state of such a query
  can hold the wrong keys.

- Since Spark 4.4, the output column of a generator such as `explode` in the Dataset API, or a
  struct field or an array element in it, is nullable when the input of the generator is computed
  from a column that an outer join makes nullable, for example `explode(array(right("b")))` or
  `explode(array(struct(right("b").as("x"))))` after a left outer join with `right`. Previously,
  it was non-nullable and returned a wrong value, such as `-1` or `0`, instead of NULL. As a
  result, an existing query started before Spark 4.3 that keeps such a value in its state, for
  example as a grouping key of a streaming aggregation, fails to restart with
  `STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE`. To recover, use a new checkpoint location, or set
  `spark.sql.streaming.stateStore.stateSchemaCheck` to `false`. Note that the state of such a
  query can hold the wrong keys. The same holds for a query started on Spark 4.3 or later, which
  restarts and then puts new NULL values under a separate NULL key. To drop such keys, use a new
  checkpoint location.

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
