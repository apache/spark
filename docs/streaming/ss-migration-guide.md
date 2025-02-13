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
