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

* Table of contents
{:toc}

Note that this migration guide describes the items specific to Structured Streaming.
Many items of SQL migration can be applied when migrating Structured Streaming to higher versions.
Please refer [Migration Guide: SQL, Datasets and DataFrame](sql-migration-guide.html).

## Upgrading from Structured Streaming 2.4 to 3.0

- In Spark 3.0, Structured Streaming forces the source schema into nullable when file-based datasources such as text, json, csv, parquet and orc are used via `spark.readStream(...)`. Previously, it respected the nullability in source schema; however, it caused issues tricky to debug with NPE. To restore the previous behavior, set `spark.sql.streaming.fileSource.schema.forceNullable` to `false`.

- Spark 3.0 fixes the correctness issue on Stream-stream outer join, which changes the schema of state. (See [SPARK-26154](https://issues.apache.org/jira/browse/SPARK-26154) for more details). If you start your query from checkpoint constructed from Spark 2.x which uses stream-stream outer join, Spark 3.0 fails the query. To recalculate outputs, discard the checkpoint and replay previous inputs.

- In Spark 3.0, the deprecated class `org.apache.spark.sql.streaming.ProcessingTime` has been removed. Use `org.apache.spark.sql.streaming.Trigger.ProcessingTime` instead. Likewise, `org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger` has been removed in favor of `Trigger.Continuous`, and `org.apache.spark.sql.execution.streaming.OneTimeTrigger` has been hidden in favor of `Trigger.Once`.