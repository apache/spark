---
layout: global
title: "Migration Guide: Spark Core"
displayTitle: "Migration Guide: Spark Core"
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

## Upgrading from Core 3.4 to 3.5

- Since Spark 3.5, `spark.yarn.executor.failuresValidityInterval` is deprecated. Use `spark.executor.failuresValidityInterval` instead.

- Since Spark 3.5, `spark.yarn.max.executor.failures` is deprecated. Use `spark.executor.maxNumFailures` instead.

## Upgrading from Core 3.3 to 3.4

- Since Spark 3.4, Spark driver will own `PersistentVolumnClaim`s and try to reuse if they are not assigned to live executors. To restore the behavior before Spark 3.4, you can set `spark.kubernetes.driver.ownPersistentVolumeClaim` to `false` and `spark.kubernetes.driver.reusePersistentVolumeClaim` to `false`.

- Since Spark 3.4, Spark driver will track shuffle data when dynamic allocation is enabled without shuffle service. To restore the behavior before Spark 3.4, you can set `spark.dynamicAllocation.shuffleTracking.enabled` to `false`.

- Since Spark 3.4, Spark will try to decommission cached RDD and shuffle blocks if both `spark.decommission.enabled` and `spark.storage.decommission.enabled` are true. To restore the behavior before Spark 3.4, you can set both `spark.storage.decommission.rddBlocks.enabled` and `spark.storage.decommission.shuffleBlocks.enabled` to `false`.

- Since Spark 3.4, Spark will use RocksDB store if `spark.history.store.hybridStore.enabled` is true. To restore the behavior before Spark 3.4, you can set `spark.history.store.hybridStore.diskBackend` to `LEVELDB`.

## Upgrading from Core 3.2 to 3.3

- Since Spark 3.3, Spark migrates its log4j dependency from 1.x to 2.x because log4j 1.x has reached end of life and is no longer supported by the community. Vulnerabilities reported after August 2015 against log4j 1.x were not checked and will not be fixed. Users should rewrite original log4j properties files using log4j2 syntax (XML, JSON, YAML, or properties format). Spark rewrites the `conf/log4j.properties.template` which is included in Spark distribution, to `conf/log4j2.properties.template` with log4j2 properties format.

## Upgrading from Core 3.1 to 3.2

- Since Spark 3.2, `spark.scheduler.allocation.file` supports read remote file using hadoop filesystem which means if the path has no scheme Spark will respect hadoop configuration to read it. To restore the behavior before Spark 3.2, you can specify the local scheme for `spark.scheduler.allocation.file` e.g. `file:///path/to/file`.

- Since Spark 3.2, `spark.hadoopRDD.ignoreEmptySplits` is set to `true` by default which means Spark will not create empty partitions for empty input splits. To restore the behavior before Spark 3.2, you can set `spark.hadoopRDD.ignoreEmptySplits` to `false`.

- Since Spark 3.2, `spark.eventLog.compression.codec` is set to `zstd` by default which means Spark will not fallback to use `spark.io.compression.codec` anymore.

- Since Spark 3.2, `spark.storage.replication.proactive` is enabled by default which means Spark tries to replenish in case of the loss of cached RDD block replicas due to executor failures. To restore the behavior before Spark 3.2, you can set `spark.storage.replication.proactive` to `false`.

- In Spark 3.2, `spark.launcher.childConectionTimeout` is deprecated (typo) though still works. Use `spark.launcher.childConnectionTimeout` instead.

- In Spark 3.2, support for Apache Mesos as a resource manager is deprecated and will be removed in a future version. 

- In Spark 3.2, Spark will delete K8s driver service resource when the application terminates by itself. To restore the behavior before Spark 3.2, you can set `spark.kubernetes.driver.service.deleteOnTermination` to `false`.

## Upgrading from Core 3.0 to 3.1

- In Spark 3.0 and below, `SparkContext` can be created in executors. Since Spark 3.1, an exception will be thrown when creating `SparkContext` in executors. You can allow it by setting the configuration `spark.executor.allowSparkContext` when creating `SparkContext` in executors.

- In Spark 3.0 and below, Spark propagated the Hadoop classpath from `yarn.application.classpath` and `mapreduce.application.classpath` into the Spark application submitted to YARN when Spark distribution is with the built-in Hadoop. Since Spark 3.1, it does not propagate anymore when the Spark distribution is with the built-in Hadoop in order to prevent the failure from the different transitive dependencies picked up from the Hadoop cluster such as Guava and Jackson. To restore the behavior before Spark 3.1, you can set `spark.yarn.populateHadoopClasspath` to `true`.

## Upgrading from Core 2.4 to 3.0

- The `org.apache.spark.ExecutorPlugin` interface and related configuration has been replaced with
  `org.apache.spark.api.plugin.SparkPlugin`, which adds new functionality. Plugins using the old
  interface must be modified to extend the new interfaces. Check the
  [Monitoring](monitoring.html) guide for more details.

- Deprecated method `TaskContext.isRunningLocally` has been removed. Local execution was removed and it always has returned `false`.

- Deprecated method `shuffleBytesWritten`, `shuffleWriteTime` and `shuffleRecordsWritten` in `ShuffleWriteMetrics` have been removed. Instead, use `bytesWritten`, `writeTime ` and `recordsWritten` respectively.

- Deprecated method `AccumulableInfo.apply` have been removed because creating `AccumulableInfo` is disallowed.

- Deprecated accumulator v1 APIs have been removed and please use v2 APIs instead.

- Event log file will be written as UTF-8 encoding, and Spark History Server will replay event log files as UTF-8 encoding. Previously Spark wrote the event log file as default charset of driver JVM process, so Spark History Server of Spark 2.x is needed to read the old event log files in case of incompatible encoding.

- A new protocol for fetching shuffle blocks is used. It's recommended that external shuffle services be upgraded when running Spark 3.0 apps. You can still use old external shuffle services by setting the configuration `spark.shuffle.useOldFetchProtocol` to `true`. Otherwise, Spark may run into errors with messages like `IllegalArgumentException: Unexpected message type: <number>`.

- `SPARK_WORKER_INSTANCES` is deprecated in Standalone mode. It's recommended to launch multiple executors in one worker and launch one worker per node instead of launching multiple workers per node and launching one executor per worker.
