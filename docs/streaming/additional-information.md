---
layout: global
displayTitle: Structured Streaming Programming Guide
title: Structured Streaming Programming Guide
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
{:toc}

# Miscellaneous Notes

- Several configurations are not modifiable after the query has run. To change them, discard the checkpoint and start a new query. These configurations include:
  - `spark.sql.shuffle.partitions`
    - This is due to the physical partitioning of state: state is partitioned via applying hash function to key, hence the number of partitions for state should be unchanged.
    - If you want to run fewer tasks for stateful operations, `coalesce` would help with avoiding unnecessary repartitioning.
      - After `coalesce`, the number of (reduced) tasks will be kept unless another shuffle happens.
  - `spark.sql.streaming.stateStore.providerClass`: To read the previous state of the query properly, the class of state store provider should be unchanged.
  - `spark.sql.streaming.multipleWatermarkPolicy`: Modification of this would lead inconsistent watermark value when query contains multiple watermarks, hence the policy should be unchanged.

# Related Resources 

## Further Reading

- See and run the
  [Python]({{site.SPARK_GITHUB_URL}}/tree/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/python/sql/streaming)/[Scala]({{site.SPARK_GITHUB_URL}}/tree/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/scala/org/apache/spark/examples/sql/streaming)/[Java]({{site.SPARK_GITHUB_URL}}/tree/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/java/org/apache/spark/examples/sql/streaming)/[R]({{site.SPARK_GITHUB_URL}}/tree/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/r/streaming)
  examples.
    - [Instructions](../index.html#running-the-examples-and-shell) on how to run Spark examples
- Read about integrating with Kafka in the [Structured Streaming Kafka Integration Guide](structured-streaming-kafka-integration.html)
- Read more details about using DataFrames/Datasets in the [Spark SQL Programming Guide](../sql-programming-guide.html)
- Third-party Blog Posts
    - [Real-time Streaming ETL with Structured Streaming in Apache Spark 2.1 (Databricks Blog)](https://databricks.com/blog/2017/01/19/real-time-streaming-etl-structured-streaming-apache-spark-2-1.html)
    - [Real-Time End-to-End Integration with Apache Kafka in Apache Spark’s Structured Streaming (Databricks Blog)](https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html)
    - [Event-time Aggregation and Watermarking in Apache Spark’s Structured Streaming (Databricks Blog)](https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html)

## Talks

- Spark Summit Europe 2017
  - Easy, Scalable, Fault-tolerant Stream Processing with Structured Streaming in Apache Spark -
    [Part 1 slides/video](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark), [Part 2 slides/video](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark-continues)
  - Deep Dive into Stateful Stream Processing in Structured Streaming - [slides/video](https://databricks.com/session/deep-dive-into-stateful-stream-processing-in-structured-streaming)
- Spark Summit 2016
  - A Deep Dive into Structured Streaming - [slides/video](https://spark-summit.org/2016/events/a-deep-dive-into-structured-streaming/)

# Migration Guide

The migration guide is now archived [on this page](/streaming/ss-migration-guide.html).