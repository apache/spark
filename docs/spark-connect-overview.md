---
layout: global
title: Spark Connect Overview
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

In Apache Spark 3.4, Spark Connect introduced a decoupled client-server
architecture that allows remote connectivity to Spark clusters using the
DataFrame API and unresolved logical plans as the protocol. The separation
between client and server allows Spark and its open ecosystem to be
leveraged from everywhere. It can be embedded in modern data applications,
in IDEs, Notebooks and programming languages.

To get started, see [Quickstart: Spark Connect](api/python/getting_started/quickstart_connect.html).

* This will become a table of contents (this text will be scraped).
{:toc}

<p style="text-align: center;">
  <img src="img/spark-connect-api.png" title="Spark Connect API" alt="Spark Connect API Diagram" />
</p>

# How Spark Connect works

The Spark Connect client library is designed to simplify Spark application
development. It is a thin API that can be embedded everywhere: in application
servers, IDEs, notebooks, and programming languages. The Spark Connect API
builds on Spark's DataFrame API using unresolved logical plans as a
language-agnostic protocol between the client and the Spark driver.

The Spark Connect client translates DataFrame operations into unresolved
logical query plans which are encoded using protocol buffers. These are sent
to the server using the gRPC framework.

The Spark Connect endpoint embedded on the Spark Server receives and
translates unresolved logical plans into Spark's logical plan operators.
This is similar to parsing a SQL query, where attributes and relations are
parsed and an initial parse plan is built. From there, the standard Spark
execution process kicks in, ensuring that Spark Connect leverages all of
Spark's optimizations and enhancements. Results are streamed back to the
client through gRPC as Apache Arrow-encoded row batches.

<p style="text-align: center;">
  <img src="img/spark-connect-communication.png" title="Spark Connect communication" alt="Spark Connect communication" />
</p>

## How Spark Connect client applications differ from classic Spark applications

One of the main design goals of Spark Connect is to enable a full separation and
isolation of the client from the server. As a consequence, there are some changes
that developers need to be aware of when using Spark Connect:

1. The client does not run in the same process as the Spark driver. This means that
   the client cannot directly access and interact with the driver JVM to manipulate
   the execution environment. In particular, in PySpark, the client does not use Py4J
   and thus the accessing the private fields holding the JVM implementation of `DataFrame`,
   `Column`, `SparkSession`, etc. is not possible (e.g. `df._jdf`).
2. By design, the Spark Connect protocol uses Sparks logical
   plans as the abstraction to be able to declaratively describe the operations to be executed
   on the server. Consequently, the Spark Connect protocol does not support all the
   execution APIs of Spark, most importantly RDDs.
3. Spark Connect provides a session-based client for its consumers. This means that the
   client does not have access to properties of the cluster that manipulate the
   environment for all connected clients. Most importantly, the client does not have access
   to the static Spark configuration or the SparkContext.

# Operational benefits of Spark Connect

With this new architecture, Spark Connect mitigates several multi-tenant
operational issues:

**Stability**: Applications that use too much memory will now only impact their
own environment as they can run in their own processes. Users can define their
own dependencies on the client and don't need to worry about potential conflicts
with the Spark driver.

**Upgradability**: The Spark driver can now seamlessly be upgraded independently
of applications, for example to benefit from performance improvements and security fixes.
This means applications can be forward-compatible, as long as the server-side RPC
definitions are designed to be backwards compatible.

**Remote connectivity**: The decoupled architecture allows remote connectivity to Spark beyond SQL
and JDBC: any application can now interactively use Spark “as a service”.

**Debuggability and observability**: Spark Connect enables interactive debugging
during development directly from your favorite IDE. Similarly, applications can
be monitored using the application's framework native metrics and logging libraries.

# Client application authentication

While Spark Connect does not have built-in authentication, it is designed to
work seamlessly with your existing authentication infrastructure. Its gRPC
HTTP/2 interface allows for the use of authenticating proxies, which makes
it possible to secure Spark Connect without having to implement authentication
logic in Spark directly.

# What is supported

**PySpark**: Since Spark 3.4, Spark Connect supports most PySpark APIs, including
[DataFrame](api/python/reference/pyspark.sql/dataframe.html),
[Functions](api/python/reference/pyspark.sql/functions.html), and
[Column](api/python/reference/pyspark.sql/column.html). However,
some APIs such as [SparkContext](api/python/reference/api/pyspark.SparkContext.html)
and [RDD](api/python/reference/api/pyspark.RDD.html) are not supported.
You can check which APIs are currently
supported in the [API reference](api/python/reference/index.html) documentation.
Supported APIs are labeled "Supports Spark Connect" so you can check whether the
APIs you are using are available before migrating existing code to Spark Connect.

**Scala**: Since Spark 3.5, Spark Connect supports most Scala APIs, including
[Dataset](api/scala/org/apache/spark/sql/Dataset.html),
[functions](api/scala/org/apache/spark/sql/functions$.html),
[Column](api/scala/org/apache/spark/sql/Column.html),
[Catalog](api/scala/org/apache/spark/sql/catalog/Catalog.html) and
[KeyValueGroupedDataset](api/scala/org/apache/spark/sql/KeyValueGroupedDataset.html).

User-Defined Functions (UDFs) are supported, by default for the shell and in standalone applications with
additional set-up requirements.

Majority of the Streaming API is supported, including
[DataStreamReader](api/scala/org/apache/spark/sql/streaming/DataStreamReader.html),
[DataStreamWriter](api/scala/org/apache/spark/sql/streaming/DataStreamWriter.htmll),
[StreamingQuery](api/scala/org/apache/spark/sql/streaming/StreamingQuery.html) and
[StreamingQueryListener](api/scala/org/apache/spark/sql/streaming/StreamingQueryListener.html).

APIs such as [SparkContext](api/scala/org/apache/spark/SparkContext.html)
and [RDD](api/scala/org/apache/spark/rdd/RDD.html) are unsupported in Spark Connect.

Support for more APIs is planned for upcoming Spark releases.
