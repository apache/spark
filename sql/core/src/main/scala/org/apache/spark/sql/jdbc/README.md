---
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

# JDBC Connection Provider Handling In Spark

This document aims to explain and demystify JDBC connection providers as they are used by Spark
and the usage or custom provider implementation is not obvious.

## What are JDBC connection providers and why use them?

Spark initially provided non-authenticated or user/password authenticated connections.
This is quite insecure and must be avoided when possible.

JDBC connection providers (CPs from now on) are making JDBC connections initiated by JDBC sources
which can be a more secure alternative.

Spark provides two ways to deal with stronger authentication:
* Built-in CPs added which support kerberos authentication using `keytab` and `principal` (but only if the JDBC driver supports keytab)
* `org.apache.spark.sql.jdbc.JdbcConnectionProvider` developer API added which allows developers
  to implement any kind of database/use-case specific authentication method.

## How JDBC connection providers loaded?

CPs are loaded with service loader independently. So, if one CP is failed to load it has no
effect on all other CPs.

## How to disable JDBC connection providers?

There are cases where the built-in CP doesn't provide the exact feature which needed
so they can be turned off and can be replaced with custom implementation. All CPs must provide a `name`
which must be unique. One can set the following configuration entry in `SparkConf` to turn off CPs:
`spark.sql.sources.disabledJdbcConnProviderList=name1,name2`.

## How to enforce a specific JDBC connection provider?

When more than one JDBC connection provider can handle a specific driver and options, it is possible to
disambiguate and enforce a particular CP for the JDBC data source. One can set the DataFrame
option `connectionProvider` to specify the name of the CP they want to use.

## How a JDBC connection provider found when new connection initiated?

When a Spark source initiates JDBC connection it looks for a CP which supports the included driver,
the user just need to provide the `keytab` location and the `principal`. The `keytab` file must exist
on each node where connection is initiated.

CPs has a mandatory API which must be implemented:

`def canHandle(driver: Driver, options: Map[String, String]): Boolean`

If this function returns `true` then `Spark` considers the CP can handle the connection setup.
Built-in CPs returning `true` in the following cases:
* If the connection is not secure (no `keytab` or `principal` provided) then the `basic` named CP responds.
* If the connection is secure (`keytab` and `principal` provided) then the database specific CP responds.
  Database specific providers are checking the JDBC driver class name and the decision is made based
  on that. For example `PostgresConnectionProvider` responds only when the driver class name is `org.postgresql.Driver`.

Important to mention that exactly one CP must return `true` from `canHandle` for a particular connection
request because otherwise `Spark` can't decide which CP need to be used to make the connection.
Such cases exception is thrown and the data processing stops.

## How to implement a custom JDBC connection provider?

Spark provides an example CP in the examples project (which does nothing).
There are basically 2 files:
* `examples/src/main/scala/org/apache/spark/examples/sql/jdbc/ExampleJdbcConnectionProvider.scala`
  which contains the main logic that can be further developed.
* `examples/src/main/resources/META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider`
  which manifest file is used by service loader (this tells `Spark` that this CP needs to be loaded).

Implementation considerations:
* CPs are running in heavy multi-threaded environment and adding a state into a CP is not advised.
  If any state added then it must be synchronized properly. It could cause quite some headache to
  hunt down such issues.
* Some of the CPs are modifying the JVM global security context so `getConnection` method is
  synchronized by `org.apache.spark.security.SecurityConfigurationLock` to avoid race.
