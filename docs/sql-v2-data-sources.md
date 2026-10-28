---
layout: global
title: Data Source V2
displayTitle: Data Source V2
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

## Overview

Data Source V2 (DSV2) is Spark's extensible API for integrating external data systems.
It is a set of Java interfaces in the `org.apache.spark.sql.connector` package that allow
connectors to plug into Spark's query planning and execution. Connectors opt in to features
and optimizations &mdash; such as filter pushdown, columnar reads, or catalog support &mdash;
by implementing specific mix-in interfaces, so a minimal connector can add
capabilities incrementally. Notable users include:

- [JDBC data source](sql-data-sources-jdbc.html) (Spark built-in)
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Delta Lake](https://docs.delta.io/latest/delta-spark.html)
- [Lance](https://lance.org/integrations/spark/)

Compared to the earlier Data Source V1 API, DSV2 offers:

| Feature | Description |
|---------|-------------|
| **Java API** | The connector interfaces are pure Java (`org.apache.spark.sql.connector`), removing the Scala dependency that DSV1 required. A [Python Data Source API](api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html) (`pyspark.sql.datasource`) is also available as a wrapper for lightweight connectors written entirely in Python. |
| **Catalog integration** | Connectors can expose namespaces, tables, views, and functions natively through Spark SQL. |
| **Operator pushdown** | Connectors can accept pushed-down filters, required columns, aggregates, limits, offsets, and more. |
| **Report partitioning and ordering** | Connectors can report the physical layout of data so that Spark can avoid unnecessary shuffles and sorts. |
| **Requested distribution and ordering** | Write connectors can request that Spark repartition and sort incoming data before writing, enabling optimized data layouts such as clustering or Z-ordering. |
| **Columnar reads** | Connectors can return data in columnar batches for vectorized processing. |
| **Row-level DML** | Connectors can natively support `DELETE`, `UPDATE`, and `MERGE INTO` operations through dedicated interfaces. |
| **Streaming support** | A unified `Table` abstraction supports batch, micro-batch, and continuous processing through the same interfaces. |

## Entry Points

There are two ways to plug a data source into Spark:

| Entry Point | Use Case |
|-------------|----------|
| [`TableProvider`](#tableprovider) | The simpler entry point, for sources that identify a table by **options** (e.g. a file path or Kafka topic) rather than through a catalog. Can implement `SupportsCatalogOptions` to map reader and writer options to a table in a catalog. |
| [`CatalogPlugin`](#catalogplugin) | Typically used by external data sources that manage their own **catalog** of namespaces, tables, and optionally views and functions (e.g. Iceberg, Delta Lake). Registered via `spark.sql.catalog.<name>=com.example.MyCatalog`. |


### TableProvider

[`TableProvider`](api/java/org/apache/spark/sql/connector/catalog/TableProvider.html) returns a
[`Table`](#table) given a set of options. Implementations must have a public no-arg constructor.

The key methods are:

| Method | Description |
|--------|-------------|
| `inferSchema(options)` | Infer the table's schema from the given options |
| `inferPartitioning(options)` | Optionally infer the table's partitioning |
| `getTable(schema, partitioning, properties)` | Return a `Table` for the resolved schema and partitioning |

A `TableProvider` can also implement
[`SupportsCatalogOptions`](api/java/org/apache/spark/sql/connector/catalog/SupportsCatalogOptions.html)
so that `DataFrameReader.load` and `DataFrameWriter.save` resolve the user-supplied options to an
identifier in a catalog (the session catalog unless the options name another), which enables
existence checks and create-on-save for those calls.


### CatalogPlugin

[`CatalogPlugin`](api/java/org/apache/spark/sql/connector/catalog/CatalogPlugin.html) is a marker
interface for catalog implementations. After instantiation, Spark calls
`initialize(name, options)` with the catalog name and all configuration properties that share the
prefix `spark.sql.catalog.<name>.`.

A catalog adds capabilities by mixing in additional
[catalog interfaces](#catalog-interfaces):

| Interface | Capability |
|-----------|------------|
| `TableCatalog` | List, load, create, alter, and drop tables |
| `StagingTableCatalog` | Atomic create-table-as-select / replace-table-as-select |
| `SupportsNamespaces` | Create, alter, drop, and list namespaces |
| `ViewCatalog` | List, load, create, replace, rename, and drop views |
| `RelationCatalog` | Tables and views that share one identifier namespace (required when a catalog exposes both) |
| `FunctionCatalog` | List and load functions |
| `ProcedureCatalog` | Load and list stored procedures |

`CatalogExtension` is a special variant that wraps Spark's built-in session catalog and can be
used to add custom behavior while delegating to the default implementation.


## Catalog Interfaces

The interfaces below are mix-ins that a `CatalogPlugin` implements to expose specific
categories of metadata operations through Spark SQL.

### TableCatalog

[`TableCatalog`](api/java/org/apache/spark/sql/connector/catalog/TableCatalog.html) extends
`CatalogPlugin` and provides methods for [`Table`](#table) lifecycle management:

| Method | Description |
|--------|-------------|
| `listTables(namespace)` | List tables in a namespace |
| `loadTable(ident)` | Load a table by identifier |
| `createTable(ident, tableInfo)` | Create a new table from a [`TableInfo`](api/java/org/apache/spark/sql/connector/catalog/TableInfo.html) carrying columns, partitioning, properties, and constraints |
| `alterTable(ident, changes...)` | Alter a table's schema, properties, or constraints |
| `dropTable(ident)` | Drop a table |
| `renameTable(oldIdent, newIdent)` | Rename a table |

Spark calls `createTable(ident, tableInfo)` (since 4.1.0) for `CREATE TABLE`, `REPLACE TABLE`, and
CTAS, so a new catalog overrides that form. Its default forwards to the deprecated
`createTable(ident, columns, partitions, properties)` and then
`createTable(ident, schema, partitions, properties)` (deprecated since 4.1.0 and 3.4.0), which keeps
older catalogs working but drops the table's constraints. A
[`StagingTableCatalog`](#stagingtablecatalog) is reached through its own methods instead.

### StagingTableCatalog

[`StagingTableCatalog`](api/java/org/apache/spark/sql/connector/catalog/StagingTableCatalog.html)
extends `TableCatalog` and enables **atomic** `CREATE TABLE AS SELECT` and
`REPLACE TABLE AS SELECT`. It returns a `StagedTable` whose changes only become visible when
`commitStagedChanges()` is called. If the write fails, the catalog remains unchanged. Spark reaches
these paths through `stageCreate(ident, tableInfo)`, `stageReplace(ident, tableInfo)`, and
`stageCreateOrReplace(ident, tableInfo)` rather than `createTable`, so a staging catalog overrides
those three in addition to the `TableCatalog` methods; their defaults forward to the deprecated
forms and throw.

### SupportsNamespaces

[`SupportsNamespaces`](api/java/org/apache/spark/sql/connector/catalog/SupportsNamespaces.html)
adds namespace (database/schema) management:

| Method | Description |
|--------|-------------|
| `listNamespaces()` | List child namespaces |
| `createNamespace(namespace, metadata)` | Create a namespace |
| `alterNamespace(namespace, changes...)` | Alter namespace properties |
| `dropNamespace(namespace, cascade)` | Drop a namespace |

### ViewCatalog

[`ViewCatalog`](api/java/org/apache/spark/sql/connector/catalog/ViewCatalog.html) extends
`CatalogPlugin` and provides methods for view lifecycle management. View DDL
(`CREATE`/`REPLACE`/`ALTER`/`DROP`/`SHOW VIEWS`) and reading from a view (`SELECT`) are resolved
and executed against the catalog. Connectors that expose both
tables and views in a single shared identifier namespace implement
[`RelationCatalog`](api/java/org/apache/spark/sql/connector/catalog/RelationCatalog.html)
(since 4.2.0), which extends both `TableCatalog` and `ViewCatalog` and adds
`loadRelation(ident)` so a lookup that may be either kind resolves in one round trip. A catalog
that implements `TableCatalog` and `ViewCatalog` directly, without `RelationCatalog`, is rejected
when it is initialized. A view's metadata is represented by [`View`](#view).

| Method | Description |
|--------|-------------|
| `listViews(namespace)` | List views in a namespace |
| `loadView(ident)` | Load a view's `View` by identifier |
| `viewExists(ident)` | Test whether a view exists (has a default implementation) |
| `invalidateView(ident)` | Invalidate cached metadata for a view (has a default implementation) |
| `createView(ident, info)` | Create a new view |
| `replaceView(ident, info)` | Atomically replace an existing view (`ALTER VIEW ... AS`) |
| `createOrReplaceView(ident, info)` | Create a view, or replace it if it exists (`CREATE OR REPLACE VIEW`); the default calls `replaceView` and falls back to `createView`, which is not atomic, so override it for an atomic swap |
| `dropView(ident)` | Drop a view |
| `renameView(oldIdent, newIdent)` | Rename a view |

> **Note:** The built-in session catalog (`V2SessionCatalog`) is not a `ViewCatalog`; views in
> the session catalog continue to use the v1 (Hive-compatible) code path. DSV2 view support
> applies to custom catalogs registered via `spark.sql.catalog.<name>`.

### FunctionCatalog

[`FunctionCatalog`](api/java/org/apache/spark/sql/connector/catalog/FunctionCatalog.html)
adds user-defined function management:

| Method | Description |
|--------|-------------|
| `listFunctions(namespace)` | List functions in a namespace |
| `loadFunction(ident)` | Load an `UnboundFunction` by identifier |

### ProcedureCatalog

[`ProcedureCatalog`](api/java/org/apache/spark/sql/connector/catalog/ProcedureCatalog.html)
adds stored-procedure support:

| Method | Description |
|--------|-------------|
| `listProcedures(namespace)` | List procedures in a namespace |
| `loadProcedure(ident)` | Load an `UnboundProcedure` by identifier |

Procedures are invoked via `CALL catalog.procedure(args)`.


## Table

[`Table`](api/java/org/apache/spark/sql/connector/catalog/Table.html) is the central abstraction
representing a logical dataset &mdash; for example, a directory of Parquet files, a Kafka topic,
or a table managed by an external metastore. A `Table` gains read and write abilities through
[mix-in interfaces](#read-and-write-mix-ins).

A `Table` provides:

| Method | Description |
|--------|-------------|
| `name()` | A human-readable identifier for the table |
| `columns()` | The table's columns (replaces the deprecated `schema()` method) |
| `partitioning()` | Physical partitioning expressed as `Transform` arrays |
| `properties()` | A string map of table properties |
| `capabilities()` | A set of `TableCapability` values declaring what the table supports |

### Read and Write Mix-ins

A `Table` gains read and write abilities by implementing mix-in interfaces:

- **`SupportsRead`** adds `newScanBuilder(options)`, which returns a `ScanBuilder` for
  batch or streaming reads. See [Read Path](#read-path).
- **`SupportsWrite`** adds `newWriteBuilder(info)`, which returns a `WriteBuilder` for
  batch or streaming writes. See [Write Path](#write-path).
- **`SupportsRowLevelOperations`** enables `DELETE`, `UPDATE`, and `MERGE INTO` through
  a read-and-rewrite cycle. See [Row-Level DML](#row-level-dml).

Additional mix-ins enable further capabilities:

| Mix-in | Capability |
|--------|------------|
| `SupportsDelete` / `SupportsDeleteV2` | Filter-based row delete |
| `TruncatableTable` | `TRUNCATE TABLE` |
| `SupportsPartitionManagement` | Partition DDL (`ADD`/`DROP`/`RENAME PARTITION`) |
| `SupportsMetadataColumns` | Expose hidden metadata columns (e.g.&nbsp;file name, row position) |

## View

[`View`](api/java/org/apache/spark/sql/connector/catalog/View.html) (since 4.2.0) is the metadata
abstraction for a view &mdash; a named SQL query managed through a [`ViewCatalog`](#viewcatalog).
Unlike a [`Table`](#table), a view has no data of its own: it stores a SQL query along with the
context needed to resolve and expand that query consistently whenever the view is referenced.

A `View` provides:

| Method | Description |
|--------|-------------|
| `queryText()` | The SQL text of the view |
| `columns()` | The view's output columns |
| `currentCatalog()` | The current catalog captured at creation time, used to resolve unqualified identifiers in the query text |
| `currentNamespace()` | The current namespace captured at creation time, used alongside `currentCatalog()` |
| `sqlConfigs()` | The SQL configs captured at creation time, applied when parsing and analyzing the view body |
| `schemaMode()` | The view's [schema binding mode](sql-ref-syntax-ddl-create-view.html) (`BINDING`, `COMPENSATION`, `TYPE EVOLUTION`, or `EVOLUTION`) |
| `queryColumnNames()` | Output column names of the query, used to map the query output to the view's declared columns |
| `viewDependencies()` | The structured list of objects (source tables and functions) the view depends on |
| `properties()` | A string map of view properties |

When a view is read (`SELECT`), Spark resolves and expands `queryText()` using the captured
catalog, namespace, and SQL configs, so the view behaves consistently regardless of the session
state at query time.

## Read Path

The read path follows a builder pattern that separates logical planning from physical execution:

```
SupportsRead.newScanBuilder(options)
  └─▸ ScanBuilder          (logical: pushdown negotiation)
        └─▸ Scan            (logical: read schema, description)
              └─▸ Batch     (physical: partitions + reader factory)
                    ├─ InputPartition[]
                    └─ PartitionReaderFactory
                         └─▸ PartitionReader   (per-task I/O)
```

### ScanBuilder

[`ScanBuilder`](api/java/org/apache/spark/sql/connector/read/ScanBuilder.html) is the starting
point for configuring a read. Spark calls `build()` to obtain the final `Scan`.

Before calling `build()`, Spark negotiates operator pushdown with the scan builder by checking
for mix-in interfaces. The pushdown order is:

1. **Sample** (`SupportsPushDownTableSample`)
2. **Filter** (`SupportsPushDownV2Filters` / `SupportsPushDownFilters`)
3. **Join** (`SupportsPushDownJoin`, since 4.1.0)
4. **Aggregate** (`SupportsPushDownAggregates`)
5. **Variant extraction** (`SupportsPushDownVariantExtractions`, since 4.1.0)
6. **Limit, top-N and offset** (`SupportsPushDownLimit`, `SupportsPushDownTopN`, `SupportsPushDownOffset`)
7. **Column pruning** (`SupportsPushDownRequiredColumns`)

Spark offers aggregate, limit, top-N and offset pushdown only when no filter is left to evaluate
above the scan, so a filter returned by `pushPredicates` blocks them. For the operators that can be
partly handled, the return value tells Spark what it must still evaluate itself:

- [`SupportsPushDownV2Filters`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownV2Filters.html):
  `pushPredicates(predicates)` receives the candidate [`Predicate`](#expressions) instances and
  returns the ones that still need to be evaluated **after** the scan. `pushedPredicates()`
  returns the predicates the source accepted. A predicate may appear in both: a source that can
  only skip whole files or row groups on a predicate reports it as pushed and also returns it, so
  Spark re-checks it row by row. Spark evaluates only what `pushPredicates` returns (plus anything
  it could not translate), so a predicate that is neither returned nor applied by the reader is
  silently dropped from the query.
- [`SupportsPushDownFilters`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownFilters.html)
  is the pre-3.3 form of the same contract over the V1 `org.apache.spark.sql.sources.Filter`
  type: `pushFilters(filters)` returns the post-scan filters and `pushedFilters()` reports the
  accepted ones. New connectors should implement `SupportsPushDownV2Filters`; Spark checks for
  `SupportsPushDownFilters` first, so a `ScanBuilder` should implement one or the other.
- [`SupportsPushDownRequiredColumns`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownRequiredColumns.html):
  `pruneColumns(requiredSchema)` passes the narrowest schema the query needs. Pruning may be
  partial (for example top-level columns only), and `Scan.readSchema()` must report whatever
  pruning was actually applied.
- [`SupportsPushDownLimit`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownLimit.html)`.pushLimit`,
  [`SupportsPushDownTopN`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownTopN.html)`.pushTopN`,
  [`SupportsPushDownOffset`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownOffset.html)`.pushOffset`
  and
  [`SupportsPushDownAggregates`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownAggregates.html)`.pushAggregation`
  return `true` when the source accepts the operator. For limit and top-N, `isPartiallyPushed()`
  (default `true`) tells Spark whether it must keep the operator above the scan. For aggregation,
  `supportCompletePushDown(aggregation)` tells Spark whether the source handles the aggregation
  completely.

### Scan

[`Scan`](api/java/org/apache/spark/sql/connector/read/Scan.html) is a logical representation
of a configured data source read. It provides:

| Method | Description |
|--------|-------------|
| `readSchema()` | The actual schema after any column pruning or pushdown |
| `toBatch()` | Returns a `Batch` for batch execution |
| `toMicroBatchStream(checkpointLocation)` | Returns a `MicroBatchStream` for streaming |
| `toContinuousStream(checkpointLocation)` | Returns a `ContinuousStream` for continuous processing |

Implementations must override the method corresponding to the `TableCapability` declared by their
`Table`.

### Batch

[`Batch`](api/java/org/apache/spark/sql/connector/read/Batch.html) is the physical
representation of a batch read. It has two methods:

| Method | Description |
|--------|-------------|
| `planInputPartitions()` | Returns an array of `InputPartition` objects; each partition maps to one Spark task |
| `createReaderFactory()` | Returns a `PartitionReaderFactory` that is serialized and sent to executors |

### InputPartition and PartitionReader

[`InputPartition`](api/java/org/apache/spark/sql/connector/read/InputPartition.html) is a
serializable handle representing a data split. It may optionally declare `preferredLocations()`
for data locality.

[`PartitionReaderFactory`](api/java/org/apache/spark/sql/connector/read/PartitionReaderFactory.html)
is serialized to executors and creates a
[`PartitionReader`](api/java/org/apache/spark/sql/connector/read/PartitionReader.html) for each
input partition. The reader iterates over rows (or `ColumnarBatch` instances for columnar sources)
and is closed after consumption. For a `SupportsStorageFilters` scan that Spark offered a storage
filter, `createReader(partition, request)` or `createColumnarReader(partition, request)` creates the
reader instead. The `StorageFilterRequest` carries the filter, and whether the reader appends a
boolean column that is true only on the rows the filter was evaluated on and kept. When the filter
throws `StorageFilterNotAppliedException`, the reader reads the rest of the unit without it. Scala
does not check exceptions, so a reader written in Scala must catch it all the same.

### Scan Mix-ins

`Scan` implementations can opt in to additional optimizations by implementing mix-in interfaces:

| Mix-in | Capability |
|--------|------------|
| `SupportsReportPartitioning` | Reports how the output is partitioned, allowing Spark to avoid unnecessary shuffles (e.g. [Storage Partition Join](sql-performance-tuning.html#storage-partition-join)) |
| `SupportsReportOrdering` | Reports the sort order of the output, allowing Spark to skip redundant sorts |
| `SupportsReportStatistics` | Reports table and column-level statistics for the cost-based optimizer |
| `SupportsRuntimeV2Filtering` | Accepts additional filter values at execution time, used for dynamic partition pruning and to narrow [row-level DML](#row-level-operations) rewrites |
| `SupportsStorageFilters` | Declares the columns its readers can evaluate a runtime bloom filter on, so they can skip reading the other columns of the rows it rejects. Off by default, see `spark.sql.sources.v2.storageFilterPushdown.enabled` |

## Write Path

The write path mirrors the read path with a builder pattern that separates logical configuration
from physical execution:

```
SupportsWrite.newWriteBuilder(info)
  └─▸ WriteBuilder           (logical: mode selection)
        └─▸ Write             (logical: description, metrics)
              └─▸ BatchWrite  (physical: commit protocol)
                    ├─ DataWriterFactory
                    │    └─▸ DataWriter      (per-task I/O)
                    ├─ commit(messages[])
                    └─ abort(messages[])
```

### WriteBuilder

[`WriteBuilder`](api/java/org/apache/spark/sql/connector/write/WriteBuilder.html) is the
starting point for configuring a write. Calling `build()` returns a logical `Write` object.

Write modes are configured by mixing in additional interfaces on the builder:

| Mix-in | Mode |
|--------|------|
| `SupportsTruncate` | Truncate the table before writing |
| `SupportsOverwriteV2` | Overwrite data matching a filter expression |
| `SupportsDynamicOverwrite` | Dynamically overwrite partitions |

### Write

[`Write`](api/java/org/apache/spark/sql/connector/write/Write.html) is a logical representation
of a configured write. Similar to `Scan`, it bridges to the physical layer:

| Method | Description |
|--------|-------------|
| `toBatch()` | Returns a `BatchWrite` for batch execution |
| `toStreaming()` | Returns a `StreamingWrite` for streaming execution |

### BatchWrite

[`BatchWrite`](api/java/org/apache/spark/sql/connector/write/BatchWrite.html) defines the
two-phase commit protocol for batch writes:

1. `createBatchWriterFactory(info)` &mdash; creates a `DataWriterFactory` that is serialized and
   sent to executors.
2. On each executor, the factory creates a `DataWriter` per task attempt. When the attempt has
   written its rows and the commit coordinator allows it, Spark calls `commit()`, which returns a
   `WriterCommitMessage`; if the attempt fails, Spark calls `abort()`. Failed tasks are retried.
3. On the driver, `onDataWriterCommit(message)` runs as each task's message arrives. If every task
   commits, Spark calls `commit(messages[])`; if the job fails after task retries, or
   `onDataWriterCommit` or `commit` throws, it calls `abort(messages[])`.

Data written by individual tasks should not be visible to readers until the driver-level `commit`
succeeds.

### Distribution and Ordering Requirements

A `Write` implementation can also implement
[`RequiresDistributionAndOrdering`](api/java/org/apache/spark/sql/connector/write/RequiresDistributionAndOrdering.html)
to tell Spark how input data must be distributed and sorted before writing. Spark will insert
shuffle and sort nodes as needed to satisfy these requirements.

## Row-Level DML

DSV2 provides interfaces for connectors to support `DELETE`, `UPDATE`, and `MERGE INTO`
statements.

### Filter-Based Delete

The simplest form of DML is a filter-based delete, where entire groups of rows matching a
predicate are removed without rewriting individual records.

[`SupportsDeleteV2`](api/java/org/apache/spark/sql/connector/catalog/SupportsDeleteV2.html)
(and the older `SupportsDelete`) is a `Table` mix-in for this use case. Its key methods are:

| Method | Description |
|--------|-------------|
| `canDeleteWhere(predicates)` | Returns whether the data source can efficiently delete rows matching the given predicates. If this returns `false`, Spark falls back to row-level rewriting ([Row-Level Operations](#row-level-operations)) when the table also implements `SupportsRowLevelOperations`; otherwise the `DELETE` fails |
| `deleteWhere(predicates)` | Deletes all rows matching the predicates |

This approach is well-suited for data sources that can drop entire partitions or files without
rewriting data.

### Row-Level Operations

For more complex DML &mdash; such as `UPDATE`, `MERGE INTO`, or `DELETE` operations that
cannot be handled by a simple filter &mdash; connectors implement
[`SupportsRowLevelOperations`](api/java/org/apache/spark/sql/connector/catalog/SupportsRowLevelOperations.html).
This interface returns a
[`RowLevelOperation`](api/java/org/apache/spark/sql/connector/write/RowLevelOperation.html),
which coordinates a read-and-rewrite cycle:

```
SupportsRowLevelOperations.newRowLevelOperationBuilder(info)
  └─▸ RowLevelOperation
        ├─ newScanBuilder(options)   → reads affected rows
        └─ newWriteBuilder(info)     → writes rewritten data
```

Data sources fall into two categories:

- **Delta-based** sources (sometimes called *merge-on-read*) can handle a stream of individual
  row changes (inserts, updates, deletes). The scan only needs to produce the rows that are
  changing.
- **Group-based** sources (sometimes called *copy-on-write*) replace entire groups of rows
  (e.g. files or partitions). The scan must return all rows in each affected group, including
  unchanged rows, so that the data source can rewrite the group with the modifications applied.

## Expressions

The `org.apache.spark.sql.connector.expressions` package provides a neutral expression
representation used across the DSV2 API:

- **Transforms** (`Transform`, built with factory methods on `Expressions` such as `identity`,
  `bucket` and `years`)
  describe table partitioning schemes.
- **Sort orders** (`SortOrder`) describe ordering requirements.
- **Filter predicates** (`Predicate` and subclasses) represent pushed-down filter conditions.
- **Aggregates** represent pushed-down aggregate functions.

These expression types are independent of Spark's internal Catalyst expressions, allowing
connectors to avoid a dependency on Spark internals.

## Streaming

The same `Table` and `Scan` abstractions support streaming queries. A `Table` that declares
`MICRO_BATCH_READ` or `CONTINUOUS_READ` capabilities provides streaming reads through:

| Method | Description |
|--------|-------------|
| `Scan.toMicroBatchStream(checkpointLocation)` | Returns a `MicroBatchStream` that reads data in micro-batches, tracking progress through offsets |
| `Scan.toContinuousStream(checkpointLocation)` | Returns a `ContinuousStream` for low-latency continuous processing |

A `Table` that declares `STREAMING_WRITE` supports streaming writes through
`Write.toStreaming()`, which returns a `StreamingWrite`.

## Building a Connector

Spark's own test connectors make a working example of each step:
[`JavaSimpleDataSourceV2`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleDataSourceV2.java)
for a minimal read,
[`JavaAdvancedDataSourceV2WithV2Filter`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaAdvancedDataSourceV2WithV2Filter.java)
for pushdown, and
[`JavaSimpleWritableDataSource`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleWritableDataSource.java)
for writes, exercised by
[`DataSourceV2Suite`](https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2Suite.scala).
The three share a few test helpers. `TestingV2Source` is a test trait over `TableProvider`: it
implements `inferSchema` (two `int` columns, `i` and `j`) and forwards
`getTable(schema, partitioning, properties)` to a one-argument `getTable(options)`. The excerpts
override the one-argument form; a connector implements the three-argument form. `JavaSimpleBatchTable` is a
`Table` that declares `BATCH_READ` and returns that schema (through the deprecated `schema()`; a
new `Table` implements `columns()`); `JavaRangeInputPartition` carries a `start` and `end`; and
`JavaSimpleScanBuilder` is a `ScanBuilder` that is also its own `Scan` and `Batch`.

### Step 1: A Minimal Batch Read

The smallest connector is a `TableProvider` that returns a `Table`, a `Table` that declares
`BATCH_READ` and returns a `ScanBuilder`, and a `ScanBuilder` whose `Batch` plans partitions and
creates readers. `JavaSimpleDataSourceV2` is exactly that:

{% highlight java %}
public class JavaSimpleDataSourceV2 implements TestingV2Source {

  static class MyScanBuilder extends JavaSimpleScanBuilder {
    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new JavaRangeInputPartition(0, 5);
      partitions[1] = new JavaRangeInputPartition(5, 10);
      return partitions;
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder();
      }
    };
  }
}
{% endhighlight %}

`planInputPartitions()` runs once on the driver and returns one `InputPartition` per task. Each
partition is serialized to an executor, so it should hold only what identifies the split (here a
range of integers; for a file source, a path and a byte range). The `PartitionReaderFactory` is
serialized the same way and creates a `PartitionReader` on the executor. The reader is a cursor:
`next()` advances, `get()` returns the current row, `close()` releases any handle. The one in
`JavaSimpleReaderFactory` counts through the partition's range:

{% highlight java %}
return new PartitionReader<InternalRow>() {
  private int current = p.start - 1;

  @Override
  public boolean next() {
    current += 1;
    return current < p.end;
  }

  @Override
  public InternalRow get() {
    return new GenericInternalRow(new Object[] {current, -current});
  }

  @Override
  public void close() {
  }
};
{% endhighlight %}

A `PartitionReader<InternalRow>` produces `InternalRow`, which holds each column
in Spark's internal representation rather than the external one a `Row` carries: a boxed `int`
is fine for an `IntegerType` column, but a `StringType` column holds `UTF8String` (not
`java.lang.String`), `DecimalType` holds `Decimal`, `DateType` holds an `int` of days,
`TimestampType` a `long` of microseconds, `BinaryType` a `byte[]`, and nested types hold
`ArrayData`, `MapData` and `InternalRow`. `InternalRow` and `GenericInternalRow` live in Spark's
internal `catalyst` package and have no API docs, but they are the row types the connector API
uses. The accessor names on `InternalRow` (`getUTF8String`,
`getDecimal`, `getInt`, `getLong`, `getBinary`, `getArray`, `getMap`, `getStruct`) are the
reference for each type. `GenericInternalRow` takes `Object[]`, so a value of the wrong
representation compiles and fails on the executor with a `ClassCastException`.

With this much, `spark.read.format("...JavaSimpleDataSourceV2").load()` returns ten rows, and
the `select` and `filter` in the "simplest implementation" test work too: Spark applies the
projection and the filter itself, above the scan.

### Step 2: Projection and Filter Pushdown (Optional)

A connector that can skip columns or rows adds the pushdown mix-ins to its `ScanBuilder`. The
list, the order Spark applies them in, and the residual-filter contract are under
[ScanBuilder](#scanbuilder). `JavaAdvancedDataSourceV2WithV2Filter` accepts only
`i > <int literal>` and returns everything else for Spark to evaluate. Unlike the test, which
asserts the shapes it expects, the following version checks them, because Spark can also push `i > j`
as a `>` predicate; it also matches on the public `NamedReference` rather than the test's
`FieldReference`. It accepts a threshold only from `-1` up, because the planner starts the first
partition at the threshold plus one and the source's data is `0` to `9`; a lower threshold stays
with Spark. The planner enforces one lower bound, so the example accepts the first matching
predicate and returns any others to Spark. Replacing `predicates` on each call is right while `supportsIterativePushdown()`
returns `false` (the default); an iterative source accumulates them instead:

{% highlight java %}
@Override
public Predicate[] pushPredicates(Predicate[] predicates) {
  List<Predicate> supported = new ArrayList<>();
  List<Predicate> unsupported = new ArrayList<>();
  for (Predicate p : predicates) {
    if (supported.isEmpty() && isIGreaterThanInt(p)) {
      supported.add(p);
    } else {
      unsupported.add(p);
    }
  }
  this.predicates = supported.toArray(new Predicate[0]);
  return unsupported.toArray(new Predicate[0]);
}

private static boolean isIGreaterThanInt(Predicate p) {
  return p.name().equals(">")
      && p.children()[0] instanceof NamedReference column
      && column.fieldNames().length == 1
      && column.fieldNames()[0].equals("i")
      && p.children()[1] instanceof Literal<?> value
      && value.value() instanceof Integer bound
      && bound >= -1;
}

@Override
public Predicate[] pushedPredicates() {
  return predicates;
}

@Override
public void pruneColumns(StructType requiredSchema) {
  this.requiredSchema = requiredSchema;
}
{% endhighlight %}

`pushedPredicates()` reports what the source accepted. The `Batch` in the same class uses the
accepted predicate to plan fewer partitions, and its reader fills only the columns in
`requiredSchema`. The "advanced implementation with V2 Filter" test in
[`DataSourceV2Suite`](https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2Suite.scala)
checks the planned `Batch` for a few `filter` and `select` combinations. `EXPLAIN` does not show
`pushedPredicates()`; it shows `Scan.description()` inside the `BatchScan` node, after the output
columns. Its default is `getClass().toString()`, so override it to include the accepted predicates
and the pruned columns.

### Step 3: Batch Writes

A writable table declares `BATCH_WRITE` and implements `SupportsWrite`. For `SaveMode.Overwrite`
it also declares `TRUNCATE` (or `OVERWRITE_BY_FILTER`) and implements the matching builder mix-in,
such as `SupportsTruncate`; see
[`TableCapability`](api/java/org/apache/spark/sql/connector/catalog/TableCapability.html) for the
others. Spark checks the capability set during analysis, before calling any builder, so a missing
value fails the query even when the mix-in is implemented. `JavaSimpleWritableDataSource` declares
`BATCH_READ`, `BATCH_WRITE` and `TRUNCATE`. A `TableProvider` like this one, which does not
implement `SupportsCatalogOptions`, supports only the `append` and `overwrite` save modes; the
default, `errorifexists`, and `ignore` fail with `UNSUPPORTED_DATA_SOURCE_SAVE_MODE`. A provider
that implements `SupportsCatalogOptions` takes its catalog and identifier from the write options,
and Spark runs those two modes as `CREATE TABLE ... AS SELECT` in that catalog.

The write then follows the protocol under [BatchWrite](#batchwrite). A file-based connector
typically has each `DataWriter` write its task's output to a staging location, and
`BatchWrite.commit` on the driver publish the staged files. The staging location must be reachable from both sides:
executors write there and the driver renames or registers from there, usually on different
machines. `JavaSimpleWritableDataSource` stages under `<path>/_temporary/<queryId>` on the
table's own file system, writes one file per task
([`JavaCSVDataWriterFactory.createWriter`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleWritableDataSource.java)),
and commits by renaming the staged files into the table directory one at a time
([`MyBatchWrite.commit`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleWritableDataSource.java)).
`commit` is a single driver-side call, but that alone does not make the publication atomic: a
reader listing the directory between two renames sees a partial result. A connector that needs
all-or-nothing visibility has to provide it itself, for example by writing a manifest or
committing a transaction as the last step.

Two shortcuts in the test connector are not safe outside a test. Its `commit` publishes every file
under the staging directory rather than the ones named in `messages`, so a partial file left by an
executor lost mid-task would be published next to the retry's file; a real connector commits only
the files its messages name. And it truncates eagerly in `Write.toBatch()`, before any task runs,
so a failed overwrite leaves the table empty; a real connector defers the delete to
`BatchWrite.commit`.

A [`WriterCommitMessage`](api/java/org/apache/spark/sql/connector/write/WriterCommitMessage.html)
is returned by `DataWriter.commit()` on an executor and delivered to `BatchWrite.commit` on the
driver, where the whole array is held in memory. It should carry a reference to what the task
wrote (a path, an object key, a transaction id), never the rows. This connector keeps its state
in the staging directory and returns `null`. `BatchWrite.abort(messages)` removes the staged
output; it receives `null` entries for tasks that never committed and must tolerate them.
`DataWriter.abort()` on the executor side deletes that task's own file.

`DataWriter.write` receives rows in the internal representation described in Step 1, so a
`StringType` column is read back with `record.getUTF8String(i).toString()`.

### Step 4: Catalog Integration (Optional)

A `TableProvider` is reached by class name from `spark.read.format(...)`. To reach the same
tables through SQL identifiers and DDL (`CREATE TABLE demo.db.t`, `INSERT INTO demo.db.t`,
`SHOW TABLES IN demo.db`), implement [`TableCatalog`](#tablecatalog), whose `loadTable` returns
the same `Table` class the `TableProvider` builds, and register it as under
[CatalogPlugin](#catalogplugin):

```
spark.sql.catalog.demo=com.example.DemoCatalog
spark.sql.catalog.demo.warehouse=/path/to/warehouse    # arrives as options.get("warehouse")
```

`CREATE TABLE` arrives as `createTable(ident, tableInfo)`, and `tableInfo.columns()` is a
`Column[]`; `Table.columns()` returns the same type, while the read and write paths speak
`StructType`. Build the array with
[`Column.create(name, dataType, nullable)`](api/java/org/apache/spark/sql/connector/catalog/Column.html).
`spark.sql.defaultCatalog` names the catalog that is current when a session has not set one, so
after `SET spark.sql.defaultCatalog=demo`, `db.t` resolves to `demo.db.t`.

## Further Reading

- [API documentation (Javadoc)](api/java/org/apache/spark/sql/connector/package-summary.html)
  for the full interface reference.
- [Data Sources](sql-data-sources.html) for the user-facing guide to built-in data sources (DSV1).
- [Python Data Source API](api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html)
  for writing lightweight connectors entirely in Python.
- [Storage Partition Join](sql-performance-tuning.html#storage-partition-join) for how DSV2
  partitioning reporting enables join optimizations.
