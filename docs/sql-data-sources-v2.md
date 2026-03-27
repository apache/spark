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
| [`TableProvider`](#tableprovider) | The simpler entry point, for sources that identify a table by **options** (e.g. a file path or Kafka topic) rather than through a catalog. Can implement `SupportsCatalogOptions` to also participate in DDL via the session catalog. |
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
to participate in DDL such as `CREATE TABLE` by extracting a catalog identifier from the
user-supplied options. This bridges option-based sources to the session catalog, which is how
built-in file data sources (Parquet, ORC, etc.) support table creation.


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
| `ViewCatalog` | List, load, create, alter, and drop views *(work in progress &mdash; not yet integrated into query resolution)* |
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
| `createTable(ident, columns, partitions, properties)` | Create a new table |
| `alterTable(ident, changes...)` | Alter a table's schema, properties, or constraints |
| `dropTable(ident)` | Drop a table |

### StagingTableCatalog

[`StagingTableCatalog`](api/java/org/apache/spark/sql/connector/catalog/StagingTableCatalog.html)
extends `TableCatalog` and enables **atomic** `CREATE TABLE AS SELECT` and
`REPLACE TABLE AS SELECT`. It returns a `StagedTable` whose changes only become visible when
`commitStagedChanges()` is called. If the write fails, the catalog remains unchanged.

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

> **Note:** `ViewCatalog` is a work in progress. The interface is defined but is not yet
> integrated into Spark's query resolution or planning.

[`ViewCatalog`](api/java/org/apache/spark/sql/connector/catalog/ViewCatalog.html) extends
`CatalogPlugin` and provides methods for view lifecycle management:

| Method | Description |
|--------|-------------|
| `listViews(namespace)` | List views in a namespace |
| `loadView(ident)` | Load a view by identifier |
| `createView(viewInfo)` | Create a new view |
| `replaceView(viewInfo, orCreate)` | Replace (or create) a view |
| `alterView(ident, changes...)` | Alter a view's properties or schema |
| `dropView(ident)` | Drop a view |
| `renameView(oldIdent, newIdent)` | Rename a view |

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

1. **Sample** (`SupportsPushDownSample`)
2. **Filter** (`SupportsPushDownFilters` / `SupportsPushDownV2Filters`)
3. **Aggregate** (`SupportsPushDownAggregates`)
4. **Limit / Top-N** (`SupportsPushDownLimit` / `SupportsPushDownTopN`)
5. **Offset** (`SupportsPushDownOffset`)
6. **Column pruning** (`SupportsPushDownRequiredColumns`)

Each mix-in interface has a method that Spark calls with the relevant operators. The
implementation returns the operators it can handle, and Spark applies the remaining operators
itself.

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
and is closed after consumption.

### Scan Mix-ins

`Scan` implementations can opt in to additional optimizations by implementing mix-in interfaces:

| Mix-in | Capability |
|--------|------------|
| `SupportsReportPartitioning` | Reports how the output is partitioned, allowing Spark to avoid unnecessary shuffles (e.g. [Storage Partition Join](sql-performance-tuning.html#storage-partition-join)) |
| `SupportsReportOrdering` | Reports the sort order of the output, allowing Spark to skip redundant sorts |
| `SupportsReportStatistics` | Reports table and column-level statistics for the cost-based optimizer |
| `SupportsRuntimeV2Filtering` | Accepts additional filter values at execution time, used for dynamic partition pruning and to narrow [row-level DML](#row-level-operations) rewrites |

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
2. On each executor, the factory creates a `DataWriter` per partition. If all rows are written
   successfully, the writer calls `commit()`; otherwise it calls `abort()`.
3. After all tasks complete, the driver calls either `commit(messages[])` (if all tasks
   succeeded) or `abort(messages[])` (if any task failed).

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
| `canDeleteWhere(predicates)` | Returns whether the data source can efficiently delete rows matching the given predicates. If this returns `false`, Spark falls back to row-level rewriting (see below) |
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

- **Transforms** (`IdentityTransform`, `BucketTransform`, `YearsTransform`, etc.)
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

## Further Reading

- [API documentation (Javadoc)](api/java/org/apache/spark/sql/connector/package-summary.html)
  for the full interface reference.
- [Data Sources](sql-data-sources.html) for the user-facing guide to built-in data sources (DSV1).
- [Python Data Source API](api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html)
  for writing lightweight connectors entirely in Python.
- [Storage Partition Join](sql-performance-tuning.html#storage-partition-join) for how DSV2
  partitioning reporting enables join optimizations.
