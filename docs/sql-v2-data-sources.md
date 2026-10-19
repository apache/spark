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

`createTable(ident, tableInfo)` is the form Spark calls (since 4.1.0). The older overloads
`createTable(ident, columns, partitions, properties)` and `createTable(ident, schema, partitions, properties)`
are deprecated (since 4.1.0 and 3.4.0 respectively); their default implementations delegate down the
chain, so a catalog that overrides only the `TableInfo` form serves every `CREATE TABLE`,
`REPLACE TABLE`, and CTAS path.

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

[`ViewCatalog`](api/java/org/apache/spark/sql/connector/catalog/ViewCatalog.html) extends
`CatalogPlugin` and provides methods for view lifecycle management. View DDL
(`CREATE`/`REPLACE`/`ALTER`/`DROP`/`SHOW VIEWS`) and reading from a view (`SELECT`) are resolved
and executed against the catalog. Connectors that expose both
tables and views in a single shared identifier namespace implement
[`RelationCatalog`](api/java/org/apache/spark/sql/connector/catalog/RelationCatalog.html)
(since 4.2.0), which extends both `TableCatalog` and `ViewCatalog` and adds
`loadRelation(ident)` so a lookup that may be either kind resolves in one round trip. A catalog
that implements `TableCatalog` and `ViewCatalog` directly, without `RelationCatalog`, is rejected
when it is initialized. A view's metadata is represented by [`ViewInfo`](#view).

| Method | Description |
|--------|-------------|
| `listViews(namespace)` | List views in a namespace |
| `loadView(ident)` | Load a view's `ViewInfo` by identifier |
| `viewExists(ident)` | Test whether a view exists (has a default implementation) |
| `invalidateView(ident)` | Invalidate cached metadata for a view (has a default implementation) |
| `createView(ident, info)` | Create a new view |
| `replaceView(ident, info)` | Atomically replace an existing view (`ALTER VIEW ... AS`) |
| `createOrReplaceView(ident, info)` | Create a view, or atomically replace it if it exists (`CREATE OR REPLACE VIEW`; has a default implementation) |
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

[`ViewInfo`](api/java/org/apache/spark/sql/connector/catalog/ViewInfo.html) is the metadata
abstraction for a view &mdash; a named SQL query managed through a [`ViewCatalog`](#viewcatalog).
Unlike a [`Table`](#table), a view has no data of its own: it stores a SQL query along with the
context needed to resolve and expand that query consistently whenever the view is referenced.

A `ViewInfo` provides:

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

1. **Sample** (`SupportsPushDownSample`)
2. **Filter** (`SupportsPushDownFilters` / `SupportsPushDownV2Filters`)
3. **Aggregate** (`SupportsPushDownAggregates`)
4. **Limit / Top-N** (`SupportsPushDownLimit` / `SupportsPushDownTopN`)
5. **Offset** (`SupportsPushDownOffset`)
6. **Column pruning** (`SupportsPushDownRequiredColumns`)

Each mix-in has a method that Spark calls with the candidate operator, and the return value tells
Spark what it must still evaluate itself after the scan:

- [`SupportsPushDownFilters`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownFilters.html):
  `pushFilters(filters)` receives the filters Spark could translate to
  `org.apache.spark.sql.sources.Filter` and returns the ones that still need to be evaluated
  **after** the scan. `pushedFilters()` returns the filters the source accepted. A filter may appear
  in both: a source that can only skip whole files or row groups on a predicate reports it as pushed
  and also returns it, so Spark re-checks it row by row. Spark evaluates only what `pushFilters`
  returns (plus anything it could not translate), so a filter that is neither returned nor applied
  by the reader is silently dropped from the query.
- [`SupportsPushDownV2Filters`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownV2Filters.html)
  (since 3.3.0) is the preferred form of the same contract: `pushPredicates(predicates)` returns
  the post-scan predicates as [`Predicate`](#expressions) instances and `pushedPredicates()`
  reports the accepted ones,
  without the conversion to the V1 `Filter` type. Spark checks for `SupportsPushDownFilters`
  first, so a `ScanBuilder` should implement one or the other.
- [`SupportsPushDownRequiredColumns`](api/java/org/apache/spark/sql/connector/read/SupportsPushDownRequiredColumns.html):
  `pruneColumns(requiredSchema)` passes the narrowest schema the query needs. Pruning may be
  partial (for example top-level columns only), and `Scan.readSchema()` must report whatever
  pruning was actually applied.
- `pushLimit`, `pushTopN`, `pushOffset`, and `pushAggregation` return `true` when the source
  will apply the operator. `isPartiallyPushed()` (limit, top-N) and
  `supportCompletePushDown(aggregation)` tell Spark whether it still has to apply the operator
  on top of the scan.

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

## Building a Connector

A minimal batch connector is a `TableProvider`, a `Table` that declares what it can do, a
`ScanBuilder` that plans partitions and creates readers, and a `WriteBuilder` that implements the
commit protocol. The examples read and write a CSV file in which every column is a string, so the
API surface stays visible.

### Declaring the Table and Its Capabilities

The `Table` names the dataset, exposes its columns, declares its capabilities, and hands out
builders through `SupportsRead` and `SupportsWrite`. It does no I/O itself.

{% highlight java %}
public class CsvTable implements SupportsRead, SupportsWrite {

  private static final Set<TableCapability> CAPABILITIES = EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE);

  private final StructType schema;
  private final CsvFile file;

  public CsvTable(StructType schema, CsvFile file) {
    this.schema = schema;
    this.file = file;
  }

  @Override
  public String name() {
    return "csv:" + file.path();
  }

  @Override
  public Column[] columns() {
    return CatalogHelpers.asColumns(schema);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new CsvScanBuilder(schema, file);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new CsvWriteBuilder(info.schema(), file);
  }
}
{% endhighlight %}

Spark checks [`TableCapability`](api/java/org/apache/spark/sql/connector/catalog/TableCapability.html)
during analysis, before it calls `newScanBuilder` or `newWriteBuilder`, so the set decides which
operations are planned regardless of which mix-ins the table implements:

| Capability | What it enables |
|------------|-----------------|
| `BATCH_READ` | Batch reads through `newScanBuilder` and `Scan.toBatch()` |
| `MICRO_BATCH_READ` / `CONTINUOUS_READ` | Streaming reads through `Scan.toMicroBatchStream` / `Scan.toContinuousStream` |
| `BATCH_WRITE` | Append writes through `newWriteBuilder` and `Write.toBatch()` |
| `STREAMING_WRITE` | Streaming writes through `Write.toStreaming()` |
| `TRUNCATE` | Overwriting the whole table (`SaveMode.Overwrite`, `INSERT OVERWRITE` without a partition spec); the builder also implements `SupportsTruncate` |
| `OVERWRITE_BY_FILTER` | Overwriting the rows that match a predicate (`DataFrameWriterV2.overwrite(condition)`, `INSERT OVERWRITE` with static partition values); the builder also implements `SupportsOverwriteV2` |
| `OVERWRITE_DYNAMIC` | Dynamic partition overwrite; the builder also implements `SupportsDynamicOverwrite` |
| `ACCEPT_ANY_SCHEMA` | Skips Spark's write-side schema resolution; the table accepts input of any schema |
| `AUTOMATIC_SCHEMA_EVOLUTION` | Lets Spark alter the table schema as part of an operation when the input requires it |
| `V1_BATCH_WRITE` | Append writes through a [`V1Write`](api/java/org/apache/spark/sql/connector/write/V1Write.html) wrapping a V1 `InsertableRelation`; combines with `TRUNCATE` and `OVERWRITE_BY_FILTER` but not `OVERWRITE_DYNAMIC` |
| `GENERATE_COLUMN_VALUES_ON_WRITE` | Spark computes missing generated-column values on by-name writes and validates explicitly supplied ones (since 4.3.0) |
| `SCAN_MERGING` | Spark may fuse two scans of the table that differ only in pruned columns or pushed filters into one scan (since 4.3.0) |

A table loaded through a catalog without `BATCH_READ` fails with
`[UNSUPPORTED_FEATURE.TABLE_OPERATION] ... does not support batch scan`; a table from a
`TableProvider` without it is not treated as a V2 source at all, and Spark falls back to the V1
lookup for the class name. `SaveMode.Overwrite` needs `BATCH_WRITE` plus `TRUNCATE` or
`OVERWRITE_BY_FILTER`, and `SupportsTruncate` on the builder does not stand in for the
capability.

### Implementing the Read Path

`Batch.planInputPartitions()` runs once on the driver and returns one
[`InputPartition`](api/java/org/apache/spark/sql/connector/read/InputPartition.html) per task.
Each partition is serialized to an executor, so it holds only what identifies the split and
nothing that cannot cross the wire (no connections, file handles, or references back to the
`Scan`):

{% highlight java %}
public class CsvInputPartition implements InputPartition {

  private static final long serialVersionUID = 1L;

  final long startByte;
  final long endByte;

  public CsvInputPartition(long startByte, long endByte) {
    this.startByte = startByte;
    this.endByte = endByte;
  }
}
{% endhighlight %}

Here the split is a byte range computed from the file length, so planning never reads data.
The `PartitionReaderFactory` is serialized the same way and creates a `PartitionReader` on the
executor. The reader is a cursor: `next()` advances, `get()` returns the current row, and
`close()` releases the handle.

{% highlight java %}
@Override
public boolean next() {
  while (range.next()) {
    List<String> cells = CsvFile.splitLine(range.get());
    if (!matchesPushedFilters(cells, fullSchema, pushedFilters)) {
      continue;
    }
    Object[] values = new Object[projection.length];
    for (int i = 0; i < projection.length; i++) {
      int src = projection[i];
      values[i] = src < cells.size() ? UTF8String.fromString(cells.get(src)) : null;
    }
    currentRow = new GenericInternalRow(values);
    return true;
  }
  currentRow = null;
  return false;
}

@Override
public InternalRow get() {
  return currentRow;
}
{% endhighlight %}

A `PartitionReader<InternalRow>` produces `org.apache.spark.sql.catalyst.InternalRow`, which
holds each column in Spark's internal representation rather than the external one a `Row`
carries: `UTF8String` for strings, `Decimal` for decimals, an `int` of days for dates, a `long`
of microseconds for timestamps, `byte[]` for binary, and `ArrayData`, `MapData`, and
`InternalRow` for nested types. The accessor names on `InternalRow` (`getUTF8String`,
`getDecimal`, `getInt`, `getLong`, `getBinary`, `getArray`, `getMap`, `getStruct`) are the
reference for each type. `GenericInternalRow` takes `Object[]`, so a `java.lang.String` in a
string column compiles and fails on the executor with
`ClassCastException: class java.lang.String cannot be cast to class org.apache.spark.unsafe.types.UTF8String`.

### Pushdown and How to Confirm It

Pushdown is negotiated on the `ScanBuilder` through the mix-ins described under
[ScanBuilder](#scanbuilder). The builder records what it accepted and carries it into the
`Scan` and the reader factory, so the reader applies exactly the filters and the projection the
builder claimed:

{% highlight java %}
public class CsvScanBuilder
    implements ScanBuilder, Scan, Batch, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final StructType fullSchema;
  private final CsvFile file;

  private StructType requiredSchema;
  private Filter[] pushedFilters = new Filter[0];

  public CsvScanBuilder(StructType fullSchema, CsvFile file) {
    this.fullSchema = fullSchema;
    this.file = file;
    this.requiredSchema = fullSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> accepted = new ArrayList<>();
    List<Filter> rejected = new ArrayList<>();
    for (Filter f : filters) {
      if (f instanceof EqualTo eq && fullSchema.getFieldIndex(eq.attribute()).isDefined()) {
        accepted.add(f);
      } else {
        rejected.add(f);
      }
    }
    this.pushedFilters = accepted.toArray(new Filter[0]);
    return rejected.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Scan build() {
    return this;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public StructType readSchema() {
    return requiredSchema;
  }

  @Override
  public String description() {
    return "CsvScan(path=" + file.path()
        + ", columns=" + String.join(",", requiredSchema.fieldNames())
        + ", pushedFilters=" + pushedFilters.length + ")";
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<long[]> ranges = file.byteRanges();
    InputPartition[] partitions = new InputPartition[ranges.size()];
    for (int i = 0; i < ranges.size(); i++) {
      partitions[i] = new CsvInputPartition(ranges.get(i)[0], ranges.get(i)[1]);
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new CsvReaderFactory(fullSchema, requiredSchema, pushedFilters, file);
  }
}
{% endhighlight %}

`Scan.description()` renders inside the `BatchScan` node of the physical plan, after the table
name and the pruned output columns, which makes it the place to report what the connector was
actually given. For `df.select("name").filter(df.dept == "eng")` the plan shows the pruned
columns and the pushed filter at the scan, while the residual `isnotnull` stays in Spark's own
`Filter` node:

```
== Physical Plan ==
*(1) Project [name#6]
+- *(1) Filter isnotnull(dept#7)
   +- BatchScan csv:/tmp/dsv2lab/people.csv[name#6, dept#7] CsvScan(path=/tmp/dsv2lab/people.csv, columns=name,dept, pushedFilters=1) RuntimeFilters: []
```

The default `description()` is the implementation's class name.

### Implementing the Write Path

The `WriteBuilder` records the write mode and builds a `Write` whose `toBatch()` returns the
`BatchWrite`. Implementing `SupportsTruncate` on the builder, together with the `TRUNCATE`
capability on the table, is what makes `SaveMode.Overwrite` work:

{% highlight java %}
public class CsvWriteBuilder implements WriteBuilder, SupportsTruncate {

  private final StructType schema;
  private final CsvFile file;
  private boolean truncate = false;

  public CsvWriteBuilder(StructType schema, CsvFile file) {
    this.schema = schema;
    this.file = file;
  }

  @Override
  public WriteBuilder truncate() {
    this.truncate = true;
    return this;
  }

  @Override
  public Write build() {
    return new Write() {
      @Override
      public BatchWrite toBatch() {
        return new CsvBatchWrite(schema, file, truncate);
      }
    };
  }
}
{% endhighlight %}

Spark then runs the commit protocol:

```
driver:    BatchWrite.createBatchWriterFactory(info)
executor:  DataWriter.write(row) per row
           DataWriter.commit()  -> WriterCommitMessage    (or abort() on failure)
driver:    BatchWrite.commit(messages[])                  (or abort(messages[]))
```

Each `DataWriter` streams its rows to its own staging location and returns a
[`WriterCommitMessage`](api/java/org/apache/spark/sql/connector/write/WriterCommitMessage.html)
that identifies what it wrote. The messages are collected on the driver, so a message carries a
reference (a path, an object key, a transaction id), not the rows:

{% highlight java %}
static final class CsvCommitMessage implements WriterCommitMessage {
  private static final long serialVersionUID = 1L;
  final String partFile;

  CsvCommitMessage(String partFile) {
    this.partFile = partFile;
  }
}

static final class CsvDataWriter implements DataWriter<InternalRow> {
  private final StructType schema;
  private final Path partFile;
  private final BufferedWriter out;

  CsvDataWriter(StructType schema, int partitionId, long taskId) {
    this.schema = schema;
    try {
      this.partFile = Files.createTempFile("csvv2-part-" + partitionId + "-" + taskId + "-", ".csv");
      this.out = Files.newBufferedWriter(partFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void write(InternalRow record) {
    String[] cells = new String[schema.fields().length];
    for (int i = 0; i < cells.length; i++) {
      cells[i] = record.isNullAt(i) ? "" : record.getUTF8String(i).toString();
    }
    try {
      out.write(String.join(",", cells));
      out.newLine();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public WriterCommitMessage commit() {
    try {
      out.flush();
      out.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new CsvCommitMessage(partFile.toString());
  }

  @Override
  public void abort() {
    try {
      out.close();
      Files.deleteIfExists(partFile);
    } catch (IOException e) {
      // Best effort: abort must not throw and mask the original failure.
    }
  }

  @Override
  public void close() {
    try {
      out.close();
    } catch (IOException e) {
      // Already closed by commit() in the normal path.
    }
  }
}
{% endhighlight %}

`BatchWrite.commit(messages)` makes the result visible in one step; here it concatenates the part
files into a staged copy and moves it over the destination, so a reader never sees a half-written
file. `BatchWrite.abort(messages)` removes the parts that did succeed; it receives `null` entries
for tasks that never committed and must tolerate them.

### Registering Through a Catalog

A `TableProvider` is reached by class name from `spark.read.format(...)`. To reach the same
tables through SQL identifiers and DDL, implement [`TableCatalog`](#tablecatalog) and register
the catalog by name. Every configuration key under the catalog's prefix is passed to
`CatalogPlugin.initialize(name, options)` with the prefix removed:

```
spark.sql.catalog.demo=com.example.CsvCatalog
spark.sql.catalog.demo.warehouse=/tmp/warehouse      # arrives as options.get("warehouse")
```

{% highlight java %}
@Override
public void initialize(String name, CaseInsensitiveStringMap options) {
  this.name = name;
  String dir = options.get("warehouse");
  if (dir == null) {
    throw new IllegalArgumentException(
        "CsvCatalog needs spark.sql.catalog." + name + ".warehouse set to a directory");
  }
  this.warehouse = Paths.get(dir);
  try {
    Files.createDirectories(warehouse);
  } catch (IOException e) {
    throw new UncheckedIOException(e);
  }
}
{% endhighlight %}

`CREATE TABLE demo.analytics.people (id STRING, name STRING, dept STRING)` arrives as
`createTable(ident, tableInfo)`, and `tableInfo.columns()` is a `Column[]`. `Table.columns()`
returns the same type, while the read and write paths still speak `StructType`. The converter
Spark uses internally (`CatalogV2Util.structTypeToV2Columns`) is `private[sql]`, so a connector
builds the array with
[`Column.create`](api/java/org/apache/spark/sql/connector/catalog/Column.html):

{% highlight java %}
public static Column[] asColumns(StructType schema) {
  StructField[] fields = schema.fields();
  Column[] columns = new Column[fields.length];
  for (int i = 0; i < fields.length; i++) {
    columns[i] = Column.create(fields[i].name(), fields[i].dataType(), fields[i].nullable());
  }
  return columns;
}
{% endhighlight %}

`spark.sql.defaultCatalog` names the catalog that is current when a session has not set one
explicitly, so unqualified identifiers resolve against it. After
`SET spark.sql.defaultCatalog=demo`, `current_catalog()` reports `demo` and
`SELECT name FROM analytics.people` resolves to `demo.analytics.people`.

### Running and Debugging Outside spark-submit

`spark-submit`, `spark-shell`, and `pyspark` add a set of JVM options for JDK 17 and later
(`--add-opens` for the `java.base` packages Spark and its serializers reach into, plus a few
system properties); the list lives in
`launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java`. A connector started
from a plain `main()` in an IDE or from a JUnit runner does not get them, so copy the options
from `JavaModuleOptions` into the run configuration or the test runner's `argLine`. Without them,
code paths that reflect into those packages (Kryo serializing a `ByteBuffer`, for example) fail
with `java.lang.reflect.InaccessibleObjectException: ... module java.base does not "opens java.nio" to unnamed module`.

Running in `local[n]` executes tasks in the driver JVM, which hides serialization mistakes in an
`InputPartition` or `PartitionReaderFactory`; `local-cluster[2,1,1024]` starts separate
executor JVMs and surfaces them.

## Migrating from Data Source V1

### Interface Mapping

| Data Source V1 | Data Source V2 | Note |
|----------------|----------------|------|
| `RelationProvider` | [`TableProvider`](#tableprovider) | Entry point for path and option based sources |
| `SchemaRelationProvider` | `TableProvider.inferSchema` plus `getTable(schema, partitioning, properties)` | Schema handling is two explicit steps; `supportsExternalMetadata()` must return `true` for a user-supplied schema to be accepted |
| `BaseRelation` | [`Table`](#table) | An interface rather than an abstract class, with no `SQLContext` |
| `BaseRelation.schema()` | `Table.columns()` | `Table.schema()` exists but is deprecated (since 3.4.0) |
| `TableScan.buildScan()` | `ScanBuilder` to `Scan` to `Batch` to `PartitionReader` | The connector describes the read, Spark executes it |
| `PrunedScan` | `SupportsPushDownRequiredColumns` | Opt-in mix-in on the `ScanBuilder` |
| `PrunedFilteredScan` | `SupportsPushDownFilters` or `SupportsPushDownV2Filters` | Opt-in mix-in on the `ScanBuilder` |
| `BaseRelation.unhandledFilters` | The return value of `pushFilters` / `pushPredicates` | One method decides what the source applies and what Spark keeps |
| `InsertableRelation.insert` | `WriteBuilder` to `Write` to `BatchWrite` to `DataWriter` | A two-phase commit replaces a single method with no failure story |
| `overwrite` flag on `insert` | `SupportsTruncate` plus the `TRUNCATE` capability | Overwrite is a declared capability, not a boolean |
| `RDD<Row>` | `PartitionReader<InternalRow>` | `Row` becomes `InternalRow`; see [Implementing the Read Path](#implementing-the-read-path) |
| no equivalent | `TableCatalog`, `SupportsNamespaces`, `ViewCatalog`, `FunctionCatalog`, `ProcedureCatalog` | DDL and multi-part namespaces through Spark SQL |
| no equivalent | `SupportsRowLevelOperations`, `SupportsDeleteV2` | `DELETE`, `UPDATE`, `MERGE INTO` |

A V1 connector can keep its `InsertableRelation` during the transition by returning a `V1Write`
from the `WriteBuilder` and declaring `V1_BATCH_WRITE` instead of `BATCH_WRITE`.

### Parallelism Moves to InputPartition

A V1 relation returns an `RDD<Row>`, so the connector chooses its parallelism by constructing the
RDD, and a relation that calls `parallelize` on a list has already materialized the data on the
driver. In V2, `Batch.planInputPartitions()` returns serializable descriptions of work, Spark
starts one task per `InputPartition`, and the `PartitionReaderFactory` creates the reader on the
executor. Nothing is read on the driver. Everything reachable from an `InputPartition` or a
`PartitionReaderFactory` must therefore be `Serializable` and should be small: a byte range or a
shard id, not an open connection, a file handle, or a reference back to the `Scan`. The failure
mode for a stray reference is `java.io.NotSerializableException` when the first task is scheduled.

### Writes Become a Commit Protocol

V1's `insert(data, overwrite)` is a single call; if it throws halfway through, the destination
keeps whatever partial state it reached. V2 splits the write across the driver and the executors:

```
driver:    BatchWrite.createBatchWriterFactory(info)
executor:  DataWriter.write(row) per row
           DataWriter.commit()  -> WriterCommitMessage    (or abort() on failure)
driver:    BatchWrite.commit(messages[])                  (or abort(messages[]))
```

The connector's job is to make the driver-side `commit` atomic and to keep task output invisible
until then, typically by writing each task's output to a staging location and moving or
registering it in `commit`. `abort(messages)` receives `null` entries for tasks that never
committed, so it must tolerate them.

### Filters: unhandledFilters Becomes a Return Value

V1 splits filter handling between `buildScan(columns, filters)` and `unhandledFilters(filters)`.
If the two disagree, Spark either evaluates a filter twice (harmless) or skips one the scan never
applied (wrong results). V2 folds both into `pushFilters`, which returns the filters the source
did **not** accept, while `pushedFilters()` reports the ones it did; the `CsvScanBuilder` under
[Pushdown and How to Confirm It](#pushdown-and-how-to-confirm-it) shows the shape. Only claim a
filter the reader actually applies, because Spark does not re-check a filter reported as pushed.

## Further Reading

- [API documentation (Javadoc)](api/java/org/apache/spark/sql/connector/package-summary.html)
  for the full interface reference.
- [Data Sources](sql-data-sources.html) for the user-facing guide to built-in data sources (DSV1).
- [Python Data Source API](api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html)
  for writing lightweight connectors entirely in Python.
- [Storage Partition Join](sql-performance-tuning.html#storage-partition-join) for how DSV2
  partitioning reporting enables join optimizations.
