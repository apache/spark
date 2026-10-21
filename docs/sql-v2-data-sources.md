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

The sections above describe the interfaces one at a time. This section puts them together in
the order a connector author needs them, using the connectors Spark tests itself with:
[`JavaSimpleDataSourceV2`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleDataSourceV2.java)
for a minimal read,
[`JavaAdvancedDataSourceV2WithV2Filter`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaAdvancedDataSourceV2WithV2Filter.java)
for pushdown, and
[`JavaSimpleWritableDataSource`](https://github.com/apache/spark/blob/master/sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleWritableDataSource.java)
for writes, exercised by
[`DataSourceV2Suite`](https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2Suite.scala).
The three share a few test helpers: `TestingV2Source` is a `TableProvider` whose schema is two
`int` columns, `i` and `j`; `JavaSimpleBatchTable` is a `Table` that declares `BATCH_READ` and
returns that schema; `JavaRangeInputPartition` carries a `start` and `end`; and
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

A `PartitionReader<InternalRow>` produces
[`InternalRow`](api/java/org/apache/spark/sql/catalyst/InternalRow.html), which holds each column
in Spark's internal representation rather than the external one a `Row` carries: a boxed `int`
is fine for an `IntegerType` column as above, but a `StringType` column holds `UTF8String` (not
`java.lang.String`), `DecimalType` holds `Decimal`, `DateType` holds an `int` of days,
`TimestampType` a `long` of microseconds, `BinaryType` a `byte[]`, and nested types hold
`ArrayData`, `MapData` and `InternalRow`. The accessor names on `InternalRow` (`getUTF8String`,
`getDecimal`, `getInt`, `getLong`, `getBinary`, `getArray`, `getMap`, `getStruct`) are the
reference for each type. `GenericInternalRow` takes `Object[]`, so a value of the wrong
representation compiles and fails on the executor with a `ClassCastException`.

With this much, `spark.read.format("...JavaSimpleDataSourceV2").load()` returns ten rows, and
the `select` and `filter` in the "simplest implementation" test work too: Spark applies the
projection and the filter itself, above the scan.

### Step 2: Projection and Filter Pushdown (Optional)

A connector that can skip columns or rows adds the pushdown mix-ins to its `ScanBuilder` (the
full list and order are under [ScanBuilder](#scanbuilder)). The contract that matters most is
for filters: the method receives the candidate predicates, keeps the ones the connector will
apply, and returns the **residual**, the predicates Spark must still evaluate after the scan.
`JavaAdvancedDataSourceV2WithV2Filter` accepts only `i > <int literal>` and hands everything
else back (the test's `assertInstanceOf` checks and the mirror-image `unsupported` stream are
elided here):

{% highlight java %}
@Override
public Predicate[] pushPredicates(Predicate[] predicates) {
  Predicate[] supported = Arrays.stream(predicates).filter(f -> {
    if (f.name().equals(">")) {
      FieldReference column = (FieldReference) f.children()[0];
      Literal value = (Literal) f.children()[1];
      return column.describe().equals("i") && value.value() instanceof Integer;
    } else {
      return false;
    }
  }).toArray(Predicate[]::new);
  Predicate[] unsupported = /* the complement of supported */;
  this.predicates = supported;
  return unsupported;
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

Spark evaluates only what `pushPredicates` returns (plus anything it could not translate), so a
predicate that is accepted here and then not applied by the reader is silently dropped from the
query. If the connector can only use a predicate to skip whole files or row groups, report it as
pushed and also return it, so Spark re-checks it row by row. `pushedPredicates()` is what the
query plan reports; the `Batch` in the same class uses the accepted predicate to plan fewer
partitions, and its reader fills only the columns in `requiredSchema`.

The "advanced implementation with V2 Filter" test shows the contract from the outside by
inspecting the `Batch` Spark planned: `df.filter($"i" > 3)` leaves one pushed predicate on `i`
and reads both columns, `df.select($"i").filter($"i" > 6)` keeps the predicate and prunes the
schema to `i`, and `df.select($"j").filter($"j" < -10)` pushes nothing, because the connector
only accepts predicates on `i`, so Spark evaluates that filter itself. To see the same on a
connector of your own, override `Scan.description()`, which renders inside the `BatchScan` node
of `EXPLAIN` after the output columns; the default is the class name.

### Step 3: Batch Writes

A writable table declares `BATCH_WRITE` and implements `SupportsWrite`; it adds `TRUNCATE` and
`SupportsTruncate` on the builder if `SaveMode.Overwrite` should work (the full list is under
[Table](#table) and in
[`TableCapability`](api/java/org/apache/spark/sql/connector/catalog/TableCapability.html)).
Spark checks the capability set during analysis, before calling any builder, so a missing value
fails the query even when the mix-in is implemented. From `JavaSimpleWritableDataSource`:

{% highlight java %}
@Override
public Set<TableCapability> capabilities() {
  return EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE);
}
{% endhighlight %}

The write is split across the driver and the executors:

```
driver:    WriteBuilder.build() -> Write.toBatch() -> BatchWrite.createBatchWriterFactory(info)
executor:  DataWriterFactory.createWriter(partitionId, taskId)
           DataWriter.write(row) per row
           DataWriter.commit() -> WriterCommitMessage       (or abort() on failure)
driver:    BatchWrite.commit(messages[])                     (or abort(messages[]))
```

Each `DataWriter` writes its task's output to a staging location, and `BatchWrite.commit` on the
driver makes all of it visible in one step. The staging location must be reachable from both
sides: executors write there and the driver renames or registers from there, usually on
different machines. `JavaSimpleWritableDataSource` stages under `<path>/_temporary/<queryId>` on
the table's own file system, writes one file per task, and commits by renaming the staged files
into place (error handling elided):

{% highlight java %}
@Override
public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
  Path jobPath = new Path(new Path(path, "_temporary"), jobId);
  Path filePath = new Path(jobPath, String.format("%s-%d-%d", jobId, partitionId, taskId));
  FileSystem fs = filePath.getFileSystem(conf.value());
  return new JavaCSVDataWriter(fs, filePath);
}
{% endhighlight %}

{% highlight java %}
@Override
public void commit(WriterCommitMessage[] messages) {
  Path finalPath = new Path(this.path);
  Path jobPath = new Path(new Path(finalPath, "_temporary"), queryId);
  FileSystem fs = jobPath.getFileSystem(conf);
  try {
    for (FileStatus status : fs.listStatus(jobPath)) {
      Path file = status.getPath();
      Path dest = new Path(finalPath, file.getName());
      if (!fs.rename(file, dest)) {
        throw new IOException(String.format("failed to rename(%s, %s)", file, dest));
      }
    }
  } finally {
    fs.delete(jobPath, true);
  }
}
{% endhighlight %}

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
`SHOW TABLES IN demo.db`), implement [`TableCatalog`](#tablecatalog) and register it by name.
Every configuration key under the catalog's prefix is passed to
`CatalogPlugin.initialize(name, options)` with the prefix removed:

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
