/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

/**
 * Capabilities that can be provided by a {@link Table} implementation.
 * <p>
 * Tables use {@link Table#capabilities()} to return a set of capabilities. Each capability signals
 * to Spark that the table supports a feature identified by the capability. For example, returning
 * {@link #BATCH_READ} allows Spark to read from the table using a batch scan.
 *
 * @since 3.0.0
 */
@Evolving
public enum TableCapability {
  /**
   * Signals that the table supports reads in batch execution mode.
   */
  BATCH_READ,

  /**
   * Signals that the table supports reads in micro-batch streaming execution mode.
   */
  MICRO_BATCH_READ,

  /**
   * Signals that the table supports reads in continuous streaming execution mode.
   */
  CONTINUOUS_READ,

  /**
   * Signals that the table supports append writes in batch execution mode.
   * <p>
   * Tables that return this capability must support appending data and may also support additional
   * write modes, like {@link #TRUNCATE}, {@link #OVERWRITE_BY_FILTER}, and
   * {@link #OVERWRITE_DYNAMIC}.
   */
  BATCH_WRITE,

  /**
   * Signals that the table supports append writes in streaming execution mode.
   * <p>
   * Tables that return this capability must support appending data and may also support additional
   * write modes, like {@link #TRUNCATE}, {@link #OVERWRITE_BY_FILTER}, and
   * {@link #OVERWRITE_DYNAMIC}.
   */
  STREAMING_WRITE,

  /**
   * Signals that the table can be truncated in a write operation.
   * <p>
   * Truncating a table removes all existing rows.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsTruncate}.
   */
  TRUNCATE,

  /**
   * Signals that the table can replace existing data that matches a filter with appended data in
   * a write operation.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsOverwriteV2}.
   */
  OVERWRITE_BY_FILTER,

  /**
   * Signals that the table can dynamically replace existing data partitions with appended data in
   * a write operation.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsDynamicOverwrite}.
   */
  OVERWRITE_DYNAMIC,

  /**
   * Signals that the table accepts input of any schema in a write operation.
   */
  ACCEPT_ANY_SCHEMA,

  /**
   * Signals that table supports Spark altering the schema if necessary
   * as part of an operation.
   */
  AUTOMATIC_SCHEMA_EVOLUTION,

  /**
   * Signals that the table supports append writes using the V1 InsertableRelation interface.
   * <p>
   * Tables that return this capability must create a V1Write and may also support additional
   * write modes, like {@link #TRUNCATE}, and {@link #OVERWRITE_BY_FILTER}, but cannot support
   * {@link #OVERWRITE_DYNAMIC}.
   */
  V1_BATCH_WRITE,

  /**
   * Signals that the table wants Spark to auto-fill generated column values and enforce generated
   * column constraints during writes.
   * <p>
   * When this capability is present, Spark will:
   * <ul>
   *   <li>Auto-compute missing generated column values using the generation expression for
   *       by-name writes. Ordinary by-position writes still require the input to provide a
   *       value for every table column.</li>
   *   <li>Validate explicitly-provided generated column values against the generation
   *       expression.</li>
   * </ul>
   * <p>
   * Without this capability, the connector is responsible for handling generated column values
   * during writes.
   *
   * @since 4.3.0
   */
  GENERATE_COLUMN_VALUES_ON_WRITE,

  /**
   * Signals that Spark may fuse two batch scans of this table that differ only in their projected
   * columns and/or pushed filters into a single scan (Spark-side scan merging).
   * <p>
   * By returning this capability a table declares a determinism contract: holding the scan options
   * constant, the rows and columns a scan reads are fully determined by the filters pushed via
   * {@link org.apache.spark.sql.connector.read.SupportsPushDownV2Filters} and the columns pruned
   * via {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns}. Equivalently,
   * obtaining a fresh {@link org.apache.spark.sql.connector.read.ScanBuilder} with the same options
   * and re-applying the same pushed filters and pruned columns yields an equivalent scan.
   * <p>
   * Determinism alone is not enough: widening the set of pruned columns, with the options and
   * pushed filters held constant, must not change which rows the scan returns nor the values it
   * returns for the columns already asked for. It may at most surface a read error. A source whose
   * parser decides what counts as a malformed record from the set of columns it was asked for does
   * not meet this, and neither does one that resolves a column by its position in the projection.
   * <p>
   * Given that contract, Spark builds the merged scan itself: it prunes a fresh ScanBuilder to the
   * union of both read schemas, re-pushes the (possibly OR-widened) filters, and builds. The merged
   * scan reads the union of the two scans' columns and a superset of their rows; each original
   * scan's result is recovered by a projection and filter applied above it. The connector supplies
   * no merge logic of its own.
   * <p>
   * This capability lives on the table rather than on the scan so that a source using the V1 scan
   * fallback (whose scan Spark wraps in an internal wrapper) can still opt in. A table need not
   * reason about pushdowns that are not reproducible this way (a pushed aggregate, join, variant
   * extraction, limit, offset, top-N, or table sample): Spark tracks those on its own side while
   * building the scan and never merges a scan that carries one, whether or not the table returns
   * this capability.
   *
   * @since 4.3.0
   */
  SCAN_MERGING,

  /**
   * Signals that the table has at least one parameterized lookup access path. The table must
   * implement {@link SupportsRead}, its scan builder must implement
   * {@link org.apache.spark.sql.connector.read.SupportsExternalLookup}, and the built scan must
   * implement {@link org.apache.spark.sql.connector.read.Scan#toExternalLookup()}.
   *
   * @since 4.4.0
   */
  EXTERNAL_LOOKUP,

  /**
   * Signals that the scans of the streaming reads of the table may only read the columns that the
   * streaming query reads, which Spark asks a scan builder that implements
   * {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns} for, as it does
   * for the scans of batch reads.
   * <p>
   * Spark builds the scan of a streaming read once, when the query starts, from the plan of the
   * query optimized then, and creates the micro-batch or continuous stream of the read from that
   * scan. By returning this capability, a table declares that the partition readers of such a
   * stream return the columns of {@link org.apache.spark.sql.connector.read.Scan#readSchema()},
   * in its order, as those of the batches of the scan do. Spark asks the scan for its read schema
   * before it creates the stream, whether the scan reads all the columns or not. Spark only prunes
   * the columns of these scans when {@code spark.sql.streaming.v2ColumnPruning.enabled} is true,
   * which it is not by default. Without this capability, the scans of streaming reads read all the
   * columns of the table, in its order.
   *
   * @since 4.4.0
   */
  STREAMING_COLUMN_PRUNING,

  /**
   * Signals that the scans of the streaming reads of the table may get the filters that the
   * streaming query applies to all the rows that it reads from the table, which Spark pushes to a
   * scan builder that implements
   * {@link org.apache.spark.sql.connector.read.SupportsPushDownFilters} or
   * {@link org.apache.spark.sql.connector.read.SupportsPushDownV2Filters}, as it does for the scans
   * of batch reads: the filters that Spark plans with the scan, over the relation of the read, up
   * to the first nondeterministic one, so the filters after a nondeterministic filter or
   * projection, or above another operator, such as a filter on the event-time column above its
   * watermark, are not pushed. Nor are the filters on columns of floating types, of strings of a
   * collation that is not binary, or of structs, arrays or user-defined types with such values:
   * each micro-batch checks that its plan applies the pushed filters, also once its optimizer
   * rewrote them with the values that the plan gives to their columns, which Spark only can for the
   * other columns. The filters of a {@code SupportsPushDownFilters} builder compare columns with
   * values, which cannot fail, so it gets such filters whatever their order among the filters over
   * the relation. A builder whose filters may fail on some rows, as a cast does with ANSI, gets the
   * filters that the query applies before any other filter, in their order, and then those that
   * cannot fail, such as comparisons of columns with values, wherever they are. The streams of its
   * scans must not evaluate a filter on a row that a filter before it drops, so that a filter does
   * not see a row that the query drops with a filter written before it. Spark may still evaluate
   * other conditions before these filters in a micro-batch, such as those that it infers from them,
   * e.g. {@code a > 5} from {@code a = b AND b > 5}, or an equality with a literal that it moves
   * first, so such a condition may drop a row in Spark on which a pushed filter that may fail, such
   * as a division with ANSI, fails in the stream.
   * <p>
   * Spark builds the scan of a streaming read once, when the query starts, from the plan of the
   * query optimized then, and creates the micro-batch or continuous stream of the read from that
   * scan. By returning this capability, a table declares that its scan builders can take filters
   * for such a scan, as for the scan of a batch read. The stream may then drop the rows that fail
   * the filters that the builder took: the query applies all the filters that the builder got to
   * the rows that it reads from the table, and Spark evaluates all the filters of the query after
   * the scan. Its offsets, including those of {@code latestOffset}, must still be positions in all
   * the rows of the table, as without filters: the filters are decided again each time the query
   * starts, so a query that restarts from the offsets of its checkpoint may push other filters, or
   * none. The progress of the query only counts the rows that the stream returns. Spark only
   * pushes the filters to these scans when {@code spark.sql.streaming.v2FilterPushdown.enabled} is
   * true, which it is not by default. Without this capability, the scans of streaming reads get no
   * filter.
   *
   * @since 4.4.0
   */
  STREAMING_FILTER_PUSHDOWN
}
