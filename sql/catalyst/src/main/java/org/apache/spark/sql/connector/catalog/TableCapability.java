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
  SCAN_MERGING
}
