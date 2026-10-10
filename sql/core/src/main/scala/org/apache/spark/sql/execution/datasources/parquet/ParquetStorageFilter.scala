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

package org.apache.spark.sql.execution.datasources.parquet

import scala.util.control.NonFatal

import org.apache.spark.{SparkContext, SparkEnv, SparkException, TaskContext, TaskKilledException}
import org.apache.spark.executor.Executor
import org.apache.spark.internal.config.KILL_ON_FATAL_ERROR_DEPTH
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BloomFilterMightContain, BoundReference, Expression, Predicate}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.util.Utils

/**
 * The SQL metrics the reader updates while applying a [[ParquetStorageFilter]], created by
 * `ParquetFileFormat.storageFilterMetrics`. They count only what the filter saved against a read of
 * the same projection without one. A row group whose filter was given up counts nothing, so a scan
 * that read more than with the feature off can still show a saving. Bytes are the compressed bytes
 * the reader did not ask parquet for, which a vectored read can still fetch when it merges nearby
 * ranges.
 *
 *  - [[rowGroupsSkipped]] counts row groups whose non-key columns were never read.
 *  - [[rowsExcludedByRowGroup]] sums those row groups' rows that the pushed data filter kept.
 *  - [[rowsExcludedWithinRowGroup]] sums rows excluded inside row groups that were kept.
 *  - [[bytesAvoidedByRowGroup]] sums, per skipped row group, the non-key bytes a plain read of this
 *    projection would have asked for.
 *  - [[bytesAvoidedByPageFiltering]] sums, per kept row group, that same baseline minus the bytes
 *    phase 2 read, a second key read included, down to zero.
 *
 * A row excluded within a row group may still have been read, as part of a page that held a
 * survivor.
 */
case class StorageFilterMetrics(
    rowGroupsSkipped: SQLMetric,
    rowsExcludedByRowGroup: SQLMetric,
    rowsExcludedWithinRowGroup: SQLMetric,
    bytesAvoidedByRowGroup: SQLMetric,
    bytesAvoidedByPageFiltering: SQLMetric) {

  /** Counts a row group whose non-key columns the filter kept the reader from reading at all. */
  def recordRowGroupSkipped(excludedRows: Long, avoidedBytes: Long): Unit = {
    rowGroupsSkipped.add(1L)
    rowsExcludedByRowGroup.add(excludedRows)
    bytesAvoidedByRowGroup.add(avoidedBytes)
  }
}

object StorageFilterMetrics {

  // The keys these counters are exposed under.
  val ROW_GROUPS_SKIPPED = "storageFilterRowGroupsSkipped"
  val ROWS_EXCLUDED_BY_ROW_GROUP = "storageFilterRowsExcludedByRowGroup"
  val ROWS_EXCLUDED_WITHIN_ROW_GROUP = "storageFilterRowsExcludedWithinRowGroup"
  val BYTES_AVOIDED_BY_ROW_GROUP = "storageFilterBytesAvoidedByRowGroup"
  val BYTES_AVOIDED_BY_PAGE_FILTERING = "storageFilterBytesAvoidedByPageFiltering"

  /** A fresh set of counters, keyed for the scan, with the labels the SQL UI shows. */
  def create(sparkContext: SparkContext): Map[String, SQLMetric] = Map(
    ROW_GROUPS_SKIPPED ->
      SQLMetrics.createMetric(sparkContext, "row groups skipped by storage filter"),
    ROWS_EXCLUDED_BY_ROW_GROUP ->
      SQLMetrics.createMetric(sparkContext, "rows excluded by storage filter (whole row group)"),
    ROWS_EXCLUDED_WITHIN_ROW_GROUP ->
      SQLMetrics.createMetric(sparkContext, "rows excluded by storage filter (within row group)"),
    BYTES_AVOIDED_BY_ROW_GROUP -> SQLMetrics.createSizeMetric(
      sparkContext, "bytes avoided by storage filter (whole row group)"),
    BYTES_AVOIDED_BY_PAGE_FILTERING -> SQLMetrics.createSizeMetric(
      sparkContext, "bytes avoided by storage filter (page filtering)"))

  /** The counters the scan carries back, which [[create]] made. */
  def fromMap(metrics: Map[String, SQLMetric]): StorageFilterMetrics = StorageFilterMetrics(
    rowGroupsSkipped = metrics(ROW_GROUPS_SKIPPED),
    rowsExcludedByRowGroup = metrics(ROWS_EXCLUDED_BY_ROW_GROUP),
    rowsExcludedWithinRowGroup = metrics(ROWS_EXCLUDED_WITHIN_ROW_GROUP),
    bytesAvoidedByRowGroup = metrics(BYTES_AVOIDED_BY_ROW_GROUP),
    bytesAvoidedByPageFiltering = metrics(BYTES_AVOIDED_BY_PAGE_FILTERING))
}

/**
 * A runtime filter the vectorized Parquet reader applies while it reads, see
 * `LateMaterializationParquetRecordReader`. [[keyColumnIndices]] are the requested data schema's
 * indices of the columns the filter reads, and [[preparedPredicate]] evaluates a row of those
 * columns in that order.
 */
class ParquetStorageFilter private (
    val keyColumnIndices: Array[Int],
    private var boundExpression: Expression,
    val metrics: StorageFilterMetrics,
    val maxSplicedRowGroupBytes: Long) extends Serializable {

  /**
   * The predicate this filter evaluates, built on first use on the executor, since a generated one
   * can be awkward to serialize.
   */
  @transient lazy val preparedPredicate: BasePredicate = {
    // The expression is released below, so a driver-side evaluation would ship a copy with nothing
    // to build from. That is named here rather than left to an NPE.
    if (boundExpression == null) {
      throw SparkException.internalError("a storage filter must only be evaluated on an " +
        "executor, and this copy has released the expression it would build its predicate from")
    }
    val created = Predicate.create(boundExpression)
    // A bloom's bytes sit in the expression as a binary literal, while a generated predicate holds
    // the deserialized filter instead, so dropping the expression lets the task reclaim them. Safe
    // because every task attempt deserializes its own copy, and this is the field's only reader.
    boundExpression = null
    created
  }
}

object ParquetStorageFilter {

  /**
   * Builds a [[ParquetStorageFilter]] from storage filters bound to the scan's requested data
   * schema, combined with AND. Every condition below is asserted, since `storageFiltersFor`
   * pre-checks them and a violation is a planner bug.
   */
  def create(
      boundExpressions: Seq[Expression],
      requestedSchema: StructType,
      metrics: StorageFilterMetrics,
      maxSplicedRowGroupBytes: Long): ParquetStorageFilter = {
    require(boundExpressions.nonEmpty,
      "storage filters must be non-empty; callers with nothing to push must not call create")
    val expr = boundExpressions.reduce(And)

    // The ordinals this predicate reads, deduplicated and sorted. The order only makes the key
    // row's layout deterministic.
    val originalOrdinals = expr.collect { case b: BoundReference => b.ordinal }.distinct.sorted
    // These messages name the expression by its node, since a bloom's literal renders as
    // megabytes of hex.
    require(originalOrdinals.nonEmpty,
      s"storage filter ${expr.prettyName} has no bound reference to a key column")
    require(originalOrdinals.forall(i => i >= 0 && i < requestedSchema.length),
      s"storage filter ${expr.prettyName} references ordinals " +
        s"${originalOrdinals.mkString("[", ", ", "]")} outside the ${requestedSchema.length} " +
        s"fields of ${requestedSchema.catalogString}")
    val keyFields = originalOrdinals.map(requestedSchema.fields(_))
    val unsupported = keyFields.filterNot(field => isSupportedKeyType(field.dataType))
    require(unsupported.isEmpty,
      "storage filter key columns must have a type the vectorized reader can copy, but " +
        unsupported.map(f => s"${f.name} ${f.dataType.catalogString}").mkString(", ") +
        " do not; see ParquetStorageFilter.isSupportedKeyType")
    require(!keyFields.exists(f => f.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME),
      "a storage filter must not have a key column named " +
        s"${ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME}, because the reader writes row " +
        "indexes over that column, so what it reads back depends on how its row group was read")
    require(!keyFields.exists(f => f.name == FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME),
      "a storage filter must not have a key column named " +
        s"${FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME}, because the reader writes the rows " +
        "it checked over that column")

    val indexMap = originalOrdinals.zipWithIndex.toMap
    val remapped = expr.transform {
      case b: BoundReference => BoundReference(indexMap(b.ordinal), b.dataType, b.nullable)
    }

    new ParquetStorageFilter(originalOrdinals.toArray, remapped, metrics, maxSplicedRowGroupBytes)
  }

  /**
   * The index of the checked column in `schema`, or -1 when the scan has none, see
   * `FileFormat.buildReaderWithStorageFilters`. The reader writes booleans into it, so another type
   * is a planner bug.
   */
  def checkedColumnIndex(schema: StructType): Int = {
    val index = schema.fieldNames.indexOf(FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME)
    if (index >= 0 && schema.fields(index).dataType != BooleanType) {
      internalError(s"${FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME} must be a boolean " +
        s"column, but it is ${schema.fields(index).dataType.catalogString}")
    }
    index
  }

  /**
   * Whether `dt` can be a key column type, which is whether `ValueCopier` can copy it. The planner
   * and [[create]] both ask this, so `ValueCopier` is only asked for a type it has.
   */
  def isSupportedKeyType(dt: DataType): Boolean = ValueCopier.supports(dt)

  /**
   * Whether the reader can evaluate `expr` as a storage filter, which is what
   * `ParquetFileFormat.supportsStorageFilter` answers for the planner:
   *  - the whole conjunct is a `BloomFilterMightContain`, since the reader drops a row on false;
   *  - every column it references has a supported key type, since [[create]] binds all of them;
   *  - no referenced column is named like the row-index metadata column, which the reader writes
   *    row indexes over (SPARK-40059).
   *
   * Whether it is safe to evaluate on every row is handled where it arises, see
   * [[rethrowIfMustPropagate]].
   */
  def isSupportedStorageFilter(expr: Expression): Boolean = expr match {
    case bloom: BloomFilterMightContain =>
      bloom.references.forall(a => isSupportedKeyType(a.dataType)) &&
        !bloom.references.exists(a => a.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
    case _ => false
  }

  /**
   * Rethrows an error the reader met while applying a storage filter, unless it may give the filter
   * up on it. `fromRead` says the error came from decoding the key pages. In the executor's order:
   *  - a fatal error goes out as it is;
   *  - a pending task kill goes out wrapped, see [[throwIfKilled]], since the error may be the
   *    kill's interrupt surfacing through a UDF;
   *  - a fatal error deeper in the cause chain, as deep as the executor looks, goes out as itself.
   *
   * An `InternalError` among those goes out in an `UnknownError`, another fatal error. A bare one
   * would be taken for a corrupt file under `ignoreCorruptFiles`, and a `NonFatal` wrapper would
   * let the executor run on under a pending kill.
   *
   * An error from decoding differs in three ways:
   *  - `FileScanRDD` wraps a `NonFatal` one before the executor looks, so a fatal cause counts one
   *    level shallower;
   *  - an `InternalError` is corrupt data, as `DataSourceUtils.shouldIgnoreCorruptFileException`
   *    takes it;
   *  - a key vector that cannot grow is phase 1's own, so giving the extra work up answers it.
   *
   * Anything else falls back. The post-scan Filter raises an evaluation error again for a row it
   * evaluates, and the plain read of the same pages meets a decoding error again.
   */
  def rethrowIfMustPropagate(e: Throwable, fromRead: Boolean): Unit = {
    def corrupt(t: Throwable): Boolean = fromRead && t.isInstanceOf[InternalError]
    def out(fatal: Throwable): Throwable = fatal match {
      case internal: InternalError =>
        new UnknownError("A storage filter met a fatal error it must not absorb")
          .initCause(internal)
      case other => other
    }
    if (Executor.isFatalError(e, 1) && !corrupt(e)) throw out(e)
    throwIfKilled(e)
    if (fromRead && isVectorGrowthFailure(e)) return
    val depth = if (fromRead && NonFatal(e)) fatalErrorDepth - 1 else fatalErrorDepth
    if (Executor.isFatalError(e, depth)) {
      // The one `Executor.isFatalError` found, which no SparkOutOfMemoryError can precede.
      val fatal = Iterator.iterate(e)(_.getCause).find(Utils.isFatalError).get
      if (!corrupt(fatal)) throw out(fatal)
    }
  }

  /**
   * Whether `e` is how `WritableColumnVector.reserve` reports a vector that ran out of memory to
   * grow. The one without a cause, for a capacity too large, has no fatal error to find, so the
   * reader falls back on it anyway.
   */
  private def isVectorGrowthFailure(e: Throwable): Boolean =
    e.getClass == classOf[RuntimeException] && e.getCause.isInstanceOf[OutOfMemoryError]

  /**
   * Throws a pending task kill, for the reader's loops that can run long without returning to
   * `FileScanRDD`, which is where a plain read meets one.
   */
  def throwIfKilled(): Unit = throwIfKilled(null)

  /**
   * Throws a pending task kill, wrapped in a checked [[SparkException]] around `surfaced`, the
   * error the kill surfaced as, if any. Checked, since under `ignoreCorruptFiles` `FileScanRDD`
   * would take a `RuntimeException` or `IOException` for a corrupt file and go on to the split's
   * remaining files. A kill that surfaces as an `IOException` from parquet's own reads is not
   * covered, as for a plain read.
   *
   * The executor reports a task as killed only for an `InterruptedException` or a `NonFatal`
   * error, so a `surfaced` error of any other type goes out as it is, the way a plain read raises
   * it.
   */
  private def throwIfKilled(surfaced: Throwable): Unit = {
    val context = TaskContext.get()
    if (context != null && context.isInterrupted()) {
      val cause = surfaced match {
        case null => new TaskKilledException(context.getKillReason().get)
        case _: InterruptedException | NonFatal(_) => surfaced
        case other => throw other
      }
      throw new SparkException("A storage filter met a task kill it must not absorb", cause)
    }
  }

  /**
   * Throws `SparkException.internalError(message)` for a broken invariant of this reader. It is
   * checked, so `FileScanRDD` cannot take it for a corrupt file. Typed as returning a
   * `RuntimeException` so Java code can write `throw internalError(...)`.
   */
  def internalError(message: String): RuntimeException = throw SparkException.internalError(message)

  // Read the way `SparkUncaughtExceptionHandler` reads it, since there may be no SparkEnv.
  private def fatalErrorDepth: Int = Option(SparkEnv.get).map(_.conf.get(KILL_ON_FATAL_ERROR_DEPTH))
    .getOrElse(KILL_ON_FATAL_ERROR_DEPTH.defaultValue.get)
}
