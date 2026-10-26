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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BloomFilterMightContain, BoundReference, Expression, Predicate}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.connector.StorageFilterErrors
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

/**
 * The SQL metrics the reader updates while applying a [[ParquetStorageFilter]], created by
 * `ParquetFileFormat.storageFilterMetrics`. The first five count only what the filter saved against
 * a read of the same projection without one. A row group whose filter was given up counts nothing
 * there, so a scan that read more than with the feature off can still show a saving. Nor do they
 * take off the offset indexes phase 2 reads when no data filter is pushed, which a plain read does
 * not. Bytes are the compressed bytes the reader did not ask parquet for, which a vectored read can
 * still fetch when it merges nearby ranges.
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
 *
 * The other four count row groups the survivor buffer's budget limited, each at most once, under
 * the last thing it gave up and the limit that made it: `maxSplicedRowGroupBytes`, or the task's
 * memory manager denying the reservation.
 *
 *  - [[rowGroupsSplicingGivenUpByCap]] and [[rowGroupsSplicingGivenUpByMemory]] count row groups
 *    that kept the filter but read their key columns a second time.
 *  - [[rowGroupsFilterGivenUpByCap]] and [[rowGroupsFilterGivenUpByMemory]] count row groups read
 *    without the filter.
 */
case class StorageFilterMetrics(
    rowGroupsSkipped: SQLMetric,
    rowsExcludedByRowGroup: SQLMetric,
    rowsExcludedWithinRowGroup: SQLMetric,
    bytesAvoidedByRowGroup: SQLMetric,
    bytesAvoidedByPageFiltering: SQLMetric,
    rowGroupsSplicingGivenUpByCap: SQLMetric,
    rowGroupsSplicingGivenUpByMemory: SQLMetric,
    rowGroupsFilterGivenUpByCap: SQLMetric,
    rowGroupsFilterGivenUpByMemory: SQLMetric) {

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
  val ROW_GROUPS_SPLICING_GIVEN_UP_BY_CAP = "storageFilterRowGroupsSplicingGivenUpByCap"
  val ROW_GROUPS_SPLICING_GIVEN_UP_BY_MEMORY = "storageFilterRowGroupsSplicingGivenUpByMemory"
  val ROW_GROUPS_FILTER_GIVEN_UP_BY_CAP = "storageFilterRowGroupsFilterGivenUpByCap"
  val ROW_GROUPS_FILTER_GIVEN_UP_BY_MEMORY = "storageFilterRowGroupsFilterGivenUpByMemory"

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
      sparkContext, "bytes avoided by storage filter (page filtering)"),
    ROW_GROUPS_SPLICING_GIVEN_UP_BY_CAP -> SQLMetrics.createMetric(
      sparkContext, "row groups with key columns read twice by storage filter (buffer cap)"),
    ROW_GROUPS_SPLICING_GIVEN_UP_BY_MEMORY -> SQLMetrics.createMetric(
      sparkContext, "row groups with key columns read twice by storage filter (memory denied)"),
    ROW_GROUPS_FILTER_GIVEN_UP_BY_CAP -> SQLMetrics.createMetric(
      sparkContext, "row groups read without storage filter (buffer cap)"),
    ROW_GROUPS_FILTER_GIVEN_UP_BY_MEMORY -> SQLMetrics.createMetric(
      sparkContext, "row groups read without storage filter (memory denied)"))

  /** The counters the scan carries back, which [[create]] made. */
  def fromMap(metrics: Map[String, SQLMetric]): StorageFilterMetrics = StorageFilterMetrics(
    rowGroupsSkipped = metrics(ROW_GROUPS_SKIPPED),
    rowsExcludedByRowGroup = metrics(ROWS_EXCLUDED_BY_ROW_GROUP),
    rowsExcludedWithinRowGroup = metrics(ROWS_EXCLUDED_WITHIN_ROW_GROUP),
    bytesAvoidedByRowGroup = metrics(BYTES_AVOIDED_BY_ROW_GROUP),
    bytesAvoidedByPageFiltering = metrics(BYTES_AVOIDED_BY_PAGE_FILTERING),
    rowGroupsSplicingGivenUpByCap = metrics(ROW_GROUPS_SPLICING_GIVEN_UP_BY_CAP),
    rowGroupsSplicingGivenUpByMemory = metrics(ROW_GROUPS_SPLICING_GIVEN_UP_BY_MEMORY),
    rowGroupsFilterGivenUpByCap = metrics(ROW_GROUPS_FILTER_GIVEN_UP_BY_CAP),
    rowGroupsFilterGivenUpByMemory = metrics(ROW_GROUPS_FILTER_GIVEN_UP_BY_MEMORY))
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
    // the deserialized filter instead, so dropping the expression lets a columnar scan's task
    // reclaim them. An interpreted predicate, the fallback, keeps the expression, so its bytes
    // stay. Safe because every task attempt deserializes its own copy, and this is the field's only
    // reader.
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
   * `StorageFilterErrors.rethrowIfMustPropagate`.
   */
  def isSupportedStorageFilter(expr: Expression): Boolean = expr match {
    case bloom: BloomFilterMightContain =>
      bloom.references.forall(a => isSupportedKeyType(a.dataType)) &&
        !bloom.references.exists(a => a.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
    case _ => false
  }

  /**
   * Throws what reserving the survivor buffer's memory raised, in a form `FileScanRDD` cannot take
   * for a corrupt or missing file. The task's memory manager runs other consumers' spills inside
   * the reservation, and can evict cached blocks to disk there, so it can raise anything, an
   * `IOException` included. What `StorageFilterErrors.rethrowIfMustPropagate` rethrows goes out as
   * it does there, and anything else wrapped in a checked [[SparkException]]. Typed as returning a
   * `RuntimeException` so Java code can write `throw memoryReservationFailed(e)`.
   */
  def memoryReservationFailed(e: Throwable): RuntimeException = {
    StorageFilterErrors.rethrowIfMustPropagate(e, fromRead = false)
    throw new SparkException("A storage filter failed to reserve memory", e)
  }

  /**
   * Throws `SparkException.internalError(message)` for a broken invariant of this reader. It is
   * checked, so `FileScanRDD` cannot take it for a corrupt file. Typed as returning a
   * `RuntimeException` so Java code can write `throw internalError(...)`.
   */
  def internalError(message: String): RuntimeException = throw SparkException.internalError(message)
}
