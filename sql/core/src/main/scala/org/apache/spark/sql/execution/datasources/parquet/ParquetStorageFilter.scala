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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BloomFilterMightContain, BoundReference, Expression, Predicate}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The SQL metrics the reader updates while applying a [[ParquetStorageFilter]], created by
 * `ParquetFileFormat.storageFilterMetrics`. Every counter is scoped to what the storage filter
 * added on top of a read of the same projection without one.
 *
 *  - [[rowGroupsSkipped]] counts row groups whose data columns were never read.
 *  - [[rowsExcludedByRowGroup]] sums those skipped row groups' rows, counting per block the rows
 *    that survived the pushed data filter.
 *  - [[rowsExcludedWithinRowGroup]] sums rows excluded inside row groups that were kept.
 *  - [[bytesAvoidedByRowGroup]] sums, per skipped row group, the non-key bytes a plain read of this
 *    projection would have transferred for the rows that survived the pushed data filter. Phase 1
 *    reads the key columns of every block, so key bytes are never part of it, and it is zero on an
 *    all-keys projection, which can avoid nothing.
 *  - [[bytesAvoidedByPageFiltering]] sums, per kept row group, that same non-key baseline minus the
 *    bytes phase 2 read, which is what `finalRanges` page selection pruned.
 *
 * The row counters' suffix says where a row was excluded, not whether reading it was avoided. A row
 * excluded inside a kept row group may have been read as part of a page that held a survivor, or
 * not read at all because phase 2 skipped its page. An all-keys projection has no page filtering at
 * all, and [[rowsExcludedWithinRowGroup]] still counts every row the filter dropped.
 */
case class StorageFilterMetrics(
    rowGroupsSkipped: SQLMetric,
    rowsExcludedByRowGroup: SQLMetric,
    rowsExcludedWithinRowGroup: SQLMetric,
    bytesAvoidedByRowGroup: SQLMetric,
    bytesAvoidedByPageFiltering: SQLMetric) {

  /** These counters keyed for the scan, which carries them without naming any of them. */
  def toMap: Map[String, SQLMetric] = Map(
    StorageFilterMetrics.ROW_GROUPS_SKIPPED -> rowGroupsSkipped,
    StorageFilterMetrics.ROWS_EXCLUDED_BY_ROW_GROUP -> rowsExcludedByRowGroup,
    StorageFilterMetrics.ROWS_EXCLUDED_WITHIN_ROW_GROUP -> rowsExcludedWithinRowGroup,
    StorageFilterMetrics.BYTES_AVOIDED_BY_ROW_GROUP -> bytesAvoidedByRowGroup,
    StorageFilterMetrics.BYTES_AVOIDED_BY_PAGE_FILTERING -> bytesAvoidedByPageFiltering)
}

object StorageFilterMetrics {

  // The keys these counters are exposed under, next to the fields they name. What they count is
  // this reader's vocabulary, which is why the format creates them rather than the scan.
  val ROW_GROUPS_SKIPPED = "storageFilterRowGroupsSkipped"
  val ROWS_EXCLUDED_BY_ROW_GROUP = "storageFilterRowsExcludedByRowGroup"
  val ROWS_EXCLUDED_WITHIN_ROW_GROUP = "storageFilterRowsExcludedWithinRowGroup"
  val BYTES_AVOIDED_BY_ROW_GROUP = "storageFilterBytesAvoidedByRowGroup"
  val BYTES_AVOIDED_BY_PAGE_FILTERING = "storageFilterBytesAvoidedByPageFiltering"

  /** A fresh set of counters, with the labels the SQL UI shows. */
  def create(sparkContext: SparkContext): StorageFilterMetrics = StorageFilterMetrics(
    rowGroupsSkipped =
      SQLMetrics.createMetric(sparkContext, "row groups skipped by storage filter"),
    rowsExcludedByRowGroup =
      SQLMetrics.createMetric(sparkContext, "rows excluded by storage filter (whole row group)"),
    rowsExcludedWithinRowGroup =
      SQLMetrics.createMetric(sparkContext, "rows excluded by storage filter (within row group)"),
    bytesAvoidedByRowGroup = SQLMetrics.createSizeMetric(
      sparkContext, "bytes avoided by storage filter (whole row group)"),
    bytesAvoidedByPageFiltering = SQLMetrics.createSizeMetric(
      sparkContext, "bytes avoided by storage filter (page filtering)"))

  /**
   * The counters the scan carries back, which are the ones [[create]] made: the map is this
   * format's own transport, and a scan with storage filters on a Parquet relation has no other.
   */
  def fromMap(metrics: Map[String, SQLMetric]): StorageFilterMetrics = StorageFilterMetrics(
    rowGroupsSkipped = metrics(ROW_GROUPS_SKIPPED),
    rowsExcludedByRowGroup = metrics(ROWS_EXCLUDED_BY_ROW_GROUP),
    rowsExcludedWithinRowGroup = metrics(ROWS_EXCLUDED_WITHIN_ROW_GROUP),
    bytesAvoidedByRowGroup = metrics(BYTES_AVOIDED_BY_ROW_GROUP),
    bytesAvoidedByPageFiltering = metrics(BYTES_AVOIDED_BY_PAGE_FILTERING))
}

/**
 * A runtime filter that the vectorized Parquet reader uses to drive late materialization: read
 * key-column pages first, evaluate this filter per row to decide which rows survive, and skip
 * data-column pages that do not overlap any surviving row range.
 *
 * [[keyColumnIndices]] are indices into the scan's requested data schema identifying the leaf
 * columns referenced by the filter. [[test]] expects a row whose fields are those key columns in
 * that order: the predicate's references are rewritten to [[BoundReference]]s pointing at positions
 * 0..(keyColumnIndices.length - 1) when the filter is built.
 */
class ParquetStorageFilter private (
    val keyColumnIndices: Array[Int],
    private var boundExpression: Expression,
    val metrics: StorageFilterMetrics,
    val maxSplicedRowGroupBytes: Long) extends Serializable {

  // Codegen-produced predicates can be awkward to serialize from driver to executor, so we defer
  // construction to first use on the executor.
  @transient private lazy val predicate: BasePredicate = {
    val created = Predicate.create(boundExpression)
    // A prepared bloom reaches the executor as a binary Literal inside this expression, up to
    // `spark.sql.optimizer.runtime.bloomFilter.maxNumBits` of it, while `created` holds the
    // deserialized filter instead. Dropping the expression lets the task reclaim those bytes for
    // the rest of its life. Safe because every task attempt deserializes its own copy from the
    // driver's bytes, and this lazy val is the only reader of the field.
    //
    // It does mean the filter must only ever be evaluated on an executor. Were the driver's copy
    // to run this, it is the copy serialized to every attempt, and it would go out with nothing
    // left to build a predicate from.
    boundExpression = null
    created
  }

  def test(keyRow: InternalRow): Boolean = predicate.eval(keyRow)
}

object ParquetStorageFilter {

  /**
   * Builds a [[ParquetStorageFilter]] from the given storage-filter expressions, already bound to
   * the scan's requested data schema (i.e. [[BoundReference]]s with ordinals in
   * `[0, requestedSchema.length)`). Multiple filters are combined with logical AND, so a row must
   * satisfy all of them to survive.
   *
   * Every condition below is asserted rather than handled: `storageFiltersFor` pre-checks all
   * of them, so a violation here is a planner bug. A reader giving a filter up at read time is a
   * different matter. These conditions are about the filter being well formed at all.
   *
   * Callers that have no storage filters must not call this at all.
   */
  def create(
      boundExpressions: Seq[Expression],
      requestedSchema: StructType,
      metrics: StorageFilterMetrics,
      maxSplicedRowGroupBytes: Long): ParquetStorageFilter = {
    require(boundExpressions.nonEmpty,
      "storage filters must be non-empty; callers with nothing to push must not call create")
    val expr = boundExpressions.reduce(And)

    // The requested-schema ordinals this predicate reads, deduplicated (a column referenced twice
    // is still one key column) and sorted. Sorting only makes the key row's layout deterministic:
    // nothing depends on the order, because both `keyColumnIndices` and the remapped references
    // derive from this list, and the reader pairs each key column with the slot this list names.
    val originalOrdinals = expr.collect { case b: BoundReference => b.ordinal }.distinct.sorted
    // These messages name the expression by its node rather than printing it: a prepared bloom
    // holds its filter as a binary literal, which renders as megabytes of hex.
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
    require(!keyFields.exists(_.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME),
      "a storage filter must not have a key column named " +
        s"${ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME}: the reader writes row indexes " +
        "over that column, so what it reads back depends on how its row group was read")

    val indexMap = originalOrdinals.zipWithIndex.toMap
    val remapped = expr.transform {
      case b: BoundReference => BoundReference(indexMap(b.ordinal), b.dataType, b.nullable)
    }

    new ParquetStorageFilter(originalOrdinals.toArray, remapped, metrics, maxSplicedRowGroupBytes)
  }

  /**
   * Whether `dt` is usable as a storage-filter key column type, which is whether the reader's
   * per-type value copier handles it. [[isSupportedStorageFilter]] asks this for the planner and
   * [[create]] re-checks it, so `ValueCopier` is only ever asked for a type it has. It is the one
   * list: see `ValueCopier` for which types are in it and why.
   */
  def isSupportedKeyType(dt: DataType): Boolean = ValueCopier.supports(dt)

  /**
   * Whether the reader can evaluate `expr` as a storage filter, which is what
   * `ParquetFileFormat.supportsStorageFilter` answers for the planner. Three conditions:
   *
   *  - the whole conjunct is a `BloomFilterMightContain`, not something with a bloom nested under
   *    an OR or a NOT: the reader evaluates the expression it is given and reads a false as "drop
   *    this row";
   *  - every column it references has a type the reader's value copier can handle. Every reference
   *    is checked, not only the ones under the hash, because [[create]] binds all of them;
   *  - no referenced column is named like the synthetic row-index metadata column. This reader
   *    finds that column by name and writes row indexes over whatever the file holds (SPARK-40059),
   *    so such a column would read back differently depending on how its row group was read: from
   *    the survivor queues where the row group was spliced, from the overwritten vector where it
   *    was not.
   *
   * Nothing here asks whether the expression is safe to evaluate on every row. The reader evaluates
   * the predicate without the conjuncts that precede it in the plan, so one that throws on a row an
   * earlier conjunct would have rejected throws where a plain scan does not. That is handled where
   * it arises: the reader gives the filter up for the row group and reads it plainly, and the
   * post-scan `Filter` then evaluates every conjunct in its own order.
   */
  def isSupportedStorageFilter(expr: Expression): Boolean = expr match {
    case bloom: BloomFilterMightContain =>
      bloom.references.forall(a => isSupportedKeyType(a.dataType)) &&
        !bloom.references.exists(_.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
    case _ => false
  }
}
