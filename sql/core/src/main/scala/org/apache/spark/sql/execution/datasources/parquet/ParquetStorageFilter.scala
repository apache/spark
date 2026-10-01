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

import org.apache.spark.{SparkContext, SparkEnv, SparkException, TaskContext, TaskKilledException}
import org.apache.spark.executor.Executor
import org.apache.spark.internal.config.KILL_ON_FATAL_ERROR_DEPTH
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BloomFilterMightContain, BoundReference, Expression, Predicate}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

/**
 * The SQL metrics the reader updates while applying a [[ParquetStorageFilter]], created by
 * `ParquetFileFormat.storageFilterMetrics`. Every counter is scoped to what the storage filter
 * saved against a read of the same projection without one. The one cost taken off is a second
 * key read in a row group that gave splicing up, below. A row group whose filter was given up
 * reads its key columns twice and counts nothing, so the counters can show a saving on a scan
 * that read more than it would have with the feature off. Nor do they take off the offset indexes
 * phase 2 reads for a narrowed row group when no data filter is pushed, which a plain read does not
 * read. Bytes are the compressed bytes the reader did not ask parquet for. The file system can
 * still fetch some of them, for example when a vectored read merges nearby ranges.
 *
 *  - [[rowGroupsSkipped]] counts row groups whose non-key columns were never read. Phase 1 read
 *    their key columns, so key bytes are never counted as avoided. In a file that has none of its
 *    key columns, nothing of a skipped row group was read.
 *  - [[rowsExcludedByRowGroup]] sums those row groups' rows that the pushed data filter's column
 *    index kept.
 *  - [[rowsExcludedWithinRowGroup]] sums rows excluded inside row groups that were kept.
 *  - [[bytesAvoidedByRowGroup]] sums, per skipped row group, the non-key bytes a plain read of this
 *    projection would have asked for over those rows.
 *  - [[bytesAvoidedByPageFiltering]] sums, per kept row group, that same non-key baseline minus the
 *    bytes phase 2 read, which is the pages it skipped. A row group that gave splicing up reads its
 *    key columns again in phase 2, which is taken off, down to zero.
 *
 * The row counters' suffix says where a row was excluded, not whether reading it was avoided. A row
 * excluded inside a kept row group may have been read as part of a page that held a survivor, or
 * not read at all because phase 2 skipped its page.
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

  // The keys these counters are exposed under, next to the fields they name. What they count is
  // this reader's vocabulary, which is why the format creates them rather than the scan.
  val ROW_GROUPS_SKIPPED = "storageFilterRowGroupsSkipped"
  val ROWS_EXCLUDED_BY_ROW_GROUP = "storageFilterRowsExcludedByRowGroup"
  val ROWS_EXCLUDED_WITHIN_ROW_GROUP = "storageFilterRowsExcludedWithinRowGroup"
  val BYTES_AVOIDED_BY_ROW_GROUP = "storageFilterBytesAvoidedByRowGroup"
  val BYTES_AVOIDED_BY_PAGE_FILTERING = "storageFilterBytesAvoidedByPageFiltering"

  /**
   * A fresh set of counters, with the labels the SQL UI shows, keyed for the scan, which carries
   * them without naming any of them.
   */
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

  /**
   * The counters the scan carries back, which are the ones [[create]] made. The map is this
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
 * columns referenced by the filter. [[preparedPredicate]] expects a row whose fields are those key
 * columns in that order. The predicate's references are rewritten to [[BoundReference]]s pointing
 * at positions 0..(keyColumnIndices.length - 1) when the filter is built.
 */
class ParquetStorageFilter private (
    val keyColumnIndices: Array[Int],
    private var boundExpression: Expression,
    val metrics: StorageFilterMetrics,
    val maxSplicedRowGroupBytes: Long) extends Serializable {

  /**
   * The predicate this filter evaluates, which the reader resolves once and then runs per row. It
   * is handed out rather than wrapped, because resolving the `lazy val` costs a volatile read that
   * a per-row loop would otherwise pay. Codegen-produced predicates can be awkward to serialize
   * from driver to executor, so it is built on first use on the executor.
   */
  @transient lazy val preparedPredicate: BasePredicate = {
    // The field below is nulled the first time this runs, and the copy that reaches every task is
    // the driver's. So a driver-side evaluation would leave each executor building a predicate from
    // nothing, which is named here rather than left to an NPE from `Predicate.create`.
    if (boundExpression == null) {
      throw SparkException.internalError("a storage filter must only be evaluated on an " +
        "executor, and this copy has released the expression it would build its predicate from")
    }
    val created = Predicate.create(boundExpression)
    // A prepared bloom reaches the executor as a binary Literal inside this expression, up to
    // `spark.sql.optimizer.runtime.bloomFilter.maxNumBits` of it, while `created` holds the
    // deserialized filter instead. Dropping the expression lets a columnar scan's task reclaim
    // those bytes for the rest of its life. A row-based scan keeps them anyway, since its closure
    // captures the scan node, whose storage filters hold the same bytes. Safe because every task
    // attempt deserializes its own copy from the driver's bytes, and this lazy val is the only
    // reader of the field.
    //
    // It does mean the filter must only ever be evaluated on an executor. Were the driver's copy
    // to run this, it is the copy serialized to every attempt, and it would go out with nothing
    // left to build a predicate from.
    boundExpression = null
    created
  }
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
    require(!keyFields.exists(f => f.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME),
      "a storage filter must not have a key column named " +
        s"${ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME}, because the reader writes row " +
        "indexes over that column, so what it reads back depends on how its row group was read")

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
   * list. See `ValueCopier` for which types are in it and why.
   */
  def isSupportedKeyType(dt: DataType): Boolean = ValueCopier.supports(dt)

  /**
   * Whether the reader can evaluate `expr` as a storage filter, which is what
   * `ParquetFileFormat.supportsStorageFilter` answers for the planner. Three conditions:
   *
   *  - the whole conjunct is a `BloomFilterMightContain`, not something with a bloom nested under
   *    an OR or a NOT. The reader evaluates the expression it is given and reads a false as "drop
   *    this row";
   *  - every column it references has a type the reader's value copier can handle. Every reference
   *    is checked, not only the ones under the hash, because [[create]] binds all of them;
   *  - no referenced column is named like the synthetic row-index metadata column. This reader
   *    finds that column by name and writes row indexes over whatever the file holds (SPARK-40059),
   *    so such a column would read back differently depending on how its row group was read, from
   *    the survivor queue where the row group was spliced, from the overwritten vector where it
   *    was not.
   *
   * Nothing here asks whether the expression is safe to evaluate on every row. The reader handles
   * that where it arises, see [[rethrowIfMustPropagate]].
   */
  def isSupportedStorageFilter(expr: Expression): Boolean = expr match {
    case bloom: BloomFilterMightContain =>
      bloom.references.forall(a => isSupportedKeyType(a.dataType)) &&
        !bloom.references.exists(a => a.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
    case _ => false
  }

  /**
   * Rethrows an error the reader met while evaluating a storage filter, if it must not fall back
   * on it by giving the filter up. The checks run in the executor's own order:
   *  - a fatal error goes out as it is, kill or no kill.
   *  - a pending task kill goes out wrapped, see [[throwIfKilled]]. The error may be the kill's
   *    interrupt surfacing through a UDF that wrapped it. `PythonRunner` reads a kill off the task
   *    context in the same way.
   *  - a fatal error deeper in the cause chain, found by `Executor.isFatalError` as deep as the
   *    executor would look had the post-scan Filter raised `e`, which it raises as it is, goes out
   *    as itself, so the executor finds it at the top whatever wraps it on the way.
   *
   * An `InternalError` among those goes out in a checked wrapper. `FileScanRDD` would take a bare
   * one for a corrupt file under `ignoreCorruptFiles`, where a plain plan raises it from the
   * post-scan Filter, outside that catch. The executor finds it under the wrapper and the
   * FAILED_READ_FILE around that at a `spark.executor.killOnFatalError.depth` of 3 or more, the
   * default being 5.
   *
   * Anything else is the filter's own error, which the post-scan Filter raises again for a row it
   * does evaluate, so the reader falls back on it.
   */
  def rethrowIfMustPropagate(e: Throwable): Unit = rethrowIfMustPropagate(e, fromRead = false)

  /**
   * As [[rethrowIfMustPropagate]], for an error decoding the key pages, with three differences:
   *  - a plain read raises that from the reader, where `FileScanRDD` wraps it in FAILED_READ_FILE
   *    before the executor looks, so a fatal error deeper in the chain has to sit one level
   *    shallower to count.
   *  - an `InternalError` is corrupt data, as `DataSourceUtils.shouldIgnoreCorruptFileException`
   *    takes it.
   *  - a key vector that cannot grow (see [[isVectorGrowthFailure]]) is phase 1's own, since
   *    phase 1 decodes into vectors only this reader allocates. Giving the extra work up is the
   *    answer to that.
   * The reader falls back on those, and the plain read of the same pages meets the corrupt data
   * again, or allocates what a plain read allocates.
   */
  def rethrowIfMustPropagateFromRead(e: Throwable): Unit =
    rethrowIfMustPropagate(e, fromRead = true)

  private def rethrowIfMustPropagate(e: Throwable, fromRead: Boolean): Unit = {
    def corrupt(t: Throwable): Boolean = fromRead && t.isInstanceOf[InternalError]
    def out(fatal: Throwable): Throwable = fatal match {
      case internal: InternalError =>
        new SparkException("A storage filter met a fatal error it must not absorb", internal)
      case other => other
    }
    if (Executor.isFatalError(e, 1) && !corrupt(e)) throw out(e)
    throwIfKilled(e)
    if (fromRead && isVectorGrowthFailure(e)) return
    if (Executor.isFatalError(e, if (fromRead) fatalErrorDepth - 1 else fatalErrorDepth)) {
      // The one `Executor.isFatalError` found, which no SparkOutOfMemoryError can precede.
      val fatal = Iterator.iterate(e)(_.getCause).find(Utils.isFatalError).get
      if (!corrupt(fatal)) throw out(fatal)
    }
  }

  /**
   * Whether `e` is how `WritableColumnVector.reserve` reports a vector that cannot grow: a plain
   * `RuntimeException`, caused by an `OutOfMemoryError` or by nothing, for a capacity past what a
   * vector can hold.
   */
  private def isVectorGrowthFailure(e: Throwable): Boolean =
    e.getClass == classOf[RuntimeException] &&
      (e.getCause == null || e.getCause.isInstanceOf[OutOfMemoryError])

  /**
   * Throws a pending task kill, for the reader's loops that can run long without returning to
   * `FileScanRDD`, which is where a plain read meets one.
   */
  def throwIfKilled(): Unit = throwIfKilled(null)

  /**
   * Throws a pending task kill, wrapped in a checked [[SparkException]] around `surfaced`, the
   * error the kill surfaced as, if any. Under `ignoreCorruptFiles`, `FileScanRDD` would log any
   * `RuntimeException` or `IOException` from a reader as a corrupt file and go on to open the
   * split's remaining files, since it checks for a kill only between batches. The executor reports
   * the task as killed either way.
   */
  private def throwIfKilled(surfaced: Throwable): Unit = {
    val context = TaskContext.get()
    if (context != null && context.isInterrupted()) {
      val cause =
        if (surfaced != null) surfaced else new TaskKilledException(context.getKillReason().get)
      throw new SparkException("A storage filter met a task kill it must not absorb", cause)
    }
  }

  /**
   * Throws `SparkException.internalError(message, cause)`, for a broken invariant of this reader.
   * The exception is checked, so `FileScanRDD` cannot read it as a corrupt file under
   * `ignoreCorruptFiles` and skip the rest of the file silently. Typed as returning a
   * `RuntimeException` only so that Java code can write `throw internalError(...)` without
   * declaring a checked exception. It never returns.
   */
  def internalError(message: String, cause: Throwable): RuntimeException =
    throw SparkException.internalError(message, cause)

  /** [[internalError]] with no cause. */
  def internalError(message: String): RuntimeException = internalError(message, null)

  // Read the way `SparkUncaughtExceptionHandler` reads it, since there may be no SparkEnv.
  private def fatalErrorDepth: Int = Option(SparkEnv.get).map(_.conf.get(KILL_ON_FATAL_ERROR_DEPTH))
    .getOrElse(KILL_ON_FATAL_ERROR_DEPTH.defaultValue.get)
}
