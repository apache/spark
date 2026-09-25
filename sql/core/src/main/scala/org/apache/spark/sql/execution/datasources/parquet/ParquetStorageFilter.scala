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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BloomFilterMightContain, BoundReference, Expression, Literal, Predicate, XxHash64}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, TimeType, YearMonthIntervalType}

/**
 * Optional SQL metrics the reader updates while applying a [[ParquetStorageFilter]]. Every
 * counter is scoped to what the storage filter added on top of a read of the same projection
 * without one. All fields are nullable; a null field disables that metric.
 *
 *  - [[rowGroupsSkipped]] counts row groups whose data columns were never read.
 *  - [[rowsExcludedByRowGroup]] sums the rows those skips excluded, per skipped block the rows that
 *    survived the pushed data filter.
 *  - [[rowsExcludedWithinRowGroup]] sums rows excluded inside row groups that were kept.
 *  - [[bytesAvoidedByRowGroup]] sums, per skipped row group, the non-key bytes a plain read of this
 *    projection would have transferred for the rows that survived the pushed data filter. Phase 1
 *    reads the key columns of every block, so key bytes are never part of it, and it is zero on an
 *    all-keys projection, which can avoid nothing.
 *  - [[bytesAvoidedByPageFiltering]] sums, per kept row group, that same non-key baseline minus the
 *    bytes phase 2 read, which is what `finalRanges` page selection pruned.
 *
 * The row counters' suffix says where a row was excluded, not what would have saved it: a row
 * inside a kept row group is read as part of its page and dropped during decode, so page
 * filtering did not save it. An all-keys projection has no page filtering at all, and
 * [[rowsExcludedWithinRowGroup]] still counts every row the filter dropped.
 */
case class StorageFilterMetrics(
    rowGroupsSkipped: SQLMetric = null,
    rowsExcludedByRowGroup: SQLMetric = null,
    rowsExcludedWithinRowGroup: SQLMetric = null,
    bytesAvoidedByRowGroup: SQLMetric = null,
    bytesAvoidedByPageFiltering: SQLMetric = null)

/**
 * A runtime filter that the vectorized Parquet reader uses to drive late materialization: read
 * key-column pages first, evaluate this filter per row to decide which rows survive, and skip
 * data-column pages that do not overlap any surviving row range.
 *
 * [[keyColumnIndices]] are indices into the scan's requested data schema identifying the leaf
 * columns referenced by the filter. [[boundExpression]] has its references rewritten to
 * [[BoundReference]]s pointing at positions 0..(keyColumnIndices.length - 1); the reader must
 * evaluate it against rows whose fields correspond to those key columns in that order.
 */
class ParquetStorageFilter private (
    val keyColumnIndices: Array[Int],
    val boundExpression: Expression,
    val metrics: StorageFilterMetrics,
    val maxSplicedRowGroupBytes: Long) extends Serializable {

  // Codegen-produced predicates can be awkward to serialize from driver to executor, so we defer
  // construction to first use on the executor.
  @transient private lazy val predicate: BasePredicate = Predicate.create(boundExpression)

  // Rewrites are cached for the task, because `rewriteForMissingKeys` rebuilds the expression with
  // `transform`, and a rebuilt `BloomFilterMightContain` has to deserialize its filter again, up to
  // megabytes per file otherwise. The missing positions are the whole key: the values a caller
  // substitutes for them are the ones the scan's schema defines, so every file of that scan passes
  // the same ones.
  @transient private lazy val rewrites =
    new ConcurrentHashMap[Seq[Int], ParquetStorageFilter]()

  def test(keyRow: InternalRow): Boolean = predicate.eval(keyRow)

  /**
   * Returns a new filter for a physical file that is missing some key columns (schema evolution).
   * The [[BoundReference]]s at `missingKeyLocalPositions`, which are indices into
   * [[keyColumnIndices]], are replaced by `missingKeyValues`, and the remaining references are
   * renumbered onto the reduced key-row layout. [[keyColumnIndices]] keeps the present columns in
   * their original relative order, and SQL metrics are shared with `this`.
   *
   * `missingKeyValues(i)` must be the internal-format value the reader produces for a missing
   * column: its existence DEFAULT when it has one, else null. The result is cached per set of
   * missing positions, so those values have to be a function of the positions, which they are: they
   * come from the scan's schema. `ParquetColumnVector` writes that
   * default into the output vector, so substituting null instead would filter on a value the scan
   * never returns and could drop matching rows.
   *
   * The predicate has to be evaluated against the substitution rather than skipped, because a null
   * key does not always mean `false`: a `Coalesce`-wrapped reference still produces a non-null
   * result, and `XxHash64` is `nullable = false` and hashes a null input to its seed.
   *
   * With every key position missing, the result holds no [[BoundReference]] at all and
   * [[evalAllMissing]] can read off its constant truth value.
   */
  def rewriteForMissingKeys(
      missingKeyLocalPositions: Array[Int],
      missingKeyValues: Array[Any]): ParquetStorageFilter = {
    require(missingKeyLocalPositions.length == missingKeyValues.length,
      "missingKeyLocalPositions and missingKeyValues must have the same length")
    rewrites.computeIfAbsent(missingKeyLocalPositions.toSeq,
      _ => rewrite(missingKeyLocalPositions, missingKeyValues))
  }

  private def rewrite(
      missingKeyLocalPositions: Array[Int],
      missingKeyValues: Array[Any]): ParquetStorageFilter = {
    val substitution = missingKeyLocalPositions.zip(missingKeyValues).toMap
    val presentPositions = keyColumnIndices.indices.filterNot(substitution.contains)
    val newPosOf = presentPositions.zipWithIndex.toMap
    val rewritten = boundExpression.transform {
      case b: BoundReference if substitution.contains(b.ordinal) =>
        Literal(substitution(b.ordinal), b.dataType)
      case b: BoundReference => BoundReference(newPosOf(b.ordinal), b.dataType, b.nullable)
    }
    val newKeyColumnIndices = presentPositions.map(keyColumnIndices(_)).toArray
    new ParquetStorageFilter(newKeyColumnIndices, rewritten, metrics, maxSplicedRowGroupBytes)
  }

  /**
   * When every key column is missing (i.e. [[keyColumnIndices]] is empty after
   * [[rewriteForMissingKeys]]), the bound expression is fully constant. Evaluates it and returns
   * `true` iff the predicate is literally true; a null or false result is interpreted as "drop
   * every row" by the reader.
   */
  def evalAllMissing(): Option[Boolean] = {
    require(keyColumnIndices.isEmpty, "evalAllMissing only valid when all key columns are missing")
    // The substituted constant can be one this expression throws on, the same way a row's value can
    // be, so the same rule applies: fail open. None means the reader must not decide from this
    // filter at all and has to read the file the way a plain scan would.
    try {
      Some(boundExpression.eval(InternalRow.empty) == true)
    } catch {
      case e: RuntimeException if isEvaluationError(e) => None
    }
  }

  /**
   * Whether `e` is an error from evaluating the predicate rather than a defect in the reader. Only
   * the first kind may be swallowed, and the error class is what tells them apart: a value produces
   * one (an invalid cast under ANSI, an overflow, a division by zero), while a null dereference or
   * a failed assertion produces none, and an internal error says so in the class itself. Asked of
   * an instance rather than of the companion, so the Java reader can call it plainly.
   */
  private[parquet] def isEvaluationError(e: RuntimeException): Boolean = e match {
    case t: SparkThrowable => !t.isInternalError
    case _ => false
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
      metrics: StorageFilterMetrics = StorageFilterMetrics(),
      maxSplicedRowGroupBytes: Long = Long.MaxValue): ParquetStorageFilter = {
    require(boundExpressions.nonEmpty,
      "storage filters must be non-empty; callers with nothing to push must not call create")
    val expr = boundExpressions.reduce(And)

    // The requested-schema ordinals this predicate reads, deduplicated (a column referenced twice
    // is still one key column) and sorted.
    //
    // `sorted` is load-bearing. Both `keyColumnIndices` and the remapped references derive from
    // this list, so any order would keep those two consistent, but the reader's emit path does
    // not go through the remapping: it pairs the k-th key slot of the output batch with key-row
    // position k, which is the identity only while this list is ascending.
    val originalOrdinals = expr.collect { case b: BoundReference => b.ordinal }.distinct.sorted
    // These messages name the expression by its node rather than printing it: a prepared bloom
    // holds its filter as a binary literal, which renders as megabytes of hex.
    require(originalOrdinals.nonEmpty,
      s"storage filter ${expr.prettyName} has no bound reference to a key column")
    require(originalOrdinals.forall(i => i >= 0 && i < requestedSchema.length),
      s"storage filter ${expr.prettyName} references ordinals " +
        s"${originalOrdinals.mkString("[", ", ", "]")} outside the ${requestedSchema.length} " +
        s"fields of ${requestedSchema.catalogString}")
    val unsupported = originalOrdinals.map(requestedSchema.fields(_))
      .filterNot(field => isSupportedKeyType(field.dataType))
    require(unsupported.isEmpty,
      "storage filter key columns must have a type the vectorized reader can copy, but " +
        unsupported.map(f => s"${f.name} ${f.dataType.catalogString}").mkString(", ") +
        " do not; see ParquetStorageFilter.isSupportedKeyType")

    val indexMap = originalOrdinals.zipWithIndex.toMap
    val remapped = expr.transform {
      case b: BoundReference => BoundReference(indexMap(b.ordinal), b.dataType, b.nullable)
    }

    new ParquetStorageFilter(originalOrdinals.toArray, remapped, metrics, maxSplicedRowGroupBytes)
  }

  /**
   * Whether `dt` is usable as a storage-filter key column type. This is the single authority on
   * that: [[isSupportedStorageFilter]] consults it for what the planner asks, and [[create]]
   * re-checks it, so the reader's per-type value copier
   * (`VectorizedParquetRecordReader.copierFor`) is only ever asked for a type listed here. Adding
   * a type here without teaching `copierFor` about it turns a planning-time rejection into a task
   * failure.
   *
   * Narrower than `AtomicType`, for two different reasons:
   *  - `VariantType` cannot be supported: its Parquet representation is a group, not a primitive
   *    leaf, so phase 1 has nothing flat to read it into. (It is unreachable anyway, since
   *    `HashExpression.checkInputDataTypes` rejects variant, so no bloom can be built on one.)
   *  - `GeometryType` and `GeographyType` could be supported. Both map to a primitive Parquet
   *    BINARY and both are handled by `WritableColumnVector.isArray`, so the existing byte-array
   *    copier would work. But no bloom can currently reference them: `HashExpression`'s codegen
   *    type dispatch has no case for either, so hashing one fails at codegen. They are left out
   *    until something can actually produce such a filter.
   */
  def isSupportedKeyType(dt: DataType): Boolean = dt match {
    case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType => true
    case _: FloatType | _: DoubleType | _: DecimalType => true
    case _: DateType | _: TimestampType | _: TimestampNTZType | _: TimeType => true
    case _: YearMonthIntervalType | _: DayTimeIntervalType => true
    // StringType also covers CharType and VarcharType, which extend it.
    case _: StringType | _: BinaryType => true
    case _ => false
  }

  /**
   * Whether the reader can evaluate `expr` as a storage filter, which is what
   * `ParquetFileFormat.supportsStorageFilter` answers for the planner.
   */
  def isSupportedStorageFilter(expr: Expression): Boolean = expr match {
    case bloom: BloomFilterMightContain =>
      // The whole conjunct has to be the bloom, not something with a bloom nested under an OR or a
      // NOT: the reader evaluates the expression it is given and treats a false as "drop this row".
      // Every reference is checked, not just the ones on the value side, because [[create]] binds
      // and type-checks all of them.
      bloom.references.forall(a => isSupportedKeyType(a.dataType)) && canEvaluateInTheReader(bloom)
    case _ => false
  }

  /**
   * Whether the reader can evaluate this bloom's value side at all.
   *
   * It does not have to be an expression that is safe to evaluate on every row. The reader
   * evaluates the predicate without the conjuncts that precede it in the plan, so an expression
   * that throws on a row an earlier conjunct would have rejected throws where a plain scan does
   * not. That is
   * handled where it arises rather than here: the reader gives the filter up for the row group and
   * reads it plainly, and the post-scan `Filter` then evaluates every conjunct in its own order.
   *
   * What is left is what the reader cannot evaluate at all. A subquery has no plan to run on an
   * executor, and a non-deterministic expression needs the `initialize(partitionIndex)` that
   * `GeneratePredicate` emits for it and the reader never calls.
   */
  private def canEvaluateInTheReader(bloom: BloomFilterMightContain): Boolean =
    bloom.valueExpression match {
      case hash: XxHash64 =>
        hash.children.forall(c => c.deterministic && !c.containsPattern(PLAN_EXPRESSION))
      case _ => false
    }
}
