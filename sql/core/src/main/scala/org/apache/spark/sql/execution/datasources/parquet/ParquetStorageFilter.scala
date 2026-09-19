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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BasePredicate, BoundReference, Expression, Literal, Predicate}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, TimeType, YearMonthIntervalType}

/**
 * Optional SQL metrics the reader updates while applying a [[ParquetStorageFilter]]. All counters
 * are scoped to what the storage filter added on top of a no-storage-filter read of the same
 * projection. All fields are nullable; a null field disables that metric.
 *
 * Each counter names its own quantity, so the three verbs are deliberate. A row group is *skipped*,
 * meaning its data columns were never read, though phase 1 did read its key columns. A row is
 * *excluded*, meaning it never reached the output. A byte is *avoided*, meaning it was never
 * transferred.
 *
 * The row counters' suffix says *where* the row was excluded, not by which mechanism: a row inside
 * a kept row group is read as part of its page and dropped during decode, so page filtering did not
 * save it. On an all-keys projection there is no page filtering at all, and
 * [[rowsExcludedWithinRowGroup]] still counts every row the filter dropped.
 *
 *  - [[rowGroupsSkipped]] counts row groups whose data columns were never read.
 *  - [[rowsExcludedByRowGroup]] sums rows excluded by full row-group skips (per skipped block, the
 *    count of rows that survived the pushed data filter).
 *  - [[rowsExcludedWithinRowGroup]] sums rows excluded inside row groups that were kept.
 *  - [[bytesAvoidedByRowGroup]] sums `baseline - phase1` for skipped row groups (phase 1 still
 *    reads the key column on every block, so the savings are the non-key bytes the no-filter path
 *    would have read; zero on all-keys-projection scans).
 *  - [[bytesAvoidedByPageFiltering]] sums `baseline - phase1 - phase2` for kept row groups: the
 *    non-key bytes pruned by `finalRanges` page selection beyond what phase 1 already read.
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
 *
 * [[metrics]] carries the optional SQL metrics the reader updates as it applies the filter (always
 * non-negative under splicing: phase 1 reads only the key columns, phase 2 reads only non-key
 * columns under surviving row ranges, so the avoided bytes are exactly the non-key bytes the
 * no-filter path would have read but we didn't).
 */
class ParquetStorageFilter private (
    val keyColumnIndices: Array[Int],
    val boundExpression: Expression,
    val metrics: StorageFilterMetrics) extends Serializable {

  // Codegen-produced predicates can be awkward to serialize from driver to executor, so we defer
  // construction to first use on the executor.
  @transient private lazy val predicate: BasePredicate = Predicate.create(boundExpression)

  def test(keyRow: InternalRow): Boolean = predicate.eval(keyRow)

  /**
   * Returns a new filter with the [[BoundReference]]s at `missingKeyLocalPositions` (positions in
   * the local key-row layout, i.e. indices into [[keyColumnIndices]]) replaced by the constant the
   * reader will actually materialize for that column, and the remaining [[BoundReference]]s
   * renumbered to index into the reduced key-row layout. [[keyColumnIndices]] on the returned
   * filter contains only the present columns in their original relative order. SQL metrics are
   * shared with `this`.
   *
   * Used when a key column is missing from the physical parquet file (schema evolution). The
   * predicate has to be evaluated against the substituted constant rather than skipped, because a
   * null does not always mean `false` in a filter -- a `Coalesce`-wrapped reference still produces
   * a non-null result, and `XxHash64` is `nullable = false` and hashes a null input to its seed.
   *
   * `missingKeyValues(i)` is the internal-format value the reader produces for
   * `missingKeyLocalPositions(i)`: the column's existence DEFAULT when it has one, else null.
   * Passing the default matters for correctness -- `ParquetColumnVector` writes the existence
   * default into the output vector for a missing column, so evaluating the predicate against null
   * would filter on a value the scan never returns and could drop rows that match.
   *
   * If all key positions are missing, the returned filter's [[boundExpression]] contains no
   * [[BoundReference]]s and can be evaluated against [[InternalRow.empty]] to obtain a constant
   * truth value (see [[evalAllMissing]]).
   */
  def rewriteForMissingKeys(
      missingKeyLocalPositions: Array[Int],
      missingKeyValues: Array[Any]): ParquetStorageFilter = {
    require(missingKeyLocalPositions.length == missingKeyValues.length,
      "missingKeyLocalPositions and missingKeyValues must have the same length")
    val substitution = missingKeyLocalPositions.zip(missingKeyValues).toMap
    val presentPositions = keyColumnIndices.indices.filterNot(substitution.contains)
    val newPosOf = presentPositions.zipWithIndex.toMap
    val rewritten = boundExpression.transform {
      case b: BoundReference if substitution.contains(b.ordinal) =>
        Literal(substitution(b.ordinal), b.dataType)
      case b: BoundReference => BoundReference(newPosOf(b.ordinal), b.dataType, b.nullable)
    }
    val newKeyColumnIndices = presentPositions.map(keyColumnIndices(_)).toArray
    new ParquetStorageFilter(newKeyColumnIndices, rewritten, metrics)
  }

  /**
   * When every key column is missing (i.e. [[keyColumnIndices]] is empty after
   * [[rewriteForMissingKeys]]), the bound expression is fully constant. Evaluates it and returns
   * `true` iff the predicate is literally true; a null or false result is interpreted as "drop
   * every row" by the reader.
   */
  def evalAllMissing(): Boolean = {
    require(keyColumnIndices.isEmpty, "evalAllMissing only valid when all key columns are missing")
    boundExpression.eval(InternalRow.empty) == true
  }
}

object ParquetStorageFilter {

  /**
   * Builds a [[ParquetStorageFilter]] from the given storage-filter expressions, already bound to
   * the scan's requested data schema (i.e. [[BoundReference]]s with ordinals in
   * `[0, requestedSchema.length)`). Multiple filters are combined with logical AND, so a row must
   * satisfy all of them to survive.
   *
   * Every condition below is a hard precondition rather than a soft rejection. By the time this is
   * called, `FileSourceStrategy.extractStorageFilters` has removed these conjuncts from the
   * post-scan `Filter`, so nothing left in the plan would apply them; returning a "no filter"
   * result would silently return rows the filter rejects. `extractStorageFilters` pre-checks all of
   * it, so a violation here is a planner bug and failing is the only safe response.
   *
   * Callers that have no storage filters must not call this at all.
   */
  def create(
      boundExpressions: Seq[Expression],
      requestedSchema: StructType,
      metrics: StorageFilterMetrics = StorageFilterMetrics()): ParquetStorageFilter = {
    require(boundExpressions.nonEmpty,
      "storage filters must be non-empty; callers with nothing to push must not call create")
    val expr = boundExpressions.reduce(And)

    // The requested-schema ordinals this predicate reads, deduplicated (a column referenced twice
    // is still one key column) and sorted.
    //
    // `sorted` is load-bearing, not cosmetic. It is not needed to keep `keyColumnIndices` and the
    // remapped BoundReferences consistent with each other -- both derive from this list, so any
    // order would agree. It is needed by a third consumer that does NOT go through the remapping:
    // `VectorizedParquetRecordReader.nextBatchSplicing` walks the output batch slots in ascending
    // order and pulls the survivor queues in key-row-position order, so it pairs "the k-th smallest
    // key slot" with "key-row position k". That pairing is the identity only while this list is
    // ascending. It cannot recover the order itself, because the reader records which slots are
    // keys in a boolean array (`isKeyTopLevel`) that does not preserve their position here.
    val originalOrdinals = expr.collect { case b: BoundReference => b.ordinal }.distinct.sorted
    require(originalOrdinals.nonEmpty,
      s"storage filter $expr has no bound reference to a key column")
    require(originalOrdinals.forall(i => i >= 0 && i < requestedSchema.length),
      s"storage filter $expr references ordinals ${originalOrdinals.mkString("[", ", ", "]")} " +
        s"outside the ${requestedSchema.length} fields of ${requestedSchema.catalogString}")
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

    new ParquetStorageFilter(originalOrdinals.toArray, remapped, metrics)
  }

  /**
   * Whether `dt` is usable as a storage-filter key column type.
   *
   * This is the single authority on key-type eligibility:
   * `FileSourceStrategy.extractStorageFilters` consults it at planning time and [[create]]
   * re-checks it, so the vectorized reader's per-type value copier
   * (`VectorizedParquetRecordReader.copierFor`) is only ever asked for a type listed here. The two
   * must stay in lockstep -- adding a type here without teaching `copierFor` about it turns a
   * planning-time rejection into a task failure.
   *
   * Narrower than `AtomicType`, for two different reasons:
   *  - `VariantType` cannot be supported: its Parquet representation is a group, not a primitive
   *    leaf, so phase 1 has nothing flat to read it into. (It is unreachable anyway --
   *    `HashExpression.checkInputDataTypes` rejects variant, so no bloom can be built on one.)
   *  - `GeometryType` and `GeographyType` could be supported -- both map to a primitive Parquet
   *    BINARY and both are handled by `WritableColumnVector.isArray`, so the existing byte-array
   *    copier would work -- but no bloom can currently reference them: `HashExpression`'s codegen
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
}
