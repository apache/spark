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

package org.apache.spark.sql.columnar

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BindReferences, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, Length, LessThan, LessThanOrEqual, Literal, Or, Predicate, StartsWith}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.{ColumnStatisticsSchema, PartitionStatistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, BinaryType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Basic interface that all cached batches of data must support. This is primarily to allow
 * for metrics to be handled outside of the encoding and decoding steps in a standard way.
 */
@DeveloperApi
@Since("3.1.0")
trait CachedBatch {
  def numRows: Int
  def sizeInBytes: Long
}

/**
 * Provides APIs for compressing, filtering, and decompressing SQL data that will be
 * persisted/cached.
 */
@DeveloperApi
@Since("3.1.0")
trait CachedBatchSerializer extends Serializable {
  /**
   * Run the given plan and convert its output to a implementation of [[CachedBatch]].
   * @param cachedPlan the plan to run.
   * @return the RDD containing the batches of data to cache.
   */
  def convertForCache(cachedPlan: SparkPlan): RDD[CachedBatch]

  /**
   * Builds a function that can be used to filter batches prior to being decompressed.
   * In most cases extending [[SimpleMetricsCachedBatchSerializer]] will provide the filter logic
   * necessary. You will need to provide metrics for this to work. [[SimpleMetricsCachedBatch]]
   * provides the APIs to hold those metrics and explains the metrics used, really just min and max.
   * Note that this is intended to skip batches that are not needed, and the actual filtering of
   * individual rows is handled later.
   * @param predicates the set of expressions to use for filtering.
   * @param cachedAttributes the schema/attributes of the data that is cached. This can be helpful
   *                         if you don't store it with the data.
   * @return a function that takes the partition id and the iterator of batches in the partition.
   *         It returns an iterator of batches that should be decompressed.
   */
  def buildFilter(predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch]

  /**
   * Can `decompressColumnar()` be called instead of `decompressToRows()` for this given
   * schema? True if it can and false if it cannot. Columnar output is typically preferred
   * because it is more efficient. Note that `decompressToRows()` must always be supported
   * as there are other checks that can force row based output.
   * @param schema the schema of the data being checked.
   * @return true if columnar output should be used for this schema, else false.
   */
  def supportsColumnar(schema: StructType): Boolean

  /**
   * Decompress the cached data into a ColumnarBatch. This currently is only used if
   * `supportsColumnar()`` returned true for the associated schema, but there are other checks
   * that can force row based output. One of the main advantages of doing columnar output over row
   * based output is the code generation is more standard and can be combined with code generation
   * for downstream operations.
   * @param input the cached batches that should be decompressed.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data, and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  def decompressColumnar(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch]

  /**
   * Decompress the cached batch into `InternalRow`. If you want this to be performant, code
   * generation is advised.
   * @param input the cached batches that should be decompressed.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data, and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return RDD of the rows that were stored in the cached batches.
   */
  def decompressToRows(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow]
}

/**
 * A [[CachedBatch]] that stores some simple metrics that can be used for filtering of batches with
 * the [[SimpleMetricsCachedBatchSerializer]].
 * The metrics are returned by the stats value. For each column in the batch 5 columns of metadata
 * are needed in the row.
 */
@DeveloperApi
@Since("3.1.0")
trait SimpleMetricsCachedBatch extends CachedBatch {
  /**
   * Holds the same as ColumnStats.
   * upperBound (optional), lowerBound (Optional), nullCount: Int, rowCount: Int, sizeInBytes: Long
   * Which is repeated for each column in the original data.
   */
  val stats: InternalRow
  override def sizeInBytes: Long =
    Range.apply(4, stats.numFields, 5).map(stats.getLong).sum
}

// Currently, only use statistics from atomic types except binary type only.
private object ExtractableLiteral {
  def unapply(expr: Expression): Option[Literal] = expr match {
    case lit: Literal => lit.dataType match {
      case BinaryType => None
      case _: AtomicType => Some(lit)
      case _ => None
    }
    case _ => None
  }
}

/**
 * Provides basic filtering for [[CachedBatchSerializer]] implementations.
 * The requirement to extend this is that all of the batches produced by your serializer are
 * instances of [[SimpleMetricsCachedBatch]].
 * This does not calculate the metrics needed to be stored in the batches. That is up to each
 * implementation. The metrics required are really just min and max values and those are optional
 * especially for complex types. Because those metrics are simple and it is likely that compression
 * will also be done on the data we thought it best to let each implementation decide on the most
 * efficient way to calculate the metrics, possibly combining them with compression passes that
 * might also be done across the data.
 */
@DeveloperApi
@Since("3.1.0")
abstract class SimpleMetricsCachedBatchSerializer extends CachedBatchSerializer with Logging {
  override def buildFilter(predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val stats = new PartitionStatistics(cachedAttributes)
    val statsSchema = stats.schema

    def statsFor(a: Attribute): ColumnStatisticsSchema = {
      stats.forAttribute(a)
    }

    // Returned filter predicate should return false iff it is impossible for the input expression
    // to evaluate to `true` based on statistics collected about this partition batch.
    @transient lazy val buildFilter: PartialFunction[Expression, Expression] = {
      case And(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
        (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

      case Or(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
        buildFilter(lhs) || buildFilter(rhs)

      case EqualTo(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualTo(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case EqualNullSafe(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualNullSafe(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case LessThan(a: AttributeReference, ExtractableLiteral(l)) => statsFor(a).lowerBound < l
      case LessThan(ExtractableLiteral(l), a: AttributeReference) => l < statsFor(a).upperBound

      case LessThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l
      case LessThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
        l <= statsFor(a).upperBound

      case GreaterThan(a: AttributeReference, ExtractableLiteral(l)) => l < statsFor(a).upperBound
      case GreaterThan(ExtractableLiteral(l), a: AttributeReference) => statsFor(a).lowerBound < l

      case GreaterThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
        l <= statsFor(a).upperBound
      case GreaterThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l

      case IsNull(a: Attribute) => statsFor(a).nullCount > 0
      case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0

      case In(a: AttributeReference, list: Seq[Expression])
        if list.forall(ExtractableLiteral.unapply(_).isDefined) && list.nonEmpty =>
        list.map(l => statsFor(a).lowerBound <= l.asInstanceOf[Literal] &&
            l.asInstanceOf[Literal] <= statsFor(a).upperBound).reduce(_ || _)
      // This is an example to explain how it works, imagine that the id column stored as follows:
      // __________________________________________
      // | Partition ID | lowerBound | upperBound |
      // |--------------|------------|------------|
      // |      p1      |    '1'     |    '9'     |
      // |      p2      |    '10'    |    '19'    |
      // |      p3      |    '20'    |    '29'    |
      // |      p4      |    '30'    |    '39'    |
      // |      p5      |    '40'    |    '49'    |
      // |______________|____________|____________|
      //
      // A filter: df.filter($"id".startsWith("2")).
      // In this case it substr lowerBound and upperBound:
      // ________________________________________________________________________________________
      // | Partition ID | lowerBound.substr(0, Length("2")) | upperBound.substr(0, Length("2")) |
      // |--------------|-----------------------------------|-----------------------------------|
      // |      p1      |    '1'                            |    '9'                            |
      // |      p2      |    '1'                            |    '1'                            |
      // |      p3      |    '2'                            |    '2'                            |
      // |      p4      |    '3'                            |    '3'                            |
      // |      p5      |    '4'                            |    '4'                            |
      // |______________|___________________________________|___________________________________|
      //
      // We can see that we only need to read p1 and p3.
      case StartsWith(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound.substr(0, Length(l)) <= l &&
            l <= statsFor(a).upperBound.substr(0, Length(l))
    }

    // When we bind the filters we need to do it against the stats schema
    val partitionFilters: Seq[Expression] = {
      predicates.flatMap { p =>
        val filter = buildFilter.lift(p)
        val boundFilter =
          filter.map(
            BindReferences.bindReference(
              _,
              statsSchema,
              allowFailures = true))

        boundFilter.foreach(_ =>
          filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

        // If the filter can't be resolved then we are missing required statistics.
        boundFilter.filter(_.resolved)
      }
    }

    def ret(index: Int, cachedBatchIterator: Iterator[CachedBatch]): Iterator[CachedBatch] = {
      val partitionFilter = Predicate.create(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        cachedAttributes)

      partitionFilter.initialize(index)
      val schemaIndex = cachedAttributes.zipWithIndex

      cachedBatchIterator.filter { cb =>
        val cachedBatch = cb.asInstanceOf[SimpleMetricsCachedBatch]
        if (!partitionFilter.eval(cachedBatch.stats)) {
          logDebug {
            val statsString = schemaIndex.map { case (a, i) =>
              val value = cachedBatch.stats.get(i, a.dataType)
              s"${a.name}: $value"
            }.mkString(", ")
            s"Skipping partition based on stats $statsString"
          }
          false
        } else {
          true
        }
      }
    }
    ret
  }
}
