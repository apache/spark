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

package org.apache.spark.sql.execution.columnar

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ConvertToUnsafe, LeafNode, SparkPlan}
import org.apache.spark.sql.types.UserDefinedType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulable, Accumulator, Accumulators}

private[sql] object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String]): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel,
      if (child.outputsUnsafeRows) child else ConvertToUnsafe(child),
      tableName)()
}

/**
 * CachedBatch is a cached batch of rows.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
private[columnar]
case class CachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    @transient child: SparkPlan,
    tableName: Option[String])(
    @transient private[sql] var _cachedColumnBuffers: RDD[CachedBatch] = null,
    @transient private[sql] var _statistics: Statistics = null,
    private[sql] var _batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] = null)
  extends LogicalPlan with MultiInstanceRelation {

  private val batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] =
    if (_batchStats == null) {
      child.sqlContext.sparkContext.accumulableCollection(ArrayBuffer.empty[InternalRow])
    } else {
      _batchStats
    }

  @transient val partitionStatistics = new PartitionStatistics(output)

  private def computeSizeInBytes = {
    val sizeOfRow: Expression =
      BindReferences.bindReference(
        output.map(a => partitionStatistics.forAttribute(a).sizeInBytes).reduce(Add),
        partitionStatistics.schema)

    batchStats.value.map(row => sizeOfRow.eval(row).asInstanceOf[Long]).sum
  }

  // Statistics propagation contracts:
  // 1. Non-null `_statistics` must reflect the actual statistics of the underlying data
  // 2. Only propagate statistics when `_statistics` is non-null
  private def statisticsToBePropagated = if (_statistics == null) {
    val updatedStats = statistics
    if (_statistics == null) null else updatedStats
  } else {
    _statistics
  }

  override def statistics: Statistics = {
    if (_statistics == null) {
      if (batchStats.value.isEmpty) {
        // Underlying columnar RDD hasn't been materialized, no useful statistics information
        // available, return the default statistics.
        Statistics(sizeInBytes = child.sqlContext.conf.defaultSizeInBytes)
      } else {
        // Underlying columnar RDD has been materialized, required information has also been
        // collected via the `batchStats` accumulator, compute the final statistics,
        // and update `_statistics`.
        _statistics = Statistics(sizeInBytes = computeSizeInBytes)
        _statistics
      }
    } else {
      // Pre-computed statistics
      _statistics
    }
  }

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

  def recache(): Unit = {
    _cachedColumnBuffers.unpersist()
    _cachedColumnBuffers = null
    buildBuffers()
  }

  private def buildBuffers(): Unit = {
    val output = child.output
    val cached = child.execute().mapPartitionsInternal { rowIterator =>
      new Iterator[CachedBatch] {
        def next(): CachedBatch = {
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          var totalSize = 0L
          while (rowIterator.hasNext && rowCount < batchSize
            && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {
            val row = rowIterator.next()

            // Added for SPARK-6082. This assertion can be useful for scenarios when something
            // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
            // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
            // hard to decipher.
            assert(
              row.numFields == columnBuilders.size,
              s"Row column number mismatch, expected ${output.size} columns, " +
                s"but got ${row.numFields}." +
                s"\nRow content: $row")

            var i = 0
            totalSize = 0
            while (i < row.numFields) {
              columnBuilders(i).appendFrom(row, i)
              totalSize += columnBuilders(i).columnStats.sizeInBytes
              i += 1
            }
            rowCount += 1
          }

          val stats = InternalRow.fromSeq(columnBuilders.map(_.columnStats.collectedStatistics)
                        .flatMap(_.values))

          batchStats += stats
          CachedBatch(rowCount, columnBuilders.map(_.build().array()), stats)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(tableName.map(n => s"In-memory table $n").getOrElse(child.toString))
    _cachedColumnBuffers = cached
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child, tableName)(
      _cachedColumnBuffers, statisticsToBePropagated, batchStats)
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
      _cachedColumnBuffers,
      statisticsToBePropagated,
      batchStats).asInstanceOf[this.type]
  }

  def cachedColumnBuffers: RDD[CachedBatch] = _cachedColumnBuffers

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(_cachedColumnBuffers, statisticsToBePropagated, batchStats)

  private[sql] def uncache(blocking: Boolean): Unit = {
    Accumulators.remove(batchStats.id)
    cachedColumnBuffers.unpersist(blocking)
    _cachedColumnBuffers = null
  }
}

private[sql] case class InMemoryColumnarTableScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  override def outputPartitioning: Partitioning = relation.child.outputPartitioning

  // The cached version does not change the outputOrdering of the original SparkPlan.
  override def outputOrdering: Seq[SortOrder] = relation.child.outputOrdering

  override def outputsUnsafeRows: Boolean = true

  private def statsFor(a: Attribute) = relation.partitionStatistics.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  @transient val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
    case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
    case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
    case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
    case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
  }

  val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            relation.partitionStatistics.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulators: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions: Accumulator[Int] = sparkContext.accumulator(0)
  lazy val readBatches: Accumulator[Int] = sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  protected override def doExecute(): RDD[InternalRow] = {
    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    // Using these variables here to avoid serialization of entire objects (if referenced directly)
    // within the map Partitions closure.
    val schema = relation.partitionStatistics.schema
    val schemaIndex = schema.zipWithIndex
    val relOutput = relation.output
    val buffers = relation.cachedColumnBuffers

    buffers.mapPartitionsInternal { cachedBatchIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        schema)

      // Find the ordinals and data types of the requested columns.
      val (requestedColumnIndices, requestedColumnDataTypes) =
        attributes.map { a =>
          relOutput.indexWhere(_.exprId == a.exprId) -> a.dataType
        }.unzip

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
        if (inMemoryPartitionPruningEnabled) {
          cachedBatchIterator.filter { cachedBatch =>
            if (!partitionFilter(cachedBatch.stats)) {
              def statsString: String = schemaIndex.map {
                case (a, i) =>
                  val value = cachedBatch.stats.get(i, a.dataType)
                  s"${a.name}: $value"
              }.mkString(", ")
              logInfo(s"Skipping partition based on stats $statsString")
              false
            } else {
              if (enableAccumulators) {
                readBatches += 1
              }
              true
            }
          }
        } else {
          cachedBatchIterator
        }

      val columnTypes = requestedColumnDataTypes.map {
        case udt: UserDefinedType[_] => udt.sqlType
        case other => other
      }.toArray
      val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
      columnarIterator.initialize(cachedBatchesToScan, columnTypes, requestedColumnIndices.toArray)
      if (enableAccumulators && columnarIterator.hasNext) {
        readPartitions += 1
      }
      columnarIterator
    }
  }
}
