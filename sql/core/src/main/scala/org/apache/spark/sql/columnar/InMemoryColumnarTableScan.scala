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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}
import org.apache.spark.storage.StorageLevel

private[sql] object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel, child)()
}

private[sql] case class CachedBatch(buffers: Array[Array[Byte]], stats: Row)

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    child: SparkPlan)(
    private var _cachedColumnBuffers: RDD[CachedBatch] = null,
    private var _statistics: Statistics = null)
  extends LogicalPlan with MultiInstanceRelation {

  private val batchStats =
    child.sqlContext.sparkContext.accumulableCollection(ArrayBuffer.empty[Row])

  val partitionStatistics = new PartitionStatistics(output)

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

  override def statistics = if (_statistics == null) {
    if (batchStats.value.isEmpty) {
      // Underlying columnar RDD hasn't been materialized, no useful statistics information
      // available, return the default statistics.
      Statistics(sizeInBytes = child.sqlContext.defaultSizeInBytes)
    } else {
      // Underlying columnar RDD has been materialized, required information has also been collected
      // via the `batchStats` accumulator, compute the final statistics, and update `_statistics`.
      _statistics = Statistics(sizeInBytes = computeSizeInBytes)
      _statistics
    }
  } else {
    // Pre-computed statistics
    _statistics
  }

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

  def recache() = {
    _cachedColumnBuffers.unpersist()
    _cachedColumnBuffers = null
    buildBuffers()
  }

  private def buildBuffers(): Unit = {
    val output = child.output
    val cached = child.execute().mapPartitions { rowIterator =>
      new Iterator[CachedBatch] {
        def next() = {
          val columnBuilders = output.map { attribute =>
            val columnType = ColumnType(attribute.dataType)
            val initialBufferSize = columnType.defaultSize * batchSize
            ColumnBuilder(columnType.typeId, initialBufferSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          while (rowIterator.hasNext && rowCount < batchSize) {
            val row = rowIterator.next()
            var i = 0
            while (i < row.length) {
              columnBuilders(i).appendFrom(row, i)
              i += 1
            }
            rowCount += 1
          }

          val stats = Row.fromSeq(
            columnBuilders.map(_.columnStats.collectedStatistics).foldLeft(Seq.empty[Any])(_ ++ _))

          batchStats += stats
          CachedBatch(columnBuilders.map(_.build().array()), stats)
        }

        def hasNext = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(child.toString)
    _cachedColumnBuffers = cached
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child)(
      _cachedColumnBuffers, statisticsToBePropagated)
  }

  override def children = Seq.empty

  override def newInstance() = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child)(
      _cachedColumnBuffers,
      statisticsToBePropagated).asInstanceOf[this.type]
  }

  def cachedColumnBuffers = _cachedColumnBuffers

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(_cachedColumnBuffers, statisticsToBePropagated)
}

private[sql] case class InMemoryColumnarTableScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    relation: InMemoryRelation)
  extends LeafNode {

  @transient override val sqlContext = relation.child.sqlContext

  override def output: Seq[Attribute] = attributes

  private def statsFor(a: Attribute) = relation.partitionStatistics.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) && buildFilter(rhs)

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

    case IsNull(a: Attribute)    => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
  }

  val partitionFilters = {
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

  // Accumulators used for testing purposes
  val readPartitions = sparkContext.accumulator(0)
  val readBatches = sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = sqlContext.inMemoryPartitionPruning

  override def execute() = {
    readPartitions.setValue(0)
    readBatches.setValue(0)

    relation.cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        relation.partitionStatistics.schema)

      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (attributes.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          relation.output.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        attributes.map { a =>
          relation.output.indexWhere(_.exprId == a.exprId) -> a.dataType
        }.unzip
      }

      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)

      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]) = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batch =>
            ColumnAccessor(ByteBuffer.wrap(cachedBatch.buffers(batch)))
          }

          // Extract rows via column accessors
          new Iterator[Row] {
            override def next() = {
              var i = 0
              while (i < nextRow.length) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              nextRow
            }

            override def hasNext = columnAccessors(0).hasNext
          }
        }

        if (rows.hasNext) {
          readPartitions += 1
        }

        rows
      }

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
        if (inMemoryPartitionPruningEnabled) {
          cachedBatchIterator.filter { cachedBatch =>
            if (!partitionFilter(cachedBatch.stats)) {
              def statsString = relation.partitionStatistics.schema
                .zip(cachedBatch.stats)
                .map { case (a, s) => s"${a.name}: $s" }
                .mkString(", ")
              logInfo(s"Skipping partition based on stats $statsString")
              false
            } else {
              readBatches += 1
              true
            }
          }
        } else {
          cachedBatchIterator
        }

      cachedBatchesToRows(cachedBatchesToScan)
    }
  }
}
