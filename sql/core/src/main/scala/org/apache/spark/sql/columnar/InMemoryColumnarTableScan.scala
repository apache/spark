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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}

object InMemoryRelation {
  def apply(useCompression: Boolean, batchSize: Int, child: SparkPlan): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, child)()
}

private[sql] case class CachedBatch(buffers: Array[ByteBuffer], stats: Row)

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    child: SparkPlan)
    (private var _cachedColumnBuffers: RDD[CachedBatch] = null)
  extends LogicalPlan with MultiInstanceRelation {

  override lazy val statistics =
    Statistics(sizeInBytes = child.sqlContext.defaultSizeInBytes)

  val partitionStatistics = new PartitionStatistics(output)

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    val output = child.output
    val cached = child.execute().mapPartitions { baseIterator =>
      new Iterator[CachedBatch] {
        def next() = {
          val columnBuilders = output.map { attribute =>
            val columnType = ColumnType(attribute.dataType)
            val initialBufferSize = columnType.defaultSize * batchSize
            ColumnBuilder(columnType.typeId, initialBufferSize, attribute.name, useCompression)
          }.toArray

          var row: Row = null
          var rowCount = 0

          while (baseIterator.hasNext && rowCount < batchSize) {
            row = baseIterator.next()
            var i = 0
            while (i < row.length) {
              columnBuilders(i).appendFrom(row, i)
              i += 1
            }
            rowCount += 1
          }

          val stats = Row.fromSeq(
            columnBuilders.map(_.columnStats.collectedStatistics).foldLeft(Seq.empty[Any])(_ ++ _))

          CachedBatch(columnBuilders.map(_.build()), stats)
        }

        def hasNext = baseIterator.hasNext
      }
    }.cache()

    cached.setName(child.toString)
    _cachedColumnBuffers = cached
  }

  override def children = Seq.empty

  override def newInstance() = {
    new InMemoryRelation(
      output.map(_.newInstance),
      useCompression,
      batchSize,
      child)(
      _cachedColumnBuffers).asInstanceOf[this.type]
  }

  def cachedColumnBuffers = _cachedColumnBuffers
}

private[sql] case class InMemoryColumnarTableScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    relation: InMemoryRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.expressions._

  /**   */
  val buildFilter: PartialFunction[Expression, Expression] = {
    case EqualTo(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l >= aStats.lowerBound && l <= aStats.upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l >= aStats.lowerBound && l <= aStats.upperBound
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
        filter.foreach(f => logWarning(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  val readPartitions = sparkContext.accumulator(0)
  val readBatches = sparkContext.accumulator(0)

  override def execute() = {
    readPartitions.setValue(0)

    relation.cachedColumnBuffers.mapPartitions { iterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        relation.partitionStatistics.schema)

      // Find the ordinals of the requested columns.  If none are requested, use the first.
      val requestedColumns = if (attributes.isEmpty) {
        Seq(0)
      } else {
        attributes.map(a => relation.output.indexWhere(_.exprId == a.exprId))
      }

      iterator
        // Skip pruned batches
        .filter { cachedBatch =>
          val stats = cachedBatch.stats
          val chosen = partitionFilter(stats)

          if (!chosen) {
            def statsString = relation.partitionStatistics.schema
              .zip(stats)
              .map { case (a, s) => s"${a.name}: $s" }
              .mkString(", ")
            logInfo(s"Skipping partition based on stats $statsString")
          }

          chosen
        }
        .map(_.buffers)
        .flatMap { buffers =>
          val columnAccessors = requestedColumns.map(buffers(_)).map(ColumnAccessor(_))
          val nextRow = new GenericMutableRow(columnAccessors.length)

          // Return an iterator that leverages ColumnAccessors to extract rows
          new Iterator[Row] {
            override def next() = {
              var i = 0
              while (i < nextRow.length) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              nextRow
            }

            override def hasNext = columnAccessors.head.hasNext
          }
        }
    }
  }
}
