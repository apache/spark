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
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericMutableRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}

object InMemoryRelation {
  def apply(useCompression: Boolean, batchSize: Int, child: SparkPlan): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, child)()
}

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    child: SparkPlan)
    (private var _cachedColumnBuffers: RDD[Array[ByteBuffer]] = null)
  extends LogicalPlan with MultiInstanceRelation {

  override lazy val statistics =
    Statistics(sizeInBytes = child.sqlContext.defaultSizeInBytes)

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    val output = child.output
    val cached = child.execute().mapPartitions { baseIterator =>
      new Iterator[Array[ByteBuffer]] {
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

          columnBuilders.map(_.build())
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
    relation: InMemoryRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  override def execute() = {
    relation.cachedColumnBuffers.mapPartitions { iterator =>
      // Find the ordinals of the requested columns.  If none are requested, use the first.
      val requestedColumns = if (attributes.isEmpty) {
        Seq(0)
      } else {
        attributes.map(a => relation.output.indexWhere(_.exprId == a.exprId))
      }

      iterator
        .map(batch => requestedColumns.map(batch(_)).map(ColumnAccessor(_)))
        .flatMap { columnAccessors =>
          val nextRow = new GenericMutableRow(columnAccessors.length)
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
