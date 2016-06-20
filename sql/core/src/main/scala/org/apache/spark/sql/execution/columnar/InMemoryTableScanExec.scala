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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.LeafExecNode


private[sql] case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override def output: Seq[Attribute] = attributes

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  override def outputPartitioning: Partitioning = relation.child.outputPartitioning

  // The cached version does not change the outputOrdering of the original SparkPlan.
  override def outputOrdering: Seq[SortOrder] = relation.child.outputOrdering

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  protected override def doExecute(): RDD[InternalRow] = {
    val childOutput = relation.child.output
    relation.cachedColumnVectors.mapPartitionsInternal { batchIter =>
      new Iterator[InternalRow] {
        private val unsafeRow = new UnsafeRow(childOutput.size)
        private val bufferHolder = new BufferHolder(unsafeRow)
        private val rowWriter = new UnsafeRowWriter(bufferHolder, childOutput.size)
        private var currentBatch: ColumnarCachedBatch = null
        private var currentRowIndex = 0 // row index within each batch

        override def hasNext: Boolean = {
          if (currentBatch == null) {
            val hasNext = batchIter.hasNext
            if (hasNext) {
              currentBatch = batchIter.next()
              currentRowIndex = 0
            }
            hasNext
          } else {
            true // currentBatch != null
          }
        }

        override def next(): InternalRow = {
          if (currentBatch == null) {
            throw new NoSuchElementException
          }
          rowWriter.zeroOutNullBytes()
          // Populate the row
          childOutput.zipWithIndex.foreach { case (attr, colIndex) =>
            val colValue = currentBatch.buffers(colIndex)(currentRowIndex)
            rowWriter.write(colIndex, colValue)
          }
          // If we have consumed this batch, move onto the next one
          currentRowIndex += 1
          if (currentRowIndex == currentBatch.numRows) {
            currentBatch = null
          }
          unsafeRow
        }
      }
    }
  }
}
