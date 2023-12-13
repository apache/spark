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

package org.apache.spark.sql.execution

import scala.jdk.CollectionConverters._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._

class ColumnarToRowEvaluatorFactory(
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    numInputBatches: SQLMetric)
    extends PartitionEvaluatorFactory[ColumnarBatch, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[ColumnarBatch, InternalRow] = {
    new ColumnarToRowEvaluator
  }

  private class ColumnarToRowEvaluator extends PartitionEvaluator[ColumnarBatch, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[ColumnarBatch]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val toUnsafe = UnsafeProjection.create(childOutput, childOutput)
      inputs.head.flatMap { input =>
        numInputBatches += 1
        numOutputRows += input.numRows()
        input.rowIterator().asScala.map(toUnsafe)
      }
    }
  }
}

class RowToColumnarEvaluatorFactory(
    enableOffHeapColumnVector: Boolean,
    numRows: Int,
    schema: StructType,
    numInputRows: SQLMetric,
    numOutputBatches: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, ColumnarBatch] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, ColumnarBatch] = {
    new RowToColumnarEvaluator
  }

  private class RowToColumnarEvaluator extends PartitionEvaluator[InternalRow, ColumnarBatch] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[ColumnarBatch] = {
      assert(inputs.length == 1)
      val rowIterator = inputs.head
      new Iterator[ColumnarBatch] {
        private lazy val converters = new RowToColumnConverter(schema)
        private lazy val vectors: Seq[WritableColumnVector] = if (enableOffHeapColumnVector) {
          OffHeapColumnVector.allocateColumns(numRows, schema).toImmutableArraySeq
        } else {
          OnHeapColumnVector.allocateColumns(numRows, schema).toImmutableArraySeq
        }
        private lazy val cb: ColumnarBatch = new ColumnarBatch(vectors.toArray)

        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          cb.close()
        }

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          cb.setNumRows(0)
          vectors.foreach(_.reset())
          var rowCount = 0
          while (rowCount < numRows && rowIterator.hasNext) {
            val row = rowIterator.next()
            converters.convert(row, vectors.toArray)
            rowCount += 1
          }
          cb.setNumRows(rowCount)
          numInputRows += rowCount
          numOutputBatches += 1
          cb
        }
      }
    }
  }
}
