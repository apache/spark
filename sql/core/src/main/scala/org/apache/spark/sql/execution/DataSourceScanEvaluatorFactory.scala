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

import java.util.concurrent.TimeUnit._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class RowDataSourceScanEvaluatorFactory(schema: StructType, numOutputRows: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new RowDataSourceScanEvaluator

  private class RowDataSourceScanEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(index: Int, inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val iter = inputs.head
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map(r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }
}

class FileSourceScanEvaluatorFactory(
    schema: StructType,
    numOutputRows: SQLMetric,
    needsUnsafeRowConversion: Boolean)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new FileSourceScanEvaluator

  private class FileSourceScanEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(index: Int, inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val iter = inputs.head
      if (needsUnsafeRowConversion) {
        val toUnsafe = UnsafeProjection.create(schema)
        toUnsafe.initialize(index)
        iter.map { row =>
          numOutputRows += 1
          toUnsafe(row)
        }
      } else {
        iter.map { row =>
          numOutputRows += 1
          row
        }
      }
    }
  }
}

class FileSourceColumnarScanEvaluatorFactory(numOutputRows: SQLMetric, scanTime: SQLMetric)
    extends PartitionEvaluatorFactory[ColumnarBatch, ColumnarBatch] {
  override def createEvaluator(): PartitionEvaluator[ColumnarBatch, ColumnarBatch] =
    new FileSourceColumnarScanEvaluator

  private class FileSourceColumnarScanEvaluator
      extends PartitionEvaluator[ColumnarBatch, ColumnarBatch] {

    override def eval(index: Int, inputs: Iterator[ColumnarBatch]*): Iterator[ColumnarBatch] = {
      val batches = inputs.head
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }
}
