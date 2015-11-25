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

package org.apache.spark.sql.execution.joins

import org.apache.spark._
import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter


private[spark]
class UnsafeCartesianRDD(rdd1 : RDD[UnsafeRow], rdd2 : RDD[UnsafeRow])
  extends CartesianRDD[UnsafeRow, UnsafeRow](rdd1.sparkContext, rdd1, rdd2) {

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {
    val sorter = UnsafeExternalSorter.create(
      context.taskMemoryManager(),
      SparkEnv.get.blockManager,
      context,
      null,
      null,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes)

    val currSplit = split.asInstanceOf[CartesianPartition]
    var numFields = 0
    for (y <- rdd2.iterator(currSplit.s2, context)) {
      numFields = y.numFields()
      sorter.insertRecord(y.getBaseObject, y.getBaseOffset, y.getSizeInBytes, 0)
    }

    def createIter(): Iterator[UnsafeRow] = {
      val iter = sorter.getIterator
      val unsafeRow = new UnsafeRow
      new Iterator[UnsafeRow] {
        override def hasNext: Boolean = {
          iter.hasNext
        }
        override def next(): UnsafeRow = {
          iter.loadNext()
          unsafeRow.pointTo(iter.getBaseObject, iter.getBaseOffset, numFields, iter.getRecordLength)
          unsafeRow
        }
      }
    }

    val resultIter =
      for (x <- rdd1.iterator(currSplit.s1, context);
           y <- createIter()) yield (x, y)
    CompletionIterator[(UnsafeRow, UnsafeRow), Iterator[(UnsafeRow, UnsafeRow)]](
      resultIter, sorter.cleanupResources)
  }
}


case class CartesianProduct(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = true
  override def outputsUnsafeRows: Boolean = true

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().map { row =>
      numLeftRows += 1
      row.asInstanceOf[UnsafeRow]
    }
    val rightResults = right.execute().map { row =>
      numRightRows += 1
      row.asInstanceOf[UnsafeRow]
    }

    val pair = new UnsafeCartesianRDD(leftResults, rightResults)
    pair.mapPartitionsInternal { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }
  }
}
