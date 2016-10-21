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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * An optimized CartesianRDD for UnsafeRow, which will cache the rows from second child RDD,
 * will be much faster than building the right partition for every row in left RDD, it also
 * materialize the right RDD (in case of the right RDD is nondeterministic).
 */
class UnsafeCartesianRDD(left : RDD[UnsafeRow], right : RDD[UnsafeRow], numFieldsOfRight: Int)
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {
    // We will not sort the rows, so prefixComparator and recordComparator are null.
    val sorter = UnsafeExternalSorter.create(
      context.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      context,
      null,
      null,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      false)

    val partition = split.asInstanceOf[CartesianPartition]
    for (y <- rdd2.iterator(partition.s2, context)) {
      sorter.insertRecord(y.getBaseObject, y.getBaseOffset, y.getSizeInBytes, 0, false)
    }

    // Create an iterator from sorter and wrapper it as Iterator[UnsafeRow]
    def createIter(): Iterator[UnsafeRow] = {
      val iter = sorter.getIterator
      val unsafeRow = new UnsafeRow(numFieldsOfRight)
      new Iterator[UnsafeRow] {
        override def hasNext: Boolean = {
          iter.hasNext
        }
        override def next(): UnsafeRow = {
          iter.loadNext()
          unsafeRow.pointTo(iter.getBaseObject, iter.getBaseOffset, iter.getRecordLength)
          unsafeRow
        }
      }
    }

    val resultIter =
      for (x <- rdd1.iterator(partition.s1, context);
           y <- createIter()) yield (x, y)
    CompletionIterator[(UnsafeRow, UnsafeRow), Iterator[(UnsafeRow, UnsafeRow)]](
      resultIter, sorter.cleanupResources())
  }
}


case class CartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val pair = new UnsafeCartesianRDD(leftResults, rightResults, right.output.size)
    pair.mapPartitionsInternal { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val filtered = if (condition.isDefined) {
        val boundCondition: (InternalRow) => Boolean =
          newPredicate(condition.get, left.output ++ right.output)
        val joined = new JoinedRow

        iter.filter { r =>
          boundCondition(joined(r._1, r._2))
        }
      } else {
        iter
      }
      filtered.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }
  }
}
