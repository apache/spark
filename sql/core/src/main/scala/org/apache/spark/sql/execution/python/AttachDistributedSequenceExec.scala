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

package org.apache.spark.sql.execution.python

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.util.Utils

/**
 * A physical plan that adds a new long column with `sequenceAttr` that
 * increases one by one. This is for 'distributed-sequence' default index
 * in pandas API on Spark.
 * Right child is only to compute the partition sizes.
 */
case class AttachDistributedSequenceExec(
    sequenceAttr: Attribute,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(sequenceAttr)

  override val output: Seq[Attribute] = sequenceAttr +: left.output

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    val leftRDD = left.execute()
    val rightRDD = right.execute()
    val n = leftRDD.getNumPartitions
    assert(rightRDD.getNumPartitions == n)

    if (n > 1) {
      val sc = rightRDD.sparkContext
      val startIndices: Array[Long] = sc.runJob(
        rightRDD,
        Utils.getIteratorSize _,
        0 until n - 1 // do not need to count the last partition
      ).scanLeft(0L)(_ + _)

      leftRDD.mapPartitionsWithIndex { case (partId, iter) =>
        val unsafeProj = UnsafeProjection.create(output, output)
        val joinedRow = new JoinedRow
        val unsafeRowWriter =
          new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1)

        var id = startIndices(partId)
        iter.map { row =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          id += 1
          unsafeRowWriter.write(0, id)
          joinedRow(unsafeRowWriter.getRow, row)
        }.map(unsafeProj)
      }
    } else {
      leftRDD.mapPartitions { iter =>
        val unsafeProj = UnsafeProjection.create(output, output)
        val joinedRow = new JoinedRow
        val unsafeRowWriter =
          new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1)

        var id = 0L
        iter.map { row =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          id += 1
          unsafeRowWriter.write(0, id)
          joinedRow(unsafeRowWriter.getRow, row)
        }.map(unsafeProj)
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val indexColumn = s"Index: $sequenceAttr"
    s"$nodeName$truncatedOutputString $indexColumn"
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): AttachDistributedSequenceExec =
    copy(left = newLeft, right = newRight)
}
