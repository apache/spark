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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftExistence}
import org.apache.spark.sql.execution.metric.SQLMetric

class BroadcastNestedLoopJoinEvaluatorFactory(
    output: Seq[Attribute],
    streamedOutput: Seq[Attribute],
    broadcastOutput: Seq[Attribute],
    joinType: JoinType,
    numOutputRows: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new BroadcastNestedLoopJoinEvaluator

  private class BroadcastNestedLoopJoinEvaluator
      extends PartitionEvaluator[InternalRow, InternalRow] {

    private def genResultProjection: UnsafeProjection = joinType match {
      case LeftExistence(_) =>
        UnsafeProjection.create(output, output)
      case _ =>
        // Always put the stream side on left to simplify implementation
        // both of left and right side could be null
        UnsafeProjection.create(
          output,
          (streamedOutput ++ broadcastOutput).map(_.withNullability(true)))
    }
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val iter = inputs(0)
      val resultProj = genResultProjection
      resultProj.initialize(partitionIndex)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }
}
