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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.metric.SQLMetric

class BroadcastHashJoinEvaluatorFactory(
    broadcastRelation: Broadcast[HashedRelation],
    join: (Iterator[InternalRow], HashedRelation, SQLMetric) => Iterator[InternalRow],
    buildSide: BuildSide,
    leftOutput: Seq[Attribute],
    rightOutput: Seq[Attribute],
    streamedKeys: Seq[Expression],
    isNullAwareAntiJoin: Boolean,
    numOutputRows: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new BroadcastHashJoinEvaluator

  private class BroadcastHashJoinEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val streamedIter = inputs(0)
      val hashed = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)

      if (isNullAwareAntiJoin) {
        if (hashed == EmptyHashedRelation) {
          streamedIter
        } else if (hashed == HashedRelationWithAllNullKeys) {
          Iterator.empty
        } else {
          val streamedOutput = {
            buildSide match {
              case BuildLeft => rightOutput
              case BuildRight => leftOutput
            }
          }
          val streamedBoundKeys =
            BindReferences.bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)
          val keyGenerator = UnsafeProjection.create(streamedBoundKeys)
          streamedIter
            .filter { row =>
              val lookupKey: UnsafeRow = keyGenerator(row)
              if (lookupKey.anyNull()) {
                false
              } else {
                // Anti Join: Drop the row on the streamed side if it is a match on the build
                hashed.get(lookupKey) == null
              }
            }
            .map { row =>
              numOutputRows += 1
              row
            }
        }
      } else {
        join(streamedIter, hashed, numOutputRows)
      }
    }
  }
}
