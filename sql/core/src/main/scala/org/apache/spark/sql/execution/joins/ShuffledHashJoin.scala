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

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.memory.MemoryMode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = joinType match {
    case Inner => PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftSemi => left.outputPartitioning
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(s"ShuffledHashJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  private def buildHashedRelation(iter: Iterator[UnsafeRow]): HashedRelation = {
    // try to acquire some memory for the hash table, it could trigger other operator to free some
    // memory. The memory acquired here will mostly be used until the end of task.
    val context = TaskContext.get()
    val memoryManager = context.taskMemoryManager()
    var acquired = 0L
    var used = 0L
    context.addTaskCompletionListener((t: TaskContext) =>
      memoryManager.releaseExecutionMemory(acquired, MemoryMode.ON_HEAP, null)
    )

    val copiedIter = iter.map { row =>
      // It's hard to guess what's exactly memory will be used, we have a rough guess here.
      // TODO: use BytesToBytesMap instead of HashMap for memory efficiency
      // Each pair in HashMap will have two UnsafeRows, one CompactBuffer, maybe 10+ pointers
      val needed = 150 + row.getSizeInBytes
      if (needed > acquired - used) {
        val got = memoryManager.acquireExecutionMemory(
          Math.max(memoryManager.pageSizeBytes(), needed), MemoryMode.ON_HEAP, null)
        if (got < needed) {
          throw new SparkException("Can't acquire enough memory to build hash map in shuffled" +
            "hash join, please use sort merge join by setting " +
            "spark.sql.join.preferSortMergeJoin=true")
        }
        acquired += got
      }
      used += needed
      // HashedRelation requires that the UnsafeRow should be separate objects.
      row.copy()
    }

    HashedRelation(canJoinKeyFitWithinLong, copiedIter, buildSideKeyGenerator)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter.asInstanceOf[Iterator[UnsafeRow]])
      val joinedRow = new JoinedRow
      joinType match {
        case Inner =>
          hashJoin(streamIter, hashed, numOutputRows)

        case LeftSemi =>
          hashSemiJoin(streamIter, hashed, numOutputRows)

        case LeftOuter =>
          val keyGenerator = streamSideKeyGenerator
          val resultProj = createResultProjection
          streamIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashed.get(rowKey), resultProj, numOutputRows)
          })

        case RightOuter =>
          val keyGenerator = streamSideKeyGenerator
          val resultProj = createResultProjection
          streamIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashed.get(rowKey), joinedRow, resultProj, numOutputRows)
          })

        case x =>
          throw new IllegalArgumentException(
            s"ShuffledHashJoin should not take $x as the JoinType")
      }
    }
  }
}
