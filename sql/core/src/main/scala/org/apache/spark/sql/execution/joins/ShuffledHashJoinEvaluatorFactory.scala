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

import scala.concurrent.duration.NANOSECONDS

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, GenericInternalRow, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.RowIterator
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

class ShuffledHashJoinEvaluatorFactory(
    output: Seq[Attribute],
    condition: Option[Expression],
    buildKeys: Seq[Expression],
    streamedKeys: Seq[Expression],
    buildPlanOutput: Seq[Attribute],
    streamedPlanOutput: Seq[Attribute],
    joinType: JoinType,
    buildSide: BuildSide,
    streamedOutput: Seq[Attribute],
    buildOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    buildDataSize: SQLMetric,
    buildTime: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new ShuffledHashJoinEvaluator

  private class ShuffledHashJoinEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 2)
      val streamIter = inputs(0)
      val buildIter = inputs(1)
      val hashed = buildHashedRelation(buildIter)
      joinType match {
        case FullOuter =>
          buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows, isFullOuterJoin = true)
        case LeftOuter if buildSide.equals(BuildLeft) =>
          buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows, isFullOuterJoin = false)
        case RightOuter if buildSide.equals(BuildRight) =>
          buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows, isFullOuterJoin = false)
        case _ =>
          HashJoin.join(
            HashJoinParams(
              streamIter,
              hashed,
              condition,
              streamedKeys,
              streamedOutput,
              buildPlanOutput,
              streamedPlanOutput,
              output,
              joinType,
              buildSide,
              numOutputRows))
      }
    }

    private lazy val boundCondition: InternalRow => Boolean =
      HashJoin.boundCondition(condition, joinType, buildSide, buildPlanOutput, streamedPlanOutput)

    lazy val ignoreDuplicatedKey = joinType match {
      case LeftExistence(_) =>
        // For building hash relation, ignore duplicated rows with same join keys if:
        // 1. Join condition is empty, or
        // 2. Join condition only references streamed attributes and build join keys.
        val streamedOutputAndBuildKeys = AttributeSet(streamedOutput ++ buildKeys)
        condition.forall(_.references.subsetOf(streamedOutputAndBuildKeys))
      case _ => false
    }

    private lazy val streamedBoundKeys =
      BindReferences.bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)

    private lazy val buildBoundKeys =
      BindReferences.bindReferences(HashJoin.rewriteKeyExpr(buildKeys), buildOutput)

    private def streamSideKeyGenerator(): UnsafeProjection =
      UnsafeProjection.create(streamedBoundKeys)
    private def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
      val start = System.nanoTime()
      val context = TaskContext.get()
      val relation = HashedRelation(
        iter,
        buildBoundKeys,
        taskMemoryManager = context.taskMemoryManager(),
        // build-side or full outer join needs support for NULL key in HashedRelation.
        allowsNullKey = joinType == FullOuter ||
          (joinType == LeftOuter && buildSide == BuildLeft) ||
          (joinType == RightOuter && buildSide == BuildRight),
        ignoresDuplicatedKey = ignoreDuplicatedKey)
      buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
      buildDataSize += relation.estimatedSize
      // This relation is usually used until the end of task.
      context.addTaskCompletionListener[Unit](_ => relation.close())
      relation
    }

    private def buildSideOrFullOuterJoin(
        streamIter: Iterator[InternalRow],
        hashedRelation: HashedRelation,
        numOutputRows: SQLMetric,
        isFullOuterJoin: Boolean): Iterator[InternalRow] = {
      val joinKeys = streamSideKeyGenerator()
      val joinRow = new JoinedRow
      val (joinRowWithStream, joinRowWithBuild) = {
        buildSide match {
          case BuildLeft => (joinRow.withRight _, joinRow.withLeft _)
          case BuildRight => (joinRow.withLeft _, joinRow.withRight _)
        }
      }
      val buildNullRow = new GenericInternalRow(buildOutput.length)
      val streamNullRow = new GenericInternalRow(streamedOutput.length)
      lazy val streamNullJoinRowWithBuild = {
        buildSide match {
          case BuildLeft =>
            joinRow.withRight(streamNullRow)
            joinRow.withLeft _
          case BuildRight =>
            joinRow.withLeft(streamNullRow)
            joinRow.withRight _
        }
      }

      val iter = if (hashedRelation.keyIsUnique) {
        buildSideOrFullOuterJoinUniqueKey(
          streamIter,
          hashedRelation,
          joinKeys,
          joinRowWithStream,
          joinRowWithBuild,
          streamNullJoinRowWithBuild,
          buildNullRow,
          isFullOuterJoin)
      } else {
        buildSideOrFullOuterJoinNonUniqueKey(
          streamIter,
          hashedRelation,
          joinKeys,
          joinRowWithStream,
          joinRowWithBuild,
          streamNullJoinRowWithBuild,
          buildNullRow,
          isFullOuterJoin)
      }

      val resultProj = UnsafeProjection.create(output, output)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }

    /**
     * Shuffled hash join with unique join keys, where an outer side is the build side.
     *   1. Process rows from stream side by looking up hash relation. Mark the matched rows from
     *      build side be looked up. A bit set is used to track matched rows with key index. 2.
     *      Process rows from build side by iterating hash relation. Filter out rows from build
     *      side being matched already, by checking key index from bit set.
     */
    private def buildSideOrFullOuterJoinUniqueKey(
        streamIter: Iterator[InternalRow],
        hashedRelation: HashedRelation,
        joinKeys: UnsafeProjection,
        joinRowWithStream: InternalRow => JoinedRow,
        joinRowWithBuild: InternalRow => JoinedRow,
        streamNullJoinRowWithBuild: => InternalRow => JoinedRow,
        buildNullRow: GenericInternalRow,
        isFullOuterJoin: Boolean): Iterator[InternalRow] = {
      val matchedKeys = new BitSet(hashedRelation.maxNumKeysIndex)
      buildDataSize += matchedKeys.capacity / 8

      def noMatch = if (isFullOuterJoin) {
        Some(joinRowWithBuild(buildNullRow))
      } else {
        None
      }

      // Process stream side with looking up hash relation
      val streamResultIter = streamIter.flatMap { srow =>
        joinRowWithStream(srow)
        val keys = joinKeys(srow)
        if (keys.anyNull) {
          noMatch
        } else {
          val matched = hashedRelation.getValueWithKeyIndex(keys)
          if (matched != null) {
            val keyIndex = matched.getKeyIndex
            val buildRow = matched.getValue
            val joinRow = joinRowWithBuild(buildRow)
            if (boundCondition(joinRow)) {
              matchedKeys.set(keyIndex)
              Some(joinRow)
            } else {
              noMatch
            }
          } else {
            noMatch
          }
        }
      }

      // Process build side with filtering out the matched rows
      val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap { valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        val isMatched = matchedKeys.get(keyIndex)
        if (!isMatched) {
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
      }

      streamResultIter ++ buildResultIter
    }

    /**
     * Shuffled hash join with non-unique join keys, where an outer side is the build side.
     *   1. Process rows from stream side by looking up hash relation. Mark the matched rows from
     *      build side be looked up. A [[OpenHashSet]] (Long) is used to track matched rows with
     *      key index (Int) and value index (Int) together. 2. Process rows from build side by
     *      iterating hash relation. Filter out rows from build side being matched already, by
     *      checking key index and value index from [[OpenHashSet]].
     *
     * The "value index" is defined as the index of the tuple in the chain of tuples having the
     * same key. For example, if certain key is found thrice, the value indices of its tuples will
     * be 0, 1 and 2. Note that value indices of tuples with different keys are incomparable.
     */
    private def buildSideOrFullOuterJoinNonUniqueKey(
        streamIter: Iterator[InternalRow],
        hashedRelation: HashedRelation,
        joinKeys: UnsafeProjection,
        joinRowWithStream: InternalRow => JoinedRow,
        joinRowWithBuild: InternalRow => JoinedRow,
        streamNullJoinRowWithBuild: => InternalRow => JoinedRow,
        buildNullRow: GenericInternalRow,
        isFullOuterJoin: Boolean): Iterator[InternalRow] = {
      val matchedRows = new OpenHashSet[Long]
      TaskContext
        .get()
        .addTaskCompletionListener[Unit](_ => {
          // At the end of the task, update the task's memory usage for this
          // [[OpenHashSet]] to track matched rows, which has two parts:
          // [[OpenHashSet._bitset]] and [[OpenHashSet._data]].
          val bitSetEstimatedSize = matchedRows.getBitSet.capacity / 8
          val dataEstimatedSize = matchedRows.capacity * 8
          buildDataSize += bitSetEstimatedSize + dataEstimatedSize
        })

      def markRowMatched(keyIndex: Int, valueIndex: Int): Unit = {
        val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
        matchedRows.add(rowIndex)
      }

      def isRowMatched(keyIndex: Int, valueIndex: Int): Boolean = {
        val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
        matchedRows.contains(rowIndex)
      }

      // Process stream side with looking up hash relation
      val streamResultIter = streamIter.flatMap { srow =>
        val joinRow = joinRowWithStream(srow)
        val keys = joinKeys(srow)
        if (keys.anyNull) {
          // return row with build side NULL row to satisfy full outer join semantics if enabled
          if (isFullOuterJoin) {
            Iterator.single(joinRowWithBuild(buildNullRow))
          } else {
            Iterator.empty
          }
        } else {
          val buildIter = hashedRelation.getWithKeyIndex(keys)
          new RowIterator {
            private var found = false
            private var valueIndex = -1

            override def advanceNext(): Boolean = {
              while (buildIter != null && buildIter.hasNext) {
                val buildRowWithKeyIndex = buildIter.next()
                val keyIndex = buildRowWithKeyIndex.getKeyIndex
                val buildRow = buildRowWithKeyIndex.getValue
                valueIndex += 1
                if (boundCondition(joinRowWithBuild(buildRow))) {
                  markRowMatched(keyIndex, valueIndex)
                  found = true
                  return true
                }
              }
              // When we reach here, it means no match is found for this key.
              // So we need to return one row with build side NULL row,
              // to satisfy the full outer join semantic if enabled.
              if (!found && isFullOuterJoin) {
                joinRowWithBuild(buildNullRow)
                // Set `found` to be true as we only need to return one row
                // but no more.
                found = true
                return true
              }
              false
            }

            override def getRow: InternalRow = joinRow
          }.toScala
        }
      }

      // Process build side with filtering out the matched rows
      var prevKeyIndex = -1
      var valueIndex = -1
      val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap { valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        if (prevKeyIndex == -1 || keyIndex != prevKeyIndex) {
          valueIndex = 0
          prevKeyIndex = keyIndex
        } else {
          valueIndex += 1
        }

        val isMatched = isRowMatched(keyIndex, valueIndex)
        if (!isMatched) {
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
      }

      streamResultIter ++ buildResultIter
    }

  }
}
