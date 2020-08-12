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

import java.util.concurrent.TimeUnit._

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.collection.BitSet

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends HashJoin with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def output: Seq[Attribute] = super[ShuffledJoin].output

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil
    case _ => super.outputOrdering
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(
      iter,
      buildBoundKeys,
      taskMemoryManager = context.taskMemoryManager(),
      // Full outer join needs support for NULL key in HashedRelation.
      allowsNullKey = joinType == FullOuter)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      joinType match {
        case FullOuter => fullOuterJoin(streamIter, hashed, numOutputRows)
        case _ => join(streamIter, hashed, numOutputRows)
      }
    }
  }

  private def fullOuterJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
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
    val streamNullJoinRow = new JoinedRow
    val streamNullJoinRowWithBuild = {
      buildSide match {
        case BuildLeft =>
          streamNullJoinRow.withRight(streamNullRow)
          streamNullJoinRow.withLeft _
        case BuildRight =>
          streamNullJoinRow.withLeft(streamNullRow)
          streamNullJoinRow.withRight _
      }
    }

    val iter = if (hashedRelation.keyIsUnique) {
      fullOuterJoinWithUniqueKey(streamIter, hashedRelation, joinKeys, joinRow, streamNullJoinRow,
        joinRowWithStream, joinRowWithBuild, streamNullJoinRowWithBuild, buildNullRow,
        streamNullRow)
    } else {
      fullOuterJoinWithNonUniqueKey(streamIter, hashedRelation, joinKeys, joinRow,
        streamNullJoinRow, joinRowWithStream, joinRowWithBuild, streamNullJoinRowWithBuild,
        buildNullRow, streamNullRow)
    }

    val resultProj = UnsafeProjection.create(output, output)
    iter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  /**
   * Full outer shuffled hash join with unique join keys:
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A `BitSet` is used to track matched rows with key index.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index from `BitSet`.
   */
  private def fullOuterJoinWithUniqueKey(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      joinKeys: UnsafeProjection,
      joinRow: JoinedRow,
      streamNullJoinRow: JoinedRow,
      joinRowWithStream: InternalRow => JoinedRow,
      joinRowWithBuild: InternalRow => JoinedRow,
      streamNullJoinRowWithBuild: InternalRow => JoinedRow,
      buildNullRow: GenericInternalRow,
      streamNullRow: GenericInternalRow): Iterator[InternalRow] = {
    val matchedKeys = new BitSet(hashedRelation.numKeysIndex)

    // Process stream side with looking up hash relation
    val streamResultIter = streamIter.map { srow =>
      joinRowWithStream(srow)
      val keys = joinKeys(srow)
      if (keys.anyNull) {
        joinRowWithBuild(buildNullRow)
      } else {
        val matched = hashedRelation.getWithKeyIndex(keys)
        if (matched != null) {
          val (keyIndex, buildIter) = (matched._1, matched._2)
          val buildRow = buildIter.next
          if (boundCondition(joinRowWithBuild(buildRow))) {
            matchedKeys.set(keyIndex)
            joinRow
          } else {
            joinRowWithBuild(buildNullRow)
          }
        } else {
          joinRowWithBuild(buildNullRow)
        }
      }
    }

    // Process build side with filtering out rows looked up and
    // passed join condition already
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap {
      case (keyIndex, brow) =>
        val isMatched = matchedKeys.get(keyIndex)
        if (!isMatched) {
          streamNullJoinRowWithBuild(brow)
          Some(streamNullJoinRow)
        } else {
          None
        }
    }

    streamResultIter ++ buildResultIter
  }

  /**
   * Full outer shuffled hash join with unique join keys:
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A `HashSet[Long]` is used to track matched rows with
   *    key index (Int) and value index (Int) together.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index and value index from `HashSet`.
   */
  private def fullOuterJoinWithNonUniqueKey(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      joinKeys: UnsafeProjection,
      joinRow: JoinedRow,
      streamNullJoinRow: JoinedRow,
      joinRowWithStream: InternalRow => JoinedRow,
      joinRowWithBuild: InternalRow => JoinedRow,
      streamNullJoinRowWithBuild: InternalRow => JoinedRow,
      buildNullRow: GenericInternalRow,
      streamNullRow: GenericInternalRow): Iterator[InternalRow] = {
    val matchedRows = new mutable.HashSet[Long]

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
      joinRowWithStream(srow)
      val keys = joinKeys(srow)
      if (keys.anyNull) {
        Iterator.single(joinRowWithBuild(buildNullRow))
      } else {
        val matched = hashedRelation.getWithKeyIndex(keys)
        if (matched != null) {
          val (keyIndex, buildIter) = (matched._1, matched._2.zipWithIndex)

          new RowIterator {
            private var found = false
            override def advanceNext(): Boolean = {
              while (buildIter.hasNext) {
                val (buildRow, valueIndex) = buildIter.next()
                if (boundCondition(joinRowWithBuild(buildRow))) {
                  markRowMatched(keyIndex, valueIndex)
                  found = true
                  return true
                }
              }
              if (!found) {
                joinRowWithBuild(buildNullRow)
                found = true
                return true
              }
              false
            }
            override def getRow: InternalRow = joinRow
          }.toScala
        } else {
          Iterator.single(joinRowWithBuild(buildNullRow))
        }
      }
    }

    // Process build side with filtering out rows looked up and
    // passed join condition already
    var prevKeyIndex = -1
    var valueIndex = -1
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap {
      case (keyIndex, brow) =>
        if (prevKeyIndex == -1 || keyIndex != prevKeyIndex) {
          prevKeyIndex = keyIndex
          valueIndex = -1
        }
        valueIndex += 1
        val isMatched = isRowMatched(keyIndex, valueIndex)
        if (!isMatched) {
          streamNullJoinRowWithBuild(brow)
          Some(streamNullJoinRow)
        } else {
          None
        }
    }

    streamResultIter ++ buildResultIter
  }

  // TODO(SPARK-32567): support full outer shuffled hash join code-gen
  override def supportCodegen: Boolean = {
    joinType != FullOuter
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  override def needCopyResult: Boolean = true

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[1]);", forceInline = true)
    HashedRelationInfo(relationTerm, keyIsUnique = false, isEmpty = false)
  }
}
