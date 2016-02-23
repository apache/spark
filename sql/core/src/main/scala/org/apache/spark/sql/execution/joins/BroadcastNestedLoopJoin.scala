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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopJoin(
  left: SparkPlan,
  right: SparkPlan,
  buildSide: BuildSide,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  private[this] def genResultProjection: InternalRow => InternalRow = {
    UnsafeProjection.create(schema)
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def output: Seq[Attribute] = {
    joinType match {
      case Inner =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case LeftSemi =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      newPredicate(condition.get, left.output ++ right.output)
    } else {
      (r: InternalRow) => true
    }
  }

  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildSide match {
          case BuildRight =>
            buildRows.iterator.map(r => joinedRow(streamedRow, r))
          case BuildLeft =>
            buildRows.iterator.map(r => joinedRow(r, streamedRow))
        }
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  private def leftOuterJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  private def rightOuterJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    assert(buildSide == BuildLeft)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(r, streamedRow))
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  private def leftSemiJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) {
        streamedIter.filter(l =>
          buildRows.exists(r => boundCondition(joinedRow(l, r)))
        )
      } else {
        streamedIter.filter(r => !buildRows.isEmpty)
      }
    }
  }

  /**
   * The implementation these joins:
   *
   * LeftOuter with BuildLeft
   * RightOuter with BuildRight
   * FullOuter
   * LeftSemi with BuildLeft
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    /** All rows that either match both-way, or rows from streamed joined with nulls. */
    val streamRdd = streamed.execute()
    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val includedBroadcastTuples = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          val broadcastedRow = buildRows(i)
          buildSide match {
            case BuildRight if boundCondition(joinedRow(streamedRow, broadcastedRow)) =>
              includedBroadcastTuples.set(i)
            case BuildLeft if boundCondition(joinedRow(broadcastedRow, streamedRow)) =>
              includedBroadcastTuples.set(i)
            case _ =>
          }
          i += 1
        }
      }
      Iterator(includedBroadcastTuples)
    }

    val allIncludedBroadcastTuples = matchedBuildRows.fold(
      new BitSet(relation.value.length)
    )(_ | _)

    if (joinType == LeftSemi) {
      assert(buildSide == BuildLeft)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      var i = 0
      val rel = relation.value
      while (i < rel.length) {
        if (allIncludedBroadcastTuples.get(i)) {
          buf += rel(i).copy()
        }
        i += 1
      }
      return sparkContext.makeRDD(buf.toSeq)
    }

    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      val leftNulls = new GenericMutableRow(left.output.size)
      val rightNulls = new GenericMutableRow(right.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var streamRowMatched = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          val broadcastedRow = buildRows(i)
          buildSide match {
            case BuildRight if boundCondition(joinedRow(streamedRow, broadcastedRow)) =>
              matchedRows += joinedRow(streamedRow, broadcastedRow).copy()
              streamRowMatched = true
            case BuildLeft if boundCondition(joinedRow(broadcastedRow, streamedRow)) =>
              matchedRows += joinedRow(broadcastedRow, streamedRow).copy()
              streamRowMatched = true
            case _ =>
          }
          i += 1
        }

        (streamRowMatched, joinType, buildSide) match {
          case (false, LeftOuter | FullOuter, BuildRight) =>
            matchedRows += joinedRow(streamedRow, rightNulls).copy()
          case (false, RightOuter | FullOuter, BuildLeft) =>
            matchedRows += joinedRow(leftNulls, streamedRow).copy()
          case _ =>
        }
        matchedRows.iterator
      }
    }

    val leftNulls = new GenericMutableRow(left.output.size)
    val rightNulls = new GenericMutableRow(right.output.size)

    /** Rows from broadcasted joined with nulls. */
    val broadcastRowsWithNulls: Seq[InternalRow] = {
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      var i = 0
      val rel = relation.value
      (joinType, buildSide) match {
        case (RightOuter | FullOuter, BuildRight) =>
          val joinedRow = new JoinedRow
          joinedRow.withLeft(leftNulls)
          while (i < rel.length) {
            if (!allIncludedBroadcastTuples.get(i)) {
              buf += joinedRow.withRight(rel(i)).copy()
            }
            i += 1
          }
        case (LeftOuter | FullOuter, BuildLeft) =>
          val joinedRow = new JoinedRow
          joinedRow.withRight(rightNulls)
          while (i < rel.length) {
            if (!allIncludedBroadcastTuples.get(i)) {
              buf += joinedRow.withLeft(rel(i)).copy()
            }
            i += 1
          }
        case _ =>
      }
      buf.toSeq
    }

    sparkContext.union(
      matchedStreamRows,
      sparkContext.makeRDD(broadcastRowsWithNulls)
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (Inner, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) =>
        leftOuterJoin(broadcastedRelation)
      case (RightOuter, BuildLeft) =>
        rightOuterJoin(broadcastedRelation)
      case (LeftSemi, BuildRight) =>
        leftSemiJoin(broadcastedRelation)
      case _ =>
        defaultJoin(broadcastedRelation)
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsInternal { iter =>
      val resultProj = genResultProjection
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }
}
