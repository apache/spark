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
    if (joinType == LeftSemi) {
      UnsafeProjection.create(output, output)
    } else {
      // Always put the stream side on left to simplify implementation
      UnsafeProjection.create(output, streamed.output ++ broadcast.output)
    }
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
      newPredicate(condition.get, streamed.output ++ broadcast.output)
    } else {
      (r: InternalRow) => true
    }
  }

  /**
   * The implementation for InnerJoin.
   */
  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
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

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  private def outerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericMutableRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        private var foundMatch: Boolean = false
        // the matched result row
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        private var nextIndex: Int = 0

        private def findNextMatch(): Boolean = {
          if (streamRow == null) {
            if (!streamedIter.hasNext) {
              return false
            }
            streamRow = streamedIter.next()
            nextIndex = 0
            foundMatch = false
          }
          while (nextIndex < buildRows.length) {
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            nextIndex += 1
            if (boundCondition(resultRow)) {
              foundMatch = true
              return true
            }
          }
          if (!foundMatch) {
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext(): Boolean = {
          resultRow != null || findNextMatch()
        }
        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftSemi with BuildRight
   */
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
   * The implementation for these joins:
   *
   *   LeftOuter with BuildLeft
   *   RightOuter with BuildRight
   *   FullOuter
   *   LeftSemi with BuildLeft
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    /** All rows that either match both-way, or rows from streamed joined with nulls. */
    val streamRdd = streamed.execute()

    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matched.set(i)
          }
          i += 1
        }
      }
      Seq(matched).toIterator
    }

    val matchedBroadcastRows = matchedBuildRows.fold(
      new BitSet(relation.value.length)
    )(_ | _)

    if (joinType == LeftSemi) {
      assert(buildSide == BuildLeft)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      var i = 0
      val rel = relation.value
      while (i < rel.length) {
        if (matchedBroadcastRows.get(i)) {
          buf += rel(i).copy()
        }
        i += 1
      }
      return sparkContext.makeRDD(buf.toSeq)
    }

    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericMutableRow(broadcast.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
          i += 1
        }

        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    val notMatchedBroadcastRows: Seq[InternalRow] = {
      val nulls = new GenericMutableRow(streamed.output.size)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      var i = 0
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      joinedRow.withLeft(nulls)
      while (i < buildRows.length) {
        if (!matchedBroadcastRows.get(i)) {
          buf += joinedRow.withRight(buildRows(i)).copy()
        }
        i += 1
      }
      buf.toSeq
    }

    sparkContext.union(
      matchedStreamRows,
      sparkContext.makeRDD(notMatchedBroadcastRows)
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (Inner, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation)
      case (LeftSemi, BuildRight) =>
        leftSemiJoin(broadcastedRelation)
      case _ =>
        /**
         * LeftOuter with BuildLeft
         * RightOuter with BuildRight
         * FullOuter
         * LeftSemi with BuildLeft
         */
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
