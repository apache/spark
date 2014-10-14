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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning}
import org.apache.spark.sql.catalyst.plans.{Inner, FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer
/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations by first shuffling the data using the join
 * keys.
 */
@DeveloperApi
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override def outputPartitioning: Partitioning = joinType match {
    case Inner => left.outputPartitioning
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case x => throw new Exception(s"ShuffledHashJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output = {
    joinType match {
      case Inner =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case x =>
        throw new Exception(s"ShuffledHashJoin should not take $x as the JoinType")
    }
  }

  private[this] lazy val nullRow = joinType match {
      case LeftOuter => new GenericRow(right.output.length)
      case RightOuter => new GenericRow(left.output.length)
      case _ => null
  }

  private[this] lazy val boundCondition =
      condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

  private def outerJoin(streamIter: Iterator[Row], hashedRelation: HashedRelation):Iterator[Row] = {
    new Iterator[Row] {
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: CompactBuffer[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow2

      private[this] val joinKeys = streamSideKeyGenerator()

      override final def hasNext: Boolean =
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

      override final def next() = {
        val ret = joinType match {
          case LeftOuter =>
            if (currentMatchPosition == -1) {
              joinRow(currentStreamedRow, nullRow)
            } else {
              val rightRow = currentHashMatches(currentMatchPosition)
              val joinedRow = joinRow(currentStreamedRow, rightRow)
              currentMatchPosition += 1
              if (!boundCondition(joinedRow)) {
                joinRow(currentStreamedRow, nullRow)
              } else {
                joinedRow
              }
            }
          case RightOuter =>
           if (currentMatchPosition == -1) {
              joinRow(nullRow, currentStreamedRow)
            } else {
              val leftRow = currentHashMatches(currentMatchPosition)
              val joinedRow = joinRow(leftRow, currentStreamedRow)
              currentMatchPosition += 1
              if (!boundCondition(joinedRow)) {
                joinRow(nullRow, currentStreamedRow)
              } else {
                joinedRow
              }
            }
        }
        ret
      }

      private final def fetchNext(): Boolean = {
        currentMatchPosition = -1
        currentHashMatches = null
        currentStreamedRow = streamIter.next()
        if (!joinKeys(currentStreamedRow).anyNull) {
          currentHashMatches = hashedRelation.get(joinKeys.currentValue)
        }
        if (currentHashMatches != null) {
          currentMatchPosition = 0
        }
        true
      }
    }
  }

  override def execute() = {
    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashed = HashedRelation(buildIter, buildSideKeyGenerator)
      joinType match {
        case Inner => hashJoin(streamIter, hashed)
        case LeftOuter => outerJoin(streamIter, hashed)
        case RightOuter => outerJoin(streamIter, hashed)
        case x => throw new Exception(s"ShuffledHashJoin should not take $x as the JoinType")
      }
    }
  }
}
