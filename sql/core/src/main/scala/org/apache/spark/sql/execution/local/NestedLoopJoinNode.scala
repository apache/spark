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

package org.apache.spark.sql.execution.local

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, RightOuter, LeftOuter, JoinType}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.CompactBuffer

case class NestedLoopJoinNode(
    conf: SQLConf,
    left: LocalNode,
    right: LocalNode,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryLocalNode(conf) {

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(
          s"NestedLoopJoin should not take $x as the JoinType")
    }
  }

  private[this] def genResultProjection: InternalRow => InternalRow = {
    if (outputsUnsafeRows) {
      UnsafeProjection.create(schema)
    } else {
      identity[InternalRow]
    }
  }

  private[this] var currentRow: InternalRow = _

  private[this] var iterator: Iterator[InternalRow] = _

  override def open(): Unit = {
    val (streamed, build) = buildSide match {
      case BuildRight => (left, right)
      case BuildLeft => (right, left)
    }
    build.open()
    val buildRelation = new CompactBuffer[InternalRow]
    while (build.next()) {
      buildRelation += build.fetch().copy()
    }
    build.close()

    val boundCondition =
      newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

    val leftNulls = new GenericMutableRow(left.output.size)
    val rightNulls = new GenericMutableRow(right.output.size)
    val joinedRow = new JoinedRow
    val includedBuildTuples = new BitSet(buildRelation.size)
    val resultProj = genResultProjection
    streamed.open()

    val matchesOrStreamedRowsWithNulls = streamed.asIterator.flatMap { streamedRow =>
      val matchedRows = new CompactBuffer[InternalRow]

      var i = 0
      var streamRowMatched = false

      while (i < buildRelation.size) {
        val buildRow = buildRelation(i)
        buildSide match {
          case BuildRight if boundCondition(joinedRow(streamedRow, buildRow)) =>
            matchedRows += resultProj(joinedRow(streamedRow, buildRow)).copy()
            streamRowMatched = true
            includedBuildTuples.set(i)
          case BuildLeft if boundCondition(joinedRow(buildRow, streamedRow)) =>
            matchedRows += resultProj(joinedRow(buildRow, streamedRow)).copy()
            streamRowMatched = true
            includedBuildTuples.set(i)
          case _ =>
        }
        i += 1
      }

      (streamRowMatched, joinType, buildSide) match {
        case (false, LeftOuter | FullOuter, BuildRight) =>
          matchedRows += resultProj(joinedRow(streamedRow, rightNulls)).copy()
        case (false, RightOuter | FullOuter, BuildLeft) =>
          matchedRows += resultProj(joinedRow(leftNulls, streamedRow)).copy()
        case _ =>
      }

      matchedRows.iterator
    }

    iterator = (joinType, buildSide) match {
      case (RightOuter | FullOuter, BuildRight) =>
        var i = 0
        matchesOrStreamedRowsWithNulls ++ buildRelation.filter { row =>
          val r = !includedBuildTuples.get(i)
          i += 1
          r
        }.iterator.map { buildRow =>
          joinedRow.withLeft(leftNulls)
          resultProj(joinedRow.withRight(buildRow))
        }
      case (LeftOuter | FullOuter, BuildLeft) =>
        var i = 0
        matchesOrStreamedRowsWithNulls ++ buildRelation.filter { row =>
          val r = !includedBuildTuples.get(i)
          i += 1
          r
        }.iterator.map { buildRow =>
          joinedRow.withRight(rightNulls)
          resultProj(joinedRow.withLeft(buildRow))
        }
      case _ => matchesOrStreamedRowsWithNulls
    }
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      currentRow = iterator.next()
      true
    } else {
      false
    }
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = {
    left.close()
    right.close()
  }

}
