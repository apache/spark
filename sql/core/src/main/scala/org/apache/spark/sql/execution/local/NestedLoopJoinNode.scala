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
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

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
    val matchedBuildTuples = new BitSet(buildRelation.size)
    val resultProj = genResultProjection
    streamed.open()

    // streamedRowMatches also contains null rows if using outer join
    val streamedRowMatches: Iterator[InternalRow] = streamed.asIterator.flatMap { streamedRow =>
      val matchedRows = new CompactBuffer[InternalRow]

      var i = 0
      var streamRowMatched = false

      // Scan the build relation to look for matches for each streamed row
      while (i < buildRelation.size) {
        val buildRow = buildRelation(i)
        buildSide match {
          case BuildRight => joinedRow(streamedRow, buildRow)
          case BuildLeft => joinedRow(buildRow, streamedRow)
        }
        if (boundCondition(joinedRow)) {
          matchedRows += resultProj(joinedRow).copy()
          streamRowMatched = true
          matchedBuildTuples.set(i)
        }
        i += 1
      }

      // If this row had no matches and we're using outer join, join it with the null rows
      if (!streamRowMatched) {
        (joinType, buildSide) match {
          case (LeftOuter | FullOuter, BuildRight) =>
            matchedRows += resultProj(joinedRow(streamedRow, rightNulls)).copy()
          case (RightOuter | FullOuter, BuildLeft) =>
            matchedRows += resultProj(joinedRow(leftNulls, streamedRow)).copy()
          case _ =>
        }
      }

      matchedRows.iterator
    }

    // If we're using outer join, find rows on the build side that didn't match anything
    // and join them with the null row
    lazy val unmatchedBuildRows: Iterator[InternalRow] = {
      var i = 0
      buildRelation.filter { row =>
        val r = !matchedBuildTuples.get(i)
        i += 1
        r
      }.iterator
    }
    iterator = (joinType, buildSide) match {
      case (RightOuter | FullOuter, BuildRight) =>
        streamedRowMatches ++
          unmatchedBuildRows.map { buildRow => resultProj(joinedRow(leftNulls, buildRow)) }
      case (LeftOuter | FullOuter, BuildLeft) =>
        streamedRowMatches ++
          unmatchedBuildRows.map { buildRow => resultProj(joinedRow(buildRow, rightNulls)) }
      case _ => streamedRowMatches
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
