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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.util.collection.CompactBuffer


trait HashOuterJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val joinType: JoinType
  val condition: Option[Expression]
  val left: SparkPlan
  val right: SparkPlan

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(s"HashOuterJoin should not take $x as the JoinType")
    }
  }

  protected[this] lazy val (buildPlan, streamedPlan) = joinType match {
    case RightOuter => (left, right)
    case LeftOuter => (right, left)
    case x =>
      throw new IllegalArgumentException(
        s"HashOuterJoin should not take $x as the JoinType")
  }

  protected[this] lazy val (buildKeys, streamedKeys) = joinType match {
    case RightOuter => (leftKeys, rightKeys)
    case LeftOuter => (rightKeys, leftKeys)
    case x =>
      throw new IllegalArgumentException(
        s"HashOuterJoin should not take $x as the JoinType")
  }

  protected def buildKeyGenerator: Projection =
    UnsafeProjection.create(buildKeys, buildPlan.output)

  protected[this] def streamedKeyGenerator: Projection =
    UnsafeProjection.create(streamedKeys, streamedPlan.output)

  protected[this] def resultProjection: InternalRow => InternalRow =
    UnsafeProjection.create(output, output)

  @transient private[this] lazy val DUMMY_LIST = CompactBuffer[InternalRow](null)
  @transient protected[this] lazy val EMPTY_LIST = CompactBuffer[InternalRow]()

  @transient private[this] lazy val leftNullRow = new GenericInternalRow(left.output.length)
  @transient private[this] lazy val rightNullRow = new GenericInternalRow(right.output.length)
  @transient private[this] lazy val boundCondition = if (condition.isDefined) {
    newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)
  } else {
    (row: InternalRow) => true
  }

  // TODO we need to rewrite all of the iterators with our own implementation instead of the Scala
  // iterator for performance purpose.

  protected[this] def leftOuterIterator(
      key: InternalRow,
      joinedRow: JoinedRow,
      rightIter: Iterable[InternalRow],
      resultProjection: InternalRow => InternalRow,
      numOutputRows: LongSQLMetric): Iterator[InternalRow] = {
    val ret: Iterable[InternalRow] = {
      if (!key.anyNull) {
        val temp = if (rightIter != null) {
          rightIter.collect {
            case r if boundCondition(joinedRow.withRight(r)) => {
              numOutputRows += 1
              resultProjection(joinedRow).copy()
            }
          }
        } else {
          List.empty
        }
        if (temp.isEmpty) {
          numOutputRows += 1
          resultProjection(joinedRow.withRight(rightNullRow)) :: Nil
        } else {
          temp
        }
      } else {
        numOutputRows += 1
        resultProjection(joinedRow.withRight(rightNullRow)) :: Nil
      }
    }
    ret.iterator
  }

  protected[this] def rightOuterIterator(
      key: InternalRow,
      leftIter: Iterable[InternalRow],
      joinedRow: JoinedRow,
      resultProjection: InternalRow => InternalRow,
      numOutputRows: LongSQLMetric): Iterator[InternalRow] = {
    val ret: Iterable[InternalRow] = {
      if (!key.anyNull) {
        val temp = if (leftIter != null) {
          leftIter.collect {
            case l if boundCondition(joinedRow.withLeft(l)) => {
              numOutputRows += 1
              resultProjection(joinedRow).copy()
            }
          }
        } else {
          List.empty
        }
        if (temp.isEmpty) {
          numOutputRows += 1
          resultProjection(joinedRow.withLeft(leftNullRow)) :: Nil
        } else {
          temp
        }
      } else {
        numOutputRows += 1
        resultProjection(joinedRow.withLeft(leftNullRow)) :: Nil
      }
    }
    ret.iterator
  }
}
