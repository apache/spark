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

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.{IntegralType, LongType}

trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val buildSide: BuildSide
  val condition: Option[Expression]
  val left: SparkPlan
  val right: SparkPlan

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  /**
    * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
    *
    * If not, returns the original expressions.
    */
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    var keyExpr: Expression = null
    var width = 0
    keys.foreach { e =>
      e.dataType match {
        case dt: IntegralType if dt.defaultSize <= 8 - width =>
          if (width == 0) {
            if (e.dataType != LongType) {
              keyExpr = Cast(e, LongType)
            } else {
              keyExpr = e
            }
            width = dt.defaultSize
          } else {
            val bits = dt.defaultSize * 8
            keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
              BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
            width -= bits
          }
        // TODO: support BooleanType, DateType and TimestampType
        case other =>
          return keys
      }
    }
    keyExpr :: Nil
  }

  protected val canJoinKeyFitWithinLong: Boolean = {
    val sameTypes = buildKeys.map(_.dataType) == streamedKeys.map(_.dataType)
    val key = rewriteKeyExpr(buildKeys)
    sameTypes && key.length == 1 && key.head.dataType.isInstanceOf[LongType]
  }

  protected def buildSideKeyGenerator: Projection =
    UnsafeProjection.create(rewriteKeyExpr(buildKeys), buildPlan.output)

  protected def streamSideKeyGenerator: Projection =
    UnsafeProjection.create(rewriteKeyExpr(streamedKeys), streamedPlan.output)

  @transient private[this] lazy val boundCondition = if (condition.isDefined) {
    newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)
  } else {
    (r: InternalRow) => true
  }

  protected def hashJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: LongSQLMetric): Iterator[InternalRow] =
  {
    new Iterator[InternalRow] {
      private[this] var currentStreamedRow: InternalRow = _
      private[this] var currentHashMatches: Seq[InternalRow] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow
      private[this] val resultProjection: (InternalRow) => InternalRow =
        UnsafeProjection.create(self.schema)

      private[this] val joinKeys = streamSideKeyGenerator

      override final def hasNext: Boolean = {
        while (true) {
          // check if it's end of current matches
          if (currentHashMatches != null && currentMatchPosition == currentHashMatches.length) {
            currentHashMatches = null
            currentMatchPosition = -1
          }

          // find the next match
          while (currentHashMatches == null && streamIter.hasNext) {
            currentStreamedRow = streamIter.next()
            val key = joinKeys(currentStreamedRow)
            if (!key.anyNull) {
              currentHashMatches = hashedRelation.get(key)
              if (currentHashMatches != null) {
                currentMatchPosition = 0
              }
            }
          }
          if (currentHashMatches == null) {
            return false
          }

          // found some matches
          buildSide match {
            case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
            case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
          }
          if (boundCondition(joinRow)) {
            return true
          } else {
            currentMatchPosition += 1
          }
        }
        false  // unreachable
      }

      override final def next(): InternalRow = {
        // next() could be called without calling hasNext()
        if (hasNext) {
          currentMatchPosition += 1
          numOutputRows += 1
          resultProjection(joinRow)
        } else {
          throw new NoSuchElementException
        }
      }
    }
  }
}
