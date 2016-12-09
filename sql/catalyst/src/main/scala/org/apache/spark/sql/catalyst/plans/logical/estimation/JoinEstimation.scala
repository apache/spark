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

package org.apache.spark.sql.catalyst.plans.logical.estimation

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}
import org.apache.spark.sql.types.DataType


object JoinEstimation {
  import EstimationUtils._

  // scalastyle:off
  /**
   * Estimate output size and number of rows after a join operator, and propogate updated column
   * statsitics.
   * The number of rows of A inner join B on A.k1 = B.k1 is estimated by this basic formula:
   * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)), where V is the number of distinct values of
   * that column. The underlying assumption for this formula is: each value of the smaller domain
   * is included in the larger domain.
   * Generally, inner join with multiple join keys can also be estimated based on the above
   * formula:
   * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
   * However, the denominator can become very large and excessively reduce the result, so we use a
   * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
   *
   * @return Return the updated statistics after join. Return `None` if the join type is not
   *         supported, or we don't have enough statistics for estimation.
   */
  // scalastyle:on
  def estimate(join: Join): Option[Statistics] = join match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if supportsJoinType(joinType) && hasRowCountStat(left, right) =>

      // 1. Compute the denominator
      var ndvDenom: BigInt = -1
      val keyPairs = extractJoinKeys(leftKeys, rightKeys)
      val leftStats = left.statistics
      val rightStats = right.statistics
      val intersectedStats = new mutable.HashMap[String, ColumnStat]()
      var i = 0
      while(i < keyPairs.length && ndvDenom != 0) {
        val (leftKey, rightKey) = keyPairs(i)
        // Do estimation if we have enough statistics
        if (hasColumnStat((left, leftKey), (right, rightKey))) {
          val leftKeyStats = leftStats.colStats(leftKey.name)
          val rightKeyStats = rightStats.colStats(rightKey.name)

          // Check if the two sides are disjoint
          val lRange = Range(leftKeyStats.min, leftKeyStats.max, leftKey.dataType)
          val rRange = Range(rightKeyStats.min, rightKeyStats.max, rightKey.dataType)
          if (Range.isIntersected(lRange, rRange)) {
            // Get the largest ndv among pairs of join keys
            val maxNdv = leftKeyStats.distinctCount.max(rightKeyStats.distinctCount)
            if (maxNdv > ndvDenom) ndvDenom = maxNdv

            // Update intersected column stats
            val minNdv = leftKeyStats.distinctCount.min(rightKeyStats.distinctCount)
            val (newMin1, newMax1, newMin2, newMax2) =
              Range.intersect(lRange, rRange, leftKey.dataType, rightKey.dataType)
            intersectedStats.put(leftKey.name, intersectedColumnStat(leftKeyStats, minNdv,
              newMin1, newMax1))
            intersectedStats.put(rightKey.name, intersectedColumnStat(rightKeyStats, minNdv,
              newMin2, newMax2))
          } else {
            // Set ndvDenom to zero to indicate that this join should have no output
            ndvDenom = 0
          }
        }
        i += 1
      }

      // 2. Estimate the number of output rows
      val leftRows = leftStats.rowCount.get
      val rightRows = rightStats.rowCount.get
      val outputRows: BigInt = if (ndvDenom == 0) {
        // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
        0
      } else if (ndvDenom < 0) {
        // There isn't join keys or column stats for any of the join key pairs, we estimate like
        // cartesian product.
        leftRows * rightRows
      } else {
        ceil(BigDecimal(leftRows * rightRows) / BigDecimal(ndvDenom))
      }

      // 3. Update statistics based on the output of join
      val originalStats = leftStats.colStats ++ rightStats.colStats
      val outputWithStat = join.output.filter(attr => originalStats.contains(attr.name))

      val outputColStats = new mutable.HashMap[String, ColumnStat]()
      if (ndvDenom == 0) {
        // empty output
        outputWithStat.foreach(a => outputColStats.put(a.name, emptyColumnStat(a.dataType)))
      } else if (ndvDenom < 0) {
        // cartesian product, column stats will not change
        outputWithStat.foreach(a => outputColStats.put(a.name, originalStats(a.name)))
      } else {
        val leftRatio = BigDecimal(outputRows) / BigDecimal(leftRows)
        val rightRatio = BigDecimal(outputRows) / BigDecimal(rightRows)
        outputWithStat.foreach { a =>
          // check if this attribute is a join key
          if (intersectedStats.contains(a.name)) {
            outputColStats.put(a.name, intersectedStats(a.name))
          } else {
            val oldColStat = originalStats(a.name)
            val oldNdv = oldColStat.distinctCount
            // We only change (scale down) the number of distinct values if the number of rows
            // decreases after join, because join won't produce new values even if the number of
            // rows increases.
            val newNdv = if (left.outputSet.contains(a) && leftRatio < 1) {
              ceil(BigDecimal(oldNdv) * leftRatio)
            } else if (right.outputSet.contains(a) && rightRatio < 1) {
              ceil(BigDecimal(oldNdv) * rightRatio)
            } else {
              oldNdv
            }
            outputColStats.put(a.name, oldColStat.copy(distinctCount = newNdv, nullCount = 0))
          }
        }
      }

      Some(Statistics(
        sizeInBytes = outputRows * getRowSize(join.output, outputColStats.toMap),
        rowCount = Some(outputRows),
        colStats = outputColStats.toMap,
        isBroadcastable = false))

    case _ => None
  }

  def emptyColumnStat(dataType: DataType): ColumnStat = {
    ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 0,
      avgLen = dataType.defaultSize, maxLen = dataType.defaultSize)
  }

  def intersectedColumnStat(
      origin: ColumnStat,
      newDistinctCount: BigInt,
      newMin: Option[Any],
      newMax: Option[Any]): ColumnStat = {
    origin.copy(distinctCount = newDistinctCount, min = newMin, max = newMax, nullCount = 0)
  }

  def extractJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[(AttributeReference, AttributeReference)] = {
    leftKeys.zip(rightKeys).flatMap {
      case (ExtractAttr(left), ExtractAttr(right)) => Some((left, right))
      // Currently we don't deal with equal joins like key1 = key2 + 5.
      // Note: join keys from EqualNullSafe also fall into this case (Coalesce), consider to
      // support it in the future by using `nullCount` in column stats.
      case _ => None
    }
  }

}
