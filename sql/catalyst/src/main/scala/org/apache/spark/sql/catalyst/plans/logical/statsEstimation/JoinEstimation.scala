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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.internal.SQLConf


object JoinEstimation extends Logging {
  /**
   * Estimate statistics after join. Return `None` if the join type is not supported, or we don't
   * have enough statistics for estimation.
   */
  def estimate(conf: SQLConf, join: Join): Option[Statistics] = {
    join.joinType match {
      case Inner | Cross | LeftOuter | RightOuter | FullOuter =>
        InnerOuterEstimation(conf, join).doEstimate()
      case LeftSemi | LeftAnti =>
        LeftSemiAntiEstimation(conf, join).doEstimate()
      case _ =>
        logDebug(s"[CBO] Unsupported join type: ${join.joinType}")
        None
    }
  }
}

case class InnerOuterEstimation(conf: SQLConf, join: Join) extends Logging {

  private val leftStats = join.left.stats(conf)
  private val rightStats = join.right.stats(conf)

  /**
   * Estimate output size and number of rows after a join operator, and update output column stats.
   */
  def doEstimate(): Option[Statistics] = join match {
    case _ if !rowCountsExist(conf, join.left, join.right) =>
      None

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, _) =>
      // 1. Compute join selectivity
      val joinKeyPairs = extractJoinKeysWithColStats(leftKeys, rightKeys)
      val selectivity = joinSelectivity(joinKeyPairs)

      // 2. Estimate the number of output rows
      val leftRows = leftStats.rowCount.get
      val rightRows = rightStats.rowCount.get
      val innerJoinedRows = ceil(BigDecimal(leftRows * rightRows) * selectivity)

      // Make sure outputRows won't be too small based on join type.
      val outputRows = joinType match {
        case LeftOuter =>
          // All rows from left side should be in the result.
          leftRows.max(innerJoinedRows)
        case RightOuter =>
          // All rows from right side should be in the result.
          rightRows.max(innerJoinedRows)
        case FullOuter =>
          // T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
          leftRows.max(innerJoinedRows) + rightRows.max(innerJoinedRows) - innerJoinedRows
        case _ =>
          // Don't change for inner or cross join
          innerJoinedRows
      }

      // 3. Update statistics based on the output of join
      val inputAttrStats = AttributeMap(
        leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
      val attributesWithStat = join.output.filter(a => inputAttrStats.contains(a))
      val (fromLeft, fromRight) = attributesWithStat.partition(join.left.outputSet.contains(_))

      val outputStats: Seq[(Attribute, ColumnStat)] = if (outputRows == 0) {
        // The output is empty, we don't need to keep column stats.
        Nil
      } else if (selectivity == 0) {
        joinType match {
          // For outer joins, if the join selectivity is 0, the number of output rows is the
          // same as that of the outer side. And column stats of join keys from the outer side
          // keep unchanged, while column stats of join keys from the other side should be updated
          // based on added null values.
          case LeftOuter =>
            fromLeft.map(a => (a, inputAttrStats(a))) ++
              fromRight.map(a => (a, nullColumnStat(a.dataType, leftRows)))
          case RightOuter =>
            fromRight.map(a => (a, inputAttrStats(a))) ++
              fromLeft.map(a => (a, nullColumnStat(a.dataType, rightRows)))
          case FullOuter =>
            fromLeft.map { a =>
              val oriColStat = inputAttrStats(a)
              (a, oriColStat.copy(nullCount = oriColStat.nullCount + rightRows))
            } ++ fromRight.map { a =>
              val oriColStat = inputAttrStats(a)
              (a, oriColStat.copy(nullCount = oriColStat.nullCount + leftRows))
            }
          case _ => Nil
        }
      } else if (selectivity == 1) {
        // Cartesian product, just propagate the original column stats
        inputAttrStats.toSeq
      } else {
        val joinKeyStats = getIntersectedStats(joinKeyPairs)
        join.joinType match {
          // For outer joins, don't update column stats from the outer side.
          case LeftOuter =>
            fromLeft.map(a => (a, inputAttrStats(a))) ++
              updateAttrStats(outputRows, fromRight, inputAttrStats, joinKeyStats)
          case RightOuter =>
            updateAttrStats(outputRows, fromLeft, inputAttrStats, joinKeyStats) ++
              fromRight.map(a => (a, inputAttrStats(a)))
          case FullOuter =>
            inputAttrStats.toSeq
          case _ =>
            // Update column stats from both sides for inner or cross join.
            updateAttrStats(outputRows, attributesWithStat, inputAttrStats, joinKeyStats)
        }
      }

      val outputAttrStats = AttributeMap(outputStats)
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputRows, outputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats))

    case _ =>
      // When there is no equi-join condition, we do estimation like cartesian product.
      val inputAttrStats = AttributeMap(
        leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
      // Propagate the original column stats
      val outputRows = leftStats.rowCount.get * rightStats.rowCount.get
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputRows, inputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = inputAttrStats))
  }

  // scalastyle:off
  /**
   * The number of rows of A inner join B on A.k1 = B.k1 is estimated by this basic formula:
   * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)), where V is the number of distinct values of
   * that column. The underlying assumption for this formula is: each value of the smaller domain
   * is included in the larger domain.
   * Generally, inner join with multiple join keys can also be estimated based on the above
   * formula:
   * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
   * However, the denominator can become very large and excessively reduce the result, so we use a
   * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
   */
  // scalastyle:on
  def joinSelectivity(joinKeyPairs: Seq[(AttributeReference, AttributeReference)]): BigDecimal = {
    var ndvDenom: BigInt = -1
    var i = 0
    while(i < joinKeyPairs.length && ndvDenom != 0) {
      val (leftKey, rightKey) = joinKeyPairs(i)
      // Check if the two sides are disjoint
      val leftKeyStats = leftStats.attributeStats(leftKey)
      val rightKeyStats = rightStats.attributeStats(rightKey)
      val lRange = Range(leftKeyStats.min, leftKeyStats.max, leftKey.dataType)
      val rRange = Range(rightKeyStats.min, rightKeyStats.max, rightKey.dataType)
      if (Range.isIntersected(lRange, rRange)) {
        // Get the largest ndv among pairs of join keys
        val maxNdv = leftKeyStats.distinctCount.max(rightKeyStats.distinctCount)
        if (maxNdv > ndvDenom) ndvDenom = maxNdv
      } else {
        // Set ndvDenom to zero to indicate that this join should have no output
        ndvDenom = 0
      }
      i += 1
    }

    if (ndvDenom < 0) {
      // We can't find any join key pairs with column stats, estimate it as cartesian join.
      1
    } else if (ndvDenom == 0) {
      // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
      0
    } else {
      1 / BigDecimal(ndvDenom)
    }
  }

  /**
   * Propagate or update column stats for output attributes.
   */
  private def updateAttrStats(
      outputRows: BigInt,
      attributes: Seq[Attribute],
      oldAttrStats: AttributeMap[ColumnStat],
      joinKeyStats: AttributeMap[ColumnStat]): Seq[(Attribute, ColumnStat)] = {
    val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()
    val leftRows = leftStats.rowCount.get
    val rightRows = rightStats.rowCount.get

    attributes.foreach { a =>
      // check if this attribute is a join key
      if (joinKeyStats.contains(a)) {
        outputAttrStats += a -> joinKeyStats(a)
      } else {
        val oldColStat = oldAttrStats(a)
        val oldNdv = oldColStat.distinctCount
        val newNdv = if (join.left.outputSet.contains(a)) {
          updateNdv(oldNumRows = leftRows, newNumRows = outputRows, oldNdv = oldNdv)
        } else {
          updateNdv(oldNumRows = rightRows, newNumRows = outputRows, oldNdv = oldNdv)
        }
        val newColStat = oldColStat.copy(distinctCount = newNdv)
        // TODO: support nullCount updates for specific outer joins
        outputAttrStats += a -> newColStat
      }
    }
    outputAttrStats
  }

  /** Get intersected column stats for join keys. */
  private def getIntersectedStats(joinKeyPairs: Seq[(AttributeReference, AttributeReference)])
    : AttributeMap[ColumnStat] = {

    val intersectedStats = new mutable.HashMap[Attribute, ColumnStat]()
    joinKeyPairs.foreach { case (leftKey, rightKey) =>
      val leftKeyStats = leftStats.attributeStats(leftKey)
      val rightKeyStats = rightStats.attributeStats(rightKey)
      val lRange = Range(leftKeyStats.min, leftKeyStats.max, leftKey.dataType)
      val rRange = Range(rightKeyStats.min, rightKeyStats.max, rightKey.dataType)
      // When we reach here, join selectivity is not zero, so each pair of join keys should be
      // intersected.
      assert(Range.isIntersected(lRange, rRange))

      // Update intersected column stats
      assert(leftKey.dataType.sameType(rightKey.dataType))
      val newNdv = leftKeyStats.distinctCount.min(rightKeyStats.distinctCount)
      val (newMin, newMax) = Range.intersect(lRange, rRange, leftKey.dataType)
      val newMaxLen = math.min(leftKeyStats.maxLen, rightKeyStats.maxLen)
      val newAvgLen = (leftKeyStats.avgLen + rightKeyStats.avgLen) / 2
      val newStats = ColumnStat(newNdv, newMin, newMax, 0, newAvgLen, newMaxLen)

      intersectedStats.put(leftKey, newStats)
      intersectedStats.put(rightKey, newStats)
    }
    AttributeMap(intersectedStats.toSeq)
  }

  private def extractJoinKeysWithColStats(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[(AttributeReference, AttributeReference)] = {
    leftKeys.zip(rightKeys).collect {
      // Currently we don't deal with equal joins like key1 = key2 + 5.
      // Note: join keys from EqualNullSafe also fall into this case (Coalesce), consider to
      // support it in the future by using `nullCount` in column stats.
      case (lk: AttributeReference, rk: AttributeReference)
        if columnStatsExist((leftStats, lk), (rightStats, rk)) => (lk, rk)
    }
  }
}

case class LeftSemiAntiEstimation(conf: SQLConf, join: Join) {
  def doEstimate(): Option[Statistics] = {
    // TODO: It's error-prone to estimate cardinalities for LeftSemi and LeftAnti based on basic
    // column stats. Now we just propagate the statistics from left side. We should do more
    // accurate estimation when advanced stats (e.g. histograms) are available.
    if (rowCountsExist(conf, join.left)) {
      val leftStats = join.left.stats(conf)
      // Propagate the original column stats for cartesian product
      val outputRows = leftStats.rowCount.get
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputRows, leftStats.attributeStats),
        rowCount = Some(outputRows),
        attributeStats = leftStats.attributeStats))
    } else {
      None
    }
  }
}
