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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.types.DataType


object JoinEstimation extends Logging {
  /**
   * Estimate statistics after join. Return `None` if the join type is not supported, or we don't
   * have enough statistics for estimation.
   */
  def estimate(conf: CatalystConf, join: Join): Option[Statistics] = {
    join.joinType match {
      case Inner | Cross | LeftOuter | RightOuter | FullOuter =>
        InnerOuterEstimation(conf, join).doEstimate()
      case LeftSemi | LeftAnti =>
        LeftSemiAntiEstimation(conf, join).doEstimate()
      case _ =>
        logDebug(s"Unsupported join type: ${join.joinType}")
        None
    }
  }
}

case class InnerOuterEstimation(conf: CatalystConf, join: Join) extends Logging {

  private val leftStats = join.left.stats(conf)
  private val rightStats = join.right.stats(conf)

  /**
   * Estimate output size and number of rows after a join operator, and update output column stats.
   */
  def doEstimate(): Option[Statistics] = join match {
    case _ if !rowCountsExist(conf, join.left, join.right) =>
      None

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
      // 1. Compute join selectivity
      val joinKeyPairs = extractJoinKeys(leftKeys, rightKeys)
      val selectivity = joinSelectivity(joinKeyPairs, leftStats, rightStats)

      // 2. Estimate the number of output rows
      val leftRows = leftStats.rowCount.get
      val rightRows = rightStats.rowCount.get
      val innerRows = ceil(BigDecimal(leftRows * rightRows) * selectivity)

      // Make sure outputRows won't be too small based on join type.
      val outputRows = joinType match {
        case LeftOuter =>
          // All rows from left side should be in the result.
          leftRows.max(innerRows)
        case RightOuter =>
          // All rows from right side should be in the result.
          rightRows.max(innerRows)
        case FullOuter =>
          // Simulate full outer join as obtaining the number of elements in the union of two
          // finite sets: A \cup B = A + B - A \cap B => A FOJ B = A + B - A IJ B.
          // But the "inner join" part can be much larger than A \cap B, making the simulated
          // result much smaller. To prevent this, we choose the larger one between the simulated
          // part and the inner part.
          (leftRows + rightRows - innerRows).max(innerRows)
        case _ =>
          // Don't change for inner or cross join
          innerRows
      }

      // 3. Update statistics based on the output of join
      val intersectedStats = if (selectivity == 0) {
        AttributeMap[ColumnStat](Nil)
      } else {
        updateIntersectedStats(joinKeyPairs, leftStats, rightStats)
      }
      val inputAttrStats = AttributeMap(
        leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
      val attributesWithStat = join.output.filter(a => inputAttrStats.contains(a))
      val (fromLeft, fromRight) = attributesWithStat.partition(join.left.outputSet.contains(_))
      val outputStats: Map[Attribute, ColumnStat] = join.joinType match {
        case LeftOuter =>
          // Don't update column stats for attributes from left side.
          fromLeft.map(a => (a, inputAttrStats(a))).toMap ++
            updateAttrStats(outputRows, fromRight, inputAttrStats, intersectedStats)
        case RightOuter =>
          // Don't update column stats for attributes from right side.
          updateAttrStats(outputRows, fromLeft, inputAttrStats, intersectedStats) ++
            fromRight.map(a => (a, inputAttrStats(a))).toMap
        case FullOuter =>
          // Don't update column stats for attributes from both sides.
          attributesWithStat.map(a => (a, inputAttrStats(a))).toMap
        case _ =>
          // Update column stats from both sides for inner or cross join.
          updateAttrStats(outputRows, attributesWithStat, inputAttrStats, intersectedStats)
      }
      val outputAttrStats = AttributeMap(outputStats.toSeq)

      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputAttrStats, outputRows),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats,
        isBroadcastable = false))

    case _ =>
      // When there is no equi-join condition, we do estimation like cartesian product.
      val inputAttrStats = AttributeMap(
        leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
      // Propagate the original column stats
      val outputAttrStats = getOutputMap(inputAttrStats, join.output)
      val outputRows = leftStats.rowCount.get * rightStats.rowCount.get
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputAttrStats, outputRows),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats,
        isBroadcastable = false))
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
  def joinSelectivity(
      joinKeyPairs: Seq[(AttributeReference, AttributeReference)],
      leftStats: Statistics,
      rightStats: Statistics): BigDecimal = {

    var ndvDenom: BigInt = -1
    var i = 0
    while(i < joinKeyPairs.length && ndvDenom != 0) {
      val (leftKey, rightKey) = joinKeyPairs(i)
      // Do estimation if we have enough statistics
      if (columnStatsExist((leftStats, leftKey), (rightStats, rightKey))) {
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
      }
      i += 1
    }

    if (ndvDenom < 0) {
      // There isn't join keys or column stats for any of the join key pairs, we do estimation like
      // cartesian product.
      1
    } else if (ndvDenom == 0) {
      // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
      0
    } else {
      1 / BigDecimal(ndvDenom)
    }
  }

  /** Update column stats for output attributes. */
  private def updateAttrStats(
      outputRows: BigInt,
      attributes: Seq[Attribute],
      oldAttrStats: AttributeMap[ColumnStat],
      joinKeyStats: AttributeMap[ColumnStat]): AttributeMap[ColumnStat] = {
    val outputAttrStats = new mutable.HashMap[Attribute, ColumnStat]()
    val leftRows = leftStats.rowCount.get
    val rightRows = rightStats.rowCount.get
    if (outputRows == 0) {
      // empty output
      attributes.foreach(a => outputAttrStats.put(a, emptyColumnStat(a.dataType)))
    } else if (outputRows == leftRows * rightRows) {
      // We do estimation like cartesian product and propagate the original column stats
      attributes.foreach(a => outputAttrStats.put(a, oldAttrStats(a)))
    } else {
      val leftRatio =
        if (leftRows != 0) BigDecimal(outputRows) / BigDecimal(leftRows) else BigDecimal(0)
      val rightRatio =
        if (rightRows != 0) BigDecimal(outputRows) / BigDecimal(rightRows) else BigDecimal(0)
      attributes.foreach { a =>
        // check if this attribute is a join key
        if (joinKeyStats.contains(a)) {
          outputAttrStats.put(a, joinKeyStats(a))
        } else {
          val oldCS = oldAttrStats(a)
          val oldNdv = oldCS.distinctCount
          // We only change (scale down) the number of distinct values if the number of rows
          // decreases after join, because join won't produce new values even if the number of
          // rows increases.
          val newNdv = if (join.left.outputSet.contains(a) && leftRatio < 1) {
            ceil(BigDecimal(oldNdv) * leftRatio)
          } else if (join.right.outputSet.contains(a) && rightRatio < 1) {
            ceil(BigDecimal(oldNdv) * rightRatio)
          } else {
            oldNdv
          }
          // TODO: support nullCount updates for specific outer joins
          outputAttrStats.put(a, oldCS.copy(distinctCount = newNdv))
        }
      }
    }
    AttributeMap(outputAttrStats.toSeq)
  }

  /** Update intersected column stats for join keys. */
  private def updateIntersectedStats(
      joinKeyPairs: Seq[(AttributeReference, AttributeReference)],
      leftStats: Statistics,
      rightStats: Statistics): AttributeMap[ColumnStat] = {
    val intersectedStats = new mutable.HashMap[Attribute, ColumnStat]()
    joinKeyPairs.foreach { case (leftKey, rightKey) =>
      // Do estimation if we have enough statistics
      if (columnStatsExist((leftStats, leftKey), (rightStats, rightKey))) {
        // Check if the two sides are disjoint
        val leftKeyStats = leftStats.attributeStats(leftKey)
        val rightKeyStats = rightStats.attributeStats(rightKey)
        val lRange = Range(leftKeyStats.min, leftKeyStats.max, leftKey.dataType)
        val rRange = Range(rightKeyStats.min, rightKeyStats.max, rightKey.dataType)
        if (Range.isIntersected(lRange, rRange)) {
          // Update intersected column stats
          val minNdv = leftKeyStats.distinctCount.min(rightKeyStats.distinctCount)
          val (newMin1, newMax1, newMin2, newMax2) =
            Range.intersect(lRange, rRange, leftKey.dataType, rightKey.dataType)
          intersectedStats.put(leftKey, intersectedColumnStat(leftKeyStats, minNdv,
            newMin1, newMax1))
          intersectedStats.put(rightKey, intersectedColumnStat(rightKeyStats, minNdv,
            newMin2, newMax2))
        }
      }
    }
    AttributeMap(intersectedStats.toSeq)
  }

  private def emptyColumnStat(dataType: DataType): ColumnStat = {
    ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 0,
      avgLen = dataType.defaultSize, maxLen = dataType.defaultSize)
  }

  private def intersectedColumnStat(
      origin: ColumnStat,
      newDistinctCount: BigInt,
      newMin: Option[Any],
      newMax: Option[Any]): ColumnStat = {
    origin.copy(distinctCount = newDistinctCount, min = newMin, max = newMax, nullCount = 0)
  }

  private def extractJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[(AttributeReference, AttributeReference)] = {
    leftKeys.zip(rightKeys).flatMap {
      case (ExtractAttr(left), ExtractAttr(right)) => Some((left, right))
      case (left, right) =>
        // Currently we don't deal with equal joins like key1 = key2 + 5.
        // Note: join keys from EqualNullSafe also fall into this case (Coalesce), consider to
        // support it in the future by using `nullCount` in column stats.
        logDebug(s"Unsupported equi-join expression: left key: $left, right key: $right")
        None
    }
  }
}

case class LeftSemiAntiEstimation(conf: CatalystConf, join: Join) {
  def doEstimate(): Option[Statistics] = {
    // TODO: It's error-prone to estimate cardinalities for LeftSemi and LeftAnti based on basic
    // column stats. Now we just propagate the statistics from left side. We should do more
    // accurate estimation when advanced stats (e.g. histograms) are available.
    if (rowCountsExist(conf, join.left)) {
      val leftStats = join.left.stats(conf)
      // Propagate the original column stats for cartesian product
      val outputAttrStats = getOutputMap(leftStats.attributeStats, join.output)
      val outputRows = leftStats.rowCount.get
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputAttrStats, outputRows),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats,
        isBroadcastable = false))
    } else {
      None
    }
  }
}
