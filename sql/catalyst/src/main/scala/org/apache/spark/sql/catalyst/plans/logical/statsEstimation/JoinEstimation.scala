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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Histogram, Join, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._


case class JoinEstimation(join: Join) extends Logging {

  private val leftStats = join.left.stats
  private val rightStats = join.right.stats

  /**
   * Estimate statistics after join. Return `None` if the join type is not supported, or we don't
   * have enough statistics for estimation.
   */
  def estimate: Option[Statistics] = {
    join.joinType match {
      case Inner | Cross | LeftOuter | RightOuter | FullOuter =>
        estimateInnerOuterJoin()
      case LeftSemi | LeftAnti =>
        estimateLeftSemiAntiJoin()
      case _ =>
        logDebug(s"[CBO] Unsupported join type: ${join.joinType}")
        None
    }
  }

  /**
   * Estimate output size and number of rows after a join operator, and update output column stats.
   */
  private def estimateInnerOuterJoin(): Option[Statistics] = join match {
    case _ if !rowCountsExist(join.left, join.right) =>
      None

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, _) =>
      // 1. Compute join selectivity
      val joinKeyPairs = extractJoinKeysWithColStats(leftKeys, rightKeys)
      val leftSideUniqueness = left.distinctKeys.exists(_.subsetOf(ExpressionSet(leftKeys)))
      val rightSideUniqueness = right.distinctKeys.exists(_.subsetOf(ExpressionSet(rightKeys)))
      val (numInnerJoinedRows, keyStatsAfterJoin) =
        computeCardinalityAndStats(joinKeyPairs, leftSideUniqueness, rightSideUniqueness)

      // 2. Estimate the number of output rows
      val leftRows = leftStats.rowCount.get
      val rightRows = rightStats.rowCount.get

      // Make sure outputRows won't be too small based on join type.
      val outputRows = joinType match {
        case LeftOuter =>
          // All rows from left side should be in the result.
          leftRows.max(numInnerJoinedRows)
        case RightOuter =>
          // All rows from right side should be in the result.
          rightRows.max(numInnerJoinedRows)
        case FullOuter =>
          // T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
          leftRows.max(numInnerJoinedRows) + rightRows.max(numInnerJoinedRows) - numInnerJoinedRows
        case _ =>
          assert(joinType == Inner || joinType == Cross)
          // Don't change for inner or cross join
          numInnerJoinedRows
      }

      // 3. Update statistics based on the output of join
      val inputAttrStats = leftStats.attributeStats ++ rightStats.attributeStats
      val attributesWithStat = join.output.filter(a =>
        inputAttrStats.get(a).map(_.hasCountStats).getOrElse(false))
      val (fromLeft, fromRight) = attributesWithStat.partition(join.left.outputSet.contains(_))

      val outputStats: Seq[(Attribute, ColumnStat)] = if (outputRows == 0) {
        // The output is empty, we don't need to keep column stats.
        Nil
      } else if (numInnerJoinedRows == 0) {
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
              (a, oriColStat.copy(nullCount = Some(oriColStat.nullCount.get + rightRows)))
            } ++ fromRight.map { a =>
              val oriColStat = inputAttrStats(a)
              (a, oriColStat.copy(nullCount = Some(oriColStat.nullCount.get + leftRows)))
            }
          case _ =>
            assert(joinType == Inner || joinType == Cross)
            Nil
        }
      } else if (numInnerJoinedRows == leftRows * rightRows) {
        // Cartesian product, just propagate the original column stats
        inputAttrStats.toSeq
      } else {
        join.joinType match {
          // For outer joins, don't update column stats from the outer side.
          case LeftOuter =>
            fromLeft.map(a => (a, inputAttrStats(a))) ++
              updateOutputStats(outputRows, fromRight, inputAttrStats, keyStatsAfterJoin)
          case RightOuter =>
            updateOutputStats(outputRows, fromLeft, inputAttrStats, keyStatsAfterJoin) ++
              fromRight.map(a => (a, inputAttrStats(a)))
          case FullOuter =>
            inputAttrStats.toSeq
          case _ =>
            assert(joinType == Inner || joinType == Cross)
            // Update column stats from both sides for inner or cross join.
            updateOutputStats(outputRows, attributesWithStat, inputAttrStats, keyStatsAfterJoin)
        }
      }

      val outputAttrStats = AttributeMap(outputStats)
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputRows, outputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats))

    case _ =>
      // When there is no equi-join condition, we do estimation like cartesian product.
      val inputAttrStats = leftStats.attributeStats ++ rightStats.attributeStats
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
   * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)),
   * where V is the number of distinct values (ndv) of that column. The underlying assumption for
   * this formula is: each value of the smaller domain is included in the larger domain.
   *
   * Generally, inner join with multiple join keys can be estimated based on the above formula:
   * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
   * However, the denominator can become very large and excessively reduce the result, so we use a
   * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
   *
   * That is, join estimation is based on the most selective join keys. We follow this strategy
   * when different types of column statistics are available. E.g., if card1 is the cardinality
   * estimated by ndv of join key A.k1 and B.k1, card2 is the cardinality estimated by histograms
   * of join key A.k2 and B.k2, then the result cardinality would be min(card1, card2).
   *
   * @param keyPairs pairs of join keys
   *
   * @return join cardinality, and column stats for join keys after the join
   */
  // scalastyle:on
  private def computeCardinalityAndStats(
      keyPairs: Seq[(AttributeReference, AttributeReference)],
      leftSideUniqueness: Boolean,
      rightSideUniqueness: Boolean): (BigInt, AttributeMap[ColumnStat]) = {
    // If there's no column stats available for join keys, estimate as cartesian product.
    var joinCard: BigInt = (leftSideUniqueness, rightSideUniqueness) match {
      case (true, true) => leftStats.rowCount.get.min(rightStats.rowCount.get)
      case (true, false) => rightStats.rowCount.get
      case (false, true) => leftStats.rowCount.get
      case _ => leftStats.rowCount.get * rightStats.rowCount.get
    }
    val keyStatsAfterJoin = new mutable.HashMap[Attribute, ColumnStat]()
    var i = 0
    while(i < keyPairs.length && joinCard != 0) {
      val (leftKey, rightKey) = keyPairs(i)
      // Check if the two sides are disjoint
      val leftKeyStat = leftStats.attributeStats(leftKey)
      val rightKeyStat = rightStats.attributeStats(rightKey)
      val lInterval = ValueInterval(leftKeyStat.min, leftKeyStat.max, leftKey.dataType)
      val rInterval = ValueInterval(rightKeyStat.min, rightKeyStat.max, rightKey.dataType)
      if (ValueInterval.isIntersected(lInterval, rInterval)) {
        val (newMin, newMax) = ValueInterval.intersect(lInterval, rInterval, leftKey.dataType)
        val (card, joinStat) = (leftKeyStat.histogram, rightKeyStat.histogram) match {
          case (Some(l: Histogram), Some(r: Histogram)) =>
            computeByHistogram(leftKey, rightKey, l, r, newMin, newMax)
          case _ =>
            computeByNdv(leftKey, rightKey, newMin, newMax)
        }
        keyStatsAfterJoin +=
          // Histograms are propagated as unchanged. During future estimation, they should be
          // truncated by the updated max/min. In this way, only pointers of the histograms are
          // propagated and thus reduce memory consumption.
          (leftKey -> joinStat.copy(histogram = leftKeyStat.histogram)) +=
            (rightKey -> joinStat.copy(histogram = rightKeyStat.histogram))
        // Return cardinality estimated from the most selective join keys.
        if (card < joinCard) joinCard = card
      } else {
        // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
        joinCard = 0
      }
      i += 1
    }
    (joinCard, AttributeMap(keyStatsAfterJoin))
  }

  /** Returns join cardinality and the column stat for this pair of join keys. */
  private def computeByNdv(
      leftKey: AttributeReference,
      rightKey: AttributeReference,
      min: Option[Any],
      max: Option[Any]): (BigInt, ColumnStat) = {
    val leftKeyStat = leftStats.attributeStats(leftKey)
    val rightKeyStat = rightStats.attributeStats(rightKey)
    val maxNdv = leftKeyStat.distinctCount.get.max(rightKeyStat.distinctCount.get)
    // Compute cardinality by the basic formula.
    val card = BigDecimal(leftStats.rowCount.get * rightStats.rowCount.get) /
      BigDecimal(leftKeyStat.distinctCount.get.min(rightKeyStat.distinctCount.get))

    // Get the intersected column stat.
    val newNdv = Some(leftKeyStat.distinctCount.get.min(rightKeyStat.distinctCount.get))
    val newMaxLen = if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
      Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
    } else {
      None
    }
    val newAvgLen = if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
      Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
    } else {
      None
    }
    val newStats = ColumnStat(newNdv, min, max, Some(0), newAvgLen, newMaxLen)

    (ceil(card), newStats)
  }

  /** Compute join cardinality using equi-height histograms. */
  private def computeByHistogram(
      leftKey: AttributeReference,
      rightKey: AttributeReference,
      leftHistogram: Histogram,
      rightHistogram: Histogram,
      newMin: Option[Any],
      newMax: Option[Any]): (BigInt, ColumnStat) = {
    val overlappedRanges = getOverlappedRanges(
      leftHistogram = leftHistogram,
      rightHistogram = rightHistogram,
      // Only numeric values have equi-height histograms.
      lowerBound = newMin.get.toString.toDouble,
      upperBound = newMax.get.toString.toDouble)

    var card: BigDecimal = 0
    var totalNdv: Double = 0
    for (i <- overlappedRanges.indices) {
      val range = overlappedRanges(i)
      if (i == 0 || range.hi != overlappedRanges(i - 1).hi) {
        // If range.hi == overlappedRanges(i - 1).hi, that means the current range has only one
        // value, and this value is already counted in the previous range. So there is no need to
        // count it in this range.
        totalNdv += math.min(range.leftNdv, range.rightNdv)
      }
      // Apply the formula in this overlapped range.
      card += range.leftNumRows * range.rightNumRows / math.max(range.leftNdv, range.rightNdv)
    }

    val leftKeyStat = leftStats.attributeStats(leftKey)
    val rightKeyStat = rightStats.attributeStats(rightKey)
    val newMaxLen = if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
      Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
    } else {
      None
    }
    val newAvgLen = if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
      Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
    } else {
      None
    }
    val newStats = ColumnStat(Some(ceil(totalNdv)), newMin, newMax, Some(0), newAvgLen, newMaxLen)
    (ceil(card), newStats)
  }

  /**
   * Propagate or update column stats for output attributes.
   */
  private def updateOutputStats(
      outputRows: BigInt,
      output: Seq[Attribute],
      oldAttrStats: AttributeMap[ColumnStat],
      keyStatsAfterJoin: AttributeMap[ColumnStat]): Seq[(Attribute, ColumnStat)] = {
    val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()
    val leftRows = leftStats.rowCount.get
    val rightRows = rightStats.rowCount.get

    output.foreach { a =>
      // check if this attribute is a join key
      if (keyStatsAfterJoin.contains(a)) {
        outputAttrStats += a -> keyStatsAfterJoin(a)
      } else {
        val oldColStat = oldAttrStats(a)
        val oldNumRows = if (join.left.outputSet.contains(a)) {
          leftRows
        } else {
          rightRows
        }
        val newColStat = oldColStat.updateCountStats(oldNumRows, outputRows)
        // TODO: support nullCount updates for specific outer joins
        outputAttrStats += a -> newColStat
      }
    }
    outputAttrStats.toSeq
  }

  private def extractJoinKeysWithColStats(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[(AttributeReference, AttributeReference)] = {
    leftKeys.zip(rightKeys).collect {
      // Currently we don't deal with equal joins like key1 = key2 + 5.
      // Note: join keys from EqualNullSafe also fall into this case (Coalesce), consider to
      // support it in the future by using `nullCount` in column stats.
      case (lk: AttributeReference, rk: AttributeReference)
        if columnStatsWithCountsExist((leftStats, lk), (rightStats, rk)) => (lk, rk)
    }
  }

  private def estimateLeftSemiAntiJoin(): Option[Statistics] = {
    // TODO: It's error-prone to estimate cardinalities for LeftSemi and LeftAnti based on basic
    // column stats. Now we just propagate the statistics from left side. We should do more
    // accurate estimation when advanced stats (e.g. histograms) are available.
    if (rowCountsExist(join.left)) {
      val leftStats = join.left.stats
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
