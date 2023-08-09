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

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, EmptyRow, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DecimalType, _}

object EstimationUtils {

  /** Check if each plan has rowCount in its statistics. */
  def rowCountsExist(plans: LogicalPlan*): Boolean =
    plans.forall(_.stats.rowCount.isDefined)

  /** Check if each attribute has column stat in the corresponding statistics. */
  def columnStatsExist(statsAndAttr: (Statistics, Attribute)*): Boolean = {
    statsAndAttr.forall { case (stats, attr) =>
      stats.attributeStats.contains(attr)
    }
  }

  /** Check if each attribute has column stat containing distinct and null counts
   *  in the corresponding statistic. */
  def columnStatsWithCountsExist(statsAndAttr: (Statistics, Attribute)*): Boolean = {
    statsAndAttr.forall { case (stats, attr) =>
      stats.attributeStats.get(attr).map(_.hasCountStats).getOrElse(false)
    }
  }

  /** Statistics for a Column containing only NULLs. */
  def nullColumnStat(dataType: DataType, rowCount: BigInt): ColumnStat = {
    ColumnStat(distinctCount = Some(0), min = None, max = None, nullCount = Some(rowCount),
      avgLen = Some(dataType.defaultSize), maxLen = Some(dataType.defaultSize))
  }

  /**
   * Updates (scales down) a statistic (eg. number of distinct values) if the number of rows
   * decreases after some operation (such as filter, join). Otherwise keep it unchanged.
   */
  def updateStat(
      oldNumRows: BigInt,
      newNumRows: BigInt,
      oldStatOpt: Option[BigInt],
      updatedStatOpt: Option[BigInt]): Option[BigInt] = {
    if (oldStatOpt.isDefined && updatedStatOpt.isDefined && updatedStatOpt.get > 1 &&
      newNumRows < oldNumRows) {
        // no need to scale down since it is already down to 1
        Some(ceil(BigDecimal(oldStatOpt.get) * BigDecimal(newNumRows) / BigDecimal(oldNumRows)))
    } else {
      updatedStatOpt
    }
  }

  def ceil(bigDecimal: BigDecimal): BigInt = bigDecimal.setScale(0, RoundingMode.CEILING).toBigInt

  /** Get column stats for output attributes. */
  def getOutputMap(inputMap: AttributeMap[ColumnStat], output: Seq[Attribute])
    : AttributeMap[ColumnStat] = {
    AttributeMap(output.flatMap(a => inputMap.get(a).map(a -> _)))
  }

  /**
   * Returns the stats for aliases of child's attributes
   */
  def getAliasStats(
      expressions: Seq[Expression],
      attributeStats: AttributeMap[ColumnStat],
      rowCount: BigInt): Seq[(Attribute, ColumnStat)] = {
    expressions.collect {
      case alias @ Alias(attr: Attribute, _) if attributeStats.contains(attr) =>
        alias.toAttribute -> attributeStats(attr)
      case alias @ Alias(expr: Expression, _) if expr.foldable && expr.deterministic =>
        val value = expr.eval(EmptyRow)
        val size = expr.dataType.defaultSize
        val columnStat = if (value == null) {
          ColumnStat(Some(0), None, None, Some(rowCount), Some(size), Some(size), None, 2)
        } else {
          ColumnStat(Some(1), Some(value), Some(value), Some(0), Some(size), Some(size), None, 2)
        }
        alias.toAttribute -> columnStat
    }
  }

  def getSizePerRow(
      attributes: Seq[Attribute],
      attrStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): BigInt = {
    // We assign a generic overhead for a Row object, the actual overhead is different for different
    // Row format.
    8 + attributes.map { attr =>
      if (attrStats.get(attr).map(_.avgLen.isDefined).getOrElse(false)) {
        attr.dataType match {
          case StringType =>
            // UTF8String: base + offset + numBytes
            attrStats(attr).avgLen.get + 8 + 4
          case _ =>
            attrStats(attr).avgLen.get
        }
      } else {
        attr.dataType.defaultSize
      }
    }.sum
  }

  def getOutputSize(
      attributes: Seq[Attribute],
      outputRowCount: BigInt,
      attrStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): BigInt = {
    // Output size can't be zero, or sizeInBytes of BinaryNode will also be zero
    // (simple computation of statistics returns product of children).
    if (outputRowCount > 0) outputRowCount * getSizePerRow(attributes, attrStats) else 1
  }

  /**
   * For simplicity we use Double to unify operations for data types whose min/max values can be
   * represented as numbers, e.g. Boolean can be represented as 0 (false) or 1 (true).
   * The two methods below are the contract of conversion.
   */
  def toDouble(value: Any, dataType: DataType): Double = {
    dataType match {
      case _: NumericType | DateType | TimestampType => value.toString.toDouble
      case BooleanType => if (value.asInstanceOf[Boolean]) 1 else 0
    }
  }

  def fromDouble(double: Double, dataType: DataType): Any = {
    dataType match {
      case BooleanType => double.toInt == 1
      case DateType => double.toInt
      case TimestampType => double.toLong
      case ByteType => double.toByte
      case ShortType => double.toShort
      case IntegerType => double.toInt
      case LongType => double.toLong
      case FloatType => double.toFloat
      case DoubleType => double
      case _: DecimalType => Decimal(double)
    }
  }

  /**
   * Returns the index of the first bin into which the given value falls for a specified
   * numeric equi-height histogram.
   */
  private def findFirstBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var i = 0
    while ((i < bins.length) && (value > bins(i).hi)) {
      i += 1
    }
    i
  }

  /**
   * Returns the index of the last bin into which the given value falls for a specified
   * numeric equi-height histogram.
   */
  private def findLastBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var i = bins.length - 1
    while ((i >= 0) && (value < bins(i).lo)) {
      i -= 1
    }
    i
  }

  /**
   * Returns the possibility of the given histogram bin holding values within the given range
   * [lowerBound, upperBound].
   */
  private def binHoldingRangePossibility(
      upperBound: Double,
      lowerBound: Double,
      bin: HistogramBin): Double = {
    assert(bin.lo <= lowerBound && lowerBound <= upperBound && upperBound <= bin.hi)
    if (bin.hi == bin.lo) {
      // the entire bin is covered in the range
      1.0
    } else if (upperBound == lowerBound) {
      // set percentage to 1/NDV
      1.0 / bin.ndv.toDouble
    } else {
      // Use proration since the range falls inside this bin.
      math.min((upperBound - lowerBound) / (bin.hi - bin.lo), 1.0)
    }
  }

  /**
   * Returns the number of histogram bins holding values within the given range
   * [lowerBound, upperBound].
   *
   * Note that the returned value is double type, because the range boundaries usually occupy a
   * portion of a bin. An extreme case is [value, value] which is generated by equal predicate
   * `col = value`, we can get higher accuracy by allowing returning portion of histogram bins.
   *
   * @param upperBound the highest value of the given range
   * @param upperBoundInclusive whether the upperBound is included in the range
   * @param lowerBound the lowest value of the given range
   * @param lowerBoundInclusive whether the lowerBound is included in the range
   * @param bins an array of bins for a given numeric equi-height histogram
   */
  def numBinsHoldingRange(
      upperBound: Double,
      upperBoundInclusive: Boolean,
      lowerBound: Double,
      lowerBoundInclusive: Boolean,
      bins: Array[HistogramBin]): Double = {
    assert(bins.head.lo <= lowerBound && lowerBound <= upperBound && upperBound <= bins.last.hi,
      "Given range does not fit in the given histogram.")
    assert(upperBound != lowerBound || upperBoundInclusive || lowerBoundInclusive,
      s"'$lowerBound < value < $upperBound' is an invalid range.")

    val upperBinIndex = if (upperBoundInclusive) {
      findLastBinForValue(upperBound, bins)
    } else {
      findFirstBinForValue(upperBound, bins)
    }
    val lowerBinIndex = if (lowerBoundInclusive) {
      findFirstBinForValue(lowerBound, bins)
    } else {
      findLastBinForValue(lowerBound, bins)
    }
    assert(lowerBinIndex <= upperBinIndex, "Invalid histogram data.")


    if (lowerBinIndex == upperBinIndex) {
      binHoldingRangePossibility(upperBound, lowerBound, bins(lowerBinIndex))
    } else {
      // Computes the occupied portion of bins of the upperBound and lowerBound.
      val lowerBin = bins(lowerBinIndex)
      val lowerPart = binHoldingRangePossibility(lowerBin.hi, lowerBound, lowerBin)

      val higherBin = bins(upperBinIndex)
      val higherPart = binHoldingRangePossibility(upperBound, higherBin.lo, higherBin)

      // The total number of bins is lowerPart + higherPart + bins between them
      lowerPart + higherPart + upperBinIndex - lowerBinIndex - 1
    }
  }

  /**
   * Returns overlapped ranges between two histograms, in the given value range
   * [lowerBound, upperBound].
   */
  def getOverlappedRanges(
    leftHistogram: Histogram,
    rightHistogram: Histogram,
    lowerBound: Double,
    upperBound: Double): Seq[OverlappedRange] = {
    val overlappedRanges = new ArrayBuffer[OverlappedRange]()
    // Only bins whose range intersect [lowerBound, upperBound] have join possibility.
    val leftBins = leftHistogram.bins
      .filter(b => b.lo <= upperBound && b.hi >= lowerBound)
    val rightBins = rightHistogram.bins
      .filter(b => b.lo <= upperBound && b.hi >= lowerBound)

    leftBins.foreach { lb =>
      rightBins.foreach { rb =>
        val (left, leftHeight) = trimBin(lb, leftHistogram.height, lowerBound, upperBound)
        val (right, rightHeight) = trimBin(rb, rightHistogram.height, lowerBound, upperBound)
        // Only collect overlapped ranges.
        if (left.lo <= right.hi && left.hi >= right.lo) {
          // Collect overlapped ranges.
          val range = if (right.lo >= left.lo && right.hi >= left.hi) {
            // Case1: the left bin is "smaller" than the right bin
            //      left.lo            right.lo     left.hi          right.hi
            // --------+------------------+------------+----------------+------->
            if (left.hi == right.lo) {
              // The overlapped range has only one value.
              OverlappedRange(
                lo = right.lo,
                hi = right.lo,
                leftNdv = 1,
                rightNdv = 1,
                leftNumRows = leftHeight / left.ndv,
                rightNumRows = rightHeight / right.ndv
              )
            } else {
              val leftRatio = (left.hi - right.lo) / (left.hi - left.lo)
              val rightRatio = (left.hi - right.lo) / (right.hi - right.lo)
              OverlappedRange(
                lo = right.lo,
                hi = left.hi,
                leftNdv = left.ndv * leftRatio,
                rightNdv = right.ndv * rightRatio,
                leftNumRows = leftHeight * leftRatio,
                rightNumRows = rightHeight * rightRatio
              )
            }
          } else if (right.lo <= left.lo && right.hi <= left.hi) {
            // Case2: the left bin is "larger" than the right bin
            //      right.lo           left.lo      right.hi         left.hi
            // --------+------------------+------------+----------------+------->
            if (right.hi == left.lo) {
              // The overlapped range has only one value.
              OverlappedRange(
                lo = right.hi,
                hi = right.hi,
                leftNdv = 1,
                rightNdv = 1,
                leftNumRows = leftHeight / left.ndv,
                rightNumRows = rightHeight / right.ndv
              )
            } else {
              val leftRatio = (right.hi - left.lo) / (left.hi - left.lo)
              val rightRatio = (right.hi - left.lo) / (right.hi - right.lo)
              OverlappedRange(
                lo = left.lo,
                hi = right.hi,
                leftNdv = left.ndv * leftRatio,
                rightNdv = right.ndv * rightRatio,
                leftNumRows = leftHeight * leftRatio,
                rightNumRows = rightHeight * rightRatio
              )
            }
          } else if (right.lo >= left.lo && right.hi <= left.hi) {
            // Case3: the left bin contains the right bin
            //      left.lo            right.lo     right.hi         left.hi
            // --------+------------------+------------+----------------+------->
            val leftRatio = (right.hi - right.lo) / (left.hi - left.lo)
            OverlappedRange(
              lo = right.lo,
              hi = right.hi,
              leftNdv = left.ndv * leftRatio,
              rightNdv = right.ndv,
              leftNumRows = leftHeight * leftRatio,
              rightNumRows = rightHeight
            )
          } else {
            assert(right.lo <= left.lo && right.hi >= left.hi)
            // Case4: the right bin contains the left bin
            //      right.lo           left.lo      left.hi          right.hi
            // --------+------------------+------------+----------------+------->
            val rightRatio = (left.hi - left.lo) / (right.hi - right.lo)
            OverlappedRange(
              lo = left.lo,
              hi = left.hi,
              leftNdv = left.ndv,
              rightNdv = right.ndv * rightRatio,
              leftNumRows = leftHeight,
              rightNumRows = rightHeight * rightRatio
            )
          }
          overlappedRanges += range
        }
      }
    }
    overlappedRanges.toSeq
  }

  /**
   * Given an original bin and a value range [lowerBound, upperBound], returns the trimmed part
   * of the bin in that range and its number of rows.
   * @param bin the input histogram bin.
   * @param height the number of rows of the given histogram bin inside an equi-height histogram.
   * @param lowerBound lower bound of the given range.
   * @param upperBound upper bound of the given range.
   * @return trimmed part of the given bin and its number of rows.
   */
  def trimBin(bin: HistogramBin, height: Double, lowerBound: Double, upperBound: Double)
  : (HistogramBin, Double) = {
    val (lo, hi) = if (bin.lo <= lowerBound && bin.hi >= upperBound) {
      //       bin.lo          lowerBound     upperBound      bin.hi
      // --------+------------------+------------+-------------+------->
      (lowerBound, upperBound)
    } else if (bin.lo <= lowerBound && bin.hi >= lowerBound) {
      //       bin.lo          lowerBound      bin.hi      upperBound
      // --------+------------------+------------+-------------+------->
      (lowerBound, bin.hi)
    } else if (bin.lo <= upperBound && bin.hi >= upperBound) {
      //    lowerBound            bin.lo     upperBound       bin.hi
      // --------+------------------+------------+-------------+------->
      (bin.lo, upperBound)
    } else {
      //    lowerBound            bin.lo        bin.hi     upperBound
      // --------+------------------+------------+-------------+------->
      assert(bin.lo >= lowerBound && bin.hi <= upperBound)
      (bin.lo, bin.hi)
    }

    if (hi == lo) {
      // Note that bin.hi == bin.lo also falls into this branch.
      (HistogramBin(lo, hi, 1), height / bin.ndv)
    } else {
      assert(bin.hi != bin.lo)
      val ratio = (hi - lo) / (bin.hi - bin.lo)
      (HistogramBin(lo, hi, math.ceil(bin.ndv * ratio).toLong), height * ratio)
    }
  }

  /**
   * A join between two equi-height histograms may produce multiple overlapped ranges.
   * Each overlapped range is produced by a part of one bin in the left histogram and a part of
   * one bin in the right histogram.
   * @param lo lower bound of this overlapped range.
   * @param hi higher bound of this overlapped range.
   * @param leftNdv ndv in the left part.
   * @param rightNdv ndv in the right part.
   * @param leftNumRows number of rows in the left part.
   * @param rightNumRows number of rows in the right part.
   */
  case class OverlappedRange(
    lo: Double,
    hi: Double,
    leftNdv: Double,
    rightNdv: Double,
    leftNumRows: Double,
    rightNumRows: Double)
}
