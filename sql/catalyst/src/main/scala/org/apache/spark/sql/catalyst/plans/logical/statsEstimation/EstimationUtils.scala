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

import scala.math.BigDecimal.RoundingMode

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
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

  def nullColumnStat(dataType: DataType, rowCount: BigInt): ColumnStat = {
    ColumnStat(distinctCount = 0, min = None, max = None, nullCount = rowCount,
      avgLen = dataType.defaultSize, maxLen = dataType.defaultSize)
  }

  /**
   * Updates (scales down) the number of distinct values if the number of rows decreases after
   * some operation (such as filter, join). Otherwise keep it unchanged.
   */
  def updateNdv(oldNumRows: BigInt, newNumRows: BigInt, oldNdv: BigInt): BigInt = {
    if (newNumRows < oldNumRows) {
      ceil(BigDecimal(oldNdv) * BigDecimal(newNumRows) / BigDecimal(oldNumRows))
    } else {
      oldNdv
    }
  }

  def ceil(bigDecimal: BigDecimal): BigInt = bigDecimal.setScale(0, RoundingMode.CEILING).toBigInt()

  /** Get column stats for output attributes. */
  def getOutputMap(inputMap: AttributeMap[ColumnStat], output: Seq[Attribute])
    : AttributeMap[ColumnStat] = {
    AttributeMap(output.flatMap(a => inputMap.get(a).map(a -> _)))
  }

  def getOutputSize(
      attributes: Seq[Attribute],
      outputRowCount: BigInt,
      attrStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): BigInt = {
    // We assign a generic overhead for a Row object, the actual overhead is different for different
    // Row format.
    val sizePerRow = 8 + attributes.map { attr =>
      if (attrStats.contains(attr)) {
        attr.dataType match {
          case StringType =>
            // UTF8String: base + offset + numBytes
            attrStats(attr).avgLen + 8 + 4
          case _ =>
            attrStats(attr).avgLen
        }
      } else {
        attr.dataType.defaultSize
      }
    }.sum

    // Output size can't be zero, or sizeInBytes of BinaryNode will also be zero
    // (simple computation of statistics returns product of children).
    if (outputRowCount > 0) outputRowCount * sizePerRow else 1
  }

  /**
   * For simplicity we use Decimal to unify operations for data types whose min/max values can be
   * represented as numbers, e.g. Boolean can be represented as 0 (false) or 1 (true).
   * The two methods below are the contract of conversion.
   */
  def toDecimal(value: Any, dataType: DataType): Decimal = {
    dataType match {
      case _: NumericType | DateType | TimestampType => Decimal(value.toString)
      case BooleanType => if (value.asInstanceOf[Boolean]) Decimal(1) else Decimal(0)
    }
  }

  def fromDecimal(dec: Decimal, dataType: DataType): Any = {
    dataType match {
      case BooleanType => dec.toLong == 1
      case DateType => dec.toInt
      case TimestampType => dec.toLong
      case ByteType => dec.toByte
      case ShortType => dec.toShort
      case IntegerType => dec.toInt
      case LongType => dec.toLong
      case FloatType => dec.toFloat
      case DoubleType => dec.toDouble
      case _: DecimalType => dec
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
   * portion of a bin. An extrema case is [value, value] which is generated by equal predicate
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

}
