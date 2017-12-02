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
   * Returns the number of the first bin into which a column values falls for a specified
   * numeric equi-height histogram.
   *
   * @param value a literal value of a column
   * @param bins an array of bins for a given numeric equi-height histogram
   * @return the number of the first bin into which a column values falls.
   */
  def findFirstBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var binId = 0
    bins.foreach { bin =>
      if (value > bin.hi) binId += 1
    }
    binId
  }

  /**
   * Returns the number of the last bin into which a column values falls for a specified
   * numeric equi-height histogram.
   *
   * @param value a literal value of a column
   * @param bins an array of bins for a given numeric equi-height histogram
   * @return the number of the last bin into which a column values falls.
   */
  def findLastBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var binId = 0
    for (i <- bins.indices) {
      if (value > bins(i).hi) {
        // increment binId to point to next bin
        binId += 1
      }
      if ((value == bins(i).hi) && (i < bins.length - 1) && (value == bins(i + 1).lo)) {
        // We assume the above 3 conditions will be evaluated from left to right sequentially.
        // If the above 3 conditions are evaluated out-of-order, then out-of-bound error may happen.
        // At that time, we should split the third condition into another if statement.
        // increment binId since the value appears in this bin and next bin
        binId += 1
      }
    }
    binId
  }

  /**
   * Returns a percentage of a bin holding values for column value in the range of
   * [lowerValue, higherValue]
   *
   * @param binId a given bin id in a specified histogram
   * @param higherValue a given upper bound value of a specified column value range
   * @param lowerValue a given lower bound value of a specified column value range
   * @param histogram a numeric equi-height histogram
   * @return the percentage of a single bin holding values in [lowerValue, higherValue].
   */
  private def getOccupation(
      binId: Int,
      higherValue: Double,
      lowerValue: Double,
      histogram: Histogram): Double = {
    val curBin = histogram.bins(binId)
    if (binId == 0 && curBin.hi == curBin.lo) {
      // the Min of the histogram occupies the whole first bin
      1.0
    } else if (binId == 0 && curBin.hi != curBin.lo) {
      if (higherValue == lowerValue) {
        // set percentage to 1/NDV
        1.0 / curBin.ndv.toDouble
      } else {
        // Use proration since the range falls inside this bin.
        (higherValue - lowerValue) / (curBin.hi - curBin.lo)
      }
    } else {
      if (curBin.hi == curBin.lo) {
        // the entire bin is covered in the range
        1.0
      } else if (higherValue == lowerValue) {
        // set percentage to 1/NDV
        1.0 / curBin.ndv.toDouble
      } else {
        // Use proration since the range falls inside this bin.
        math.min((higherValue - lowerValue) / (curBin.hi - curBin.lo), 1.0)
      }
    }
  }

  /**
   * Returns the number of bins for column values in [lowerValue, higherValue].
   * The column value distribution is saved in an equi-height histogram.
   *
   * @param higherEnd a given upper bound value of a specified column value range
   * @param lowerEnd a given lower bound value of a specified column value range
   * @param histogram a numeric equi-height histogram
   * @return the selectivity percentage for column values in [lowerValue, higherValue].
   */
  def getOccupationBins(
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Histogram): Double = {
    // find bins where current min and max locate
    val lowerBinId = findFirstBinForValue(lowerEnd, histogram.bins)
    val higherBinId = findLastBinForValue(higherEnd, histogram.bins)
    assert(lowerBinId <= higherBinId)

    // compute how much current [lowerEnd, higherEnd] range occupies the histogram in the
    // number of bins
    getOccupationBins(higherBinId, lowerBinId, higherEnd, lowerEnd, histogram)
  }

  /**
   * Returns the number of bins for column values in [lowerValue, higherValue].
   * This is an overloaded method. The column value distribution is saved in an
   * equi-height histogram.
   *
   * @param higherId id of the high end bin holding the high end value of a column range
   * @param lowerId id of the low end bin holding the low end value of a column range
   * @param higherEnd a given upper bound value of a specified column value range
   * @param lowerEnd a given lower bound value of a specified column value range
   * @param histogram a numeric equi-height histogram
   * @return the selectivity percentage for column values in [lowerEnd, higherEnd].
   */
  def getOccupationBins(
      higherId: Int,
      lowerId: Int,
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Histogram): Double = {
    if (lowerId == higherId) {
      getOccupation(lowerId, higherEnd, lowerEnd, histogram)
    } else {
      // compute how much lowerEnd/higherEnd occupies its bin
      val lowercurBin = histogram.bins(lowerId)
      val lowerPart = getOccupation(lowerId, lowercurBin.hi, lowerEnd, histogram)

      // in case higherId > lowerId, higherId must be > 0
      val highercurBin = histogram.bins(higherId)
      val higherPart = getOccupation(higherId, higherEnd, highercurBin.lo,
        histogram)
      // the total length is lowerPart + higherPart + bins between them
      higherId - lowerId - 1 + lowerPart + higherPart
    }
  }

  /**
   * Returns the number of distinct values, ndv, for column values in [lowerEnd, higherEnd].
   * The column value distribution is saved in an equi-height histogram.
   *
   * @param higherId id of the high end bin holding the high end value of a column range
   * @param lowerId id of the low end bin holding the low end value of a column range
   * @param higherEnd a given upper bound value of a specified column value range
   * @param lowerEnd a given lower bound value of a specified column value range
   * @param histogram a numeric equi-height histogram
   * @return the number of distinct values, ndv, for column values in [lowerEnd, higherEnd].
   */
  def getOccupationNdv(
      higherId: Int,
      lowerId: Int,
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Histogram)
    : Long = {
    val ndv: Double = if (higherEnd == lowerEnd) {
      1
    } else if (lowerId == higherId) {
      getOccupation(lowerId, higherEnd, lowerEnd, histogram) * histogram.bins(lowerId).ndv
    } else {
      // compute how much the [lowerEnd, higherEnd] range occupies the bins in a histogram.
      // Our computation has 3 parts: the smallest/min bin, the middle bins, the largest/max bin.
      val minCurBin = histogram.bins(lowerId)
      val minPartNdv = getOccupation(lowerId, minCurBin.hi, lowerEnd, histogram) *
        minCurBin.ndv

      // in case higherId > lowerId, higherId must be > 0
      val maxCurBin = histogram.bins(higherId)
      val maxPartNdv = getOccupation(higherId, higherEnd, maxCurBin.lo, histogram) *
        maxCurBin.ndv

      // The total ndv is minPartNdv + maxPartNdv + Ndvs between them.
      // In order to avoid counting same distinct value twice, we check if the upperBound value
      // of next bin is equal to the hi value of the previous bin.  We bump up
      // ndv value only if the hi values of two consecutive bins are different.
      var middleNdv: Long = 0
      for (i <- histogram.bins.indices) {
        val bin = histogram.bins(i)
        if (bin.hi != bin.lo && i >= lowerId + 1 && i <= higherId - 1) {
          middleNdv += bin.ndv
        }
      }
      minPartNdv + maxPartNdv + middleNdv
    }
    math.round(ndv)
  }

}
