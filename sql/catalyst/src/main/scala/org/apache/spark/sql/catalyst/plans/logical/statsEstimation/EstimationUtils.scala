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
   * Returns the number of the first bin into which a column value falls for a specified
   * numeric equi-height histogram.
   *
   * @param value a literal value of a column
   * @param bins an array of bins for a given numeric equi-height histogram
   * @return the id of the first bin into which a column value falls.
   */
  def findFirstBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var i = 0
    while ((i < bins.length) && (value > bins(i).hi)) {
      i += 1
    }
    i
  }

  /**
   * Returns the number of the last bin into which a column value falls for a specified
   * numeric equi-height histogram.
   *
   * @param value a literal value of a column
   * @param bins an array of bins for a given numeric equi-height histogram
   * @return the id of the last bin into which a column value falls.
   */
  def findLastBinForValue(value: Double, bins: Array[HistogramBin]): Int = {
    var i = bins.length - 1
    while ((i >= 0) && (value < bins(i).lo)) {
      i -= 1
    }
    i
  }

  /**
   * Returns a percentage of a bin holding values for column value in the range of
   * [lowerValue, higherValue]
   *
   * @param higherValue a given upper bound value of a specified column value range
   * @param lowerValue a given lower bound value of a specified column value range
   * @param bin a single histogram bin
   * @return the percentage of a single bin holding values in [lowerValue, higherValue].
   */
  private def getOccupation(
      higherValue: Double,
      lowerValue: Double,
      bin: HistogramBin): Double = {
    assert(bin.lo <= lowerValue && lowerValue <= higherValue && higherValue <= bin.hi)
    if (bin.hi == bin.lo) {
      // the entire bin is covered in the range
      1.0
    } else if (higherValue == lowerValue) {
      // set percentage to 1/NDV
      1.0 / bin.ndv.toDouble
    } else {
      // Use proration since the range falls inside this bin.
      math.min((higherValue - lowerValue) / (bin.hi - bin.lo), 1.0)
    }
  }

  /**
   * Returns the number of bins for column values in [lowerValue, higherValue].
   * The column value distribution is saved in an equi-height histogram.  The return values is a
   * double value is because we may return a portion of a bin. For example, a predicate
   * "column = 8" may return the number of bins 0.2 if the holding bin has 5 distinct values.
   *
   * @param higherId id of the high end bin holding the high end value of a column range
   * @param lowerId id of the low end bin holding the low end value of a column range
   * @param higherEnd a given upper bound value of a specified column value range
   * @param lowerEnd a given lower bound value of a specified column value range
   * @param histogram a numeric equi-height histogram
   * @return the number of bins for column values in [lowerEnd, higherEnd].
   */
  def getOccupationBins(
      higherId: Int,
      lowerId: Int,
      higherEnd: Double,
      lowerEnd: Double,
      histogram: Histogram): Double = {
    assert(lowerId <= higherId)

    if (lowerId == higherId) {
      val curBin = histogram.bins(lowerId)
      getOccupation(higherEnd, lowerEnd, curBin)
    } else {
      // compute how much lowerEnd/higherEnd occupies its bin
      val lowerCurBin = histogram.bins(lowerId)
      val lowerPart = getOccupation(lowerCurBin.hi, lowerEnd, lowerCurBin)

      val higherCurBin = histogram.bins(higherId)
      val higherPart = getOccupation(higherEnd, higherCurBin.lo, higherCurBin)

      // the total length is lowerPart + higherPart + bins between them
      lowerPart + higherPart + higherId - lowerId - 1
    }
  }

}
