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
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, _}


object EstimationUtils {

  /** Check if each plan has rowCount in its statistics. */
  def rowCountsExist(conf: SQLConf, plans: LogicalPlan*): Boolean =
    plans.forall(_.stats(conf).rowCount.isDefined)

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

}
