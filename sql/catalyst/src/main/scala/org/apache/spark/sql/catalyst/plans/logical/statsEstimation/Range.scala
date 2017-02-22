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

import java.math.{BigDecimal => JDecimal}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{BooleanType, DateType, TimestampType, _}


/** Value range of a column. */
trait Range

/** For simplicity we use decimal to unify operations of numeric ranges. */
case class NumericRange(min: JDecimal, max: JDecimal) extends Range

/**
 * This version of Spark does not have min/max for binary/string types, we define their default
 * behaviors by this class.
 */
class DefaultRange extends Range

/** This is for columns with only null values. */
class NullRange extends Range

object Range {
  def apply(min: Option[Any], max: Option[Any], dataType: DataType): Range = dataType match {
    case StringType | BinaryType => new DefaultRange()
    case _ if min.isEmpty || max.isEmpty => new NullRange()
    case _ => toNumericRange(min.get, max.get, dataType)
  }

  def isIntersected(r1: Range, r2: Range): Boolean = (r1, r2) match {
    case (_, _: DefaultRange) | (_: DefaultRange, _) =>
      // The DefaultRange represents string/binary types which do not have max/min stats,
      // we assume they are intersected to be conservative on estimation
      true
    case (_, _: NullRange) | (_: NullRange, _) =>
      false
    case (n1: NumericRange, n2: NumericRange) =>
      n1.min.compareTo(n2.max) <= 0 && n1.max.compareTo(n2.min) >= 0
  }

  /**
   * Intersected results of two ranges. This is only for two overlapped ranges.
   * The outputs are the intersected min/max values.
   */
  def intersect(r1: Range, r2: Range, dt: DataType): (Option[Any], Option[Any]) = {
    (r1, r2) match {
      case (_, _: DefaultRange) | (_: DefaultRange, _) =>
        // binary/string types don't support intersecting.
        (None, None)
      case (n1: NumericRange, n2: NumericRange) =>
        val newRange = NumericRange(n1.min.max(n2.min), n1.max.min(n2.max))
        val (newMin, newMax) = fromNumericRange(newRange, dt)
        (Some(newMin), Some(newMax))
    }
  }

  /**
   * For simplicity we use decimal to unify operations of numeric types, the two methods below
   * are the contract of conversion.
   */
  private def toNumericRange(min: Any, max: Any, dataType: DataType): NumericRange = {
    dataType match {
      case _: NumericType =>
        NumericRange(new JDecimal(min.toString), new JDecimal(max.toString))
      case BooleanType =>
        val min1 = if (min.asInstanceOf[Boolean]) 1 else 0
        val max1 = if (max.asInstanceOf[Boolean]) 1 else 0
        NumericRange(new JDecimal(min1), new JDecimal(max1))
      case DateType =>
        val min1 = DateTimeUtils.fromJavaDate(min.asInstanceOf[Date])
        val max1 = DateTimeUtils.fromJavaDate(max.asInstanceOf[Date])
        NumericRange(new JDecimal(min1), new JDecimal(max1))
      case TimestampType =>
        val min1 = DateTimeUtils.fromJavaTimestamp(min.asInstanceOf[Timestamp])
        val max1 = DateTimeUtils.fromJavaTimestamp(max.asInstanceOf[Timestamp])
        NumericRange(new JDecimal(min1), new JDecimal(max1))
    }
  }

  private def fromNumericRange(n: NumericRange, dataType: DataType): (Any, Any) = {
    dataType match {
      case _: IntegralType =>
        (n.min.longValue(), n.max.longValue())
      case FloatType | DoubleType =>
        (n.min.doubleValue(), n.max.doubleValue())
      case _: DecimalType =>
        (n.min, n.max)
      case BooleanType =>
        (n.min.longValue() == 1, n.max.longValue() == 1)
      case DateType =>
        (DateTimeUtils.toJavaDate(n.min.intValue()), DateTimeUtils.toJavaDate(n.max.intValue()))
      case TimestampType =>
        (DateTimeUtils.toJavaTimestamp(n.min.longValue()),
          DateTimeUtils.toJavaTimestamp(n.max.longValue()))
    }
  }
}
