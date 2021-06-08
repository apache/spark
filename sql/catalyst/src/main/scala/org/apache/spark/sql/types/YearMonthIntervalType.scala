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

package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}

/**
 * The type represents year-month intervals of the SQL standard. A year-month interval is made up
 * of a contiguous subset of the following fields:
 *   - MONTH, months within years [0..11],
 *   - YEAR, years in the range [0..178956970].
 *
 * `YearMonthIntervalType` represents positive as well as negative year-month intervals.
 *
 * Please use `DataTypes.createYearMonthIntervalType()` to create a specific instance.
 *
 * @param start The start unit which the year-month interval type comprises from.
 *              Valid values: 0 (YEAR) and 1 (MONTH).
 * @param end The end unit of the interval type.
 *            Valid values: 0 (YEAR) and 1 (MONTH).
 *
 * @since 3.2.0
 */
@Unstable
case class YearMonthIntervalType(start: Byte, end: Byte) extends AtomicType {
  // default constructor for Java
  def this() = this(YEAR, MONTH)

  /**
   * Internally, values of year-month intervals are stored in `Int` values as amount of months
   * that are calculated by the formula:
   *   -/+ (12 * YEAR + MONTH)
   */
  private[sql] type InternalType = Int

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * Year-month interval values always occupy 4 bytes.
   * The YEAR field is constrained by the upper bound 178956970 to fit to `Int`.
   */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: YearMonthIntervalType = this

  override val typeName: String = (start, end) match {
    case (YEAR, MONTH) => "interval year to month"
    case (YEAR, YEAR) => "interval year"
    case (MONTH, MONTH) => "interval month"
    case _ => throw new AnalysisException(
      s"""Invalid interval units: ($start, $end). Supported units:
         | (0, 0) - interval year
         | (0, 1) - interval year to month
         | (1, 1) - interval month
         |""".stripMargin)
  }
}

/**
 * Extra factory methods and pattern matchers for the year-month interval type.
 *
 * @since 3.2.0
 */
@Unstable
object YearMonthIntervalType extends AbstractDataType {
  val YEAR: Byte = 0
  val MONTH: Byte = 1

  def apply(): YearMonthIntervalType = new YearMonthIntervalType()

  override private[sql] def defaultConcreteType: DataType = new YearMonthIntervalType()

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[YearMonthIntervalType]
  }

  override private[sql] def simpleString: String = defaultConcreteType.simpleString
}
