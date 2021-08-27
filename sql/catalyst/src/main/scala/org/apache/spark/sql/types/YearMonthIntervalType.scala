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
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.YearMonthIntervalType.fieldToString

/**
 * The type represents year-month intervals of the SQL standard. A year-month interval is made up
 * of a contiguous subset of the following fields:
 *   - MONTH, months within years [0..11],
 *   - YEAR, years in the range [0..178956970].
 *
 * `YearMonthIntervalType` represents positive as well as negative year-month intervals.
 *
 * @param startField The leftmost field which the type comprises of. Valid values:
 *                   0 (YEAR), 1 (MONTH).
 * @param endField The rightmost field which the type comprises of. Valid values:
 *                 0 (YEAR), 1 (MONTH).
 *
 * @since 3.2.0
 */
@Unstable
case class YearMonthIntervalType(startField: Byte, endField: Byte) extends AnsiIntervalType {
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

  override val typeName: String = {
    val startFieldName = fieldToString(startField)
    val endFieldName = fieldToString(endField)
    if (startFieldName == endFieldName) {
      s"interval $startFieldName"
    } else if (startField < endField) {
      s"interval $startFieldName to $endFieldName"
    } else {
      throw QueryCompilationErrors.invalidDayTimeIntervalType(startFieldName, endFieldName)
    }
  }
}

/**
 * Extra factory methods and pattern matchers for YearMonthIntervalType.
 *
 * @since 3.2.0
 */
@Unstable
case object YearMonthIntervalType extends AbstractDataType {
  val YEAR: Byte = 0
  val MONTH: Byte = 1
  val yearMonthFields = Seq(YEAR, MONTH)

  def fieldToString(field: Byte): String = field match {
    case YEAR => "year"
    case MONTH => "month"
    case invalid => throw QueryCompilationErrors.invalidYearMonthField(invalid)
  }

  val stringToField: Map[String, Byte] = yearMonthFields.map(i => fieldToString(i) -> i).toMap

  val DEFAULT = YearMonthIntervalType(YEAR, MONTH)

  def apply(): YearMonthIntervalType = DEFAULT
  def apply(field: Byte): YearMonthIntervalType = YearMonthIntervalType(field, field)

  override private[sql] def defaultConcreteType: DataType = DEFAULT

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[YearMonthIntervalType]
  }

  override private[sql] def simpleString: String = defaultConcreteType.simpleString
}
