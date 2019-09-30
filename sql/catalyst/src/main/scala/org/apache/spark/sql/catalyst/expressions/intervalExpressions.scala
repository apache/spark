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

package org.apache.spark.sql.catalyst.expressions.interval

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class IntervalPart(
    child: Expression,
    val dataType: DataType,
    func: CalendarInterval => Any,
    funcName: String)
    extends UnaryExpression
    with ExpectsInputTypes
    with Serializable {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override protected def nullSafeEval(interval: Any): Any = {
    func(interval.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.$funcName($c)")
  }
}

case class Millenniums(child: Expression)
    extends IntervalPart(child, IntegerType, getMillenniums, "getMillenniums")

case class Centuries(child: Expression)
    extends IntervalPart(child, IntegerType, getCenturies, "getCenturies")

case class Decades(child: Expression)
    extends IntervalPart(child, IntegerType, getDecades, "getDecades")

case class Years(child: Expression) extends IntervalPart(child, IntegerType, getYears, "getYears")

case class Quarters(child: Expression)
    extends IntervalPart(child, ByteType, getQuarters, "getQuarters")

case class Months(child: Expression) extends IntervalPart(child, ByteType, getMonths, "getMonths")

case class Days(child: Expression) extends IntervalPart(child, LongType, getDays, "getDays")

case class Hours(child: Expression) extends IntervalPart(child, ByteType, getHours, "getHours")

case class Minutes(child: Expression)
    extends IntervalPart(child, ByteType, getMinutes, "getMinutes")

case class Seconds(child: Expression)
    extends IntervalPart(child, DecimalType(8, 6), getSeconds, "getSeconds")

case class Milliseconds(child: Expression)
  extends IntervalPart(child, DecimalType(8, 3), getMilliseconds, "getMilliseconds")

case class Microseconds(child: Expression)
  extends IntervalPart(child, LongType, getMicroseconds, "getMicroseconds")

object IntervalPart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => Millenniums(source)
    case "CENTURY" | "CENTURIES" | "C" | "CENT" => Centuries(source)
    case "DECADE" | "DECADES" | "DEC" | "DECS" => Decades(source)
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => Years(source)
    case "QUARTER" | "QTR" => Quarters(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => Months(source)
    case "DAY" | "D" | "DAYS" => Days(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => Hours(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => Minutes(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => Seconds(source)
    case "MILLISECONDS" | "MSEC" | "MSECS" | "MILLISECON" | "MSECONDS" | "MS" =>
      Milliseconds(source)
    case "MICROSECONDS" | "USEC" | "USECS" | "USECONDS" | "MICROSECON" | "US" =>
      Microseconds(source)
//    case "EPOCH" => Epoch(source)
    case _ => errorHandleFunc
  }
}
