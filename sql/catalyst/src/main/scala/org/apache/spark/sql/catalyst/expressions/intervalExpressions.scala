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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

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
  extends UnaryExpression with ExpectsInputTypes with Serializable {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override protected def nullSafeEval(interval: Any): Any = {
    func(interval.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.$funcName($c)")
  }
}

case class IntervalMillenniums(child: Expression)
  extends IntervalPart(child, IntegerType, getMillenniums, "getMillenniums")

case class IntervalCenturies(child: Expression)
  extends IntervalPart(child, IntegerType, getCenturies, "getCenturies")

case class IntervalDecades(child: Expression)
  extends IntervalPart(child, IntegerType, getDecades, "getDecades")

case class IntervalYears(child: Expression)
  extends IntervalPart(child, IntegerType, getYears, "getYears")

case class IntervalQuarters(child: Expression)
  extends IntervalPart(child, ByteType, getQuarters, "getQuarters")

case class IntervalMonths(child: Expression)
  extends IntervalPart(child, ByteType, getMonths, "getMonths")

case class IntervalDays(child: Expression)
  extends IntervalPart(child, LongType, getDays, "getDays")

case class IntervalHours(child: Expression)
  extends IntervalPart(child, ByteType, getHours, "getHours")

case class IntervalMinutes(child: Expression)
  extends IntervalPart(child, ByteType, getMinutes, "getMinutes")

case class IntervalSeconds(child: Expression)
  extends IntervalPart(child, DecimalType(8, 6), getSeconds, "getSeconds")

case class IntervalMilliseconds(child: Expression)
  extends IntervalPart(child, DecimalType(8, 3), getMilliseconds, "getMilliseconds")

case class IntervalMicroseconds(child: Expression)
  extends IntervalPart(child, LongType, getMicroseconds, "getMicroseconds")

// Number of seconds in 10000 years is 315576000001 (30 days per one month)
// which is 12 digits + 6 digits for the fractional part of seconds.
case class IntervalEpoch(child: Expression)
  extends IntervalPart(child, DecimalType(18, 6), getEpoch, "getEpoch")

object IntervalPart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => IntervalMillenniums(source)
    case "CENTURY" | "CENTURIES" | "C" | "CENT" => IntervalCenturies(source)
    case "DECADE" | "DECADES" | "DEC" | "DECS" => IntervalDecades(source)
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => IntervalYears(source)
    case "QUARTER" | "QTR" => IntervalQuarters(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => IntervalMonths(source)
    case "DAY" | "D" | "DAYS" => IntervalDays(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => IntervalHours(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => IntervalMinutes(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => IntervalSeconds(source)
    case "MILLISECONDS" | "MSEC" | "MSECS" | "MILLISECON" | "MSECONDS" | "MS" =>
      IntervalMilliseconds(source)
    case "MICROSECONDS" | "USEC" | "USECS" | "USECONDS" | "MICROSECON" | "US" =>
      IntervalMicroseconds(source)
    case "EPOCH" => IntervalEpoch(source)
    case _ => errorHandleFunc
  }
}
