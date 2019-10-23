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

abstract class ExtractIntervalPart(
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

case class ExtractIntervalMillenniums(child: Expression)
    extends ExtractIntervalPart(child, IntegerType, getMillenniums, "getMillenniums")

case class ExtractIntervalCenturies(child: Expression)
    extends ExtractIntervalPart(child, IntegerType, getCenturies, "getCenturies")

case class ExtractIntervalDecades(child: Expression)
    extends ExtractIntervalPart(child, IntegerType, getDecades, "getDecades")

case class ExtractIntervalYears(child: Expression)
    extends ExtractIntervalPart(child, IntegerType, getYears, "getYears")

case class ExtractIntervalQuarters(child: Expression)
    extends ExtractIntervalPart(child, ByteType, getQuarters, "getQuarters")

case class ExtractIntervalMonths(child: Expression)
    extends ExtractIntervalPart(child, ByteType, getMonths, "getMonths")

case class ExtractIntervalDays(child: Expression)
    extends ExtractIntervalPart(child, LongType, getDays, "getDays")

case class ExtractIntervalHours(child: Expression)
    extends ExtractIntervalPart(child, ByteType, getHours, "getHours")

case class ExtractIntervalMinutes(child: Expression)
    extends ExtractIntervalPart(child, ByteType, getMinutes, "getMinutes")

case class ExtractIntervalSeconds(child: Expression)
    extends ExtractIntervalPart(child, DecimalType(8, 6), getSeconds, "getSeconds")

case class ExtractIntervalMilliseconds(child: Expression)
    extends ExtractIntervalPart(child, DecimalType(8, 3), getMilliseconds, "getMilliseconds")

case class ExtractIntervalMicroseconds(child: Expression)
    extends ExtractIntervalPart(child, LongType, getMicroseconds, "getMicroseconds")

// Number of seconds in 10000 years is 315576000001 (30 days per one month)
// which is 12 digits + 6 digits for the fractional part of seconds.
case class ExtractIntervalEpoch(child: Expression)
    extends ExtractIntervalPart(child, DecimalType(18, 6), getEpoch, "getEpoch")

object ExtractIntervalPart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => ExtractIntervalMillenniums(source)
    case "CENTURY" | "CENTURIES" | "C" | "CENT" => ExtractIntervalCenturies(source)
    case "DECADE" | "DECADES" | "DEC" | "DECS" => ExtractIntervalDecades(source)
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => ExtractIntervalYears(source)
    case "QUARTER" | "QTR" => ExtractIntervalQuarters(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => ExtractIntervalMonths(source)
    case "DAY" | "D" | "DAYS" => ExtractIntervalDays(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => ExtractIntervalHours(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => ExtractIntervalMinutes(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => ExtractIntervalSeconds(source)
    case "MILLISECONDS" | "MSEC" | "MSECS" | "MILLISECON" | "MSECONDS" | "MS" =>
      ExtractIntervalMilliseconds(source)
    case "MICROSECONDS" | "USEC" | "USECS" | "USECONDS" | "MICROSECON" | "US" =>
      ExtractIntervalMicroseconds(source)
    case "EPOCH" => ExtractIntervalEpoch(source)
    case _ => errorHandleFunc
  }
}
