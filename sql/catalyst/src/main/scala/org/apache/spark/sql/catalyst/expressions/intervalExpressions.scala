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
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, IntegerType, LongType}
import org.apache.spark.unsafe.types.CalendarInterval

case class Millennium(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getMillennium(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getMillennium($c)")
  }
}

case class Century(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getCentury(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getCentury($c)")
  }
}

case class Decade(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getDecade(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getDecade($c)")
  }
}

case class Year(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getYear(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getYear($c)")
  }
}

case class Quarter(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getQuarter(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getQuarter($c)")
  }
}

case class Month(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getMonth(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getMonth($c)")
  }
}

case class Day(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = LongType
  override protected def nullSafeEval(date: Any): Any = {
    IntervalUtils.getDay(date.asInstanceOf[CalendarInterval])
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.getDay($c)")
  }
}

object IntervalPart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => Millennium(source)
    case "CENTURY" | "CENTURIES" | "C" | "CENT" => Century(source)
    case "DECADE" | "DECADES" | "DEC" | "DECS" => Decade(source)
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => Year(source)
    case "QUARTER" | "QTR" => Quarter(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => Month(source)
    case "DAY" | "D" | "DAYS" => Day(source)
//    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => Hour(source)
//    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => Minute(source)
//    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => Second(source)
//    case "MILLISECONDS" | "MSEC" | "MSECS" | "MILLISECON" | "MSECONDS" | "MS" =>
//      Milliseconds(source)
//    case "MICROSECONDS" | "USEC" | "USECS" | "USECONDS" | "MICROSECON" | "US" =>
//      Microseconds(source)
//    case "EPOCH" => Epoch(source)
    case _ => errorHandleFunc
  }
}
