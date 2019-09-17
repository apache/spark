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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(
    timeColumn: Expression,
    windowDuration: Long,
    slideDuration: Long,
    startTime: Long) extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  //////////////////////////
  // SQL Constructors
  //////////////////////////

  def this(
      timeColumn: Expression,
      windowDuration: Expression,
      slideDuration: Expression,
      startTime: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), TimeWindow.parseExpression(startTime))
  }

  def this(timeColumn: Expression, windowDuration: Expression, slideDuration: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), 0)
  }

  def this(timeColumn: Expression, windowDuration: Expression) = {
    this(timeColumn, windowDuration, windowDuration)
  }

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  /**
   * Validate the inputs for the window duration, slide duration, and start time in addition to
   * the input data type.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (windowDuration <= 0) {
        return TypeCheckFailure(s"The window duration ($windowDuration) must be greater than 0.")
      }
      if (slideDuration <= 0) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be greater than 0.")
      }
      if (slideDuration > windowDuration) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be less than or equal" +
          s" to the windowDuration ($windowDuration).")
      }
      if (startTime.abs >= slideDuration) {
        return TypeCheckFailure(s"The absolute value of start time ($startTime) must be less " +
          s"than the slideDuration ($slideDuration).")
      }
    }
    dataTypeCheck
  }
}

object TimeWindow {
  /**
   * Parses the interval string for a valid time duration. CalendarInterval expects interval
   * strings to start with the string `interval`. For usability, we prepend `interval` to the string
   * if the user omitted it.
   *
   * @param interval The interval string
   * @return The interval duration in microseconds. SparkSQL casts TimestampType has microsecond
   *         precision.
   */
  private def getIntervalInMicroSeconds(interval: String): Long = {
    val cal = CalendarInterval.fromCaseInsensitiveString(interval)
    if (cal.months > 0) {
      throw new IllegalArgumentException(
        s"Intervals greater than a month is not supported ($interval).")
    }
    cal.microseconds
  }

  /**
   * Parses the duration expression to generate the long value for the original constructor so
   * that we can use `window` in SQL.
   */
  private def parseExpression(expr: Expression): Long = expr match {
    case NonNullLiteral(s, StringType) => getIntervalInMicroSeconds(s.toString)
    case IntegerLiteral(i) => i.toLong
    case NonNullLiteral(l, LongType) => l.toString.toLong
    case _ => throw new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }

  def apply(
      timeColumn: Expression,
      windowDuration: String,
      slideDuration: String,
      startTime: String): TimeWindow = {
    TimeWindow(timeColumn,
      getIntervalInMicroSeconds(windowDuration),
      getIntervalInMicroSeconds(slideDuration),
      getIntervalInMicroSeconds(startTime))
  }
}

/**
 * Expression used internally to convert the TimestampType to Long and back without losing
 * precision, i.e. in microseconds. Used in time windowing.
 */
case class PreciseTimestampConversion(
    child: Expression,
    fromType: DataType,
    toType: DataType) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(fromType)
  override def dataType: DataType = toType
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ev.copy(code = eval.code +
      code"""boolean ${ev.isNull} = ${eval.isNull};
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${eval.value};
       """.stripMargin)
  }
  override def nullSafeEval(input: Any): Any = input
}
