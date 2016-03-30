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

import org.apache.commons.lang.StringUtils

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(
    timeColumn: Expression,
    windowDuration: Long,
    slideDuration: Long,
    startTime: Long,
    private var outputColumnName: String = "window") extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (windowDuration <= 0) {
        return TypeCheckFailure(s"The window duration ($windowDuration) must be greater than 0.")
      }
      if (slideDuration <= 0) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be greater than 0.")
      }
      if (startTime < 0) {
        return TypeCheckFailure(s"The start time ($startTime) must be greater than or equal to 0.")
      }
      if (slideDuration > windowDuration) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be less than or equal to the " +
            s"windowDuration ($windowDuration).")
      }
      if (startTime >= slideDuration) {
        return TypeCheckFailure(s"The start time ($startTime) must be less than the " +
            s"slideDuration ($slideDuration).")
      }
      return dataTypeCheck
    } else {
      return dataTypeCheck
    }
  }
  /**
   * Validate the inputs for the window duration, slide duration, and start time.
   *
   * @return Some string with a useful error message for the invalid input.
   */
  def validate(): Option[String] = {
    if (windowDuration <= 0) {
      return Some(s"The window duration ($windowDuration) must be greater than 0.")
    }
    if (slideDuration <= 0) {
      return Some(s"The slide duration ($slideDuration) must be greater than 0.")
    }
    if (startTime < 0) {
      return Some(s"The start time ($startTime) must be greater than or equal to 0.")
    }
    if (slideDuration > windowDuration) {
      return Some(s"The slide duration ($slideDuration) must be less than or equal to the " +
        s"windowDuration ($windowDuration).")
    }
    if (startTime >= slideDuration) {
      return Some(s"The start time ($startTime) must be less than the " +
        s"slideDuration ($slideDuration).")
    }
    None
  }
}

object TimeWindow {
  /**
   * Parses the interval string for a valid time duration. CalendarInterval expects interval
   * strings to start with the string `interval`. For usability, we prepend `interval` to the string
   * if the user ommitted it.
   *
   * @param interval The interval string
   * @return The interval duration in seconds. SparkSQL casts TimestampType to Long in seconds,
   *         therefore we use seconds here as well.
   */
  private def getIntervalInSeconds(interval: String): Long = {
    if (StringUtils.isBlank(interval)) {
      throw new IllegalArgumentException(
        "The window duration, slide duration and start time cannot be null or blank.")
    }
    val intervalString = if (interval.startsWith("interval")) {
      interval
    } else {
      "interval " + interval
    }
    val cal = CalendarInterval.fromString(intervalString)
    if (cal == null) {
      throw new IllegalArgumentException(
        s"The provided interval ($interval) did not correspond to a valid interval string.")
    }
    (cal.months * 4 * CalendarInterval.MICROS_PER_WEEK + cal.microseconds) / 1000000
  }

  def apply(
      timeColumn: Expression,
      windowDuration: String,
      slideDuration: String,
      startTime: String): TimeWindow = {
    TimeWindow(timeColumn,
      getIntervalInSeconds(windowDuration),
      getIntervalInSeconds(slideDuration),
      getIntervalInSeconds(startTime))
  }
}
