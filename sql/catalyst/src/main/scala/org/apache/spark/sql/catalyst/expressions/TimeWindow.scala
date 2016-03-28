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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(
		originalTimeColumn: Expression,
	 	private val _windowDuration: String,
		private val _slideDuration: String,
		private val _startTime: String) extends UnaryExpression
	with ExpectsInputTypes
	with Unevaluable
	with NonSQLExpression {

	override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(TimestampType, LongType))
	// the time column in Timestamp format
	lazy val timeColumn = Cast(originalTimeColumn, TimestampType)
	override def child: Expression = timeColumn
	override def dataType: DataType = outputType

	private def outputType: StructType = StructType(Seq(
		StructField("start", TimestampType), StructField("end", TimestampType)))
	lazy val output: Seq[Attribute] = outputType.toAttributes
	def outputColumn: NamedExpression = Alias(CreateStruct(output), "window")()
	def windowStartCol: Attribute = output.head
	def windowEndCol: Attribute = output.last

	/**
	 * Parses the interval string for a valid time duration. CalendarInterval expects interval
	 * strings to start with the string `interval`. For usability, we prepend `interval` to the string
	 * if the user ommitted it.
	 * @param interval The interval string
	 * @return The interval duration in seconds. SparkSQL casts TimestampType to Long in seconds,
	 *         therefore we use seconds here as well.
	 */
	private def getIntervalInSeconds(interval: String): Long = {
		if (StringUtils.isBlank(interval)) {
			throw new AnalysisException(
				"The window duration, slide duration and start time cannot be null.")
		}
		val intervalString = if (interval.startsWith("interval")) {
			interval
		} else {
			"interval " + interval
		}
		val cal = CalendarInterval.fromString(intervalString)
		if (cal == null) {
			throw new AnalysisException(
				s"The provided interval ($interval) did not correspond to a valid interval string.")
		}
		(cal.months * 4 * CalendarInterval.MICROS_PER_WEEK + cal.microseconds) / 1000000
	}

	// The window duration in seconds
	lazy val windowDuration: Long = getIntervalInSeconds(_windowDuration)
	// The slide duration in seconds
	lazy val slideDuration: Long = getIntervalInSeconds(_slideDuration)
	// The start time offset in seconds
	lazy val startTime: Long = getIntervalInSeconds(_startTime)

	/**
	 * Validate the inputs for the window duration, slide duration, and start time.
	 * @return Some string with a useful error message for the invalid input.
	 */
	def validate(): Option[String] = {
		if (windowDuration <= 0) {
			return Some(s"The window duration (${_windowDuration}) must be greater than 0.")
		}
		if (slideDuration <= 0) {
			return Some(s"The slide duration (${_slideDuration}) must be greater than 0.")
		}
		if (startTime < 0) {
			return Some(s"The start time (${_startTime}) must be greater than or equal to 0.")
		}
		if (slideDuration > windowDuration) {
			return Some(s"The slide duration (${_slideDuration}) must be less than or equal to the " +
				s"windowDuration (${_windowDuration}).")
		}
		if (startTime >= slideDuration) {
			return Some(s"The start time (${_startTime}) must be less than the " +
				s"slideDuration (${_slideDuration}).")
		}
		None
	}

	/**
	 * Returns the maximum possible number of overlapping windows we will have with the given
	 * window and slide durations.
	 */
	def maxNumOverlapping: Int = math.ceil(windowDuration * 1.0 / slideDuration).toInt
}
