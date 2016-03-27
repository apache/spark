package org.apache.spark.sql.catalyst.expressions

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

	lazy val timeColumn = Cast(originalTimeColumn, TimestampType)
	override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(TimestampType, LongType))
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
		val intervalString = if (interval.startsWith("interval")) {
			interval
		} else {
			"interval " + interval
		}
		val cal = CalendarInterval.fromString(intervalString)
		(cal.months * 4 * CalendarInterval.MICROS_PER_WEEK + cal.microseconds) / 1000000
	}

	lazy val windowDuration = getIntervalInSeconds(_windowDuration)
	lazy val slideDuration = getIntervalInSeconds(_slideDuration)
	lazy val startTime = getIntervalInSeconds(_startTime)

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
