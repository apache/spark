package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(
		timeColumn: Expression,
	 	private val _windowDuration: String,
		private val _slideDuration: String,
		private val _startTime: String) extends UnaryExpression
	with ExpectsInputTypes
	with Unevaluable
	with NonSQLExpression {

	override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
	override def child: Expression = timeColumn
	override def dataType: DataType = StructType(Seq(
		StructField("start", TimestampType), StructField("end", TimestampType)))

	private def getIntervalInMillis(interval: CalendarInterval): Long = {
		(interval.months * 4 * CalendarInterval.MICROS_PER_WEEK + interval.microseconds) / 1000
	}

	val windowDuration = getIntervalInMillis(CalendarInterval.fromString(_windowDuration))
	val slideDuration = getIntervalInMillis(CalendarInterval.fromString(_slideDuration))
	val startTime = getIntervalInMillis(CalendarInterval.fromString(_startTime))

	def validate(): Option[String] = {
		if (slideDuration > windowDuration) {
			return Some(s"The slide duration ($slideDuration) must be less than or equal to the " +
				s"windowDuration ($windowDuration).")
		}
		if (startTime >= windowDuration) {
			return Some(s"The start time ($startTime) must be less than the " +
				s"windowDuration ($windowDuration).")
		}
		None
	}

	/**
	 * Returns number of overlapping windows we will have with the given window and slide durations.
	 */
	def numOverlapping: Int = math.ceil(windowDuration * 1.0 / slideDuration).toInt
}
