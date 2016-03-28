package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException

class TimeWindowSuite extends SparkFunSuite with ExpressionEvalHelper {

	test("time window is unevaluable") {
		intercept[UnsupportedOperationException] {
			evaluate(TimeWindow(Literal(10L), "1 second", "1 second", "0 second"))
		}
	}

	test("validation checks") {
		assert(TimeWindow(Literal(10L), "1 second", "1 second", "0 second").validate().isEmpty,
			"Is a valid expression. Should not have returned a validation error.")
		assert(TimeWindow(Literal(10L), "1 second", "2 second", "0 second").validate().isDefined,
			"Should have thrown validation error, because slide duration is greater than window.")
		assert(TimeWindow(Literal(10L), "1 second", "1 second", "1 minute").validate().isDefined,
			"Should have thrown validation error, because start time is greater than slide duration.")
		assert(TimeWindow(Literal(10L), "1 second", "1 second", "1 second").validate().isDefined,
			"Should have thrown validation error, because start time is equal to slide duration.")
		assert(TimeWindow(Literal(10L), "-1 second", "1 second", "0 second").validate().isDefined,
			"Should have thrown validation error, because window duration is negative.")
		assert(TimeWindow(Literal(10L), "0 second", "0 second", "0 second").validate().isDefined,
			"Should have thrown validation error, because window duration is zero.")
		assert(TimeWindow(Literal(10L), "1 second", "-1 second", "0 second").validate().isDefined,
			"Should have thrown validation error, because slide duration is negative.")
		assert(TimeWindow(Literal(10L), "1 second", "0 second", "0 second").validate().isDefined,
			"Should have thrown validation error, because slide duration is zero.")
		assert(TimeWindow(Literal(10L), "1 second", "1 second", "-2 second").validate().isDefined,
			"Should have thrown validation error, because start time is negative.")
	}

	test("invalid intervals throw exception") {
		val validDuration = "10 second"
		val validTime = "5 second"
		for (invalid <- Seq(null, " ", "\n", "\t", "2 apples")) {
			intercept[AnalysisException] {
				TimeWindow(Literal(10L), invalid, validDuration, validTime).windowDuration
			}
			intercept[AnalysisException] {
				TimeWindow(Literal(10L), validDuration, invalid, validTime).slideDuration
			}
			intercept[AnalysisException] {
				TimeWindow(Literal(10L), validDuration, validDuration, invalid).startTime
			}
		}
	}

	test("interval strings work with and without 'interval' prefix and returns seconds") {
		val validDuration = "10 second"
		for ((text, seconds) <- Seq(("1 second", 1), ("1 minute", 60), ("2 hours", 7200))) {
			assert(TimeWindow(Literal(10L), text, validDuration, "0 seconds").windowDuration === seconds)
			assert(TimeWindow(Literal(10L), "interval " + text, validDuration, "0 seconds").windowDuration
				=== seconds)
		}
	}

	test("maxNumOverlapping takes ceiling of window duration over slide duration") {
		assert(TimeWindow(Literal(10L), "5 second", "5 second", "0 second").maxNumOverlapping === 1)
		assert(TimeWindow(Literal(10L), "5 second", "4 second", "0 second").maxNumOverlapping === 2)
		assert(TimeWindow(Literal(10L), "5 second", "3 second", "0 second").maxNumOverlapping === 2)
		assert(TimeWindow(Literal(10L), "5 second", "2 second", "0 second").maxNumOverlapping === 3)
		assert(TimeWindow(Literal(10L), "5 second", "1 second", "0 second").maxNumOverlapping === 5)
	}

	test("output column name is window and is a struct") {
		val expr = TimeWindow(Literal(10L), "5 second", "5 second", "0 second")
		assert(expr.outputColumn.name === "window")
		assert(expr.outputColumn.children.head.isInstanceOf[CreateStruct])
		assert(expr.windowStartCol.name === "start")
		assert(expr.windowEndCol.name === "end")
	}
}
