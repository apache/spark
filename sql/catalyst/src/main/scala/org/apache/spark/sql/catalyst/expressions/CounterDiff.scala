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

import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._

/**
 * The counter_diff window function computes the differences between consecutive cumulative counter
 * values in a time series, thereby converting the counter from the cumulative to the delta format.
 *
 * This class serves as the base class for the two versions of the counter_diff function:
 * - counter_diff(counter) -> CounterDiff(counter)
 * - counter_diff(counter, start_time) -> CounterDiffWithStartTime(counter, startTime)
 */
abstract class CounterDiffBase(val counter: Expression)
  extends AggregateWindowFunction
    with QueryErrorsBase {

  override def prettyName: String = "counter_diff"

  override def nullable: Boolean = true

  override def dataType: DataType = counter.dataType

  /**
   * Last non-NULL counter value from a previous row.
   */
  protected lazy val prevCounter: AttributeReference =
    AttributeReference("prevCounter", counter.dataType, nullable = true)()

  /**
   * Counter value from the current row.
   */
  protected lazy val currCounter: AttributeReference =
    AttributeReference("currCounter", counter.dataType, nullable = true)()

  /**
   * Null literal used as a counter_diff result, when appropriate.
   */
  protected lazy val nullResult: Expression = Literal.create(null, counter.dataType)

  /**
   * Difference between the current and previous counter values.
   */
  protected lazy val diff: Expression = {
    counter.dataType match {
      // For DECIMAL, subtraction typically widens the result type to handle possible overflow.
      // For counter_diff, since counters cannot be negative, there is no risk of overflow, and no
      // need to widen the result type, so we subtract directly in the input type.
      case dt: DecimalType => DecimalSubtractNoOverflowCheck(currCounter, prevCounter, dt)
      case _ => currCounter - prevCounter
    }
  }

  /**
   * Returns the difference, unless the counter has decreased, which is treated as a counter reset.
   * In this case, NULL is returned.
   */
  protected lazy val diffWithCounterDecreaseCheck: Expression =
    If(currCounter < prevCounter, nullResult, diff)

  /**
   * Error raised when the counter is negative.
   */
  protected lazy val negativeCounterError: Expression = RaiseError(
    Literal("COUNTER_DIFF_NEGATIVE_COUNTER_VALUE"),
    CreateMap(
      Seq(
        Literal("value"),
        Cast(currCounter, StringType),
        Literal("function"),
        Literal(toSQLId("counter_diff"))
      )
    ),
    counter.dataType
  )

  /**
   * Wraps `inner` with the "skip row on NULL counter" and "raise error on negative counter" checks.
   */
  protected def withCounterNullAndNegativeChecks(inner: Expression): Expression = {
    If(IsNull(currCounter),
      nullResult,
      If(currCounter < Literal.default(counter.dataType),
        negativeCounterError,
        inner
      )
    )
  }
}

/**
 * The single-parameter form of `counter_diff`: `counter_diff(value)`.
 * Detects counter resets only when the counter value decreases.
 */
case class CounterDiff(override val counter: Expression)
  extends CounterDiffBase(counter)
    with ExpectsInputTypes {

  override def children: Seq[Expression] = Seq(counter)

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  /**
   * The aggregation state attributes for the counter_diff function.
   * In the single-parameter form, there are two attributes:
   * - prevCounter: The last non-NULL counter value from a previous row.
   * - currCounter: The counter value from the current row.
   */
  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq(prevCounter, currCounter)

  /**
   * The initial aggregation state for the counter_diff function. Initial values are NULL.
   */
  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, counter.dataType),
    Literal.create(null, counter.dataType)
  )

  /**
   * The update expressions for the counter_diff function's aggregation state.
   *
   * Fundamentally, the current value becomes the previous value, and the new value becomes the
   * current value.
   *
   * Rows with NULL counter values should be skipped. As a result, the previous counter value
   * should not be updated in the aggregation state.
   */
  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(IsNotNull(currCounter), currCounter, prevCounter),
    counter
  )

  /**
   * The evaluation expression for the counter_diff function.
   *
   * Checks for edge cases first: NULL counter value, negative counter value and counter reset.
   * Otherwise, returns the difference between the current and previous counter values.
   */
  override lazy val evaluateExpression: Expression =
    withCounterNullAndNegativeChecks(diffWithCounterDecreaseCheck)

  /**
   * The SQL representation of the single-parameter form of the counter_diff function.
   */
  override def sql: String = s"${prettyName}(${counter.sql})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CounterDiff =
    copy(counter = newChildren.head)
}

/**
 * The two-parameter form of `counter_diff`: `counter_diff(value, start_time)`.
 * Additionally checks for counter resets when `start_time` increases, which signals a new start.
 * Requires that the start time doesn't decrease, which would indicate moving backwards in time.
 */
case class CounterDiffWithStartTime(
    override val counter: Expression,
    startTime: Expression,
    timeZoneId: Option[String] = None)
  extends CounterDiffBase(counter)
    with ExpectsInputTypes
    with TimeZoneAwareExpression {

  override def withTimeZone(timeZoneId: String): CounterDiffWithStartTime =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = Seq(counter, startTime)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(NumericType, TypeCollection(TimestampType, TimestampNTZType))

  /**
   * The start time from a previous row.
   */
  protected lazy val prevStartTime: AttributeReference =
    AttributeReference("prevStartTime", startTime.dataType, nullable = true)()

  /**
   * The start time from the current row.
   */
  protected lazy val currStartTime: AttributeReference =
    AttributeReference("currStartTime", startTime.dataType, nullable = true)()

  /**
   * The aggregation state attributes for the counter_diff function.
   * In the two-parameter form, there are four attributes:
   * - prevCounter: The last non-NULL counter value from a previous row.
   * - currCounter: The counter value from the current row.
   * - prevStartTime: The start time from a previous row.
   * - currStartTime: The start time from the current row.
   */
  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq(prevCounter, currCounter, prevStartTime, currStartTime)

  /**
   * The initial aggregation state for the counter_diff function. Initial values are NULL.
   */
  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, counter.dataType),
    Literal.create(null, counter.dataType),
    Literal.create(null, startTime.dataType),
    Literal.create(null, startTime.dataType)
  )

  /**
   * The update expressions for the counter_diff function's aggregation state.
   *
   * Fundamentally, the current value becomes the previous value, and the new value becomes the
   * current value. The same applies to the start time.
   *
   * Rows with NULL counter values should be skipped. As a result, the previous values for both
   * the counter and start time should not be updated in the aggregation state.
   */
  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(IsNotNull(currCounter), currCounter, prevCounter),
    counter,
    If(IsNotNull(currCounter), currStartTime, prevStartTime),
    startTime
  )

  /**
   * Error raised when the start time decreases.
   */
  protected lazy val decreasedStartTimeError: Expression = RaiseError(
    Literal("COUNTER_DIFF_START_TIME_DECREASED"),
    CreateMap(
      Seq(
        Literal("function"),
        Literal(toSQLId("counter_diff")),
        Literal("previousStartTime"),
        Cast(prevStartTime, StringType, timeZoneId),
        Literal("currentStartTime"),
        Cast(currStartTime, StringType, timeZoneId)
      )
    ),
    counter.dataType
  )

  /**
   * The evaluation expression for the counter_diff function.
   *
   * Checks for edge cases first: NULL counter value, negative counter value, start time decrease
   * and counter resets.
   *
   * Otherwise, returns the difference between the current and previous counter values.
   */
  override lazy val evaluateExpression: Expression = withCounterNullAndNegativeChecks {
    If(currStartTime < prevStartTime,
      decreasedStartTimeError,
      If(prevStartTime < currStartTime,
        nullResult,
        diffWithCounterDecreaseCheck
      )
    )
  }

  /**
   * The SQL representation of the two-parameter form of the counter_diff function.
   */
  override def sql: String =
    s"${prettyName}(${counter.sql}, ${startTime.sql})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CounterDiffWithStartTime =
    copy(counter = newChildren(0), startTime = newChildren(1))
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(value [, start_time]) - Computes the differences between consecutive cumulative counter
      values in a time series, thereby converting the counter from the cumulative to the delta
      format.
  """,
  arguments = """
    Arguments:
      * value - A cumulative counter. Must be a numeric data type. Must be non-negative.
      * start_time - An optional timestamp parameter which indicates when the counter was last set
          to zero. Used to signal counter resets.
  """,
  examples = """
    Examples:
      > SELECT m, t, c, _FUNC_(c) OVER (PARTITION BY m ORDER BY t) AS diff FROM VALUES ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:00:00', 100), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:01:00', 200), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:02:00', 400), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:03:00', 50), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:04:00', 100) AS tab(m, t, c) ORDER BY t;
       http_requests	2026-01-01 00:00:00	100	NULL
       http_requests	2026-01-01 00:01:00	200	100
       http_requests	2026-01-01 00:02:00	400	200
       http_requests	2026-01-01 00:03:00	50	NULL
       http_requests	2026-01-01 00:04:00	100	50
      > SELECT m, t, s, c, _FUNC_(c, s) OVER (PARTITION BY m ORDER BY t) AS diff FROM VALUES ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:00:00', 100, TIMESTAMP_NTZ '2026-01-01 00:00:00'), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:01:00', 200, TIMESTAMP_NTZ '2026-01-01 00:00:00'), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:02:00', 400, TIMESTAMP_NTZ '2026-01-01 00:00:00'), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:03:00', 500, TIMESTAMP_NTZ '2026-01-01 00:02:15'), ('http_requests', TIMESTAMP_NTZ '2026-01-01 00:04:00', 600, TIMESTAMP_NTZ '2026-01-01 00:02:15') AS tab(m, t, c, s) ORDER BY t;
       http_requests	2026-01-01 00:00:00	2026-01-01 00:00:00	100	NULL
       http_requests	2026-01-01 00:01:00	2026-01-01 00:00:00	200	100
       http_requests	2026-01-01 00:02:00	2026-01-01 00:00:00	400	200
       http_requests	2026-01-01 00:03:00	2026-01-01 00:02:15	500	NULL
       http_requests	2026-01-01 00:04:00	2026-01-01 00:02:15	600	100
  """,
  note = """
    _FUNC_ calculates the difference between the current and the previous counter value within the
    partition, according to the order defined by the ORDER BY clause.

    Use the PARTITION BY clause of the window to separate independent counters. This is done by
    specifying all columns which uniquely identify a time series. These are typically the counter
    name and any attributes tied to the counter.

    Use the ORDER BY clause of the window to order the observations by the associated timestamp
    in ascending order.

    The following special cases are handled:
    1. If the counter value is NULL, the row is skipped and NULL is returned.
    2. If the value is negative, or the start time decreases, an error is raised.
    3. In the case of a counter reset, NULL is returned.
    4. NULL is returned for the first row.

    Counter resets are detected when:
    1. The current counter value is less than the previous counter value.
    2. The current start time is greater than the previous start time, if start_time was provided.
  """,
  group = "window_funcs",
  source = "built-in",
  since = "4.2.0"
)
// scalastyle:on line.size.limit line.contains.tab
object CounterDiffExpressionBuilder extends ExpressionBuilder {
  // Placeholder start time which serves as a default value.
  private val DefaultStartTime: Literal = Literal.create(null, NullType)

  override def functionSignature: Option[FunctionSignature] = {
    val valueArg = InputParameter("value")
    val startTimeArg = InputParameter("start_time", Some(DefaultStartTime))
    Some(FunctionSignature(Seq(valueArg, startTimeArg)))
  }

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    expressions match {
      case Seq(value) =>
        CounterDiff(value)
      case Seq(value, startTime) if startTime eq DefaultStartTime =>
        CounterDiff(value)
      case Seq(value, startTime) =>
        CounterDiffWithStartTime(value, startTime)
    }
  }
}
