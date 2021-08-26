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

import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Represent the session window.
 *
 * @param timeColumn the start time of session window
 * @param gapDuration the duration of session gap. For static gap duration, meaning the session
 *                    will close if there is no new element appeared within "the last element in
 *                    session + gap". Besides a static gap duration value, users can also provide
 *                    an expression to specify gap duration dynamically based on the input row.
 *                    With dynamic gap duration, the closing of a session window does not depend
 *                    on the latest input anymore. A session window's range is the union of all
 *                    events' ranges which are determined by event start time and evaluated gap
 *                    duration during the query execution. Note that the rows with negative or
 *                    zero gap duration will be filtered out from the aggregation.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(time_column, gap_duration) - Generates session window given a timestamp specifying column and gap duration.
      See <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#types-of-time-windows">'Types of time windows'</a> in Structured Streaming guide doc for detailed explanation and examples.
  """,
  arguments = """
    Arguments:
      * time_column - The column or the expression to use as the timestamp for windowing by time. The time column must be of TimestampType.
      * gap_duration - A string specifying the timeout of the session represented as "interval value"
        (See <a href="https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal">Interval Literal</a> for more details.) for the fixed gap duration, or
        an expression which is applied for each input and evaluated to the "interval value" for the dynamic gap duration.
  """,
  group = "datetime_funcs",
  since = "3.2.0")
// scalastyle:on line.size.limit
case class SessionWindow(timeColumn: Expression, gapDuration: Expression) extends Expression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  override def children: Seq[Expression] = Seq(timeColumn, gapDuration)
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, AnyDataType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(timeColumn = newChildren(0), gapDuration = newChildren(1))
}

object SessionWindow {
  val marker = "spark.sessionWindow"

  def apply(
      timeColumn: Expression,
      gapDuration: String): SessionWindow = {
    SessionWindow(timeColumn,
      Literal(IntervalUtils.safeStringToInterval(UTF8String.fromString(gapDuration)),
        CalendarIntervalType))
  }
}
