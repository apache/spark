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
