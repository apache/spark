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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.types._

/**
 * Represent the session window.
 *
 * @param timeColumn the start time of session window
 * @param gapDuration the duration of session gap, meaning the session will close if there is
 *                    no new element appeared within "the last element in session + gap".
 */
case class SessionWindow(timeColumn: Expression, gapDuration: Long) extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  //////////////////////////
  // SQL Constructors
  //////////////////////////

  def this(timeColumn: Expression, gapDuration: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(gapDuration))
  }

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  /** Validate the inputs for the gap duration in addition to the input data type. */
  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (gapDuration <= 0) {
        return TypeCheckFailure(s"The window duration ($gapDuration) must be greater than 0.")
      }
    }
    dataTypeCheck
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(timeColumn = newChild)
}

object SessionWindow {
  val marker = "spark.sessionWindow"

  def apply(
      timeColumn: Expression,
      gapDuration: String): SessionWindow = {
    SessionWindow(timeColumn,
      TimeWindow.getIntervalInMicroSeconds(gapDuration))
  }
}
