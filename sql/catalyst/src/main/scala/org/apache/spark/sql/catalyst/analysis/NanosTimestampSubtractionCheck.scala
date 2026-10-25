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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{ExprUtils, SubtractTimestamps}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan}
import org.apache.spark.sql.types.AnyTimestampNanoType

/**
 * Rejects subtraction with a nanosecond-precision timestamp operand. The day-time interval result
 * only has microsecond precision, and adding fractional-seconds precision to it later would change
 * the result type. The as-of join ordering distance never surfaces, so it's allowed.
 */
object NanosTimestampSubtractionCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = plan.foreachUp(checkOperator)

  def checkOperator(operator: LogicalPlan): Unit = {
    val expressions = operator match {
      case j: AsOfJoin => j.expressions.filterNot(_ eq j.orderExpression)
      case o => o.expressions
    }
    expressions.foreach(_.foreach {
      case s: SubtractTimestamps
          if s.children.exists(c => AnyTimestampNanoType.acceptsType(c.dataType)) =>
        s.failAnalysis(
          errorClass = "UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_SUBTRACTION",
          messageParameters = Map("sqlExpr" -> ExprUtils.toSQLExpr(s)))
      case _ =>
    })
  }
}
