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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A rule to validate that non-deterministic expressions only appear in operators that allow them.
 */
object NonDeterministicExpressionCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreachUp { operator =>
      if (nonDeterministic(operator.expressions) &&
        !operatorAllowsNonDeterministicExpressions(operator) &&
        !operator.isInstanceOf[Project] &&
        !operator.isInstanceOf[CollectMetrics] &&
        !operator.isInstanceOf[Filter] &&
        !operator.isInstanceOf[Aggregate] &&
        !operator.isInstanceOf[Window] &&
        !operator.isInstanceOf[Expand] &&
        !operator.isInstanceOf[CreateVariable] &&
        !operator.isInstanceOf[MapInPandas] &&
        !operator.isInstanceOf[MapInArrow] &&
        !operator.isInstanceOf[LateralJoin] &&
        !operator.isInstanceOf[Generate]) {
        operator.failAnalysis(
          errorClass = "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
          messageParameters = Map(
            "sqlExprs" ->
            operator.expressions.map(ExprUtils.toSQLExpr(_)).mkString(", ")
          )
        )
      }
    }
  }

  private def nonDeterministic(expressions: Seq[Expression]): Boolean = {
    expressions.exists { expression =>
      !expression.deterministic
    }
  }

  /**
   * Checks whether the operator allows non-deterministic expressions.
   */
  private def operatorAllowsNonDeterministicExpressions(plan: LogicalPlan): Boolean = {
    plan match {
      case supportsNonDeterministicExpression: SupportsNonDeterministicExpression =>
        supportsNonDeterministicExpression.allowNonDeterministicExpression
      case _ => false
    }
  }
}
