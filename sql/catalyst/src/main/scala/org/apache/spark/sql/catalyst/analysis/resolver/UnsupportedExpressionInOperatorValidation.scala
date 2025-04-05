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

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Generator,
  WindowExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  BaseEvalPythonUDTF,
  CollectMetrics,
  Generate,
  LateralJoin,
  LogicalPlan,
  Project,
  Window
}
import org.apache.spark.sql.errors.QueryCompilationErrors

object UnsupportedExpressionInOperatorValidation {

  /**
   * Check that `expression` is allowed to exist in `operator`'s expression tree.
   */
  def isExpressionInUnsupportedOperator(expression: Expression, operator: LogicalPlan): Boolean = {
    expression match {
      case _: WindowExpression => operator.isInstanceOf[Window]
      case _: AggregateExpression =>
        !(operator.isInstanceOf[Project] ||
        operator.isInstanceOf[Aggregate] ||
        operator.isInstanceOf[Window] ||
        operator.isInstanceOf[CollectMetrics] ||
        onlyInLateralSubquery(operator))
      case _: Generator =>
        !(operator.isInstanceOf[Generate] ||
        operator.isInstanceOf[BaseEvalPythonUDTF])
      case _ =>
        false
    }
  }

  private def onlyInLateralSubquery(operator: LogicalPlan): Boolean = {
    operator.isInstanceOf[LateralJoin] && {
      // TODO: check if we are resolving a lateral join condition once lateral join is supported.
      throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
        s"${operator.getClass} operator resolution"
      )
    }
  }
}
