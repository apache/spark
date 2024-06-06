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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

/**
 * [[PlanHelper]] contains utility methods that can be used by Analyzer and Optimizer.
 * It can also be container of methods that are common across multiple rules in Analyzer
 * and Optimizer.
 */
object PlanHelper {
  /**
   * Check if there's any expression in this query plan operator that is
   * - A WindowExpression but the plan is not Window
   * - An AggregateExpression but the plan is not Aggregate or Window
   * - A Generator but the plan is not Generate
   * Returns the list of invalid expressions that this operator hosts. This can happen when
   * 1. The input query from users contain invalid expressions.
   *    Example : SELECT * FROM tab WHERE max(c1) > 0
   * 2. Query rewrites inadvertently produce plans that are invalid.
   */
  def specialExpressionsInUnsupportedOperator(plan: LogicalPlan): Seq[Expression] = {
    val exprs = plan.expressions
    val invalidExpressions = exprs.flatMap { root =>
      root.collect {
        case e: WindowExpression
          if !plan.isInstanceOf[Window] => e
        case e: AggregateExpression
          if !(plan.isInstanceOf[Aggregate] ||
               plan.isInstanceOf[Window] ||
               plan.isInstanceOf[CollectMetrics] ||
               onlyInLateralSubquery(plan)) => e
        case e: Generator
          if !(plan.isInstanceOf[Generate] ||
               plan.isInstanceOf[BaseEvalPythonUDTF]) => e
      }
    }
    invalidExpressions
  }

  private def onlyInLateralSubquery(plan: LogicalPlan): Boolean = {
    lazy val noAggInJoinCond = {
      val join = plan.asInstanceOf[LateralJoin]
      !(join.condition ++ join.right.joinCond).exists(AggregateExpression.containsAggregate)
    }
    plan.isInstanceOf[LateralJoin] && noAggInJoinCond
  }
}
