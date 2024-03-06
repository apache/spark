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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, IntegerLiteral, InterpretedMutableProjection, Predicate, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, Filter, Limit, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND_RESULT

/**
 * Converts local operations (i.e. ones that don't require data exchange) on `CommandResult`
 * to `LocalRelation`.
 */
object ConvertCommandResultToLocalRelation extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(COMMAND_RESULT)) {
    case Project(projectList, CommandResult(output, _, _, rows))
      if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedMutableProjection(projectList, output)
      projection.initialize(0)
      LocalRelation(projectList.map(_.toAttribute), rows.map(projection(_).copy()))

    case Limit(IntegerLiteral(limit), CommandResult(output, _, _, rows)) =>
      LocalRelation(output, rows.take(limit))

    case Filter(condition, CommandResult(output, _, _, rows))
      if !hasUnevaluableExpr(condition) =>
      val predicate = Predicate.create(condition, output)
      predicate.initialize(0)
      LocalRelation(output, rows.filter(row => predicate.eval(row)))
  }

  private def hasUnevaluableExpr(expr: Expression): Boolean = {
    expr.exists(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference])
  }
}
