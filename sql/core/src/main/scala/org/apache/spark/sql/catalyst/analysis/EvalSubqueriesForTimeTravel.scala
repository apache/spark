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

import org.apache.spark.sql.catalyst.expressions.{Literal, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.RELATION_TIME_TRAVEL
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, ScalarSubquery => ScalarSubqueryExec, SubqueryExec}

class EvalSubqueriesForTimeTravel extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(RELATION_TIME_TRAVEL)) {
    case r @ RelationTimeTravel(_, Some(ts), _)
        if ts.resolved && SubqueryExpression.hasSubquery(ts) =>
      val subqueryEvaluated = ts.transform {
        case s: ScalarSubquery =>
          // `RelationTimeTravel` is a leaf node. Subqueries in it cannot resolve any
          // outer references and should not be correlated.
          assert(!s.isCorrelated, "Correlated subquery should not appear in " +
            classOf[EvalSubqueriesForTimeTravel].getSimpleName)
          SimpleAnalyzer.checkSubqueryExpression(r, s)
          val executedPlan = QueryExecution.prepareExecutedPlan(SparkSession.active, s.plan)
          val physicalSubquery = ScalarSubqueryExec(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${s.exprId.id}", executedPlan),
            s.exprId)
          evalSubqueries(physicalSubquery)
          Literal(physicalSubquery.eval(), s.dataType)
      }
      r.copy(timestamp = Some(subqueryEvaluated))
  }

  // Evaluate subqueries in a bottom-up way.
  private def evalSubqueries(subquery: ScalarSubqueryExec): Unit = {
    subquery.plan.foreachUp { plan =>
      plan.expressions.foreach(_.foreachUp {
        case s: ScalarSubqueryExec => evalSubqueries(s)
        case _ =>
      })
    }
    subquery.updateResult()
  }
}
