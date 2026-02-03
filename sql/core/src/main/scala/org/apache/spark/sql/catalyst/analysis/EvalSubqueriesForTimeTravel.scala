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

import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.RELATION_TIME_TRAVEL
import org.apache.spark.sql.classic.SparkSession

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
          // Wrap the scalar subquery in a Project over OneRowRelation to execute it
          // through the normal query execution path. This properly handles table
          // references in the subquery (e.g., V2 tables).
          val wrappedPlan = Project(Seq(Alias(s, "result")()), OneRowRelation())
          val spark = SparkSession.active
          val qe = spark.sessionState.executePlan(wrappedPlan)
          val result = qe.executedPlan.executeCollect().head.get(0, s.dataType)
          Literal(result, s.dataType)
      }
      r.copy(timestamp = Some(subqueryEvaluated))
  }
}
