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

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, With}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Analyze WITH nodes and substitute child plan with CTE definitions.
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case With(child, relations) =>
      // substitute CTE expressions right-to-left to resolve references to previous CTEs:
      // with a as (select * from t), b as (select * from a) select * from b
      relations.foldRight(child) {
        case ((cteName, ctePlan), currentPlan) => substituteCTE(currentPlan, cteName, ctePlan)
      }
    case other => other
  }

  private def substituteCTE(
      plan: LogicalPlan,
      cteName: String,
      ctePlan: LogicalPlan): LogicalPlan = {
    plan resolveOperatorsUp {
      case UnresolvedRelation(Seq(table)) if plan.conf.resolver(cteName, table) => ctePlan

      case o =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        o transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
        }
    }
  }
}
