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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsAllPatterns(CTE)) {
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      resolveWithCTE(plan, cteDefMap)
    } else {
      plan
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case w @ WithCTE(_, cteDefs) =>
        cteDefs.foreach { cteDef =>
          if (cteDef.resolved) {
            cteDefMap.put(cteDef.id, cteDef)
          }
        }
        w

      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
        }.getOrElse {
          ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }
}
