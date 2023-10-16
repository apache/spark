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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE

/**
 * This rule transforms recursive [[Union]] nodes into [[UnionLoop]] and recursive
 * [[CTERelationRef]] nodes into [[UnionLoopRef]] nodes in recursive CTE definitions.
 */
case object InsertLoops extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDownWithPruning(
      _.containsPattern(CTE)) {
    case w: WithCTE =>
      val newCTEDefs = w.cteDefs.map {
        case cte if cte.recursive =>

          def transformRefs(plan: LogicalPlan, accumulated: Boolean) = {
            plan.transformWithPruning(_.containsPattern(CTE)) {
              case r: CTERelationRef if r.recursive =>
                UnionLoopRef(r.cteId, r.output, accumulated)
            }
          }

          cte.child match {
            case Union(Seq(anchor, recursion), false, false) =>
              cte.copy(child = UnionLoop(cte.id, anchor, transformRefs(recursion, false)))
            case a @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
              cte.copy(child =
                a.copy(child =
                  UnionLoop(cte.id, anchor, transformRefs(recursion, false))))
            case p @ Project(_, Union(Seq(anchor, recursion), false, false)) =>
              cte.copy(child =
                p.copy(child =
                  UnionLoop(cte.id, anchor, transformRefs(recursion, false))))
            case a @ SubqueryAlias(_,
                p @ Project(_, Union(Seq(anchor, recursion), false, false))) =>
              cte.copy(child =
                a.copy(child =
                  p.copy(child =
                    UnionLoop(cte.id, anchor, transformRefs(recursion, false)))))

            // If the recursion is described with an UNION clause then the recursive term should
            // not return those rows that have been calculated previously so we exclude those rows
            // from the current iteration result.
            case p @ Project(_, Distinct(Union(Seq(anchor, recursion), false, false))) =>
              cte.copy(child =
                p.copy(child =
                  UnionLoop(cte.id,
                    Distinct(anchor),
                    Except(
                      transformRefs(recursion, false),
                      UnionLoopRef(cte.id, cte.output, true),
                      false))))
            case a @ SubqueryAlias(_,
                p @ Project(_, Distinct(Union(Seq(anchor, recursion), false, false)))) =>
              cte.copy(child =
                a.copy(child =
                  p.copy(child =
                    UnionLoop(cte.id,
                      Distinct(anchor),
                      Except(
                        transformRefs(recursion, false),
                        UnionLoopRef(cte.id, cte.output, true),
                        false)))))
            case _ => cte
          }
        case o => o
      }
      w.copy(cteDefs = newCTEDefs)
  }
}
