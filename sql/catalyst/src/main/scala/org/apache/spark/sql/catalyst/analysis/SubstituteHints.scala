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

import scala.collection.{immutable, mutable}

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * Substitute Hints.
 * - BROADCAST/BROADCASTJOIN/MAPJOIN match the closest table with the given name parameters.
 *
 * This rule substitutes `UnresolvedRelation`s in `Substitute` batch before `ResolveRelations`
 * rule is applied. Here are two reasons.
 * - To support `MetastoreRelation` in Hive module.
 * - To reduce the effect of `Hint` on the other rules.
 *
 * After this rule, it is guaranteed that there exists no unknown `Hint` in the plan.
 * All new `Hint`s should be transformed into concrete Hint classes `BroadcastHint` here.
 */
class SubstituteHints(conf: CatalystConf) extends Rule[LogicalPlan] {
  private val BROADCAST_HINT_NAMES = immutable.Set("BROADCAST", "BROADCASTJOIN", "MAPJOIN")

  def resolver: Resolver = conf.resolver

  private def appendAllDescendant(set: mutable.Set[LogicalPlan], plan: LogicalPlan): Unit = {
    set += plan
    plan.children.foreach { child => appendAllDescendant(set, child) }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case logical: LogicalPlan => logical transformDown {
      case h @ Hint(name, parameters, child) if BROADCAST_HINT_NAMES.contains(name.toUpperCase) =>
        var resolvedChild = child
        for (table <- parameters) {
          var stop = false
          val skipNodeSet = scala.collection.mutable.Set.empty[LogicalPlan]
          resolvedChild = resolvedChild.transformDown {
            case n if skipNodeSet.contains(n) =>
              skipNodeSet -= n
              n
            case p @ Project(_, _) if p != resolvedChild =>
              appendAllDescendant(skipNodeSet, p)
              skipNodeSet -= p
              p
            case r @ BroadcastHint(UnresolvedRelation(t, _))
              if !stop && resolver(t.table, table) =>
              stop = true
              r
            case r @ UnresolvedRelation(t, alias) if !stop && resolver(t.table, table) =>
              stop = true
              if (alias.isDefined) {
                SubqueryAlias(alias.get, BroadcastHint(r.copy(alias = None)), None)
              } else {
                BroadcastHint(r)
              }
          }
        }
        resolvedChild

      // Remove unrecognized hints
      case Hint(name, _, child) => child
    }
  }
}
