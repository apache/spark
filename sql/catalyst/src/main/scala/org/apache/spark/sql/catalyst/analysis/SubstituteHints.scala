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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin


/**
 * Collection of rules related to hints. The only hint currently available is broadcast join hint.
 *
 * Note that this is separatedly into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from broadcast.
 */
object SubstituteHints {

  /**
   * Substitute Hints.
   *
   * The only hint currently available is broadcast join hint.
   *
   * For broadcast hint, we accept "BROADCAST", "BROADCASTJOIN", and "MAPJOIN", and a sequence of
   * relation aliases can be specified in the hint. A broadcast hint plan node will be inserted
   * on top of any relation (that is not aliased differently), subquery, or common table expression
   * that match the specified name.
   *
   * The hint resolution works by recursively traversing down the query plan to find a relation or
   * subquery that matches one of the specified broadcast aliases. The traversal does not go past
   * beyond any existing broadcast hints, subquery aliases.
   *
   * This rule must happen before common table expressions.
   */
  class SubstituteBroadcastHints(conf: CatalystConf) extends Rule[LogicalPlan] {
    private val BROADCAST_HINT_NAMES = Set("BROADCAST", "BROADCASTJOIN", "MAPJOIN")

    def resolver: Resolver = conf.resolver

    private def applyBroadcastHint(plan: LogicalPlan, toBroadcast: Set[String]): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case r: UnresolvedRelation =>
            val alias = r.alias.getOrElse(r.tableIdentifier.table)
            if (toBroadcast.exists(resolver(_, alias))) BroadcastHint(plan) else plan
          case r: SubqueryAlias =>
            if (toBroadcast.exists(resolver(_, r.alias))) {
              BroadcastHint(plan)
            } else {
              // Don't recurse down subquery aliases if there are no match.
              recurse = false
              plan
            }
          case _: BroadcastHint =>
            // Found a broadcast hint; don't change the plan but also don't recurse down.
            recurse = false
            plan
          case _ =>
            plan
        }
      }

      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren(child => applyBroadcastHint(child, toBroadcast))
      } else {
        newNode
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case h: Hint if BROADCAST_HINT_NAMES.contains(h.name.toUpperCase) =>
        applyBroadcastHint(h.child, h.parameters.toSet)
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  object RemoveAllHints extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case h: Hint => h.child
    }
  }

}
