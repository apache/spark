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

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.internal.SQLConf


/**
 * Collection of rules related to hints. The only hint currently available is broadcast join hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from broadcast.
 */
object ResolveHints {

  /**
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
  class ResolveBroadcastHints(conf: SQLConf) extends Rule[LogicalPlan] {
    private val BROADCAST_HINT_NAMES = Set("BROADCAST", "BROADCASTJOIN", "MAPJOIN")

    def resolver: Resolver = conf.resolver

    private def applyBroadcastHint(plan: LogicalPlan, toBroadcast: Set[String]): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case u: UnresolvedRelation if toBroadcast.exists(resolver(_, u.tableIdentifier.table)) =>
            ResolvedHint(plan, HintInfo(isBroadcastable = Option(true)))
          case r: SubqueryAlias if toBroadcast.exists(resolver(_, r.alias)) =>
            ResolvedHint(plan, HintInfo(isBroadcastable = Option(true)))

          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing broadcast hint, there is no point going down (if we do, we either
            // won't change the structure, or will introduce another broadcast hint that is useless.
            // The rest (view, with, subquery) indicates different scopes that we shouldn't traverse
            // down. Note that technically when this rule is executed, we haven't completed view
            // resolution yet and as a result the view part should be deadcode. I'm leaving it here
            // to be more future proof in case we change the view we do view resolution.
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
      case h: UnresolvedHint if BROADCAST_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
        if (h.parameters.isEmpty) {
          // If there is no table alias specified, turn the entire subtree into a BroadcastHint.
          ResolvedHint(h.child, HintInfo(isBroadcastable = Option(true)))
        } else {
          // Otherwise, find within the subtree query plans that should be broadcasted.
          applyBroadcastHint(h.child, h.parameters.map {
            case tableName: String => tableName
            case tableId: UnresolvedAttribute => tableId.name
            case unsupported => throw new AnalysisException("Broadcast hint parameter should be " +
              s"an identifier or string but was $unsupported (${unsupported.getClass}")
          }.toSet)
        }
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  object RemoveAllHints extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case h: UnresolvedHint => h.child
    }
  }

}
