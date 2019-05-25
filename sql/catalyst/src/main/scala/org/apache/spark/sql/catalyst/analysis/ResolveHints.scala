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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.internal.SQLConf


/**
 * Collection of rules related to hints. The only hint currently available is join strategy hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from join strategies.
 */
object ResolveHints {

  /**
   * The list of allowed join strategy hints is defined in [[JoinStrategyHint.strategies]], and a
   * sequence of relation aliases can be specified with a join strategy hint, e.g., "MERGE(a, c)",
   * "BROADCAST(a)". A join strategy hint plan node will be inserted on top of any relation (that
   * is not aliased differently), subquery, or common table expression that match the specified
   * name.
   *
   * The hint resolution works by recursively traversing down the query plan to find a relation or
   * subquery that matches one of the specified relation aliases. The traversal does not go past
   * beyond any view reference, with clause or subquery alias.
   *
   * This rule must happen before common table expressions.
   */
  class ResolveJoinStrategyHints(conf: SQLConf) extends Rule[LogicalPlan] {
    private val STRATEGY_HINT_NAMES = JoinStrategyHint.strategies.flatMap(_.hintAliases)

    def resolver: Resolver = conf.resolver

    private def createHintInfo(hintName: String): HintInfo = {
      HintInfo(strategy =
        JoinStrategyHint.strategies.find(
          _.hintAliases.map(
            _.toUpperCase(Locale.ROOT)).contains(hintName.toUpperCase(Locale.ROOT))))
    }

    private def applyJoinStrategyHint(
        plan: LogicalPlan,
        relations: mutable.HashSet[String],
        hintName: String): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case ResolvedHint(u: UnresolvedRelation, hint)
              if relations.exists(resolver(_, u.tableIdentifier.table)) =>
            relations.remove(u.tableIdentifier.table)
            ResolvedHint(u, createHintInfo(hintName).merge(hint, handleOverriddenHintInfo))
          case ResolvedHint(r: SubqueryAlias, hint)
              if relations.exists(resolver(_, r.alias)) =>
            relations.remove(r.alias)
            ResolvedHint(r, createHintInfo(hintName).merge(hint, handleOverriddenHintInfo))

          case u: UnresolvedRelation if relations.exists(resolver(_, u.tableIdentifier.table)) =>
            relations.remove(u.tableIdentifier.table)
            ResolvedHint(plan, createHintInfo(hintName))
          case r: SubqueryAlias if relations.exists(resolver(_, r.alias)) =>
            relations.remove(r.alias)
            ResolvedHint(plan, createHintInfo(hintName))

          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing strategy hint, there is no chance for a match from this point down.
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
        newNode.mapChildren(child => applyJoinStrategyHint(child, relations, hintName))
      } else {
        newNode
      }
    }

    private def handleOverriddenHintInfo(hint: HintInfo): Unit = {
      logWarning(s"Join hint $hint is overridden by another hint and will not take effect.")
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
      case h: UnresolvedHint if STRATEGY_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
        if (h.parameters.isEmpty) {
          // If there is no table alias specified, apply the hint on the entire subtree.
          ResolvedHint(h.child, createHintInfo(h.name))
        } else {
          // Otherwise, find within the subtree query plans to apply the hint.
          val relationNames = h.parameters.map {
            case tableName: String => tableName
            case tableId: UnresolvedAttribute => tableId.name
            case unsupported => throw new AnalysisException("Join strategy hint parameter " +
              s"should be an identifier or string but was $unsupported (${unsupported.getClass}")
          }
          val relationNameSet = new mutable.HashSet[String]
          relationNames.foreach(relationNameSet.add)

          val applied = applyJoinStrategyHint(h.child, relationNameSet, h.name)
          relationNameSet.foreach { n =>
            logWarning(s"Count not find relation '$n' for join strategy hint " +
              s"'${h.name}${relationNames.mkString("(", ", ", ")")}'.")
          }
          applied
        }
    }
  }

  /**
   * COALESCE Hint accepts name "COALESCE" and "REPARTITION".
   * Its parameter includes a partition number.
   */
  object ResolveCoalesceHints extends Rule[LogicalPlan] {
    private val COALESCE_HINT_NAMES = Set("COALESCE", "REPARTITION")

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case h: UnresolvedHint if COALESCE_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
        val hintName = h.name.toUpperCase(Locale.ROOT)
        val shuffle = hintName match {
          case "REPARTITION" => true
          case "COALESCE" => false
        }
        val numPartitions = h.parameters match {
          case Seq(IntegerLiteral(numPartitions)) =>
            numPartitions
          case Seq(numPartitions: Int) =>
            numPartitions
          case _ =>
            throw new AnalysisException(s"$hintName Hint expects a partition number as parameter")
        }
        Repartition(numPartitions, shuffle, h.child)
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  object RemoveAllHints extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
      case h: UnresolvedHint =>
        logWarning(s"Unrecognized hint: ${h.name}${h.parameters.mkString("(", ", ", ")")}")
        h.child
    }
  }

}
