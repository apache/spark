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

import org.apache.spark.sql.catalyst.expressions.StringLiteral
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf


/**
 * Collection of rules related to hints. The only hint currently available is join strategy hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from join strategies.
 */
object ResolveHints {

  /**
   * Checks if the given multi-part identifiers are matched with each other.
   *
   * The [[ResolveJoinStrategyHints]] rule is applied before the resolution batch in the analyzer
   * and we cannot semantically compare them at this stage. Therefore, we follow a simple rule;
   * they match if an identifier in a hint is a tail of an identifier in a relation. This process
   * is independent of a session catalog (`currentDb` in [[SessionCatalog]]) and it just compares
   * them literally.
   *
   * For example,
   *  - in a query `SELECT /*+ BROADCAST(t) */ * FROM db1.t JOIN t`,
   *    the broadcast hint will match both tables, `db1.t` and `t`,
   *    even when the current db is `db2`.
   *  - in a query `SELECT /*+ BROADCAST(default.t) */ * FROM default.t JOIN t`,
   *    the broadcast hint will match the left-side table only, `default.t`.
   *
   * The `resolver` is passed in explicitly so that this logic can be shared by both the
   * fixed-point analyzer and the single-pass resolver, neither of which has to depend on the
   * other's rule state.
   */
  def matchedIdentifier(
      identInHint: Seq[String],
      identInQuery: Seq[String],
      resolver: Resolver): Boolean = {
    if (identInHint.length <= identInQuery.length) {
      identInHint.zip(identInQuery.takeRight(identInHint.length))
        .forall { case (i1, i2) => resolver(i1, i2) }
    } else {
      false
    }
  }

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
  object ResolveJoinStrategyHints extends Rule[LogicalPlan] {
    private def hintErrorHandler = conf.hintErrorHandler

    def resolver: Resolver = conf.resolver

    private def createHintInfo(hintName: String): HintInfo = {
      HintInfo(strategy =
        JoinStrategyHint.strategies.find(
          _.hintAliases.map(
            _.toUpperCase(Locale.ROOT)).contains(hintName.toUpperCase(Locale.ROOT))))
    }

    private def matchedIdentifier(identInHint: Seq[String], identInQuery: Seq[String]): Boolean =
      ResolveHints.matchedIdentifier(identInHint, identInQuery, resolver)

    private def extractIdentifier(r: SubqueryAlias): Seq[String] = {
      r.identifier.qualifier :+ r.identifier.name
    }

    private def applyJoinStrategyHint(
        plan: LogicalPlan,
        relationsInHint: Set[Seq[String]],
        relationsInHintWithMatch: mutable.HashSet[Seq[String]],
        hintName: String): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      def matchedIdentifierInHint(identInQuery: Seq[String]): Boolean = {
        relationsInHint.find(matchedIdentifier(_, identInQuery))
          .map(relationsInHintWithMatch.add).nonEmpty
      }

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case ResolvedHint(u @ UnresolvedRelation(ident, _, _), hint)
              if matchedIdentifierInHint(ident) =>
            ResolvedHint(u, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case ResolvedHint(r: SubqueryAlias, hint)
              if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(r, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case UnresolvedRelation(ident, _, _) if matchedIdentifierInHint(ident) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case r: SubqueryAlias if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case _: ResolvedHint | _: View | _: UnresolvedWith | _: SubqueryAlias =>
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
        newNode.mapChildren { child =>
          applyJoinStrategyHint(child, relationsInHint, relationsInHintWithMatch, hintName)
        }
      } else {
        newNode
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case h: UnresolvedHint if JoinStrategyHint.isJoinStrategyHintName(h.name) =>
        if (h.parameters.isEmpty) {
          // If there is no table alias specified, apply the hint on the entire subtree.
          ResolvedHint(h.child, createHintInfo(h.name))
        } else {
          // Otherwise, find within the subtree query plans to apply the hint.
          val relationNamesInHint = h.parameters.map {
            case StringLiteral(tableName) => UnresolvedAttribute.parseAttributeName(tableName)
            case tableId: UnresolvedAttribute => tableId.nameParts
            case unsupported =>
              throw QueryCompilationErrors.joinStrategyHintParameterNotSupportedError(unsupported)
          }.toSet
          val relationsInHintWithMatch = new mutable.HashSet[Seq[String]]
          val applied = applyJoinStrategyHint(
            h.child, relationNamesInHint, relationsInHintWithMatch, h.name)

          // Filters unmatched relation identifiers in the hint
          val unmatchedIdents = relationNamesInHint -- relationsInHintWithMatch
          hintErrorHandler.hintRelationsNotFound(h.name, h.parameters, unmatchedIdents)
          applied
        }
    }
  }

  /**
   * COALESCE Hint accepts names "COALESCE", "REPARTITION", "REPARTITION_BY_RANGE", "REBALANCE"
   * and "REBALANCE_BY_SIZE".
   */
  object ResolveCoalesceHints extends Rule[LogicalPlan] {
    import CoalesceHintUtils._

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_HINT), ruleId) {
      case hint @ UnresolvedHint(hintName, _, _) => hintName.toUpperCase(Locale.ROOT) match {
          case "REPARTITION" =>
            createRepartition(shuffle = true, transformStringToAttribute(hint))
          case "COALESCE" =>
            createRepartition(shuffle = false, transformStringToAttribute(hint))
          case "REPARTITION_BY_RANGE" =>
            createRepartitionByRange(transformStringToAttribute(hint))
          case "REBALANCE" if conf.adaptiveExecutionEnabled =>
            createRebalance(transformStringToAttribute(hint))
          case "REBALANCE_BY_SIZE" if conf.adaptiveExecutionEnabled =>
            createRebalanceBySize(transformStringToAttribute(hint, skipFirstParameter = true))
          case _ => hint
        }
    }
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  class RemoveAllHints extends Rule[LogicalPlan] {

    private def hintErrorHandler = conf.hintErrorHandler

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_HINT)) {
      case h: UnresolvedHint =>
        hintErrorHandler.hintNotRecognized(h.name, h.parameters)
        h.child
    }
  }

  /**
   * Removes all the hints when `spark.sql.optimizer.disableHints` is set.
   * This is executed at the very beginning of the Analyzer to disable
   * the hint functionality.
   */
  class DisableHints extends RemoveAllHints {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (conf.getConf(SQLConf.DISABLE_HINTS)) super.apply(plan) else plan
    }
  }
}
