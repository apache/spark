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

import org.apache.spark.sql.catalyst.expressions.{CommonExpressionRef, Expression,
  SubqueryExpression, With}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinStrategyHint,
  LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, JOIN, WITH_EXPRESSION}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.v2.ExternalLookupRelations

/**
 * Classifies a join for external lookup. [[CheckExternalLookupJoin]] and [[FormExternalLookupJoin]]
 * share it, so a join that passes analysis is exactly a join that the optimizer converts.
 *
 * A join is an external lookup join when its right side reads a table that supports external
 * lookup but not batch read. It must be a stream-static join: the left side is a streaming query,
 * whose micro-batches tolerate lookup results that change between requests, and the lookup-only
 * table is on the right side.
 */
object ExternalLookupJoinEligibility extends QueryErrorsBase {
  sealed trait Eligibility
  /** The join reads no lookup-only table and stays an ordinary join. */
  case object NotLookupJoin extends Eligibility
  /** The join runs as an external lookup join. */
  case object Eligible extends Eligibility
  /** The join reads a lookup-only table but has no join condition. */
  case object MissingCondition extends Eligibility
  /** The join reads a lookup-only table, which no other join can read, but cannot run. */
  case class Ineligible(error: Throwable) extends Eligibility

  private val SupportedJoinTypes = Seq(Inner, LeftOuter)

  def apply(join: Join): Eligibility = {
    // The lookup-only table must be on the right side; the sides are never swapped.
    // TODO: look through projections, filters and aliases above the lookup relation.
    // TODO: allow the EXTERNAL_LOOKUP hint once it is introduced, and classify the joins it opts
    //   in.
    (ExternalLookupRelations.lookupOnlyTable(join.left),
      ExternalLookupRelations.lookupOnlyTable(join.right)) match {
      case (Some(table), _) => ineligible("LOOKUP_TABLE_ON_LEFT", table)
      case (None, Some(table)) => lookupJoinEligibility(join, table)
      case (None, None) => NotLookupJoin
    }
  }

  private def lookupJoinEligibility(join: Join, table: Table): Eligibility = {
    if (!join.left.isStreaming) return ineligible("STATIC_LEFT_SIDE", table)
    // A join hint the user wrote is never silently dropped: an external lookup join cannot run a
    // physical join strategy, and a lookup-only table has no batch scan for one.
    joinStrategyHint(join) match {
      case Some(strategy) =>
        ineligible("CONFLICTING_JOIN_STRATEGY", table,
          Map("strategy" -> toSQLStmt(strategy.displayName)))
      case None if join.condition.isEmpty => MissingCondition
      case None if !SupportedJoinTypes.contains(join.joinType) =>
        ineligible("JOIN_TYPE", table, Map(
          "joinType" -> toSQLStmt(join.joinType.sql),
          "supportedJoinTypes" -> SupportedJoinTypes.map(t => toSQLStmt(t.sql)).mkString(", ")))
      // The optimizer rewrites a subquery in a join condition only for a Join, which a lookup join
      // no longer is by then.
      case None if join.condition.exists(SubqueryExpression.hasSubquery) =>
        ineligible("SUBQUERY_IN_JOIN_CONDITION", table)
      case None => Eligible
    }
  }

  private def ineligible(
      subClass: String,
      table: Table,
      parameters: Map[String, String] = Map.empty): Ineligible =
    Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
      subClass, parameters + ("relation" -> toSQLId(table.name()))))

  private def joinStrategyHint(join: Join): Option[JoinStrategyHint] = {
    // Only the hints that EliminateResolvedHint will attach to this join count; a strategy hint
    // on a nested join belongs to that join.
    val attached = join.hint.leftHint.toSeq ++ join.hint.rightHint.toSeq ++
      Seq(join.left, join.right).flatMap(EliminateResolvedHint.extractHintsFromPlan(_)._2)
    attached.flatMap(_.strategy).headOption
  }
}

/**
 * Reports, while a query is analyzed, why a join over a lookup-only table cannot run as an
 * external lookup join. It does not change the plan: the join stays a [[Join]] until
 * [[FormExternalLookupJoin]] converts it, so the APIs and rules that expect an analyzed join keep
 * working. A join that is not resolved, for example one whose condition is not boolean, is left to
 * the analyzer's own join checks.
 *
 * A missing join condition is not reported here, because the classic Dataset API analyzes a USING
 * join first as a join without a condition.
 */
object CheckExternalLookupJoin extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    if (plan.containsPattern(JOIN)) {
      plan.foreachUp {
        case join: Join if join.resolved =>
          ExternalLookupJoinEligibility(join) match {
            case ExternalLookupJoinEligibility.Ineligible(error) => throw error
            case _ =>
          }
        case _ =>
      }
    }
  }
}

/**
 * Replaces each join that runs as an external lookup join with an [[ExternalLookupJoin]].
 *
 * It must run at the start of optimization, before Finish Analysis and operator optimization, so
 * that it sees the join and its lookup side as analysis left them: no hint or alias eliminated,
 * no predicate pushed into the join and no column pruned. These are the shapes that
 * [[CheckExternalLookupJoin]] validated.
 */
object FormExternalLookupJoin extends Rule[LogicalPlan] {
  import ExternalLookupJoinEligibility._

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(JOIN), ruleId) {
      case join: Join =>
        ExternalLookupJoinEligibility(join) match {
          case Eligible =>
            ExternalLookupJoin(
              join.left,
              join.right,
              join.joinType,
              join.condition.map(inlineCommonExpressions),
              join.right.output)
          // Operator optimization could push a filter's predicate into the join later, but the
          // condition must be written in the join.
          case MissingCondition =>
            throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
              "MISSING_JOIN_CONDITION")
          case Ineligible(error) => throw error
          case NotLookupJoin => join
        }
    }

  /**
   * Inlines the common expressions of every `With` in a lookup join condition, such as the input of
   * `BETWEEN`, at each of their references. `RewriteWithExpression` would otherwise evaluate one
   * that is not cheap in a projection below the join, over the lookup side, which the lookup join
   * cannot read. A definition can be evaluated more than once: analysis allows only deterministic
   * join conditions.
   */
  private def inlineCommonExpressions(condition: Expression): Expression =
    condition.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case With(child, defs) =>
        val definitions = defs.map(definition => definition.id -> definition.child).toMap
        // A reference to an enclosing `With` is inlined when the transformation reaches it.
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef if definitions.contains(ref.id) => definitions(ref.id)
        }
    }
}
