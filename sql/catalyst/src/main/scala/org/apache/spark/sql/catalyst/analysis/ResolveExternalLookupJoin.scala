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

import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  JoinStrategyHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.v2.ExternalLookupRelations

/**
 * Replaces an eligible join with a lookup join holder.
 *
 * A join is planned as an external lookup join when its right side reads a table that supports
 * external lookup but not batch read. It must be a stream-static join: the left side is a
 * streaming query, whose micro-batches tolerate lookup results that change between requests, and
 * the lookup-only table is on the right side.
 */
object ResolveExternalLookupJoin extends Rule[LogicalPlan] with QueryErrorsBase {
  private val SupportedJoinTypes = Seq(Inner, LeftOuter)

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsPattern(JOIN), ruleId) {
      case join: Join if join.left.resolved && join.right.resolved => resolveLookupJoin(join)
    }

  private def resolveLookupJoin(join: Join): LogicalPlan = {
    val Join(left, right, joinType, condition, hint) = join
    // The lookup-only table must be on the right side; the sides are never swapped.
    ExternalLookupRelations.lookupOnlyTable(left).foreach { table =>
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "LOOKUP_TABLE_ON_LEFT", Map("relation" -> toSQLId(table.name())))
    }
    // TODO: look through projections, filters and aliases above the lookup relation.
    val lookupTable = ExternalLookupRelations.table(right)
    // TODO: allow the EXTERNAL_LOOKUP hint once it is introduced, and apply these checks to the
    //   joins it opts in.
    if (!lookupTable.exists(ExternalLookupRelations.isLookupOnly)) return join
    val relation = lookupTable.map(table => toSQLId(table.name())).getOrElse("the lookup side")
    if (!left.isStreaming) {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "STATIC_LEFT_SIDE", Map("relation" -> relation))
    }
    // A join hint the user wrote is never silently dropped: an external lookup join cannot run a
    // physical join strategy, and a lookup-only table has no batch scan for one.
    joinStrategyHint(hint, left, right).foreach { strategy =>
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "CONFLICTING_JOIN_STRATEGY",
        Map("strategy" -> toSQLStmt(strategy.displayName), "relation" -> relation))
    }
    // A missing condition is reported by PrepareExternalLookupJoin, on the final plan.
    if (condition.isEmpty) return join
    // The join reads a lookup-only table that no other join can read, so a join that cannot be a
    // lookup join fails here with the reason.
    if (!SupportedJoinTypes.contains(joinType)) {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "JOIN_TYPE",
        Map(
          "joinType" -> toSQLStmt(joinType.sql),
          "relation" -> relation,
          "supportedJoinTypes" -> SupportedJoinTypes.map(t => toSQLStmt(t.sql)).mkString(", ")))
    }
    ExternalLookupJoin(
      left,
      right,
      joinType,
      condition,
      right.output)
  }

  private def joinStrategyHint(
      hint: JoinHint,
      left: LogicalPlan,
      right: LogicalPlan): Option[JoinStrategyHint] = {
    // Only the hints that EliminateResolvedHint will attach to this join count; a strategy hint
    // on a nested join belongs to that join.
    val attached = hint.leftHint.toSeq ++ hint.rightHint.toSeq ++ Seq(left, right).flatMap { side =>
      EliminateResolvedHint.extractHintsFromPlan(side)._2
    }
    attached.flatMap(_.strategy).headOption
  }
}
