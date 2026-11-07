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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupHint, ExternalLookupJoin, Join,
  JoinStrategyHint, LogicalPlan, ResolvedHint, SubqueryAlias, UnaryNode, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.trees.TreePattern.{DATA_SOURCE_V2_RELATION,
  EXTERNAL_LOOKUP_HINT, JOIN}
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.v2.ExternalLookupRelations

/**
 * Classifies a join for external lookup. [[CheckExternalLookupJoin]] and [[FormExternalLookupJoin]]
 * share it, so analysis rejects every join that the optimizer cannot convert, except one without
 * a condition: see [[CheckExternalLookupJoin]].
 *
 * A join is an external lookup join when its right side reads a table that supports external
 * lookup but not batch read, or when the right side carries an `EXTERNAL_LOOKUP` hint. A table
 * that supports both keeps the ordinary join strategies unless the hint opts in. It must be a
 * stream-static join: the left side is a streaming query, whose micro-batches tolerate lookup
 * results that change between requests, and the lookup table is on the right side. The join reads
 * the table directly, or through hints, aliases and views only, so a join that must be a lookup
 * join either runs as one or fails here, whatever the optimizer later does to its lookup side.
 */
object ExternalLookupJoinEligibility extends QueryErrorsBase {
  sealed trait Eligibility
  /**
   * The join stays an ordinary join: it has no `EXTERNAL_LOOKUP` hint, and neither of its inputs is
   * a lookup-only relation, directly or below unary operators. A lookup-only relation below another
   * operator, such as a union, is not recognized, and fails later as a batch scan.
   */
  case object NotLookupJoin extends Eligibility
  /** The join runs as an external lookup join. */
  case object Eligible extends Eligibility
  /**
   * The join must run as an external lookup join, with a supported join type, but has no
   * condition.
   */
  case object MissingCondition extends Eligibility
  /** The join must run as an external lookup join but cannot. */
  case class Ineligible(error: Throwable) extends Eligibility

  private val SUPPORTED_JOIN_TYPES = Seq(Inner, LeftOuter)

  def apply(join: Join): Eligibility = {
    if (lookupHints(join.left).nonEmpty) {
      return Ineligible(QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE"))
    }
    // The lookup-only table must be on the right side; the sides are never swapped.
    val leftLookupOnlyTable = ExternalLookupRelations.lookupOnlyTable(input(join.left))
    if (leftLookupOnlyTable.isDefined) {
      return ineligible("LOOKUP_TABLE_ON_LEFT", toSQLId(leftLookupOnlyTable.get.name()))
    }
    val markers = lookupHints(join.right)
    val lookupOnly = ExternalLookupRelations.lookupOnlyTable(input(join.right)).isDefined
    // A lookup-only table can be read only by a lookup join, and the hint opts a join in. Either
    // way the join must run as an external lookup join, and the checks below apply.
    if (!lookupOnly && markers.isEmpty) return NotLookupJoin
    if (markers.length > 1) {
      return Ineligible(QueryCompilationErrors.invalidExternalLookupHintError("MULTIPLE_HINTS"))
    }
    if (join.right.isStreaming) {
      return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "STREAMING_LOOKUP_SIDE"))
    }
    // The DSv2 table that the lookup side reads through hints, aliases and views only. There is
    // none when another operator stands in between or the side reads no DSv2 table.
    val table = ExternalLookupRelations.table(lookupRelation(join.right)) match {
      case Some(table) => table
      case None =>
        return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError("LOOKUP_PLAN"))
    }
    val relation = toSQLId(table.name())
    if (!table.capabilities().contains(TableCapability.EXTERNAL_LOOKUP)) {
      return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "TABLE", Map("table" -> relation)))
    }
    if (!join.left.isStreaming) return ineligible("STATIC_LEFT_SIDE", relation)
    // A join hint the user wrote is never silently dropped: an external lookup join cannot run a
    // physical join strategy, and a lookup-only table has no batch scan for one. On a table that
    // also supports batch reads, the strategy hint conflicts with the `EXTERNAL_LOOKUP` hint.
    joinStrategyHint(join) match {
      // The user asked for a physical join, which an external lookup join cannot run.
      case Some(strategy) if lookupOnly =>
        ineligible("CONFLICTING_JOIN_STRATEGY", relation,
          Map("strategy" -> toSQLStmt(strategy.displayName)))
      case Some(strategy) =>
        Ineligible(QueryCompilationErrors.invalidExternalLookupHintError(
          "CONFLICTING_JOIN_STRATEGY",
          Map("strategy" -> toSQLStmt(strategy.displayName), "relation" -> relation)))
      // No physical join was asked for: the join runs as an external lookup join unless a check
      // below fails.
      // Checked before the condition, so that a cross join fails while it is analyzed.
      case None if !SUPPORTED_JOIN_TYPES.contains(join.joinType) =>
        ineligible("JOIN_TYPE", relation, Map(
          "joinType" -> toSQLStmt(join.joinType.sql),
          "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES.map(t => toSQLStmt(t.sql)).mkString(", ")))
      case None if join.condition.isEmpty => MissingCondition
      // The optimizer rewrites a subquery in a join condition only for a Join, which a lookup join
      // no longer is by then.
      case None if join.condition.exists(SubqueryExpression.hasSubquery) =>
        ineligible("SUBQUERY_IN_JOIN_CONDITION", relation)
      case None => Eligible
    }
  }

  /**
   * The lookup side of an eligible join, without its `EXTERNAL_LOOKUP` hint. The lookup side of an
   * eligible join is a chain of hints, aliases and views over the table, so every marker in it is
   * the join's own.
   */
  def lookupSide(join: Join): LogicalPlan =
    join.right.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_HINT)) {
      case marker: ExternalLookupHint => marker.child
    }

  private def ineligible(
      subClass: String,
      relation: String,
      parameters: Map[String, String] = Map.empty): Ineligible =
    Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
      subClass, parameters + ("relation" -> relation)))

  /** The relation that a join input reads: the first operator that has no single child. */
  @tailrec
  private def input(plan: LogicalPlan): LogicalPlan = plan match {
    case unary: UnaryNode => input(unary.child)
    case other => other
  }

  /** The first operator below a join input that is not a hint, an alias or a view. */
  @tailrec
  private def lookupRelation(plan: LogicalPlan): LogicalPlan = plan match {
    case node @ (_: ExternalLookupHint | _: ResolvedHint | _: SubqueryAlias | _: View) =>
      lookupRelation(node.children.head)
    case other => other
  }

  /**
   * The `EXTERNAL_LOOKUP` hints on a join input. Like a join strategy hint, the hint applies to the
   * join that the input feeds, through any unary operator.
   */
  @tailrec
  private def lookupHints(
      plan: LogicalPlan,
      found: Seq[ExternalLookupHint] = Nil): Seq[ExternalLookupHint] = plan match {
    case marker: ExternalLookupHint => lookupHints(marker.child, found :+ marker)
    case unary: UnaryNode => lookupHints(unary.child, found)
    case _ => found
  }

  /**
   * The physical join strategy hint, such as `BROADCAST` or `MERGE`, that EliminateResolvedHint
   * would attach to the join, or None if there is none. The `EXTERNAL_LOOKUP` hint is never
   * returned: it is an [[ExternalLookupHint]] marker, not a [[JoinStrategyHint]]. A `HintInfo`
   * without a strategy asks for nothing, so it is skipped.
   */
  private def joinStrategyHint(join: Join): Option[JoinStrategyHint] = {
    // Only the hints that EliminateResolvedHint will attach to this join count; a strategy hint
    // on a nested join belongs to that join.
    val attached = join.hint.leftHint.toSeq ++ join.hint.rightHint.toSeq ++
      Seq(join.left, join.right).flatMap(EliminateResolvedHint.extractHintsFromPlan(_)._2)
    attached.flatMap(_.strategy).headOption
  }
}

/**
 * Reports, while a query is analyzed, why a join cannot run as the external lookup join it must
 * be. It does not change the plan: the join stays a [[Join]] until [[FormExternalLookupJoin]]
 * converts it, so the APIs and rules that expect an analyzed join keep working. A join that is not
 * resolved, for example one whose condition is not boolean, is left to the analyzer's own join
 * checks.
 *
 * A missing join condition is reported only for the query of a streaming write, which is analyzed
 * in full when the query starts, so that `start()` throws it. Other plans can still get their
 * condition: the classic Dataset API analyzes a USING join first as a join without a condition.
 */
object CheckExternalLookupJoin extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    // A join must run as a lookup join only over a lookup-only table, which is a DSv2 relation, or
    // under the hint, so other plans need no walk.
    if (plan.containsPattern(JOIN) &&
        plan.containsAnyPattern(DATA_SOURCE_V2_RELATION, EXTERNAL_LOOKUP_HINT)) {
      val complete = plan.isInstanceOf[WriteToStream]
      plan.foreachUp {
        case join: Join if join.resolved =>
          ExternalLookupJoinEligibility(join) match {
            case ExternalLookupJoinEligibility.Ineligible(error) => throw error
            case ExternalLookupJoinEligibility.MissingCondition if complete =>
              throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
                "MISSING_JOIN_CONDITION")
            case _ =>
          }
        case _ =>
      }
    }
  }
}

/**
 * Replaces each join that runs as an external lookup join with an [[ExternalLookupJoin]], and
 * removes the `EXTERNAL_LOOKUP` hints that no join consumed.
 *
 * It must run at the start of optimization, before Finish Analysis and operator optimization, so
 * that it sees the join and its lookup side as analysis left them: no hint or alias eliminated,
 * no predicate pushed into the join and no column pruned. These are the shapes that
 * [[CheckExternalLookupJoin]] validated.
 */
object FormExternalLookupJoin extends Rule[LogicalPlan] {
  import ExternalLookupJoinEligibility._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val formed = plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join: Join =>
        ExternalLookupJoinEligibility(join) match {
          case Eligible =>
            val lookupPlan = lookupSide(join)
            ExternalLookupJoin(
              join.left,
              lookupPlan,
              join.joinType,
              join.condition,
              lookupPlan.output)
          // Operator optimization could push a filter's predicate into the join later, but the
          // condition must be written in the join.
          case MissingCondition =>
            throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
              "MISSING_JOIN_CONDITION")
          case Ineligible(error) => throw error
          case NotLookupJoin => join
        }
    }
    // A join with the hint on its lookup side has been converted or has failed, so a hint that is
    // left is not on the lookup side of any join.
    formed.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_HINT)) {
      case hint: ExternalLookupHint =>
        logWarning("An EXTERNAL_LOOKUP hint is not on the lookup side of a join and is ignored.")
        hint.child
    }
  }
}
