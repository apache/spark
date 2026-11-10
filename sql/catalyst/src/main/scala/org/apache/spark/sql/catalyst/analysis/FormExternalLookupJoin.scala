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
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.optimizer.{EliminateResolvedHint, InlineCTE}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef,
  ExternalLookupHint, ExternalLookupJoin, Join, JoinStrategyHint, LogicalPlan, ResolvedHint,
  SetOperation, SubqueryAlias, UnaryNode, Union, View, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, EXTERNAL_LOOKUP_HINT, JOIN,
  LOOKUP_ONLY_RELATION}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
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
    // Every other outcome needs the hint or a lookup-only relation on an input of the join.
    if (!join.containsAnyPattern(EXTERNAL_LOOKUP_HINT, LOOKUP_ONLY_RELATION)) return NotLookupJoin
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
    if (join.right.isStreaming) {
      return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "STREAMING_LOOKUP_SIDE"))
    }
    // The DSv2 table that the lookup side reads through hints, aliases and views only. There is
    // none when another operator stands in between or the side reads no DSv2 table.
    val tableRelation = lookupRelation(join.right)
    val table = ExternalLookupRelations.table(tableRelation) match {
      case Some(table) => table
      case None =>
        return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError("LOOKUP_PLAN"))
    }
    val relation = toSQLId(table.name())
    if (!table.capabilities().contains(TableCapability.EXTERNAL_LOOKUP)) {
      return Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "TABLE", Map("table" -> relation)))
    }
    // A lookup returns the data columns of the table only. The relation outputs a metadata column
    // only when the query reads one.
    if (tableRelation.output.exists(_.isMetadataCol)) return ineligible("METADATA_COLUMN", relation)
    if (!join.left.isStreaming) return ineligible("STATIC_LEFT_SIDE", relation)
    // A join hint the user wrote is never silently dropped: an external lookup join cannot run a
    // physical join strategy, and a lookup-only table has no batch scan for one.
    joinStrategyHint(join) match {
      // The user asked for a physical join, which an external lookup join cannot run.
      case Some(strategy) =>
        ineligible("CONFLICTING_JOIN_STRATEGY", relation,
          Map("strategy" -> toSQLStmt(strategy.displayName)))
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
   * Whether `plan` can have a join that must run as an external lookup join: one over a lookup-only
   * table, whose relation carries LOOKUP_ONLY_RELATION, or under the hint. Classifying the joins of
   * a plan inlines its CTE references, so a plan without either is left alone. The pattern bits of
   * a plan include those of its CTE definitions and subquery plans, so they tell this without a
   * walk, and an ordinary query does not pay for lookup joins.
   */
  def mayHaveLookupJoin(plan: LogicalPlan): Boolean =
    plan.containsPattern(JOIN) &&
      plan.containsAnyPattern(EXTERNAL_LOOKUP_HINT, LOOKUP_ONLY_RELATION)

  /** The CTE definitions in `plan` by id, which [[inlineCTEs]] inlines. */
  def cteDefinitions(plan: LogicalPlan): Map[Long, CTERelationDef] =
    if (plan.containsPattern(CTE)) {
      plan.collect { case cteDef: CTERelationDef => cteDef.id -> cteDef }.toMap
    } else {
      Map.empty
    }

  /**
   * `join` with the CTE references on the paths of unary operators of its inputs inlined, as
   * InlineCTE inlines them for the analysis check. Only these paths decide how the join is
   * classified, and the lookup side of an eligible join is its path alone, so it has no other CTE
   * reference left.
   */
  def inlineCTEs(join: Join, cteDefs: Map[Long, CTERelationDef]): Join =
    if (cteDefs.isEmpty) {
      join
    } else {
      join.copy(
        left = inlinePathCTEs(join.left, cteDefs),
        right = inlinePathCTEs(join.right, cteDefs))
    }

  private def inlinePathCTEs(
      plan: LogicalPlan,
      cteDefs: Map[Long, CTERelationDef]): LogicalPlan = plan match {
    case _ if !plan.containsPattern(CTE) => plan
    case ref: CTERelationRef if cteDefs.contains(ref.cteId) =>
      val cteDef = cteDefs(ref.cteId)
      InlineCTE.inlineReference(ref, cteDef, inlinePathCTEs(cteDef.child, cteDefs))
    case WithCTE(child, _) => inlinePathCTEs(child, cteDefs)
    case unary: UnaryNode => unary.withNewChildren(Seq(inlinePathCTEs(unary.child, cteDefs)))
    case other => other
  }

  /**
   * The plans that external lookup joins in `plan` read by lookups: the lookup side of each join
   * that must run as an external lookup join, and the CTE definitions that the side reads,
   * directly or through other CTE definitions. Subqueries are not searched.
   */
  def lookupSidePlans(plan: LogicalPlan): Seq[LogicalPlan] =
    if (!mayHaveLookupJoin(plan)) {
      Nil
    } else {
      val cteDefs = cteDefinitions(plan)
      val lookupSides = plan.collect {
        case join: Join if apply(inlineCTEs(join, cteDefs)) != NotLookupJoin => join.right
      }
      val readCTEs = mutable.LinkedHashMap.empty[Long, CTERelationDef]
      def addReadCTEs(plan: LogicalPlan): Unit = plan.foreach {
        case ref: CTERelationRef if !readCTEs.contains(ref.cteId) =>
          cteDefs.get(ref.cteId).foreach { cteDef =>
            readCTEs(ref.cteId) = cteDef
            addReadCTEs(cteDef)
          }
        case _ =>
      }
      lookupSides.foreach(addReadCTEs)
      lookupSides ++ readCTEs.values
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
    if (ExternalLookupJoinEligibility.mayHaveLookupJoin(plan)) {
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
      // An `EXTERNAL_LOOKUP` hint over a join, or over a set operation such as UNION with a join
      // below one of its inputs, is on the lookup side of no join, so no join could read it. It
      // fails, as a hint that the user wrote is never silently dropped. A hint without a join
      // below it, such as one over a single table, is ignored.
      if (plan.containsPattern(EXTERNAL_LOOKUP_HINT)) {
        plan.foreach {
          case marker: ExternalLookupHint if readsJoin(marker.child) =>
            throw QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE")
          case _ =>
        }
      }
    }
  }

  private def readsJoin(plan: LogicalPlan): Boolean = plan match {
    case _: Join => true
    case unary: UnaryNode => readsJoin(unary.child)
    case union: Union => union.children.exists(readsJoin)
    case setOperation: SetOperation => setOperation.children.exists(readsJoin)
    case _ => false
  }
}

/**
 * Replaces each join that runs as an external lookup join with an [[ExternalLookupJoin]], and
 * removes the `EXTERNAL_LOOKUP` hints that no join consumed.
 *
 * It must run at the start of optimization, before Finish Analysis and operator optimization, so
 * that it sees the join and its lookup side as analysis left them: no hint or alias eliminated,
 * no predicate pushed into the join and no column pruned. These are the shapes that
 * [[CheckExternalLookupJoin]] validated. It also runs before CTEs are inlined, so it classifies a
 * join with the CTEs of its inputs inlined, as the analysis check does, and inlines the CTEs of
 * the lookup side into the lookup join.
 */
object FormExternalLookupJoin extends Rule[LogicalPlan] {
  import ExternalLookupJoinEligibility._

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (mayHaveLookupJoin(plan)) formLookupJoins(plan) else stripHints(plan)

  private def formLookupJoins(plan: LogicalPlan): LogicalPlan = {
    val cteDefs = cteDefinitions(plan)
    val formed = plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join: Join =>
        val inlined = inlineCTEs(join, cteDefs)
        ExternalLookupJoinEligibility(inlined) match {
          case Eligible =>
            val lookupPlan = lookupSide(inlined)
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
    stripHints(formed)
  }

  // A join with the hint on its lookup side has been converted or has failed, so a hint that is
  // left is not on the lookup side of any join.
  private def stripHints(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_HINT)) {
      case hint: ExternalLookupHint =>
        logWarning("An EXTERNAL_LOOKUP hint is not on the lookup side of a join and is ignored.")
        hint.child
    }
}
