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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, CTERelationDef, CTERelationRef,
  ExternalLookupHint, ExternalLookupJoin, ExternalLookupJoinShapes, Filter, Join, JoinStrategyHint,
  LateralJoin, LogicalPlan, NearestByJoin, Project, ResolvedHint, SetOperation, SubqueryAlias,
  UnaryNode, Union, View, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.trees.TreePattern.{AS_OF_JOIN, CTE, EXTERNAL_LOOKUP_HINT, JOIN,
  LATERAL_JOIN, LOOKUP_ONLY_RELATION, NEAREST_BY_JOIN, TreePattern}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
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
 * the table directly, or through hints, aliases, views, and projections and filters that it can
 * evaluate on the lookup results, so a join that must be a lookup join either runs as one or fails
 * here, whatever the optimizer later does to its lookup side.
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

  private val SUPPORTED_JOIN_TYPES = Seq(Inner, LeftOuter, LeftSemi, LeftAnti)

  /**
   * An operator that the `EXTERNAL_LOOKUP` hint treats as a join: a [[Join]], an ASOF or NEAREST BY
   * join, and a lateral join that the query writes. None of the last three is a [[Join]] when
   * EliminateResolvedHint attaches the hints, so a join strategy hint on one is dropped. None of
   * them can run as an external lookup join either, so the `EXTERNAL_LOOKUP` hint on one is
   * misplaced and fails, rather than being dropped like a hint that attaches to no join. A lateral
   * join that analysis adds for a table argument of a table function is not written by the query,
   * so it is a unary operator for the hint, as for any other analysis check.
   */
  object HintableJoin {
    def unapply(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: AsOfJoin | _: NearestByJoin => true
      case lateral: LateralJoin => !lateral.containsTag(LateralJoin.BY_TABLE_ARGUMENT)
      case _ => false
    }

    /** The tree patterns of these operators. */
    val patterns: Seq[TreePattern] = Seq(JOIN, AS_OF_JOIN, NEAREST_BY_JOIN, LATERAL_JOIN)
  }

  def apply(join: Join): Eligibility = {
    // Every other outcome needs the hint or a lookup-only relation on an input of the join.
    if (!join.containsAnyPattern(EXTERNAL_LOOKUP_HINT, LOOKUP_ONLY_RELATION)) return NotLookupJoin
    if (lookupHints(join.left).nonEmpty) {
      return Ineligible(QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE"))
    }
    // The lookup-only table must be on the right side; the sides are never swapped.
    val leftLookupOnlyTable = ExternalLookupRelations.lookupOnlyTable(input(join.left))
    if (leftLookupOnlyTable.isDefined) {
      return ineligible("LOOKUP_TABLE_ON_LEFT", toSQLFreeFormId(leftLookupOnlyTable.get.name()))
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
    val (tableRelation, table) = lookupTable(join.right) match {
      case Right(found) => found
      case Left(rejection) => return rejection
    }
    // Rendered only for an error: Table.name() is free-form, such as the path of the table's
    // files, and need not parse as an identifier.
    lazy val relation = toSQLFreeFormId(table.name())
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
   * The error of an `EXTERNAL_LOOKUP` hint on an input of an ASOF, NEAREST BY or lateral join, none
   * of which runs as an external lookup join, or None. A hint on the left input is on the lookup
   * side of no join, as for a [[Join]]. A hint on the right input of an ASOF or NEAREST BY join
   * asks for a join type that a lookup join does not support, or for a lookup side that reads no
   * DSv2 table. The subquery of a lateral join is checked as a subquery plan.
   */
  def otherJoinHintError(join: LogicalPlan): Option[Throwable] = {
    val (left, right, joinType) = join match {
      case asOf: AsOfJoin => (asOf.left, Some(asOf.right), "ASOF")
      case nearestBy: NearestByJoin => (nearestBy.left, Some(nearestBy.right), "NEAREST BY")
      case lateral: LateralJoin => (lateral.left, None, "LATERAL")
      case _ => return None
    }
    if (!join.containsPattern(EXTERNAL_LOOKUP_HINT)) None else {
      if (lookupHints(left).nonEmpty) {
        Some(QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE"))
      } else {
        right.filter(lookupHints(_).nonEmpty).map { lookupSide =>
          lookupTable(lookupSide) match {
            case Left(rejection) => rejection.error
            case Right((_, table)) =>
              ineligible("JOIN_TYPE", toSQLFreeFormId(table.name()), Map(
                "joinType" -> toSQLStmt(joinType),
                "supportedJoinTypes" ->
                  SUPPORTED_JOIN_TYPES.map(t => toSQLStmt(t.sql)).mkString(", "))).error
          }
        }
      }
    }
  }

  /**
   * Whether the analysis check of `plan` has anything to check: a join that can be one over a
   * lookup-only table, or the hint with an operator that it can annotate as a join. The pattern
   * bits tell this without a walk, as for [[mayHaveLookupJoin]].
   */
  def mayHaveHintOrLookupJoin(plan: LogicalPlan): Boolean =
    mayHaveLookupJoin(plan) || plan.containsPattern(EXTERNAL_LOOKUP_HINT) &&
      plan.containsAnyPattern(HintableJoin.patterns: _*)

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

  /**
   * Classifies `join`, a join of a plan whose CTE definitions are `cteDefs`, and returns the
   * classification with the join that [[inlineCTEs]] inlines. The analysis check, the formation,
   * cache substitution and the start check all classify a join this way.
   *
   * The inlined join has every CTE reference on the paths of its inputs inlined, so that the
   * classification sees the tables that the join reads. A join that must run as an external lookup
   * join but whose lookup side reads a CTE definition that InlineCTE keeps, such as a MATERIALIZED
   * one, is ineligible: inlining the definition into the lookup join would silently drop its
   * materialization. So a lookup join is formed from the inlined join only when the join is
   * eligible, and then no such definition is inlined.
   */
  def classify(join: Join, cteDefs: Map[Long, CTERelationDef]): (Join, Eligibility) = {
    val inlined = inlineCTEs(join, cteDefs)
    val eligibility = apply(inlined) match {
      case eligibility @ (Eligible | MissingCondition) =>
        keptCTE(join.right, cteDefs).map { cteDef =>
          val cteName = cteDef.child match {
            case alias: SubqueryAlias => alias.alias
            case _ => cteDef.id.toString
          }
          Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
            "MATERIALIZED_CTE", Map("cteName" -> toSQLId(cteName))))
        }.getOrElse(eligibility)
      case other => other
    }
    (inlined, eligibility)
  }

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
   * The first CTE definition that InlineCTE keeps, whatever its references, on the path that
   * [[inlinePathCTEs]] inlines below `plan`.
   */
  private def keptCTE(
      plan: LogicalPlan,
      cteDefs: Map[Long, CTERelationDef]): Option[CTERelationDef] = plan match {
    case _ if !plan.containsPattern(CTE) => None
    case ref: CTERelationRef if cteDefs.contains(ref.cteId) =>
      val cteDef = cteDefs(ref.cteId)
      if (InlineCTE.keepsDefinition(cteDef)) Some(cteDef) else keptCTE(cteDef.child, cteDefs)
    case WithCTE(child, _) => keptCTE(child, cteDefs)
    case unary: UnaryNode => keptCTE(unary.child, cteDefs)
    case _ => None
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
        case join: Join if classify(join, cteDefs)._2 != NotLookupJoin => join.right
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
   * eligible join is a chain of hints, aliases, views, projections and filters over the table, so
   * every marker in it is the join's own.
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

  /**
   * The DSv2 table that a lookup side reads through hints, aliases, views, and projections and
   * filters that the lookup join can evaluate, with its relation, or the rejection of a side that
   * reads none: another operator, or a stage that the join cannot evaluate, stands in between, or
   * the side reads no DSv2 table.
   */
  private def lookupTable(lookupSide: LogicalPlan): Either[Ineligible, (LogicalPlan, Table)] = {
    val tableRelation = lookupRelation(lookupSide)
    ExternalLookupRelations.table(tableRelation) match {
      case Some(table) => Right(tableRelation -> table)
      case None =>
        Left(Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError("LOOKUP_PLAN")))
    }
  }

  /**
   * The first operator below a join input that is not a hint, an alias, a view, or a projection or
   * filter that the lookup join can evaluate on the lookup results. A Python or external UDF or a
   * subquery in one cannot be evaluated there, and the optimizer would rewrite some of them into
   * other operators.
   */
  @tailrec
  private def lookupRelation(plan: LogicalPlan): LogicalPlan = plan match {
    case node @ (_: ExternalLookupHint | _: ResolvedHint | _: SubqueryAlias | _: View) =>
      lookupRelation(node.children.head)
    case node @ (_: Project | _: Filter)
        if node.expressions.forall(ExternalLookupJoinShapes.canEvaluateInLookupStage) =>
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
 *
 * The hint on an input of an ASOF, NEAREST BY or lateral join fails here too, because none of
 * them can run as an external lookup join: see [[ExternalLookupJoinEligibility.HintableJoin]].
 */
object CheckExternalLookupJoin extends (LogicalPlan => Unit) {
  import ExternalLookupJoinEligibility.HintableJoin

  override def apply(plan: LogicalPlan): Unit = {
    if (ExternalLookupJoinEligibility.mayHaveHintOrLookupJoin(plan)) {
      val complete = plan.isInstanceOf[WriteToStream]
      // CheckAnalysis inlines every CTE but those that InlineCTE keeps even then.
      val cteDefs = ExternalLookupJoinEligibility.cteDefinitions(plan)
      plan.foreachUp {
        case join: Join if join.resolved =>
          ExternalLookupJoinEligibility.classify(join, cteDefs)._2 match {
            case ExternalLookupJoinEligibility.Ineligible(error) => throw error
            case ExternalLookupJoinEligibility.MissingCondition if complete =>
              throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
                "MISSING_JOIN_CONDITION")
            case _ =>
          }
        case join @ HintableJoin() if join.resolved =>
          ExternalLookupJoinEligibility.otherJoinHintError(join).foreach(error => throw error)
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

  /**
   * Reports a join that must run as an external lookup join but whose lookup side reads a CTE
   * definition that InlineCTE keeps, such as a MATERIALIZED one. CheckAnalysis runs the other
   * checks on a copy of the plan in which it inlines MATERIALIZED CTEs too, so it runs this one on
   * the plan itself, as it runs MaterializedCTECheck. Only a plan with a join, a CTE, and the hint
   * or a lookup-only table, as its pattern bits tell, is walked.
   */
  def checkKeptCTEs(plan: LogicalPlan): Unit = {
    if (plan.containsPattern(CTE) && ExternalLookupJoinEligibility.mayHaveLookupJoin(plan)) {
      val cteDefs = ExternalLookupJoinEligibility.cteDefinitions(plan)
      if (cteDefs.values.exists(InlineCTE.keepsDefinition(_))) {
        plan.foreachUp {
          case join: Join if join.resolved =>
            ExternalLookupJoinEligibility.classify(join, cteDefs)._2 match {
              case ExternalLookupJoinEligibility.Ineligible(error) => throw error
              case _ =>
            }
          case _ =>
        }
      }
    }
  }

  private def readsJoin(plan: LogicalPlan): Boolean = plan match {
    case HintableJoin() => true
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
 * the lookup side into the lookup join. A lookup side that reads a CTE definition that InlineCTE
 * keeps fails instead, so that every such definition keeps InlineCTE's decision.
 */
object FormExternalLookupJoin extends Rule[LogicalPlan] {
  import ExternalLookupJoinEligibility._

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (mayHaveLookupJoin(plan)) formLookupJoins(plan) else stripHints(plan)

  private def formLookupJoins(plan: LogicalPlan): LogicalPlan = {
    val cteDefs = cteDefinitions(plan)
    val formed = plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join: Join =>
        val (inlined, eligibility) = classify(join, cteDefs)
        eligibility match {
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
