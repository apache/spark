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

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{AlterViewAs, Command, CreateView, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves the identifier expressions and builds the original plans/expressions.
 */
class ResolveIdentifierClause(earlyBatches: Seq[RuleExecutor[LogicalPlan]#Batch])
  extends Rule[LogicalPlan] {

  private val executor = new RuleExecutor[LogicalPlan] {
    override def batches: Seq[Batch] = earlyBatches.asInstanceOf[Seq[Batch]]
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case createView: CreateView =>
        if (conf.getConf(SQLConf.VARIABLES_UNDER_IDENTIFIER_IN_VIEW)) {
          apply0(createView)
        } else {
          val referredTempVars = new mutable.ArrayBuffer[Seq[String]]
          // The target-name identifier (child) is not part of the view definition, so a variable
          // used only to compute it must not be recorded as a variable the view refers to.
          val analyzedChild = apply0(createView.child, recordUnderIdentifier = false)
          val analyzedQuery = apply0(createView.query, Some(referredTempVars))
          if (referredTempVars.nonEmpty) {
            throw QueryCompilationErrors.notAllowedToCreatePermanentViewByReferencingTempVarError(
              Seq("unknown"),
              referredTempVars.head
            )
          }
          createView.copy(child = analyzedChild, query = analyzedQuery)
        }
      // Same as [[CreateView]]: only the query body's IDENTIFIER-clause variables are dependencies
      // of the view definition. Without this, a variable used only to compute the ALTER target is
      // recorded and then wrongly rejected by `verifyTemporaryObjectsNotExists`.
      case alterView: AlterViewAs =>
        val analyzedChild = apply0(alterView.child, recordUnderIdentifier = false)
        val analyzedQuery = apply0(alterView.query)
        alterView.copy(child = analyzedChild, query = analyzedQuery)
      case _ => apply0(plan)
    }
  }

  private def apply0(
      plan: LogicalPlan,
      referredTempVars: Option[mutable.ArrayBuffer[Seq[String]]] = None,
      recordUnderIdentifier: Boolean = true): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsAnyPattern(
      UNRESOLVED_IDENTIFIER, PLAN_WITH_UNRESOLVED_IDENTIFIER)) {
      case p: PlanWithUnresolvedIdentifier if p.identifierExpr.resolved && p.childrenResolved =>

        if (referredTempVars.isDefined) {
          referredTempVars.get ++= collectTemporaryVariablesInLogicalPlan(p)
        }

        val resolvedPlan = executor.execute(p.planBuilder.apply(
          IdentifierResolution.evalIdentifierExpr(p.identifierExpr), p.children))
        // Only record the variables when the identifier names something inside a view body. When it
        // instead supplies the *name* of a command (e.g. the target of
        // `CREATE TEMPORARY VIEW IDENTIFIER(v) AS ...`), the evaluated plan is that command and `v`
        // is not a variable the view refers to, so recording it would wrongly persist it as a
        // dependency of the created view.
        if (recordUnderIdentifier && !resolvedPlan.isInstanceOf[Command]) {
          recordTemporaryVariablesUnderIdentifier(p.identifierExpr)
        }
        resolvedPlan
      case w: V2WriteCommand if w.table.isInstanceOf[PlanWithUnresolvedIdentifier] =>
        val p = w.table.asInstanceOf[PlanWithUnresolvedIdentifier]
        if (p.identifierExpr.resolved && p.childrenResolved) {
          if (referredTempVars.isDefined) {
            referredTempVars.get ++= collectTemporaryVariablesInLogicalPlan(p)
          }
          if (recordUnderIdentifier) {
            recordTemporaryVariablesUnderIdentifier(p.identifierExpr)
          }
          executor.execute(p.planBuilder.apply(
            IdentifierResolution.evalIdentifierExpr(p.identifierExpr), p.children)) match {
            case nr: NamedRelation => w.withNewTable(nr)
            case other =>
              throw SparkException.internalError(
                "PlanWithUnresolvedIdentifier in V2WriteCommand.table must materialize " +
                  s"into a NamedRelation, but got: ${other.getClass.getName}")
          }
        } else {
          w
        }
      case other =>
        other.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_IDENTIFIER)) {
          case e: ExpressionWithUnresolvedIdentifier if e.identifierExpr.resolved =>

            if (referredTempVars.isDefined) {
              referredTempVars.get ++= collectTemporaryVariablesInExpressionTree(e)
            }
            if (recordUnderIdentifier) {
              recordTemporaryVariablesUnderIdentifier(e.identifierExpr)
            }

            e.exprBuilder.apply(
              IdentifierResolution.evalIdentifierExpr(e.identifierExpr), e.otherExprs)
        }
    }

  /**
   * Records the temporary variables read by an identifier expression in the [[AnalysisContext]].
   * Evaluating the identifier expression is the last time these references are visible: the
   * placeholder is replaced by the plan or expression built from the evaluated name, which no
   * longer mentions them. Temporary view creation persists the recorded names so that the
   * variables are still resolvable when the stored view text is analyzed again.
   */
  private def recordTemporaryVariablesUnderIdentifier(identifierExpr: Expression): Unit = {
    AnalysisContext.get.referredTempVariableNamesUnderIdentifier ++=
      collectTemporaryVariablesInExpressionTree(identifierExpr)
  }

  private def collectTemporaryVariablesInLogicalPlan(child: LogicalPlan): Seq[Seq[String]] = {
    def collectTempVars(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap { plan =>
        plan.expressions.flatMap { e => collectTemporaryVariablesInExpressionTree(e) }
      }.distinct
    }
    collectTempVars(child)
  }

  // Visits `child` itself as well as its descendants, so that an identifier expression which is
  // just a variable reference is collected too.
  private def collectTemporaryVariablesInExpressionTree(child: Expression): Seq[Seq[String]] = {
    child.flatMap {
      case e: SubqueryExpression => collectTemporaryVariablesInLogicalPlan(e.plan)
      case r: VariableReference => Seq(r.originalNameParts)
      case _ => Seq.empty
    }.distinct
  }
}
