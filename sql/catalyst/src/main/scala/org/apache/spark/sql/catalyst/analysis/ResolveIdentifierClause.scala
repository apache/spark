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

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, EvalHelper, Expression, SubqueryExpression, VariableReference}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{CreateView, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Resolves the identifier expressions and builds the original plans/expressions.
 */
class ResolveIdentifierClause(earlyBatches: Seq[RuleExecutor[LogicalPlan]#Batch])
  extends Rule[LogicalPlan] with AliasHelper with EvalHelper {

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
          val analyzedChild = apply0(createView.child)
          val analyzedQuery = apply0(createView.query, Some(referredTempVars))
          if (referredTempVars.nonEmpty) {
            throw QueryCompilationErrors.notAllowedToCreatePermanentViewByReferencingTempVarError(
              Seq("unknown"),
              referredTempVars.head
            )
          }
          createView.copy(child = analyzedChild, query = analyzedQuery)
        }
      case _ => apply0(plan)
    }
  }

  private def apply0(
      plan: LogicalPlan,
      referredTempVars: Option[mutable.ArrayBuffer[Seq[String]]] = None): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsAnyPattern(
      UNRESOLVED_IDENTIFIER, PLAN_WITH_UNRESOLVED_IDENTIFIER)) {
      case p: PlanWithUnresolvedIdentifier if p.identifierExpr.resolved && p.childrenResolved =>

        if (referredTempVars.isDefined) {
          referredTempVars.get ++= collectTemporaryVariablesInLogicalPlan(p)
        }

        executor.execute(p.planBuilder.apply(evalIdentifierExpr(p.identifierExpr), p.children))
      case other =>
        other.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_IDENTIFIER)) {
          case e: ExpressionWithUnresolvedIdentifier if e.identifierExpr.resolved =>

            if (referredTempVars.isDefined) {
              referredTempVars.get ++= collectTemporaryVariablesInExpressionTree(e)
            }

            e.exprBuilder.apply(evalIdentifierExpr(e.identifierExpr), e.otherExprs)
        }
    }

  private def collectTemporaryVariablesInLogicalPlan(child: LogicalPlan): Seq[Seq[String]] = {
    def collectTempVars(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap { plan =>
        plan.expressions.flatMap { e => collectTemporaryVariablesInExpressionTree(e) }
      }.distinct
    }
    collectTempVars(child)
  }

  private def collectTemporaryVariablesInExpressionTree(child: Expression): Seq[Seq[String]] = {
    def collectTempVars(child: Expression): Seq[Seq[String]] = {
      child.flatMap { expr =>
        expr.children.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTemporaryVariablesInLogicalPlan(e.plan)
          case r: VariableReference => Seq(r.originalNameParts)
          case _ => Seq.empty
        })
      }.distinct
    }
    collectTempVars(child)
  }

  private def evalIdentifierExpr(expr: Expression): Seq[String] = {
    trimAliases(prepareForEval(expr)) match {
      case e if !e.foldable => expr.failAnalysis(
        errorClass = "NOT_A_CONSTANT_STRING.NOT_CONSTANT",
        messageParameters = Map(
          "name" -> "IDENTIFIER",
          "expr" -> expr.sql))
      case e if e.dataType != StringType => expr.failAnalysis(
        errorClass = "NOT_A_CONSTANT_STRING.WRONG_TYPE",
        messageParameters = Map(
          "name" -> "IDENTIFIER",
          "expr" -> expr.sql,
          "dataType" -> e.dataType.catalogString))
      case e =>
        e.eval() match {
          case null => expr.failAnalysis(
            errorClass = "NOT_A_CONSTANT_STRING.NULL",
            messageParameters = Map(
              "name" -> "IDENTIFIER",
              "expr" -> expr.sql))
          case other =>
            // Parse the identifier string to name parts.
            CatalystSqlParser.parseMultipartIdentifier(other.toString)
        }
    }
  }
}
