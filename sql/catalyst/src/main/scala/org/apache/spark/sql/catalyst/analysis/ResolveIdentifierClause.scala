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

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, EvalHelper, Expression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_IDENTIFIER
import org.apache.spark.sql.types.StringType

/**
 * Resolves the identifier expressions and builds the original plans/expressions.
 */
object ResolveIdentifierClause extends Rule[LogicalPlan] with AliasHelper with EvalHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(UNRESOLVED_IDENTIFIER)) {
    case p: PlanWithUnresolvedIdentifier if p.identifierExpr.resolved && p.childrenResolved =>
      p.planBuilder.apply(evalIdentifierExpr(p.identifierExpr), p.children)
    case other =>
      other.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_IDENTIFIER)) {
        case e: ExpressionWithUnresolvedIdentifier if e.identifierExpr.resolved =>
          e.exprBuilder.apply(evalIdentifierExpr(e.identifierExpr), e.otherExprs)
      }
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
