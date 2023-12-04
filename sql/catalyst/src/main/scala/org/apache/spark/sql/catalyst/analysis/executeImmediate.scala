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

import scala.util.{Either, Left, Right}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, EXECUTE_IMMEDIATE, TreePattern}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.types.StringType

/**
 * Logical plan representing execute immediate query.
 *
 * @param args Expressions to be replaced inplace of parameters of queryText/queryVariable
 * @param queryText Query text as string literal as an option
 * @param queryVariable Unresolved query attribute used as query in execute immediate
 * @param targetVariable Variable to store result into if specified
 */
case class ExecuteImmediateQuery(
    args: Seq[Expression],
    query: Either[String, UnresolvedAttribute],
    targetVariables: Option[Seq[UnresolvedAttribute]]) extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * This rule substitutes execute immediate query node with plan that is passed as string literal
 * or session parameter.
 */
class SubstituteExecuteImmediate(val catalogManager: CatalogManager)
    extends Rule[LogicalPlan] with ColumnResolutionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
    case ExecuteImmediateQuery(expressions, query, targetVariablesOpt) =>
      val queryString = query match {
        case Left(v) => v
        case Right(u) =>
          val varReference = lookupVariable(u.nameParts) match {
            case Some(variable) => variable.copy(canFold = false)
            case _ => throw unresolvedVariableError(u.nameParts, Seq("SYSTEM", "SESSION"))
          }

          if (!varReference.dataType.sameType(StringType)) {
            throw new AnalysisException(
              errorClass = "INVALID_VARIABLE_TYPE_FOR_QUERY_EXECUTE_IMMEDIATE",
              messageParameters = Map(
                "varType" -> varReference.dataType.simpleString))
          }

          // call eval with null row.
          // this is ok as this is variable and invoking eval should
          // be independent of row
          varReference.eval(null).toString
      }

      val plan = CatalystSqlParser.parsePlan(queryString);
      val posNodes = plan.collect {
        case p: LogicalPlan =>
          p.expressions.map(_.collect { case n: PosParameter => n }).flatten
      }.flatten
      val namedNodes = plan.collect {
        case p: LogicalPlan =>
          p.expressions.map(_.collect{ case n: NamedParameter => n }).flatten
      }.flatten

      val queryPlan = if (expressions.isEmpty || (posNodes.isEmpty && namedNodes.isEmpty)) {
        plan
      } else if (posNodes.nonEmpty && namedNodes.nonEmpty) {
        throw new AnalysisException(
          errorClass = "INVALID_PARAMETRIZED_QUERY",
          messageParameters = Map.empty)
      } else {
        if (posNodes.nonEmpty) {
          // Add aggregation or a project.
          PosParameterizedQuery(
            plan,
            expressions)
        } else {
          val namedExpressions = expressions.collect {
            case (e: NamedExpression) => e
          }

          NameParameterizedQuery(
            plan,
            namedExpressions.map(_.name),
            namedExpressions)
        }
      }

      targetVariablesOpt.map (
        expressions => {
          if (queryPlan.exists(_.containsPattern(COMMAND))) {
            throw new AnalysisException(
              errorClass = "INVALID_STATEMENT_FOR_EXECUTE_INTO",
              messageParameters = Map(
                "sqlString" -> queryString
              ))
          }

          SetVariable(expressions, queryPlan)
        }
      ).getOrElse{ queryPlan }
  }
}
