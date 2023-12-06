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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
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

  def resolveVariable(e: Expression) : Expression = {
    /* We know that the expression is either UnresolvedAttribute or Alias,
     * as passed from the parser.
     * If it is an UnresolvedAttribute, we look it up in the catalog and return it.
     * If it is an Alias, we resolve the child and return an Alias with the same name.
     */
    e match {
      case u: UnresolvedAttribute =>
        lookupVariable(u.nameParts) match {
          case Some(variable) =>
            variable.copy(canFold = false)
          case _ => throw unresolvedVariableError(u.nameParts, Seq("SYSTEM", "SESSION"))
        }

      case a: Alias =>
        Alias(resolveVariable(a.child), a.name)()

      case other => throw SparkException.internalError(
        "Unexpected variable expression in ParametrizedQuery: " + other)
    }
  }

  def resolveArguments(expressions : Seq[Expression]): Seq[Expression] = {
    expressions.map { exp =>
      if (exp.resolved) {
        exp
      } else {
        resolveVariable(exp)
      }
    }
  }

  def extractQueryString(either : Either[String, UnresolvedAttribute]) : String = {
    either match {
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
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
    case ExecuteImmediateQuery(expressions, query, targetVariablesOpt) =>
      val queryString = extractQueryString(query)
      val plan = CatalystSqlParser.parsePlan(queryString);

      val posNodes = plan.collect {
        case p: LogicalPlan =>
          p.expressions.flatMap(_.collect { case n: PosParameter => n })
      }.flatten
      val namedNodes = plan.collect {
        case p: LogicalPlan =>
          p.expressions.flatMap(_.collect{ case n: NamedParameter => n })
      }.flatten

      val queryPlan = if (expressions.isEmpty || (posNodes.isEmpty && namedNodes.isEmpty)) {
        plan
      } else if (posNodes.nonEmpty && namedNodes.nonEmpty) {
        throw new AnalysisException(
          errorClass = "INVALID_PARAMETRIZED_QUERY",
          messageParameters = Map.empty)
      } else {
        // parser does not distinguish between variables and columns.
        // Make sure that we have either literal or variable in expressions.

        if (posNodes.nonEmpty) {
          // Add aggregation or a project.
          PosParameterizedQuery(
            plan,
            // we need to resolve arguments before Resolution batch to make sure
            // that some rule does not accidently resolve our parameters.
            // we do not want this as they can resolve some unsupported parameters
            resolveArguments(expressions))
        } else {
          val namedExpressions = expressions.collect {
            case (e: NamedExpression) => e
          }

          NameParameterizedQuery(
            plan,
            namedExpressions.map(_.name),
            // we need to resolve arguments before Resolution batch to make sure
            // that some rule does not accidently resolve our parameters.
            // we do not want this as they can resolve some unsupported parameters
            resolveArguments(namedExpressions))
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
