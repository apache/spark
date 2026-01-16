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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, CompoundBody, LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.trees.TreePattern.EXECUTE_IMMEDIATE
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.v2.ParameterBindingUtils
import org.apache.spark.sql.types.StringType

/**
 * Analysis rule that resolves and executes EXECUTE IMMEDIATE statements during analysis,
 * replacing them with the results, similar to how CALL statements work.
 * This rule combines resolution and execution in a single pass.
 */
case class ResolveExecuteImmediate(sparkSession: SparkSession, catalogManager: CatalogManager)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case node @ UnresolvedExecuteImmediate(sqlStmtStr, args, targetVariables) =>
        if (sqlStmtStr.resolved && targetVariables.forall(_.resolved) && args.forall(_.resolved)) {
          // All resolved - execute immediately and handle INTO clause if present
          if (targetVariables.nonEmpty) {
            // EXECUTE IMMEDIATE ... INTO should generate SetVariable plan with eagerly executed
            // source
            val finalTargetVars = extractTargetVariables(targetVariables)
            val executedSource = executeImmediateQuery(sqlStmtStr, args, hasIntoClause = true)
            SetVariable(finalTargetVars, executedSource)
          } else {
            // Regular EXECUTE IMMEDIATE without INTO - execute and return result directly
            executeImmediateQuery(sqlStmtStr, args, hasIntoClause = false)
          }
        } else {
          // Not all resolved yet - wait for next iteration
          node
        }
    }
  }

  private def extractTargetVariables(targetVariables: Seq[Expression]): Seq[VariableReference] = {
    targetVariables.map {
      case alias: Alias =>
        // Extract the VariableReference from the alias
        alias.child match {
          case varRef: VariableReference =>
            // Use resolved VariableReference directly with canFold = false
            varRef.copy(canFold = false)
          case _ =>
            throw QueryCompilationErrors.unsupportedParameterExpression(alias.child)
        }
      case varRef: VariableReference =>
        // Use resolved VariableReference directly with canFold = false
        varRef.copy(canFold = false)
      case other =>
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
  }

  private def executeImmediateQuery(
      sqlStmtStr: Expression,
      args: Seq[Expression],
      hasIntoClause: Boolean): LogicalPlan = {
    // Extract the query string from the queryParam expression
    val sqlString = extractQueryString(sqlStmtStr)

    // Create the origin for EXECUTE IMMEDIATE context - this will be used by expressions
    // during parsing to set their queryContext, similar to how views work
    val executeImmediateOrigin = Origin(
      objectType = Some("EXECUTE IMMEDIATE"),
      objectName = None, // No named object for EXECUTE IMMEDIATE, unlike views
      sqlText = Some(sqlString),
      startIndex = Some(0),
      stopIndex = Some(sqlString.length - 1)
    )

    // Execute the query recursively with isolated local variable context and EXECUTE IMMEDIATE
    // origin. The isolation must cover parsing, analysis, and execution phases.
    // CurrentOrigin.withOrigin ensures expressions created during parsing get the proper context
    val result = withIsolatedLocalVariableContext {
      CurrentOrigin.withOrigin(executeImmediateOrigin) {
        // Use shared parameterized query execution logic (same as OPEN CURSOR)
        val df = if (args.isEmpty) {
          // No parameters - execute directly
          sparkSession.sql(sqlString)
        } else {
          // For parameterized queries, use shared parameter binding utility
          val (paramValues, paramNames) = ParameterBindingUtils.buildUnifiedParameters(args)

          sparkSession.asInstanceOf[ClassicSparkSession]
            .sql(sqlString, paramValues, paramNames)
        }

        // SQL scripts (BEGIN/END blocks) are explicitly disallowed in EXECUTE IMMEDIATE.
        // This is a design constraint: EXECUTE IMMEDIATE is for executing single SQL statements,
        // and SQL scripts have their own variable scoping and control flow that would conflict
        // with EXECUTE IMMEDIATE's parameter passing semantics.
        if (df.queryExecution.logical.isInstanceOf[CompoundBody]) {
          throw QueryCompilationErrors.sqlScriptInExecuteImmediate(sqlString)
        }

        // Force analysis to happen within the isolated context to ensure local variables
        // are not accessible. This is critical because DataFrames are lazy and analysis
        // would otherwise happen outside the isolation context.
        df.queryExecution.analyzed
        df
      }
    }

    // If this EXECUTE IMMEDIATE has an INTO clause, commands are not allowed
    if (hasIntoClause && result.queryExecution.analyzed.isInstanceOf[Command]) {
      throw QueryCompilationErrors.invalidStatementForExecuteInto(sqlString)
    }

    // For commands, use commandExecuted to avoid double execution
    // For queries, use analyzed to avoid eager evaluation
    if (result.queryExecution.analyzed.isInstanceOf[Command]) {
      result.queryExecution.commandExecuted
    } else {
      result.queryExecution.analyzed
    }
  }

  private def extractQueryString(queryExpr: Expression): String = {
    // Ensure the expression resolves to string type
    if (!queryExpr.dataType.sameType(StringType)) {
      throw QueryCompilationErrors.invalidExecuteImmediateExpressionType(queryExpr.dataType)
    }

    // Evaluate the expression to get the query string
    val value = queryExpr.eval(null)
    if (value == null) {
      // Extract the original text from the expression's origin for the error message
      val originalText = extractOriginalText(queryExpr)
      throw QueryCompilationErrors.nullSQLStringExecuteImmediate(originalText)
    }

    value.toString
  }

  private def extractOriginalText(queryExpr: Expression): String = {
    val origin = queryExpr.origin
    // Try to extract the original text from the origin information
    (origin.sqlText, origin.startIndex, origin.stopIndex) match {
      case (Some(sqlText), Some(startIndex), Some(stopIndex)) =>
        // Extract the substring from the original SQL text
        sqlText.substring(startIndex, stopIndex + 1)
      case _ =>
        // Fallback to the SQL representation if origin information is not available
        queryExpr.sql
    }
  }

  /**
   * Temporarily isolates the SQL scripting context during EXECUTE IMMEDIATE execution.
   * This makes withinSqlScript() return false, ensuring that statements within EXECUTE IMMEDIATE
   * are not affected by the outer SQL script context (e.g., local variables, script-specific
   * errors).
   */
  private def withIsolatedLocalVariableContext[A](f: => A): A = {
    // Completely clear the SQL scripting context to make withinSqlScript() return false
    val handle = SqlScriptingContextManager.create(null)
    handle.runWith(f)
  }
}
