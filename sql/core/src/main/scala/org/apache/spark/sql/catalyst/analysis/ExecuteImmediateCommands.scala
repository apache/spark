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
import org.apache.spark.sql.catalyst.expressions.{Alias, EmptyRow, Expression, Literal, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, CommandResult, CompoundBody, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXECUTE_IMMEDIATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Analysis rule that executes ExecuteImmediateCommand during analysis and replaces it
 * with the results, similar to how CALL statements work.
 */
case class ExecuteImmediateCommands(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case cmd: ExecuteImmediateCommand =>
        executeImmediate(cmd)
    }
  }

  private def executeImmediate(cmd: ExecuteImmediateCommand): LogicalPlan = {
      // Extract the query string from the queryParam expression
    val sqlStmtStr = extractQueryString(cmd.sqlStmtStr)

      // Parse and validate the query
    val parsedPlan = sparkSession.sessionState.sqlParser.parsePlan(sqlStmtStr)
    validateQuery(sqlStmtStr, parsedPlan)

    // Execute the query recursively with isolated local variable context
      val result = if (cmd.args.isEmpty) {
        // No parameters - execute directly
      withIsolatedLocalVariableContext {
        sparkSession.sql(sqlStmtStr)
        }
      } else {
        // For parameterized queries, build unified parameter arrays
        val (paramValues, paramNames) = buildUnifiedParameters(cmd.args)

      withIsolatedLocalVariableContext {
        sparkSession.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
          .sql(sqlStmtStr, paramValues, paramNames)
      }
    }

    // Check if the executed statement is a Command (like DECLARE, SET VARIABLE, etc.)
    // Commands should not return result sets
    result.queryExecution.analyzed match {
      case command: Command =>
        // If this EXECUTE IMMEDIATE has an INTO clause, commands are not allowed
        if (cmd.hasIntoClause) {
          throw QueryCompilationErrors.invalidStatementForExecuteInto(sqlStmtStr)
        }
        // Commands may produce output (e.g., SET commands) - collect the actual results
        // The Thrift Server relies on CommandResult containing the actual output data
        val commandRows = result.queryExecution.executedPlan.executeCollect()
        CommandResult(
          command.output, command, result.queryExecution.executedPlan, commandRows.toSeq)
      case _ =>
        // Regular queries - return the results as a LocalRelation
        val internalRows = result.queryExecution.executedPlan.executeCollect()
        LocalRelation(result.queryExecution.analyzed.output, internalRows.toSeq)
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

  private def validateQuery(queryString: String, parsedPlan: LogicalPlan): Unit = {
    // Check for compound bodies (SQL scripting)
    if (parsedPlan.isInstanceOf[CompoundBody]) {
      throw QueryCompilationErrors.sqlScriptInExecuteImmediate(queryString)
    }
  }

  /**
   * Builds unified parameter arrays for the sql() API.
   */
  private def buildUnifiedParameters(args: Seq[Expression]): (Array[Any], Array[String]) = {
    val values = scala.collection.mutable.ListBuffer[Any]()
    val names = scala.collection.mutable.ListBuffer[String]()

    args.foreach {
      case alias: Alias =>
        val paramValue = evaluateParameterExpression(alias.child)
        values += paramValue
        names += alias.name
      case expr =>
        // Positional parameter: just a value
        val paramValue = evaluateParameterExpression(expr)
        values += paramValue
        names += null // unnamed expression
    }

    (values.toArray, names.toArray)
  }

  /**
   * Evaluates a parameter expression. Validation for unsupported constructs like subqueries
   * is already done during analysis in ResolveExecuteImmediate.validateExpressions().
   */
  private def evaluateParameterExpression(expr: Expression): Any = {
    expr match {
          case varRef: VariableReference =>
        // Variable references should be evaluated to their values
        varRef.eval(EmptyRow)
      case foldable if foldable.foldable =>
        Literal.create(foldable.eval(EmptyRow), foldable.dataType).value
      case other =>
        // Expression is not foldable - not supported for parameters
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
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
