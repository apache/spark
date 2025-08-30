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

package org.apache.spark.sql.execution.command

import scala.util.{Either, Left, Right}

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, LogicalPlan}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * Runnable command that executes an EXECUTE IMMEDIATE statement by recursively
 * invoking the SQL execution pipeline.
 */
case class ExecuteImmediateCommandExec(
    args: Seq[Expression],
    query: Either[String, VariableReference],
    targetVariables: Seq[VariableReference]) extends LeafRunnableCommand {

  override def output: Seq[Attribute] = {
    if (targetVariables.nonEmpty) {
      // For EXECUTE IMMEDIATE ... INTO, return empty output
      Seq.empty
    } else {
      // For regular EXECUTE IMMEDIATE, get the schema from the executed query
      // This is a bit of a hack, but necessary for the type system
      try {
        val queryString = extractQueryString(null, query) // Use null SparkSession for schema only
        // We can't execute the query here due to circular dependencies
        // Return a placeholder that will be corrected during execution
        Seq.empty
      } catch {
        case _: Exception => Seq.empty
      }
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Extract the query string
    val queryString = extractQueryString(sparkSession, query)

    // Prepare arguments for parameterized queries
    val resolvedArgs = resolveArguments(sparkSession, args)

    try {
      // Parse the query to understand its structure
      val parsedPlan = sparkSession.sessionState.sqlParser.parsePlan(queryString)

      // Check for unsupported features
      validateQuery(queryString, parsedPlan)

      // Execute the query recursively through the SQL execution pipeline
      val result = executeQuery(sparkSession, queryString, parsedPlan, resolvedArgs)

      // Handle target variables if specified
      if (targetVariables.nonEmpty) {
        handleTargetVariables(sparkSession, result, targetVariables)
        Seq.empty // No output for INTO queries
      } else {
        // For regular EXECUTE IMMEDIATE, we can't return rows via RunnableCommand
        // because of schema mismatch issues. The CommandResult mechanism will handle this.
        // For now, return empty and let the test fail with a clearer error
        throw new UnsupportedOperationException(
          "EXECUTE IMMEDIATE with result return is not yet fully implemented. " +
            "The query was executed but results cannot be returned via RunnableCommand " +
            "due to schema limitations.")
      }
    } catch {
      case e: AnalysisException =>
        // Re-throw AnalysisException as-is to preserve error type for tests
        throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to execute immediate query: ${e.getMessage}", e)
    }
  }

  private def extractQueryString(
      sparkSession: SparkSession,
      queryExpr: Either[String, VariableReference]): String = {
    queryExpr match {
      case Left(literal) => literal
      case Right(variable) =>
        // Evaluate the variable reference
        if (!variable.dataType.sameType(StringType)) {
          throw QueryCompilationErrors.invalidExecuteImmediateExpressionType(variable.dataType)
        }

        val value = variable.eval(null)
        if (value == null) {
          throw QueryCompilationErrors.nullSQLStringExecuteImmediate(variable.identifier.name())
        }

        value.toString
    }
  }

  private def resolveArguments(
      sparkSession: SparkSession,
      expressions: Seq[Expression]): Seq[Expression] = {
    expressions.map { expr =>
      if (expr.resolved) {
        expr
      } else {
        // For now, just return the expression as-is
        // In a complete implementation, you would need to resolve variables/parameters
        expr
      }
    }
  }

  private def validateQuery(queryString: String, parsedPlan: LogicalPlan): Unit = {
    // Check for compound bodies (SQL scripting)
    if (parsedPlan.isInstanceOf[CompoundBody]) {
      throw QueryCompilationErrors.sqlScriptInExecuteImmediate(queryString)
    }


  }

  private def executeQuery(
      sparkSession: SparkSession,
      queryString: String,
      parsedPlan: LogicalPlan,
      resolvedArgs: Seq[Expression]): DataFrame = {

    // For now, use the SparkSession.sql method which handles parameterization
    // This is the recursive SQL execution we want
    if (resolvedArgs.isEmpty) {
      // Execute within EXECUTE IMMEDIATE context to isolate variables
      AnalysisContext.withExecuteImmediateContext {
        sparkSession.sql(queryString)
      }
    } else {
      // For parameterized queries, convert resolved args to values and pass them to sql()
      val paramValues = resolvedArgs.map(_.eval(null))
      AnalysisContext.withExecuteImmediateContext {
        sparkSession.sql(queryString, paramValues.toArray)
      }
    }
  }

  private def handleTargetVariables(
      sparkSession: SparkSession,
      result: DataFrame,
      targetVars: Seq[VariableReference]): Unit = {
    // This would need to set session variables with the query results
    // For now, we'll throw an error as this functionality would need additional implementation
    throw new UnsupportedOperationException(
      "EXECUTE IMMEDIATE ... INTO with recursive execution is not yet implemented")
  }
}
