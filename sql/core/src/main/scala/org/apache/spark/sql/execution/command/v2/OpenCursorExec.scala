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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for opening cursors.
 *
 * @param cursorName Name of the cursor to open
 * @param args Parameter expressions from USING clause (for parameterized cursors)
 * @param paramNames Names for each parameter (empty string for positional parameters)
 */
case class OpenCursorExec(
    cursorName: String,
    args: Seq[Expression] = Seq.empty,
    paramNames: Seq[String] = Seq.empty) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> cursorName)))

    val scriptingContext = scriptingContextManager.getContext
      .asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext]

    // Find cursor in scope hierarchy
    val cursorDef = scriptingContext.currentFrame.findCursor(cursorName).getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> cursorName)))

    // Check if cursor is already open
    if (cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_ALREADY_OPEN",
        messageParameters = Map("cursorName" -> cursorName))
    }

    // Execute the query and collect results
    // If parameters are provided (args.nonEmpty), re-parse the query text with parameters
    // Otherwise, use the analyzed LogicalPlan as-is
    val resultData = if (args.nonEmpty) {
      // Parameterized cursor - re-parse query text with parameters
      executeParameterizedQuery(cursorDef.queryText, args)
    } else {
      // Non-parameterized cursor - use analyzed plan
      val queryExecution = session.sessionState.executePlan(cursorDef.query)
      queryExecution.executedPlan.executeCollect()
    }

    // Update cursor state
    cursorDef.isOpen = true
    cursorDef.resultData = Some(resultData)
    cursorDef.currentPosition = -1

    Nil
  }

  /**
   * Execute a parameterized query by re-parsing the SQL text with parameters.
   * Uses the same mechanism as EXECUTE IMMEDIATE.
   */
  private def executeParameterizedQuery(
      queryText: String,
      args: Seq[Expression]): Array[InternalRow] = {
    // Build unified parameters exactly like EXECUTE IMMEDIATE does
    val (paramValues, paramNames) = buildUnifiedParameters(args)

    // Call session.sql with parameters, just like EXECUTE IMMEDIATE
    // Cast to ClassicSparkSession to access the parameterized sql method
    val df = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
      .sql(queryText, paramValues, paramNames)

    // Collect and return the results
    df.queryExecution.executedPlan.executeCollect()
  }

  /**
   * Builds parameter arrays for the sql() API, exactly like EXECUTE IMMEDIATE.
   * Uses the pre-extracted parameter names to avoid issues with alias resolution.
   */
  private def buildUnifiedParameters(args: Seq[Expression]): (Array[Any], Array[String]) = {
    val values = scala.collection.mutable.ListBuffer[Any]()
    val names = scala.collection.mutable.ListBuffer[String]()

    args.zipWithIndex.foreach { case (expr, idx) =>
      val paramValue = evaluateParameterExpression(expr)
      values += paramValue
      // Use the pre-extracted parameter name
      val paramName = if (idx < paramNames.length) paramNames(idx) else ""
      names += paramName
    }

    (values.toArray, names.toArray)
  }

  /**
   * Evaluates a parameter expression to a Literal.
   * Returns a Literal to preserve type information, just like EXECUTE IMMEDIATE.
   */
  private def evaluateParameterExpression(expr: Expression): Any = {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
    import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}

    // For foldable expressions, evaluate and wrap in Literal
    if (expr.foldable) {
      Literal.create(expr.eval(InternalRow.empty), expr.dataType)
    } else {
      // Create a projection to evaluate the expression
      val namedExpr = Alias(expr, "param")()
      val project = Project(Seq(namedExpr), LocalRelation())
      val queryExecution = session.sessionState.executePlan(project)
      val row = queryExecution.executedPlan.executeCollect().head
      val value = row.get(0, expr.dataType)
      // Return as Literal to preserve type information
      Literal.create(value, expr.dataType)
    }
  }

  override def output: Seq[Attribute] = Nil
}
