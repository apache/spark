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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.{CursorDeclared, CursorOpened}

/**
 * Physical plan node for opening cursors.
 *
 * Transitions cursor from Declared to Opened state by analyzing the query and binding parameters.
 * Does not execute the query or create result iterator - that happens on first FETCH.
 *
 * @param cursor CursorReference resolved during analysis phase
 * @param args Parameter expressions from USING clause (for parameterized cursors)
 * @param paramNames Names for each parameter (empty string for positional parameters)
 */
case class OpenCursorExec(
    cursor: Expression,
    args: Seq[Expression] = Seq.empty,
    paramNames: Seq[String] = Seq.empty) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    // Extract CursorReference from the resolved cursor expression
    val cursorRef = cursor.asInstanceOf[org.apache.spark.sql.catalyst.expressions.CursorReference]

    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> cursorRef.sql)))

    val scriptingContext = scriptingContextManager.getContext
      .asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext]

    // Find cursor using normalized name from CursorReference
    val cursorDefOpt: Option[org.apache.spark.sql.scripting.CursorDefinition] =
      if (cursorRef.scopePath.nonEmpty) {
        scriptingContext.currentFrame.findCursorInScope(
          cursorRef.scopePath.head, cursorRef.normalizedName)
      } else {
        scriptingContext.currentFrame.findCursorByName(cursorRef.normalizedName)
      }.asInstanceOf[Option[org.apache.spark.sql.scripting.CursorDefinition]]
    val cursorDef = cursorDefOpt.getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> cursorRef.sql)))

    // Get current cursor state and validate it's in Declared state
    val currentState = scriptingContext.currentFrame.getCursorState(
      cursorRef.normalizedName,
      cursorRef.scopePath).getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> cursorRef.sql)))

    val query = currentState match {
      case CursorDeclared(q) => q
      case _ =>
        throw new AnalysisException(
          errorClass = "CURSOR_ALREADY_OPEN",
          messageParameters = Map("cursorName" -> cursorRef.sql))
    }

    // Execute the query and get the analyzed plan
    val analyzedQuery = if (args.nonEmpty) {
      // Parameterized cursor: re-parse query text with bound parameters
      executeParameterizedQuery(cursorDef.queryText, args)
    } else {
      // Non-parameterized cursor: analyze the declared query
      val df = Dataset.ofRows(
        session.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
        query)
      df.queryExecution.analyzed
    }

    // Transition cursor state to Opened
    scriptingContext.currentFrame.updateCursorState(
      cursorRef.normalizedName,
      cursorRef.scopePath,
      CursorOpened(analyzedQuery))

    Nil
  }

  /**
   * Executes a parameterized query by re-parsing the SQL text with bound parameters.
   * This uses the same parameter binding mechanism as EXECUTE IMMEDIATE.
   *
   * @param queryText The SQL query text with parameter markers (? or :name)
   * @param args Parameter expressions to bind
   * @return The analyzed logical plan with parameters bound
   */
  private def executeParameterizedQuery(
      queryText: String,
      args: Seq[Expression]): LogicalPlan = {
    val (paramValues, paramNames) = buildUnifiedParameters(args)

    // Use session.sql() with parameters (same as EXECUTE IMMEDIATE)
    val df = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
      .sql(queryText, paramValues, paramNames)

    df.queryExecution.analyzed
  }

  /**
   * Builds parameter arrays for the session.sql() API.
   * This mirrors the exact logic in EXECUTE IMMEDIATE to ensure identical behavior.
   *
   * @param args Parameter expressions from the USING clause
   * @return Tuple of (parameter values, parameter names)
   */
  private def buildUnifiedParameters(args: Seq[Expression]): (Array[Any], Array[String]) = {
    val values = scala.collection.mutable.ListBuffer[Any]()
    val names = scala.collection.mutable.ListBuffer[String]()

    args.zipWithIndex.foreach { case (expr, idx) =>
      val paramValue = evaluateParameterExpression(expr)
      values += paramValue
      val paramName = if (idx < paramNames.length) paramNames(idx) else ""
      names += paramName
    }

    (values.toArray, names.toArray)
  }

  /**
   * Evaluates a parameter expression and returns its value as a Literal.
   * This matches the exact behavior of EXECUTE IMMEDIATE to preserve type information.
   *
   * @param expr The expression to evaluate
   * @return Literal with evaluated value and type
   */
  private def evaluateParameterExpression(expr: Expression): Any = {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.{Literal, VariableReference}

    expr match {
      case varRef: VariableReference =>
        // Variable references: evaluate to their values and wrap in Literal
        // to preserve type information
        Literal.create(varRef.eval(InternalRow.empty), varRef.dataType)
      case foldable if foldable.foldable =>
        // For foldable expressions, return Literal to preserve type information.
        // This ensures DATE '2023-12-25' remains a DateType literal, not just an Int.
        Literal.create(foldable.eval(InternalRow.empty), foldable.dataType)
      case other =>
        // Expression is not foldable - not supported for parameters
        throw org.apache.spark.sql.errors.QueryCompilationErrors
          .unsupportedParameterExpression(other)
    }
  }

  override def output: Seq[Attribute] = Nil
}
