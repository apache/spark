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

    // Parse and normalize cursor name for case sensitivity
    val cursorNameParts = parseCursorName(cursorName)

    // Find cursor in scope hierarchy
    val cursorDef = scriptingContext.currentFrame.findCursorByNameParts(cursorNameParts)
      .getOrElse(
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_FOUND",
          messageParameters = Map("cursorName" -> cursorName)))

    // Validate cursor is not already open
    if (cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_ALREADY_OPEN",
        messageParameters = Map("cursorName" -> cursorName))
    }

    // Execute the query and get iterator over results
    val resultIterator = if (args.nonEmpty) {
      // Parameterized cursor: re-parse query text with bound parameters
      val (iterator, analyzedPlan) = executeParameterizedQuery(cursorDef.queryText, args)
      // Update the cursor's query with the analyzed plan so FETCH can access the schema
      cursorDef.query = analyzedPlan
      iterator
    } else {
      // Non-parameterized cursor: use toLocalIterator to avoid loading all data into memory
      val df = Dataset.ofRows(
        session.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
        cursorDef.query)
      df.toLocalIterator()
    }

    // Update cursor state to open
    cursorDef.isOpen = true
    cursorDef.resultIterator = Some(resultIterator)

    Nil
  }

  /**
   * Parses and normalizes cursor name parts based on case sensitivity configuration.
   */
  private def parseCursorName(cursorName: String): Seq[String] = {
    cursorName.split("\\.").toSeq.map { part =>
      if (session.sessionState.conf.caseSensitiveAnalysis) {
        part
      } else {
        part.toLowerCase(java.util.Locale.ROOT)
      }
    }
  }

  /**
   * Executes a parameterized query by re-parsing the SQL text with bound parameters.
   * This uses the same parameter binding mechanism as EXECUTE IMMEDIATE.
   * Returns an iterator to avoid loading all data into memory, matching FOR loop behavior.
   *
   * @param queryText The SQL query text with parameter markers (? or :name)
   * @param args Parameter expressions to bind
   * @return Tuple of (result iterator, analyzed logical plan)
   */
  private def executeParameterizedQuery(
      queryText: String,
      args: Seq[Expression]): (java.util.Iterator[org.apache.spark.sql.Row], LogicalPlan) = {
    val (paramValues, paramNames) = buildUnifiedParameters(args)

    // Use session.sql() with parameters (same as EXECUTE IMMEDIATE)
    val df = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
      .sql(queryText, paramValues, paramNames)

    (df.toLocalIterator(), df.queryExecution.analyzed)
  }

  /**
   * Builds parameter arrays for the session.sql() API.
   * This mirrors the logic in EXECUTE IMMEDIATE and uses pre-extracted parameter names
   * to avoid issues with alias resolution during analysis.
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
   * This matches the behavior of EXECUTE IMMEDIATE to preserve type information.
   *
   * @param expr The expression to evaluate
   * @return Literal with evaluated value and type
   */
  private def evaluateParameterExpression(expr: Expression): Any = {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, VariableReference}
    import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}

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
        // For non-foldable expressions, use a projection to evaluate
        val namedExpr = Alias(other, "param")()
        val project = Project(Seq(namedExpr), LocalRelation())
        val queryExecution = session.sessionState.executePlan(project)
        val row = queryExecution.executedPlan.executeCollect().head
        val value = row.get(0, other.dataType)
        Literal.create(value, other.dataType)
    }
  }

  override def output: Seq[Attribute] = Nil
}
