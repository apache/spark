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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, CursorReference, Expression}
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.{CursorClosed, CursorDeclared, CursorOpened}

/**
 * Physical plan node for opening cursors.
 *
 * Transitions cursor from Declared to Opened state by:
 * 1. Parsing the cursor's SQL query text to a LogicalPlan
 * 2. Binding parameters (if USING clause is provided)
 * 3. Analyzing the query (semantic analysis, resolution, type checking)
 *
 * Does not execute the query or create result iterator - that happens on first FETCH.
 *
 * Uses ParameterizedQueryExecutor trait for unified parameter binding with EXECUTE IMMEDIATE.
 *
 * @param cursor CursorReference resolved during analysis phase
 * @param args Parameter expressions from USING clause
 * @param paramNames Parameter names extracted at parse time
 */
case class OpenCursorExec(
    cursor: Expression,
    args: Seq[Expression] = Seq.empty,
    paramNames: Seq[String] = Seq.empty)
  extends LeafV2CommandExec with DataTypeErrorsBase with ParameterizedQueryExecutor {

  override protected def run(): Seq[InternalRow] = {
    // Extract CursorReference from the resolved cursor expression
    val cursorRef = cursor.asInstanceOf[CursorReference]

    val scriptingContext = CursorCommandUtils.getScriptingContext(cursorRef.definition.name)

    // Get cursor definition from CursorReference (looked up during analysis)
    val cursorDef = cursorRef.definition

    // Get current cursor state and validate it's in Declared state
    val currentState = scriptingContext.getCursorState(cursorRef).getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name))))

    currentState match {
      case CursorDeclared | CursorClosed => // Expected states - new or closed cursor
      case _ =>
        throw new AnalysisException(
          errorClass = "CURSOR_ALREADY_OPEN",
          messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name)))
    }

    // Parse and analyze the query from the stored SQL text
    // Uses shared ParameterizedQueryExecutor trait for consistency with EXECUTE IMMEDIATE
    val analyzedQuery = executeParameterizedQuery(cursorDef.queryText, args, paramNames)

    // Transition cursor state to Opened
    scriptingContext.updateCursorState(
      cursorRef.normalizedName,
      cursorRef.scopeLabel,
      CursorOpened(analyzedQuery))

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
