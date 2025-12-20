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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.CursorDefinition

/**
 * Physical plan node for declaring cursors.
 * Creates a cursor definition in the current scope and parses its query from SQL text.
 * The query is parsed at declaration time but not analyzed to preserve parameter markers.
 *
 * @param cursorName Name of the cursor
 * @param query Placeholder LogicalPlan (not used at execution)
 * @param queryText Original SQL text of the cursor query (used for execution)
 * @param asensitive Whether the cursor is ASENSITIVE (currently all cursors are INSENSITIVE)
 */
case class DeclareCursorExec(
    cursorName: String,
    query: LogicalPlan,
    queryText: String,
    asensitive: Boolean) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> cursorName)))

    val scriptingContext = scriptingContextManager.getContext
      .asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext]
    val currentScope = scriptingContext.currentScope

    // Validate cursor doesn't already exist in current scope
    if (currentScope.cursors.contains(cursorName)) {
      throw new AnalysisException(
        errorClass = "CURSOR_ALREADY_EXISTS",
        messageParameters = Map("cursorName" -> cursorName))
    }

    // Parse the query from SQL text to create a LogicalPlan
    // This ensures parameter markers are preserved and not prematurely resolved
    val parsedQuery = session.sessionState.sqlParser.parsePlan(queryText)

    // Create cursor definition with parsed query and original SQL text
    val cursorDef = CursorDefinition(
      name = cursorName,
      query = parsedQuery,
      queryText = queryText,
      isOpen = false,
      resultData = None,
      currentPosition = -1
    )

    currentScope.cursors.put(cursorName, cursorDef)

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
