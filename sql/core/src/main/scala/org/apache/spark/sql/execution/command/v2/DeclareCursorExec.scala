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
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.CursorDefinition

/**
 * Physical plan node for declaring cursors.
 * Creates a cursor definition in the current scope and parses its query from SQL text.
 *
 * @param cursorName Name of the cursor
 * @param queryText Original SQL text of the cursor query
 * @param asensitive Whether the cursor is ASENSITIVE (currently all cursors are INSENSITIVE)
 */
case class DeclareCursorExec(
    cursorName: String,
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

    // Normalize cursor name based on case sensitivity configuration
    val normalizedName = if (session.sessionState.conf.caseSensitiveAnalysis) {
      cursorName
    } else {
      cursorName.toLowerCase(java.util.Locale.ROOT)
    }

    // Validate cursor doesn't already exist in current scope
    if (currentScope.cursors.contains(normalizedName)) {
      throw new AnalysisException(
        errorClass = "CURSOR_ALREADY_EXISTS",
        messageParameters = Map("cursorName" -> cursorName))
    }

    // Parse the query from SQL text to create a LogicalPlan
    // This ensures parameter markers are preserved and not prematurely resolved
    val parsedQuery = session.sessionState.sqlParser.parsePlan(queryText)

    // Create cursor definition with parsed query and original SQL text
    // Store with original name for display, but use normalized name as key
    val cursorDef = CursorDefinition(
      name = cursorName,
      query = parsedQuery,
      queryText = queryText)

    currentScope.cursors.put(normalizedName, cursorDef)

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
