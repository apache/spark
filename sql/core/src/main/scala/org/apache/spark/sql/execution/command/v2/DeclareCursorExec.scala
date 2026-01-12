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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.{CursorDeclared, CursorDefinition}

/**
 * Physical plan node for declaring cursors.
 *
 * Creates a cursor definition and initializes it in the Declared state.
 * The cursor query is parsed but not analyzed/executed until OPEN.
 *
 * @param cursorName Name of the cursor
 * @param queryText Original SQL text of the cursor query
 * @param asensitive Whether the cursor is ASENSITIVE (sensitivity to underlying data changes,
 *                   not case sensitivity). Currently all cursors are effectively INSENSITIVE.
 */
case class DeclareCursorExec(
    cursorName: String,
    queryText: String,
    asensitive: Boolean) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val scriptingContext = CursorCommandUtils.getScriptingContext(cursorName)
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

    // Create immutable cursor definition with name and SQL text
    val cursorDef = CursorDefinition(
      name = cursorName,
      queryText = queryText)

    // Store cursor definition and initial state
    currentScope.cursors.put(normalizedName, cursorDef)
    currentScope.cursorStates.put(normalizedName, CursorDeclared(parsedQuery))

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
