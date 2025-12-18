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

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, VariableReference}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for fetching from cursors.
 */
case class FetchCursorExec(
    cursorName: String,
    targetVariables: Seq[VariableReference]) extends LeafV2CommandExec {

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

    // Check if cursor is open
    if (!cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_OPEN",
        messageParameters = Map("cursorName" -> cursorName))
    }

    val resultData = cursorDef.resultData.get

    // Move to next row
    cursorDef.currentPosition += 1

    // Check if there are more rows
    if (cursorDef.currentPosition >= resultData.length) {
      // No more data - raise NO DATA condition (SQLSTATE 02000)
      // This will be caught by NOT FOUND handlers
      throw new AnalysisException(
        errorClass = "CURSOR_NO_MORE_ROWS",
        messageParameters = Map("cursorName" -> cursorName))
    }

    val currentRow = resultData(cursorDef.currentPosition)

    // Check if number of variables matches number of columns
    if (targetVariables.length != currentRow.numFields) {
      throw new AnalysisException(
        errorClass = "CURSOR_FETCH_COLUMN_MISMATCH",
        messageParameters = Map(
          "cursorName" -> cursorName,
          "expected" -> currentRow.numFields.toString,
          "actual" -> targetVariables.length.toString))
    }

    // Get variable manager
    val scriptingVariableManager = SqlScriptingContextManager.get()
      .map(_.getVariableManager)
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> cursorName)))

    // Assign values to variables
    targetVariables.zipWithIndex.foreach { case (varRef, idx) =>
      val value = currentRow.get(idx, varRef.dataType)

      // Handle case sensitivity the same way as SetVariableExec
      val namePartsCaseAdjusted = if (session.sessionState.conf.caseSensitiveAnalysis) {
        varRef.originalNameParts
      } else {
        varRef.originalNameParts.map(_.toLowerCase(Locale.ROOT))
      }

      val varDef = VariableDefinition(
        varRef.identifier,
        varRef.varDef.defaultValueSQL,
        Literal(value, varRef.dataType)
      )

      scriptingVariableManager.set(namePartsCaseAdjusted, varDef)
    }

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
