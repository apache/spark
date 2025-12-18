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
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for opening cursors.
 */
case class OpenCursorExec(cursorName: String) extends LeafV2CommandExec {

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
    val df = Dataset.ofRows(session, cursorDef.query)
    val resultData = df.queryExecution.executedPlan.executeCollect()

    // Update cursor state
    cursorDef.isOpen = true
    cursorDef.resultData = Some(resultData)
    cursorDef.currentPosition = -1

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
