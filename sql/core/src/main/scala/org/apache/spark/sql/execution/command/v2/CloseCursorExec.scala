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
 * Physical plan node for closing cursors.
 * Releases cursor resources and resets its state to closed.
 *
 * @param cursor CursorReference resolved during analysis phase
 */
case class CloseCursorExec(cursor: Expression) extends LeafV2CommandExec {

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

    // Validate cursor is currently open
    if (!cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_OPEN",
        messageParameters = Map("cursorName" -> cursorRef.sql))
    }

    // Close the cursor and release resources
    cursorDef.close()

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
