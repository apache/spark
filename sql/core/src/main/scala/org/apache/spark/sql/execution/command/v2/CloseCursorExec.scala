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
import org.apache.spark.sql.scripting.{CursorClosed, CursorDeclared}

/**
 * Physical plan node for closing cursors.
 *
 * Transitions cursor from Opened or Fetching state to Closed state, releasing resources.
 * Closing an already closed cursor or a declared-but-not-opened cursor raises an error.
 *
 * @param cursor CursorReference resolved during analysis phase
 */
case class CloseCursorExec(cursor: Expression) extends LeafV2CommandExec with DataTypeErrorsBase {

  override protected def run(): Seq[InternalRow] = {
    // Extract CursorReference from the resolved cursor expression
    val cursorRef = cursor.asInstanceOf[CursorReference]

    val scriptingContext = CursorCommandUtils.getScriptingContext(cursorRef.definition.name)

    // Get current cursor state and validate it exists
    val currentState = scriptingContext.getCursorState(cursorRef).getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name))))

    // Validate cursor is in an open state (Opened or Fetching)
    currentState match {
      case CursorClosed | CursorDeclared =>
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_OPEN",
          messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name)))
      case _ => // Opened or Fetching - proceed with close
    }

    // Transition cursor state to Closed
    scriptingContext.updateCursorState(
      cursorRef.normalizedName,
      cursorRef.scopeLabel,
      CursorClosed)

    Nil
  }

  override def output: Seq[Attribute] = Nil
}
