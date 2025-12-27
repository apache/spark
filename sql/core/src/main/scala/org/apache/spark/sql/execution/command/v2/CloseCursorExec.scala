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

/**
 * Physical plan node for closing cursors.
 * Releases cursor resources and resets its state to closed.
 *
 * @param nameParts Cursor name parts (unqualified: Seq(name) or qualified: Seq(label, name))
 */
case class CloseCursorExec(nameParts: Seq[String]) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> nameParts.mkString("."))))

    val scriptingContext = scriptingContextManager.getContext
      .asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext]

    // Normalize cursor name parts for case sensitivity
    val normalizedParts = normalizeCursorNameParts(nameParts)

    // Find cursor in scope hierarchy
    val cursorDef = scriptingContext.currentFrame.findCursorByNameParts(normalizedParts)
      .getOrElse(
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_FOUND",
          messageParameters = Map("cursorName" -> nameParts.mkString("."))))

    // Validate cursor is currently open
    if (!cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_OPEN",
        messageParameters = Map("cursorName" -> nameParts.mkString(".")))
    }

    // Close the cursor and release resources
    cursorDef.close()

    Nil
  }

  /**
   * Normalizes cursor name parts based on case sensitivity configuration.
   */
  private def normalizeCursorNameParts(parts: Seq[String]): Seq[String] = {
    parts.map { part =>
      if (session.sessionState.conf.caseSensitiveAnalysis) {
        part
      } else {
        part.toLowerCase(java.util.Locale.ROOT)
      }
    }
  }

  override def output: Seq[Attribute] = Nil
}
