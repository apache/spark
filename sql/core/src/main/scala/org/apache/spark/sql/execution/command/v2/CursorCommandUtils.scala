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
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.scripting.SqlScriptingExecutionContext

/**
 * Utility methods for cursor commands.
 */
private[v2] object CursorCommandUtils extends DataTypeErrorsBase {

  /**
   * Gets the current SQL scripting execution context.
   * Throws an exception if cursor is used outside of a scripting context.
   *
   * @param cursorName Cursor name for error message
   * @return The SQL scripting execution context
   */
  def getScriptingContext(cursorName: String): SqlScriptingExecutionContext = {
    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> toSQLId(cursorName))))

    scriptingContextManager.getContext
      .asInstanceOf[SqlScriptingExecutionContext]
  }
}
