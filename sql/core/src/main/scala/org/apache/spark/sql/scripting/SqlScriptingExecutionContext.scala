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

package org.apache.spark.sql.scripting

import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.scripting.SqlScriptingFrameType.SqlScriptingFrameType

/**
 * SQL scripting execution context - keeps track of the current execution state.
 */
class SqlScriptingExecutionContext {
  // List of frames that are currently active.
  private[scripting] val frames: ListBuffer[SqlScriptingExecutionFrame] = ListBuffer.empty
  private[scripting] var firstHandlerScopeLabel: Option[String] = None

  def enterScope(
      label: String,
      triggerHandlerMap: TriggerToExceptionHandlerMap): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot enter scope: no frames.")
    }
    frames.last.enterScope(label, triggerHandlerMap)
  }

  def exitScope(label: String): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no frames.")
    }
    frames.last.exitScope(label)
  }

  def currentFrame: SqlScriptingExecutionFrame = frames.last
  def currentScope: SqlScriptingExecutionScope = currentFrame.currentScope

  def findHandler(condition: String, sqlState: String): Option[ExceptionHandlerExec] = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot find handler: no frames.")
    }

    // If the last frame is a handler, try to find a handler in its body first.
    if (frames.last.frameType == SqlScriptingFrameType.HANDLER) {
      val handler = frames.last.findHandler(condition, sqlState, firstHandlerScopeLabel)
      if (handler.isDefined) {
        return handler
      }
    }

    // First frame is always script frame. Skip all handler frames and try to find handler in it.
    // TODO: After introducing stored procedures, we need to handle the case with multiple
    //       script/stored procedure frames on call stack. We will have to iterate over all
    //       frames and skip frames representing error handlers.
    val scriptFrame = frames.head
    val handler = scriptFrame.findHandler(condition, sqlState, firstHandlerScopeLabel)
    if (handler.isDefined) {
      firstHandlerScopeLabel = handler.get.scopeLabel
      return handler
    }

    None
  }
}

object SqlScriptingFrameType extends Enumeration {
  type SqlScriptingFrameType = Value
  val SQL_SCRIPT, HANDLER = Value
}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 *
 * @param executionPlan CompoundBody which need to be executed.
 * @param frameType Type of the frame.
 * @param scopeLabel Label of the scope where handler is defined.
 *                   Available only for frameType = HANDLER.
 */
class SqlScriptingExecutionFrame(
    val executionPlan: CompoundBodyExec,
    val frameType: SqlScriptingFrameType,
    val scopeLabel: Option[String] = None) extends Iterator[CompoundStatementExec] {

  // List of scopes that are currently active.
  private[scripting] val scopes: ListBuffer[SqlScriptingExecutionScope] = ListBuffer.empty

  override def hasNext: Boolean = executionPlan.getTreeIterator.hasNext

  override def next(): CompoundStatementExec = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    executionPlan.getTreeIterator.next()
  }

  def enterScope(
      label: String,
      triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap): Unit = {
    scopes.append(new SqlScriptingExecutionScope(label, triggerToExceptionHandlerMap))
  }

  def exitScope(label: String): Unit = {
    if (scopes.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no scopes to exit.")
    }

    // Remove all scopes until the one with the given label.
    while (scopes.nonEmpty && scopes.last.label != label) {
      scopes.remove(scopes.length - 1)
    }

    // Remove the scope with the given label.
    if (scopes.nonEmpty) {
      scopes.remove(scopes.length - 1)
    }
  }

  def currentScope: SqlScriptingExecutionScope = scopes.last

  // TODO: Introduce a separate class for different frame types (Script, Stored Procedure,
  //       Error Handler) implementing SqlScriptingExecutionFrame interface.
  def findHandler(
      condition: String,
      sqlState: String,
      firstHandlerScopeLabel: Option[String]): Option[ExceptionHandlerExec] = {

    val searchScopes = if (frameType == SqlScriptingFrameType.HANDLER) {
      // If the frame is a handler, search for the handler in its body. Don't skip any scopes.
      scopes.reverseIterator
    } else if (firstHandlerScopeLabel.isEmpty) {
      // If no handler is active, search for the handler from the current scope.
      // Don't skip any scopes.
      scopes.reverseIterator
    } else {
      // Drop all scopes until the first most outer scope where an active handler is defined.
      // Drop one more scope to start searching from the surrounding scope.
      scopes.reverseIterator.dropWhile(_.label != firstHandlerScopeLabel.get).drop(1)
    }

    // In the remaining scopes, try to find the most appropriate handler.
    searchScopes.foreach { scope =>
      val handler = scope.findHandler(condition, sqlState)
      if (handler.isDefined) {
        return handler
      }
    }

    None
  }
}

/**
 * SQL scripting execution scope - keeps track of the current execution scope.
 *
 * @param label
 *   Label of the scope.
 * @param triggerToExceptionHandlerMap
 *   Object holding condition/sqlState/sqlexception/not found to handler mapping.
 */
class SqlScriptingExecutionScope(
    val label: String,
    val triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap) {
  val variables = new mutable.HashMap[String, VariableDefinition]

  /**
   * Finds the most appropriate error handler for exception based on its condition and SQL state.
   *
   * The method follows these rules to determine the most appropriate handler:
   * 1. Specific named condition handlers (e.g., DIVIDE_BY_ZERO) are checked first.
   * 2. If no specific condition handler is found, SQLSTATE handlers are checked.
   * 3. For SQLSTATEs starting with '02', a generic NOT FOUND handler is used if available.
   * 4. For other SQLSTATEs (except those starting with 'XX' or '02'), a generic SQLEXCEPTION
   *    handler is used if available.
   *
   * Note: Handlers defined in the innermost compound statement where the exception was raised
   * are considered.
   *
   * @param condition Error condition of the exception to find handler for.
   * @param sqlState SQLSTATE of the exception to find handler for.
   *
   * @return Handler for the given condition if exists.
   */
  def findHandler(condition: String, sqlState: String): Option[ExceptionHandlerExec] = {
    // Check if there is a specific handler for the given condition.
    var errorHandler: Option[ExceptionHandlerExec] = None
    val uppercaseCondition = condition.toUpperCase(Locale.ROOT)
    val uppercaseSqlState = sqlState.toUpperCase(Locale.ROOT)

    errorHandler = triggerToExceptionHandlerMap.getHandlerForCondition(uppercaseCondition)

    if (errorHandler.isEmpty) {
      // Check if there is a specific handler for the given SQLSTATE.
      errorHandler = triggerToExceptionHandlerMap.getHandlerForSqlState(uppercaseSqlState)
    }

    if (errorHandler.isEmpty) {
      errorHandler = triggerToExceptionHandlerMap.getNotFoundHandler match {
        case Some(handler) if uppercaseSqlState.startsWith("02") => Some(handler)
        case _ => None
      }
    }

    if (errorHandler.isEmpty) {
      // If SQLEXCEPTION handler is defined, use it only for errors with class
      // different from 'XX' and '02'.
      errorHandler = triggerToExceptionHandlerMap.getSqlExceptionHandler match {
        case Some(handler)
          if !uppercaseSqlState.startsWith("XX") && !uppercaseSqlState.startsWith("02") =>
          Some(handler)
        case _ => None
      }
    }

    errorHandler
  }
}
