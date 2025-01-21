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

import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark.SparkException
import org.apache.spark.sql.scripting.SqlScriptingFrameType.SqlScriptingFrameType

/**
 * SQL scripting execution context - keeps track of the current execution state.
 */
class SqlScriptingExecutionContext {
  // List of frames that are currently active.
  private[scripting] val frames: ListBuffer[SqlScriptingExecutionFrame] = ListBuffer.empty
  private[scripting] var firstHandlerScope: Option[String] = None

  def enterScope(
      label: String,
      conditionHandlerMap: HashMap[String, ErrorHandlerExec]): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot enter scope: no frames.")
    }
    frames.last.enterScope(label, conditionHandlerMap)
  }

  def exitScope(label: String): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no frames.")
    }
    frames.last.exitScope(label)
  }

  def findHandler(condition: String, sqlState: String): Option[ErrorHandlerExec] = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot find handler: no frames.")
    }

    // If the last frame is a handler, try to find a handler in it first.
    if (frames.last.frameType == SqlScriptingFrameType.HANDLER) {
      val handler = frames.last.findHandler(condition, sqlState, firstHandlerScope)
      if (handler.isDefined) {
        return handler
      }
    }

    frames.reverseIterator.foreach { frame =>
      // Skip the current frame if it is a handler.
      if (frame.frameType != SqlScriptingFrameType.HANDLER) {
        val handler = frame.findHandler(condition, sqlState, firstHandlerScope)
        if (handler.isDefined) {
          firstHandlerScope = handler.get.scopeLabel
          return handler
        }
      }
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
  private val scopes: ListBuffer[SqlScriptingExecutionScope] = ListBuffer.empty

  override def hasNext: Boolean = executionPlan.getTreeIterator.hasNext

  override def next(): CompoundStatementExec = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    executionPlan.getTreeIterator.next()
  }

  def enterScope(
      label: String,
      conditionHandlerMap: HashMap[String, ErrorHandlerExec]): Unit = {
    scopes.append(new SqlScriptingExecutionScope(label, conditionHandlerMap))
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

  def findHandler(
      condition: String,
      sqlState: String,
      firstHandlerScope: Option[String]): Option[ErrorHandlerExec] = {

    // Find the most outer scope where an active handler is defined.
    // Search for handler should start from the scope that is surrounding that scope.
    var found: Boolean = false
    scopes.reverseIterator.foreach { scope =>
      // If there is no active handler or the current frame is a handler, try to find a handler
      // in this scope. That is because handlers can have nested handlers defined in their body.
      if (firstHandlerScope.isEmpty || frameType == SqlScriptingFrameType.HANDLER) {
        found = true
      }

      if (found) {
        val handler = scope.findHandler(condition, sqlState)
        if (handler.isDefined) {
          return handler
        }
      }

      // If there are active handlers, iterate we reach the most outer scope where
      // an active handler is defined.
      if (firstHandlerScope.isDefined && scope.label == firstHandlerScope.get) {
        found = true
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
 * @param conditionHandlerMap
 *   Map holding condition/sqlState to handler mapping.
 * @return
 *   Handler for the given condition.
 */
class SqlScriptingExecutionScope(
    val label: String,
    val conditionHandlerMap: HashMap[String, ErrorHandlerExec]) {

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
  def findHandler(condition: String, sqlState: String): Option[ErrorHandlerExec] = {
    // Check if there is a specific handler for the given condition.
    conditionHandlerMap.get(condition)
      .orElse {
        conditionHandlerMap.get(sqlState) match {
          // If SQLSTATE handler is defined, use it only for errors with class != '02'.
          case Some(handler) if !sqlState.startsWith("02") => Some(handler)
          case _ => None
        }
      }
      .orElse {
        conditionHandlerMap.get("NOT FOUND") match {
          // If NOT FOUND handler is defined, use it only for errors with class == '02'.
          case Some(handler) if sqlState.startsWith("02") => Some(handler)
          case _ => None
        }
      }
      .orElse {
        conditionHandlerMap.get("SQLEXCEPTION") match {
          // If SQLEXCEPTION handler is defined, use it only for errors with class
          // different from 'XX' and '02'.
          case Some(handler) if
            !sqlState.startsWith("XX") && !sqlState.startsWith("02") => Some(handler)
          case _ => None
        }
      }
  }
}
