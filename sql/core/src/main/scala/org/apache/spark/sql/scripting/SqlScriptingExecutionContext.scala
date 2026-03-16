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
import org.apache.spark.sql.catalyst.catalog.{SqlScriptingExecutionContextExtension, VariableDefinition}
import org.apache.spark.sql.catalyst.expressions.CursorDefinition
import org.apache.spark.sql.scripting.SqlScriptingFrameType.SqlScriptingFrameType

/**
 * SQL scripting execution context - keeps track of the current execution state.
 */
class SqlScriptingExecutionContext extends SqlScriptingExecutionContextExtension {
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

  /**
   * Generic method to search for an object across all frames respecting scope visibility rules.
   * This encapsulates the common pattern used by both variable and cursor resolution.
   *
   * The search follows these rules:
   * 1. Search current frame first
   * 2. Search previous frames in reverse order (newest to oldest)
   * 3. For each previous frame, only search scopes that were visible where the current
   *    frame (handler) was defined - respects lexical scoping
   *
   * @param searchInCurrentFrame Function to search within the current frame
   * @param searchInScopes Function to search within a list of candidate scopes
   * @tparam T The type of object being searched for
   * @return The found object, or None if not found
   */
  private[scripting] def searchAcrossFrames[T](
      searchInCurrentFrame: SqlScriptingExecutionFrame => Option[T],
      searchInScopes: Seq[SqlScriptingExecutionScope] => Option[T]): Option[T] = {
    if (frames.isEmpty) {
      return None
    }

    // First search in current frame
    val resCurrentFrame = searchInCurrentFrame(currentFrame)
    if (resCurrentFrame.isDefined) {
      return resCurrentFrame
    }

    // When searching in previous frames, for each frame we check only scopes before
    // and including the scope where the previously checked frame (handler) was defined.
    // This ensures handlers can only access declarations from scopes visible where
    // the handler was defined, maintaining lexical scoping rules.
    var previousFrameDefinitionLabel = currentFrame.scopeLabel

    // dropRight(1) removes the current frame, which we already checked above
    frames.dropRight(1).reverseIterator.foreach { frame =>
      // Get scopes from this frame up to and including where the previous handler
      // frame was defined. If it was not defined in this scope, candidateScopes
      // will be empty.
      val candidateScopes = frame.scopes.reverse.dropWhile(
        scope => !previousFrameDefinitionLabel.contains(scope.label)).toSeq

      val result = searchInScopes(candidateScopes)
      if (result.isDefined) {
        return result
      }

      // If candidateScopes is nonEmpty, we found the previous frame definition
      // in this frame. If we still haven't found the object, we now have to find
      // the definition of this frame, so we reassign the frame definition label.
      if (candidateScopes.nonEmpty) {
        previousFrameDefinitionLabel = frame.scopeLabel
      }
    }

    None
  }

  /**
   * Perform an action in the frame containing a specific object.
   * This is similar to searchAcrossFrames but executes an action when the object is found.
   * Useful for operations like updating cursor state where we need to modify the frame.
   *
   * @param searchInFrame Function to check if object exists in a frame
   * @param searchInScopes Function to check if object exists in candidate scopes
   * @param actionInFrame Action to perform in the frame containing the object
   * @return true if object was found and action performed, false otherwise
   */
  private[scripting] def performInFrameContaining(
      searchInFrame: SqlScriptingExecutionFrame => Boolean,
      searchInScopes: Seq[SqlScriptingExecutionScope] => Boolean,
      actionInFrame: SqlScriptingExecutionFrame => Unit): Boolean = {
    if (frames.isEmpty) {
      return false
    }

    // First check current frame
    if (searchInFrame(currentFrame)) {
      actionInFrame(currentFrame)
      return true
    }

    // Search through previous frames respecting scope boundaries
    var previousFrameDefinitionLabel = currentFrame.scopeLabel

    frames.dropRight(1).reverseIterator.foreach { frame =>
      val candidateScopes = frame.scopes.reverse.dropWhile(
        scope => !previousFrameDefinitionLabel.contains(scope.label)).toSeq

      if (searchInScopes(candidateScopes)) {
        actionInFrame(frame)
        return true
      }

      if (candidateScopes.nonEmpty) {
        previousFrameDefinitionLabel = frame.scopeLabel
      }
    }

    false
  }

  /**
   * Find a cursor by its normalized name in the current scope and parent scopes.
   * Implementation of SqlScriptingExecutionContextExtension API.
   *
   * Searches through all frames in reverse order (current to oldest), respecting scope
   * visibility rules. This matches the behavior of variable resolution to ensure consistency.
   * Cursors declared in handlers shadow cursors with the same name in outer scopes.
   */
  override def findCursorByName(normalizedName: String): Option[CursorDefinition] = {
    searchAcrossFrames(
      searchInCurrentFrame = _.findCursorByName(normalizedName),
      searchInScopes = _.flatMap(_.cursors.get(normalizedName)).headOption
    )
  }

  /**
   * Find a cursor in a specific labeled scope.
   * Implementation of SqlScriptingExecutionContextExtension API.
   *
   * Searches through all frames in reverse order, respecting scope visibility rules.
   * For qualified cursor references (label.cursor), this finds the cursor in the
   * specific labeled scope across all accessible frames.
   */
  override def findCursorInScope(
      normalizedScopeLabel: String,
      normalizedName: String): Option[CursorDefinition] = {
    searchAcrossFrames(
      searchInCurrentFrame = _.findCursorInScope(normalizedScopeLabel, normalizedName),
      searchInScopes = _.find(_.label == normalizedScopeLabel)
        .flatMap(_.cursors.get(normalizedName))
    )
  }

  /**
   * Get cursor state across all frames.
   * Searches through all frames in reverse order, respecting scope visibility rules.
   * This matches the cursor definition lookup behavior.
   */
  def getCursorState(normalizedName: String, scopeLabel: Option[String]): Option[CursorState] = {
    searchAcrossFrames(
      searchInCurrentFrame = _.getCursorState(normalizedName, scopeLabel),
      searchInScopes = scopes =>
        scopeLabel match {
          case Some(label) =>
            scopes.find(_.label == label).flatMap(_.cursorStates.get(normalizedName))
          case None =>
            scopes.flatMap(_.cursorStates.get(normalizedName)).headOption
        }
    )
  }

  /**
   * Get cursor state for a resolved CursorReference.
   * This is a convenience method that avoids repeating the same pattern in cursor commands.
   */
  def getCursorState(cursorRef: org.apache.spark.sql.catalyst.expressions.CursorReference):
      Option[CursorState] = {
    getCursorState(cursorRef.normalizedName, cursorRef.scopeLabel)
  }

  /**
   * Update cursor state across all frames.
   * Uses shared frame/scope search logic to find the cursor, then updates its state.
   */
  def updateCursorState(
      normalizedName: String,
      scopeLabel: Option[String],
      newState: CursorState): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot update cursor state: no frames.")
    }

    // Use the shared helper to find and update in one pass
    val found = performInFrameContaining(
      searchInFrame = frame =>
        scopeLabel match {
          case Some(label) => frame.findCursorInScope(label, normalizedName).isDefined
          case None => frame.findCursorByName(normalizedName).isDefined
        },
      searchInScopes = scopes =>
        scopeLabel match {
          case Some(label) =>
            scopes.find(_.label == label).flatMap(_.cursors.get(normalizedName)).isDefined
          case None =>
            scopes.flatMap(_.cursors.get(normalizedName)).headOption.isDefined
        },
      actionInFrame = frame =>
        frame.updateCursorState(normalizedName, scopeLabel, newState)
    )

    if (!found) {
      throw SparkException.internalError(s"Cursor $normalizedName not found in any frame")
    }
  }

  def findHandler(condition: String, sqlState: String): Option[ExceptionHandlerExec] = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot find handler: no frames.")
    }

    // If the last frame is a handler, try to find a handler in its body first.
    if (frames.last.frameType == SqlScriptingFrameType.EXIT_HANDLER
        || frames.last.frameType == SqlScriptingFrameType.CONTINUE_HANDLER) {
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
  val SQL_SCRIPT, EXIT_HANDLER, CONTINUE_HANDLER = Value
}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 *
 * @param executionPlan CompoundBody which need to be executed.
 * @param frameType Type of the frame.
 * @param scopeLabel Label of the scope where handler is defined.
 *                   Available only for frameType = EXIT_HANDLER, frameType = CONTINUE_HANDLER
 *                   and frameType = SQL_STORED_PROCEDURE.
 */
class SqlScriptingExecutionFrame(
    val executionPlan: CompoundBodyExec,
    val frameType: SqlScriptingFrameType,
    val scopeLabel: Option[String] = None) extends Iterator[CompoundStatementExec] {

  // List of scopes that are currently active.
  private[scripting] val scopes: ListBuffer[SqlScriptingExecutionScope] = ListBuffer.empty

  /**
   * Find a cursor by its normalized name in the current scope and parent scopes.
   * Used by cursor commands after name resolution.
   *
   * @param normalizedName The normalized cursor name (considering case sensitivity)
   * @return The cursor definition if found
   */
  def findCursorByName(normalizedName: String): Option[CursorDefinition] = {
    scopes.reverseIterator.flatMap(_.cursors.get(normalizedName)).nextOption()
  }

  /**
   * Find a cursor in a specific labeled scope.
   * Used by qualified cursor references (label.cursor).
   *
   * @param scopeLabel The label of the scope to search in
   * @param normalizedName The normalized cursor name (considering case sensitivity)
   * @return The cursor definition if found
   */
  def findCursorInScope(scopeLabel: String, normalizedName: String): Option[CursorDefinition] = {
    scopes.reverseIterator.find(_.label == scopeLabel).flatMap(_.cursors.get(normalizedName))
  }

  /**
   * Find the scope object with the given label.
   * Used to access scope-specific state (e.g., cursorStates).
   *
   * @param scopeLabel The label of the scope to find
   * @return The scope if found
   */
  def findScope(scopeLabel: String): Option[SqlScriptingExecutionScope] = {
    scopes.reverseIterator.find(_.label == scopeLabel)
  }

  /**
   * Get the cursor state for a given cursor, considering scope qualifications.
   *
   * @param normalizedName The normalized cursor name
   * @param scopeLabel Optional scope label (Some("label") for qualified cursors, None for
   *                   unqualified)
   * @return The cursor state if found
   */
  def getCursorState(normalizedName: String, scopeLabel: Option[String]): Option[CursorState] = {
    scopeLabel match {
      case Some(label) =>
        findScope(label).flatMap(_.cursorStates.get(normalizedName))
      case None =>
        // Search in current scope and parent scopes
        scopes.reverseIterator.flatMap(_.cursorStates.get(normalizedName)).nextOption()
    }
  }

  /**
   * Update the cursor state for a given cursor, considering scope qualifications.
   *
   * @param normalizedName The normalized cursor name
   * @param scopeLabel Optional scope label (Some("label") for qualified cursors, None for
   *                   unqualified)
   * @param newState The new cursor state
   */
  def updateCursorState(
      normalizedName: String,
      scopeLabel: Option[String],
      newState: CursorState): Unit = {
    val targetScope = scopeLabel match {
      case Some(label) =>
        findScope(label).getOrElse(
          throw SparkException.internalError(s"Scope $label not found"))
      case None =>
        // Find the scope where this cursor is defined
        scopes.reverseIterator.find(_.cursors.contains(normalizedName)).getOrElse(
          throw SparkException.internalError(s"Cursor $normalizedName not found in any scope"))
    }
    targetScope.cursorStates.put(normalizedName, newState)
  }

  override def hasNext: Boolean = executionPlan.getTreeIterator.hasNext

  override def next(): CompoundStatementExec = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    executionPlan.getTreeIterator.next()
  }

  def enterScope(
      label: String,
      triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap): Unit = {
    // Labels are already lowercased in ParserUtils#enterLabeledScope during parsing,
    // so no need to normalize here. Labels are always case-insensitive in SQL/PSM.
    scopes.append(new SqlScriptingExecutionScope(label, triggerToExceptionHandlerMap))
  }

  def exitScope(label: String): Unit = {
    if (scopes.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no scopes to exit.")
    }

    // Remove all scopes until the one with the given label.
    while (scopes.nonEmpty && scopes.last.label != label) {
      scopes.last.cleanup()
      scopes.remove(scopes.length - 1)
    }

    // Remove the scope with the given label.
    if (scopes.nonEmpty) {
      scopes.last.cleanup()
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

    val searchScopes =
      if (frameType == SqlScriptingFrameType.EXIT_HANDLER
          || frameType == SqlScriptingFrameType.CONTINUE_HANDLER) {
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
  val cursors = new mutable.HashMap[String, CursorDefinition]
  val cursorStates = new mutable.HashMap[String, CursorState]

  /**
   * Cleanup resources when this scope is being removed.
   * Closes all open cursors and releases their result data.
   */
  def cleanup(): Unit = {
    // Close all cursors by transitioning them to Closed state
    cursorStates.keys.foreach { cursorName =>
      cursorStates.put(cursorName, CursorClosed)
    }
  }

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
      if (uppercaseCondition.contains('.')) {
        // If the condition contains a dot, it has a main error class and a subclass.
        // Check if the error class is defined in the triggerToExceptionHandlerMap.
        val errorClass = uppercaseCondition.split('.').head
        errorHandler = triggerToExceptionHandlerMap.getHandlerForCondition(errorClass)
      }
    }

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
