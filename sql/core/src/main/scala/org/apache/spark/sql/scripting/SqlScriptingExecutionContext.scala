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

/**
 * SQL scripting execution context - keeps track of the current execution state.
 */
class SqlScriptingExecutionContext {
  // List of frames that are currently active.
  val frames: ListBuffer[SqlScriptingExecutionFrame] = ListBuffer.empty

  def enterScope(label: String, conditionHandlerMap: HashMap[String, ErrorHandlerExec]): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot enter scope: no frames.")
    }
    frames.last.enterScope(label, conditionHandlerMap)
  }

  def exitScope(label: String): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot exit scope: no frames.")
    }
    frames.reverseIterator.foreach { frame =>
      if (frame.exitScope(label)) {
        return
      }
    }
  }

  def findHandler(condition: String): Option[ErrorHandlerExec] = {
    if (frames.isEmpty) {
      throw SparkException.internalError(s"Cannot find handler: no frames.")
    }

    frames.reverseIterator.foreach { frame =>
      val handler = frame.findHandler(condition)
      if (handler.isDefined) {
        return handler
      }
    }
    None
  }

}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 *
 * @param executionPlan CompoundBody which need to be executed.
 */
class SqlScriptingExecutionFrame(
    val executionPlan: CompoundBodyExec,
    val isExitHandler: Boolean = false,
    val isContinueHandler: Boolean = false,
    val scopeToExit: Option[String] = None) extends Iterator[CompoundStatementExec] {

  // List of scopes that are currently active.
  private val scopes: ListBuffer[SqlScriptingExecutionScope] = ListBuffer.empty

  override def hasNext: Boolean = executionPlan.getTreeIterator.hasNext

  override def next(): CompoundStatementExec = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    executionPlan.getTreeIterator.next()
  }

  def findHandler(condition: String): Option[ErrorHandlerExec] = {
    if (scopes.isEmpty) {
      throw SparkException.internalError(s"Cannot find handler: no scopes.")
    }

    scopes.reverseIterator.foreach { scope =>
      val handler = scope.findHandler(condition)
      if (handler.isDefined) {
        return handler
      }
    }
    None
  }

  def enterScope(label: String, conditionHandlerMap: HashMap[String, ErrorHandlerExec]): Unit = {
    scopes.addOne(new SqlScriptingExecutionScope(label, conditionHandlerMap))
  }

  def exitScope(label: String): Boolean = {
    if (scopes.isEmpty) {
      throw SparkException.internalError(s"Cannot exit scope: no scopes to exit.")
    }

    // Remove all scopes until the one with the given label.
    while (scopes.nonEmpty && scopes.last.label != label) {
      scopes.remove(scopes.length - 1)
    }

    if (scopes.nonEmpty) {
      scopes.remove(scopes.length - 1)
      return true;
    }
    false
  }
}

/**
 * SQL scripting execution scope - keeps track of the current execution scope.
 *
 * @param label
 *   Label of the scope.
 */
class SqlScriptingExecutionScope(
    val label: String,
    val conditionHandlerMap: HashMap[String, ErrorHandlerExec]) {

  def findHandler(condition: String): Option[ErrorHandlerExec] = {
    conditionHandlerMap.get(condition)
      .orElse{
        conditionHandlerMap.get("NOT FOUND") match {
          // If NOT FOUND handler is defined, use it only for errors with class '02'.
          case Some(handler) if condition.startsWith("02") => Some(handler)
          case _ => None
        }}
      .orElse{conditionHandlerMap.get("SQLEXCEPTION")}
  }
}
