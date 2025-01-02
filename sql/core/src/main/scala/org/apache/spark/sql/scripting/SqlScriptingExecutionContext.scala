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

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkException

/**
 * SQL scripting execution context - keeps track of the current execution state.
 */
class SqlScriptingExecutionContext {
  // List of frames that are currently active.
  val frames: ListBuffer[SqlScriptingExecutionFrame] = ListBuffer.empty

  def enterScope(label: String): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot enter scope: no frames.")
    }
    frames.last.enterScope(label)
  }

  def exitScope(label: String): Unit = {
    if (frames.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no frames.")
    }
    frames.last.exitScope(label)
  }
}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 *
 * @param executionPlan CompoundBody which need to be executed.
 */
class SqlScriptingExecutionFrame(
    executionPlan: Iterator[CompoundStatementExec]) extends Iterator[CompoundStatementExec] {

  // List of scopes that are currently active.
  private val scopes: ListBuffer[SqlScriptingExecutionScope] = ListBuffer.empty

  override def hasNext: Boolean = executionPlan.hasNext

  override def next(): CompoundStatementExec = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    executionPlan.next()
  }

  def enterScope(label: String): Unit = {
    scopes.addOne(new SqlScriptingExecutionScope(label))
  }

  def exitScope(label: String): Unit = {
    if (scopes.isEmpty) {
      throw SparkException.internalError("Cannot exit scope: no scopes to exit.")
    }

    // Remove all scopes until the one with the given label.
    while (scopes.nonEmpty && scopes.last.label != label) {
      scopes.remove(scopes.length - 1)
    }

    if (scopes.nonEmpty) {
      scopes.remove(scopes.length - 1)
    }
  }
}

/**
 * SQL scripting execution scope - keeps track of the current execution scope.
 *
 * @param label
 *   Label of the scope.
 */
class SqlScriptingExecutionScope(val label: String)
