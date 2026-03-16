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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.{VariableDefinition, VariableManager}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError

class SqlScriptingLocalVariableManager(context: SqlScriptingExecutionContext)
  extends VariableManager with DataTypeErrorsBase {

  override def getVariableNameForError(variableName: String): String =
    toSQLId(Seq(context.currentScope.label, variableName))

  override def create(
      nameParts: Seq[String],
      varDef: VariableDefinition,
      overrideIfExists: Boolean): Unit = {
    val name = nameParts.last

    // Sanity check, this should already be thrown by CreateVariableExec.run
    if (context.currentScope.variables.contains(name)) {
      throw new AnalysisException(
        errorClass = "VARIABLE_ALREADY_EXISTS",
        messageParameters = Map(
          "variableName" -> getVariableNameForError(name)))
    }
    context.currentScope.variables.put(name, varDef)
  }

  override def set(nameParts: Seq[String], varDef: VariableDefinition): Unit = {
    val scope = findScopeOfVariable(nameParts)
      .getOrElse(
        throw unresolvedVariableError(nameParts, varDef.identifier.namespace().toIndexedSeq))

    if (!scope.variables.contains(nameParts.last)) {
      throw unresolvedVariableError(nameParts, varDef.identifier.namespace().toIndexedSeq)
    }

    scope.variables.put(nameParts.last, varDef)
  }

  override def get(nameParts: Seq[String]): Option[VariableDefinition] = {
    findScopeOfVariable(nameParts).flatMap(_.variables.get(nameParts.last))
  }

  private def findScopeOfVariable(nameParts: Seq[String]): Option[SqlScriptingExecutionScope] = {
    // TODO: Update logic and comments once stored procedures are introduced.
    def isScopeOfVar(
        nameParts: Seq[String],
        scope: SqlScriptingExecutionScope
    ): Boolean = nameParts match {
      case Seq(name) => scope.variables.contains(name)
      // Qualified case.
      case Seq(label, _) => scope.label == label
      case _ =>
        throw SparkException.internalError("ScriptingVariableManager expects 1 or 2 nameParts.")
    }

    // Use the shared searchAcrossFrames helper to maintain consistent logic with cursors
    context.searchAcrossFrames(
      searchInCurrentFrame = frame =>
        frame.scopes.findLast(scope => isScopeOfVar(nameParts, scope)),
      searchInScopes = scopes =>
        scopes.findLast(scope => isScopeOfVar(nameParts, scope))
    )
  }

  override def qualify(name: String): ResolvedIdentifier =
    ResolvedIdentifier(FakeLocalCatalog, Identifier.of(Array(context.currentScope.label), name))

  override def remove(nameParts: Seq[String]): Boolean = {
    throw SparkException.internalError(
      "ScriptingVariableManager.remove should never be called as local variables cannot be dropped."
    )
  }

  override def clear(): Unit = context.frames.clear()

  // Empty if all scopes of all frames in the script context contain no variables.
  override def isEmpty: Boolean = context.frames.forall(_.scopes.forall(_.variables.isEmpty))
}
