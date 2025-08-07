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

  override def create(
      nameParts: Seq[String],
      varDef: VariableDefinition,
      overrideIfExists: Boolean): Unit = {
    val name = nameParts.last

    // overrideIfExists should not be supported because local variables don't support
    // DECLARE OR REPLACE. However ForStatementExec currently uses this to handle local vars,
    // so we support it for now.
    // TODO [SPARK-50785]: Refactor ForStatementExec to use local variables properly.
    if (!overrideIfExists && context.currentScope.variables.contains(name)) {
      throw new AnalysisException(
        errorClass = "VARIABLE_ALREADY_EXISTS",
        messageParameters = Map(
          "variableName" -> toSQLId(Seq(context.currentScope.label, name))))
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

    // First search for variable in entire current frame.
    val resCurrentFrame = context.currentFrame.scopes
      .findLast(scope => isScopeOfVar(nameParts, scope))
    if (resCurrentFrame.isDefined) {
      return resCurrentFrame
    }

    // When searching in previous frames, for each frame we have to check only scopes before and
    // including the scope where the previously checked exception handler frame is defined.
    // Exception handler frames should not have access to variables from scopes
    // which are nested below the scope where the handler is defined.
    var previousFrameDefinitionLabel = context.currentFrame.scopeLabel

    // dropRight(1) removes the current frame, which we already checked above.
    context.frames.dropRight(1).reverseIterator.foreach(frame => {
      // Drop scopes until we encounter the scope in which the previously checked
      // frame was defined. If it was not defined in this scope candidateScopes will be
      // empty.
      val candidateScopes = frame.scopes.reverse.dropWhile(
        scope => !previousFrameDefinitionLabel.contains(scope.label))

      val scope = candidateScopes.findLast(scope => isScopeOfVar(nameParts, scope))
      if (scope.isDefined) {
        return scope
      }
      // If candidateScopes is nonEmpty that means that we found the previous frame definition
      // in this frame. If we still have not found the variable, we now have to find the definition
      // of this new frame, so we reassign the frame definition label to search for.
      if (candidateScopes.nonEmpty) {
        previousFrameDefinitionLabel = frame.scopeLabel
      }
    })
    None
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
