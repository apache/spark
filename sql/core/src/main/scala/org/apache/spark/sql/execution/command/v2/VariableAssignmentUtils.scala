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

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, FakeSystemCatalog}
import org.apache.spark.sql.catalyst.catalog.{VariableDefinition, VariableManager}
import org.apache.spark.sql.catalyst.expressions.{Literal, VariableReference}
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.internal.SQLConf

/**
 * Utility methods for variable assignment shared between SET and FETCH commands.
 */
object VariableAssignmentUtils {

  /**
   * Assigns a value to a variable (either scripting local or session).
   * Uses the variable's catalog to determine which variable manager to use:
   * - FakeLocalCatalog: scripting local variables (managed by SqlScriptingContextManager)
   * - FakeSystemCatalog: session variables (managed by TempVariableManager)
   *
   * @param varRef The variable reference containing catalog information
   * @param value The value to assign
   * @param tempVariableManager The session-level variable manager
   * @param conf SQL configuration for case sensitivity
   */
  def assignVariable(
      varRef: VariableReference,
      value: Any,
      tempVariableManager: VariableManager,
      conf: SQLConf): Unit = {
    val namePartsCaseAdjusted = if (conf.caseSensitiveAnalysis) {
      varRef.originalNameParts
    } else {
      varRef.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val scriptingVariableManager = SqlScriptingContextManager.get().map(_.getVariableManager)

    val variableManager = varRef.catalog match {
      case FakeLocalCatalog if scriptingVariableManager.isEmpty =>
        throw SparkException.internalError("Variable has FakeLocalCatalog, " +
          "but ScriptingVariableManager is None.")

      case FakeLocalCatalog if scriptingVariableManager.get.get(namePartsCaseAdjusted).isEmpty =>
        throw SparkException.internalError("Local variable should be present " +
          "because variable resolution has already determined it exists.")

      case FakeLocalCatalog => scriptingVariableManager.get

      case FakeSystemCatalog if tempVariableManager.get(namePartsCaseAdjusted).isEmpty =>
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))

      case FakeSystemCatalog => tempVariableManager

      case c => throw SparkException.internalError("Unexpected catalog: " + c)
    }

    val varDef = VariableDefinition(
      varRef.identifier,
      varRef.varDef.defaultValueSQL,
      Literal(value, varRef.dataType)
    )

    variableManager.set(namePartsCaseAdjusted, varDef)
  }
}
