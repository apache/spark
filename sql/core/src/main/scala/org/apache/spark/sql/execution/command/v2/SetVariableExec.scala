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
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingLocalVariableManager}
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, FakeSystemCatalog}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, VariableReference}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

/**
 * Physical plan node for setting a variable.
 */
case class SetVariableExec(variables: Seq[VariableReference], query: SparkPlan)
  extends V2CommandExec with UnaryLike[SparkPlan] {

  override protected def run(): Seq[InternalRow] = {
    val values = query.executeCollect()
    if (values.length == 0) {
      variables.foreach { v =>
        setVariable(v, null)
      }
    } else if (values.length > 1) {
      throw new SparkException(
        errorClass = "ROW_SUBQUERY_TOO_MANY_ROWS",
        messageParameters = Map.empty,
        cause = null)
    } else {
      val row = values(0)
      variables.zipWithIndex.foreach { case (v, index) =>
        val value = row.get(index, v.dataType)
        setVariable(v, value)
      }
    }
    Seq.empty
  }

  private def setVariable(
      variable: VariableReference,
      value: Any): Unit = {
    val namePartsCaseAdjusted = if (session.sessionState.conf.caseSensitiveAnalysis) {
      variable.originalNameParts
    } else {
      variable.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val tempVariableManager = session.sessionState.catalogManager.tempVariableManager
    val scriptingVariableManager = SqlScriptingLocalVariableManager.get()

    val variableManager = variable.catalog match {
      case FakeLocalCatalog if scriptingVariableManager.isEmpty =>
        throw SparkException.internalError("SetVariableExec: Variable has FakeLocalCatalog, " +
          "but ScriptingVariableManager is None.")

      case FakeLocalCatalog if scriptingVariableManager.get.get(namePartsCaseAdjusted).isEmpty =>
        throw SparkException.internalError("Local variable should be present in SetVariableExec" +
          "because ResolveSetVariable has already determined it exists.")

      case FakeLocalCatalog => scriptingVariableManager.get

      case FakeSystemCatalog if tempVariableManager.get(namePartsCaseAdjusted).isEmpty =>
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))

      case FakeSystemCatalog => tempVariableManager

      case c => throw SparkException.internalError("Unexpected catalog in SetVariableExec: " + c)
    }

    val varDef = VariableDefinition(
      variable.identifier, variable.varDef.defaultValueSQL, Literal(value, variable.dataType))

    variableManager.set(namePartsCaseAdjusted, varDef)
  }

  override def output: Seq[Attribute] = Seq.empty
  override def child: SparkPlan = query
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}
