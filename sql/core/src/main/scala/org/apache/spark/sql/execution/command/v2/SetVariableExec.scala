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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, VariableReference}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

/**
 * Physical plan node for setting a variable.
 */
case class SetVariableExec(
    variables: Seq[VariableReference],
    query: SparkPlan,
    sessionVariablesOnly: Boolean) extends V2CommandExec with UnaryLike[SparkPlan] {

  override protected def run(): Seq[InternalRow] = {
    val values = query.executeCollect()
    if (values.length == 0) {
      variables.foreach { v =>
        setVariable(v, null, sessionVariablesOnly)
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
        setVariable(v, value, sessionVariablesOnly)
      }
    }
    Seq.empty
  }

  private def setVariable(
      variable: VariableReference,
      value: Any,
      sessionVariablesOnly: Boolean): Unit = {
    val namePartsCaseAdjusted = if (session.sessionState.conf.caseSensitiveAnalysis) {
      variable.originalNameParts
    } else {
      variable.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val tempVariableManager = session.sessionState.catalogManager.tempVariableManager
    val scriptingVariableManager =
      session.sessionState.catalogManager.getSqlScriptingLocalVariableManager

    val variableManager = scriptingVariableManager
      .filterNot(_ => sessionVariablesOnly)
      // If a local variable with nameParts exists, set it using scriptingVariableManager.
      .filter(_.get(namePartsCaseAdjusted).isDefined)
      // If a local variable with nameParts doesn't exist, check if a session variable exists
      // with those nameParts and set it using tempVariableManager.
      .orElse(
        Option.when(tempVariableManager.get(namePartsCaseAdjusted).isDefined) {
          tempVariableManager
        }
      // If neither local or session variable exists, throw error. This shouldn't happen as
      // existence of this variable is already checked in ResolveSetVariable.
      ).getOrElse(
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))
      )

    variableManager.set(
      namePartsCaseAdjusted,
      variable.varDef.defaultValueSQL,
      Literal(value, variable.dataType),
      variable.identifier)
  }

  override def output: Seq[Attribute] = Seq.empty
  override def child: SparkPlan = query
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}
