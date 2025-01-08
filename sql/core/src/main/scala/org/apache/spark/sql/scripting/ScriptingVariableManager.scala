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
import org.apache.spark.sql.catalyst.catalog.{VariableDefinition, VariableManager}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.DataTypeErrorsBase

class ScriptingVariableManager(context: SqlScriptingExecutionContext)
  extends VariableManager with DataTypeErrorsBase {

  override def create(
      name: String,
      defaultValueSQL: String,
      initValue: Literal,
      overrideIfExists: Boolean,
      identifier: Identifier): Unit = {
    if (!overrideIfExists && context.currentScope.variables.contains(name)) {
      throw new AnalysisException(
        errorClass = "VARIABLE_ALREADY_EXISTS",
        messageParameters = Map(
          "variableName" -> toSQLId(Seq(context.currentScope.label, name))))
    }
    context.currentScope.variables.put(
      name,
      VariableDefinition(
        identifier,
        defaultValueSQL,
        initValue
      ))
  }

  override def get(nameParts: Seq[String]): Option[VariableDefinition] = nameParts match {
    case Seq(name) =>
      context.currentFrame.scopes
      .findLast(_.variables.contains(name))
      .map(_.variables(name))
    case Seq(label, name) =>
      context.currentFrame.scopes
      .findLast(_.label == label)
      .map(_.variables(name))
    case _ =>
      throw SparkException.internalError("ScriptingVariableManager.get expects 1 or 2 nameParts.")
  }

  override def createIdentifier(name: String): Identifier =
    Identifier.of(Array(context.currentScope.label), name)

  override def remove(name: String): Boolean = {
    throw SparkException.internalError(
      "ScriptingVariableManager.remove should never be called as local variables cannot be dropped."
    )
  }

  override def clear(): Unit = context.frames.clear()

  // Empty if all scopes of all frames in the script context contain no variables.
  override def isEmpty: Boolean = context.frames.forall(_.scopes.forall(_.variables.isEmpty))
}
