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

import org.apache.spark.sql.catalyst.catalog.{VariableDefinition, VariableManager}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.Identifier

class ScriptingVariableManager(context: SqlScriptingExecutionContext) extends VariableManager {

  override def create(
      name: String,
      defaultValueSQL: String,
      initValue: Literal,
      overrideIfExists: Boolean,
      identifier: Identifier): Unit = {
    // todo LOCALVARS: qualified name
    context.currentScope.variables.put(
      name,
      VariableDefinition(
        Identifier.of(Array(context.currentScope.label), name),
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
  }

  override def remove(name: String): Boolean = {
    // probably throw error
    true
  }

  // todo LOCALVARS: do we need this
  override def clear(): Unit = ()

  override def isEmpty: Boolean = context.currentFrame.scopes.forall(_.variables.isEmpty)
}
