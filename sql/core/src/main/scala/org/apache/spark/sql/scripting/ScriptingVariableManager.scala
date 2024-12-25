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


// todo LOCALVARS: should this be thread safe / synchronized (probably not since its one per script)
class ScriptingVariableManager(context: SqlScriptingExecutionContext) extends VariableManager {

  override def create(
      name: String,
      defaultValueSQL: String,
      initValue: Literal,
      overrideIfExists: Boolean): Unit = {
    // todo LOCALVARS: qualified name
    context.currentScope.variables.put(name, VariableDefinition(defaultValueSQL, initValue))
  }

  override def get(name: String): Option[VariableDefinition] = {
    // todo LOCALVARS: add support for qualified name
    context.currentFrame.scopes
      .findLast(_.variables.contains(name))
      .map(_.variables(name))
  }

  override def remove(name: String): Boolean = {
    true
  }

  // todo LOCALVARS: do we need this
  override def clear(): Unit = ()

  override def isEmpty: Boolean = context.currentFrame.scopes.forall(_.variables.isEmpty)
}
