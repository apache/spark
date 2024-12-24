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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.{VariableDefinition, VariableManager}
import org.apache.spark.sql.catalyst.expressions.Literal


// todo LOCALVARS: should this be thread safe / synchronized, also should probably be one per frame
class ScriptingVariableManager(context: SqlScriptingExecutionContext) extends VariableManager{

  // map from scope label to map from variable name to variable definition
  private val variables = {
    val map = new mutable.HashMap[String, mutable.HashMap[String, VariableDefinition]]
    // probably unnecessary as when variable manager is initialized there are no scopes yet
    context.currentFrame.scopes.foreach(scope =>
      map.put(scope.label, new mutable.HashMap[String, VariableDefinition]))
    map
  }

  override def create(
      name: String,
      defaultValueSQL: String,
      initValue: Literal,
      overrideIfExists: Boolean): Unit = {
    // todo LOCALVARS: throw meaningful error, qualified name
    variables
//      .getOrElse(context.currentScope.label, throw Exception)
      .get(context.currentScope.label).get
      .put(name, VariableDefinition(defaultValueSQL, initValue))
  }

  override def get(name: String): Option[VariableDefinition] = {
    // todo LOCALVAR: add support for qualified name
    context.currentFrame.scopes
      .findLast(scope => variables(scope.label).contains(name))
      .map(scope => variables(scope.label)(name))
  }

  override def remove(name: String): Boolean = {
    true
  }

  override def clear(): Unit = variables.clear()

  override def isEmpty: Boolean = variables.values.forall(_.isEmpty)
}
