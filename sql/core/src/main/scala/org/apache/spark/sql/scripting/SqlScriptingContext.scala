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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.VariableDefinition

import javax.annotation.concurrent.GuardedBy

class SqlScriptingContext {

  private val scopes: mutable.ListBuffer[ScriptingScope] = mutable.ListBuffer.empty

  def enterScope(label: String): Unit = {
    scopes.append(ScriptingScope(label))
  }

  def exitScope(): Unit = {
    scopes.remove(scopes.size - 1)
  }

  def getVariable(name: String): Option[Any] = {
    scopes.reverseIterator.map(_.localVariableManager.get(name)).find(_.isDefined).flatten
  }

  def addVariable(name: String, value: VariableDefinition): Unit = {
    scopes.last.localVariableManager.create(name, value)
  }

  def updateVariable(name: String, value: VariableDefinition): Boolean = {
    // Update the variable in the first scope that contains it.
    scopes.reverseIterator
      .map(_.localVariableManager.update(name, value))
      .find(_ == true)
      .getOrElse(false)
  }

  def removeVariable(name: String): Boolean = {
    // Remove the variable in the first scope that contains it.
    scopes.reverseIterator
      .map(_.localVariableManager.remove(name))
      .find(_ == true)
      .getOrElse(false)
  }
}

/**
 * Represents a scope in the SQL script.
 */
case class ScriptingScope(label: String) extends Logging {
  val localVariableManager: LocalVariableManager = new LocalVariableManager

}

/**
 * A thread-safe manager for temporary SQL variables (that live in the SQL Script),
 * providing atomic operations to manage them, e.g. create, get, remove, etc.
 *
 * Note that, the variable name is always case-sensitive here.
 */
class LocalVariableManager {
  @GuardedBy("this")
  private val localVariableMap: mutable.HashMap[String, VariableDefinition] = mutable.HashMap.empty

  def get(name: String): Option[VariableDefinition] = synchronized {
    localVariableMap.get(name)
  }

  def create(
      name: String,
      value: VariableDefinition): Unit = synchronized {
    localVariableMap.put(name, value)
  }

  def update(name: String, value: VariableDefinition): Boolean = synchronized {
    localVariableMap.put(name, value).isDefined
  }

  def remove(name: String): Boolean = synchronized {
    localVariableMap.remove(name).isDefined
  }

  def clear(): Unit = synchronized {
    localVariableMap.clear()
  }
}
