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

package org.apache.spark.sql.catalyst.catalog

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.CatalogManager.{SESSION_NAMESPACE, SYSTEM_CATALOG_NAME}
import org.apache.spark.sql.errors.DataTypeErrorsBase

/**
 * A thread-safe manager for temporary SQL variables (that live in the schema `SYSTEM.SESSION`),
 * providing atomic operations to manage them, e.g. create, get, remove, etc.
 *
 * Note that, the variable name is always case-sensitive here, callers are responsible to format the
 * variable name w.r.t. case-sensitive config.
 */
class TempVariableManager extends DataTypeErrorsBase {

  @GuardedBy("this")
  private val variables = new mutable.HashMap[String, VariableDefinition]

  def create(
      name: String,
      defaultValueSQL: String,
      initValue: Literal,
      overrideIfExists: Boolean): Unit = synchronized {
    if (!overrideIfExists && variables.contains(name)) {
      throw new AnalysisException(
        errorClass = "VARIABLE_ALREADY_EXISTS",
        messageParameters = Map(
          "variableName" -> toSQLId(Seq(SYSTEM_CATALOG_NAME, SESSION_NAMESPACE, name))))
    }
    variables.put(name, VariableDefinition(defaultValueSQL, initValue))
  }

  def get(name: String): Option[VariableDefinition] = synchronized {
    variables.get(name)
  }

  def remove(name: String): Boolean = synchronized {
    variables.remove(name).isDefined
  }

  def clear(): Unit = synchronized {
    variables.clear()
  }

  def isEmpty: Boolean = synchronized {
    variables.isEmpty
  }
}

case class VariableDefinition(defaultValueSQL: String, currentValue: Literal)
