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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.FunctionCatalog
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing functions.
 */
case class ShowFunctionsExec(
    output: Seq[Attribute],
    catalog: FunctionCatalog,
    namespace: Seq[String],
    userScope: Boolean,
    systemScope: Boolean,
    pattern: Option[String]) extends V2CommandExec with LeafExecNode {

  private def applyPattern(names: Seq[String]): Seq[String] = {
    StringUtils.filterPattern(names, pattern.getOrElse("*"))
  }

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    val systemFunctions = if (systemScope) {
      // All built-in functions, and operators such as "<>", "||"
      val builtinFunctions = FunctionRegistry.functionSet ++ TableFunctionRegistry.functionSet
      applyPattern(builtinFunctions.map(_.unquotedString).toSeq ++
        FunctionRegistry.builtinOperators.keys.toSeq)
    } else Seq.empty
    val userFunctions = if (userScope) {
      // List all temporary functions in the session catalog
      applyPattern(session.sessionState.catalog.listTemporaryFunctions().map(_.unquotedString)) ++
        // List all functions registered in the given namespace of the catalog
        applyPattern(catalog.listFunctions(namespace.toArray).map(_.name())).map { funcName =>
          (catalog.name() +: namespace :+ funcName).quoted
        }
    } else Seq.empty
    (userFunctions ++ systemFunctions).distinct.sorted.foreach { fn =>
      rows += toCatalystRow(fn)
    }

    rows.toSeq
  }
}
