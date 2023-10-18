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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.types.DataType

/**
 * A resolved SQL variable, which contains all the information of it.
 */
case class VariableReference(
    // When creating a temp view, we need to record all the temp SQL variables that are referenced
    // by the temp view, so that they cal still be resolved as temp variables when reading the view
    // again. Here we store the original name parts of the variables, as temp view keeps the
    // original SQL string of the view query.
    originalNameParts: Seq[String],
    catalog: CatalogPlugin,
    identifier: Identifier,
    varDef: VariableDefinition,
    // This flag will be false if the `VariableReference` is used to manage the variable, like
    // setting a new value, where we shouldn't constant-fold it.
    canFold: Boolean = true)
  extends LeafExpression {

  override def dataType: DataType = varDef.currentValue.dataType
  override def nullable: Boolean = varDef.currentValue.nullable
  override def foldable: Boolean = canFold

  override def toString: String = {
    val qualifiedName = (catalog.name +: identifier.namespace :+ identifier.name).map(quoteIfNeeded)
    s"$prettyName(${qualifiedName.mkString(".")}=${varDef.currentValue.sql})"
  }

  override def sql: String = toString

  // Delegate to the underlying `Literal` for actual execution.
  override def eval(input: InternalRow): Any = varDef.currentValue.value
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    varDef.currentValue.doGenCode(ctx, ev)
  }
}
