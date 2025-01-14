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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.catalog.SQLFunction
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION, SQL_TABLE_FUNCTION, TreePattern}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A container for holding a SQL function query plan and its function identifier.
 *
 * @param function: the SQL function that this node represents.
 * @param child: the SQL function body.
 */
case class SQLFunctionNode(
    function: SQLFunction,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def stringArgs: Iterator[Any] = Iterator(function.name, child)
  override protected def withNewChildInternal(newChild: LogicalPlan): SQLFunctionNode =
    copy(child = newChild)

  // Throw a reasonable error message when trying to call a SQL UDF with TABLE argument(s).
  if (child.containsPattern(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION)) {
    throw QueryCompilationErrors
      .tableValuedArgumentsNotYetImplementedForSqlFunctions("call", toSQLId(function.name.funcName))
  }
}

/**
 * Represent a SQL table function plan resolved from the catalog SQL table function builder.
 */
case class SQLTableFunction(
    name: String,
    function: SQLFunction,
    inputs: Seq[Expression],
    override val output: Seq[Attribute]) extends LeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(SQL_TABLE_FUNCTION)

  // Throw a reasonable error message when trying to call a SQL UDF with TABLE argument(s) because
  // this functionality is not implemented yet.
  if (inputs.exists(_.containsPattern(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION))) {
    throw QueryCompilationErrors
      .tableValuedArgumentsNotYetImplementedForSqlFunctions("call", toSQLId(name))
  }
}
