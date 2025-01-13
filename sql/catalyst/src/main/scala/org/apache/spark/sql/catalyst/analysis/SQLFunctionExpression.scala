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
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.TreePattern.{SQL_FUNCTION_EXPRESSION, TreePattern}
import org.apache.spark.sql.types.DataType

/**
 * Represent a SQL function expression resolved from the catalog SQL function builder.
 */
case class SQLFunctionExpression(
    name: String,
    function: SQLFunction,
    inputs: Seq[Expression],
    returnType: Option[DataType]) extends Expression with Unevaluable {
  override def children: Seq[Expression] = inputs
  override def dataType: DataType = returnType.get
  override def nullable: Boolean = true
  override def prettyName: String = name
  override def toString: String = s"$name(${children.mkString(", ")})"
  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): SQLFunctionExpression = copy(inputs = newChildren)
  final override val nodePatterns: Seq[TreePattern] = Seq(SQL_FUNCTION_EXPRESSION)
}
