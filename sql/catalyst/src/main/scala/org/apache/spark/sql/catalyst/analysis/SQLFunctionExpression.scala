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
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.TreePattern.{SQL_FUNCTION_EXPRESSION, SQL_SCALAR_FUNCTION, TreePattern}
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

/**
 * A wrapper node for a SQL scalar function expression.
 */
case class SQLScalarFunction(function: SQLFunction, inputs: Seq[Expression], child: Expression)
    extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
  override def toString: String = s"${function.name}(${inputs.mkString(", ")})"
  override def sql: String = s"${function.name}(${inputs.map(_.sql).mkString(", ")})"
  override protected def withNewChildInternal(newChild: Expression): SQLScalarFunction = {
    copy(child = newChild)
  }
  final override val nodePatterns: Seq[TreePattern] = Seq(SQL_SCALAR_FUNCTION)
  // The `inputs` is for display only and does not matter in execution.
  override lazy val canonicalized: Expression = copy(inputs = Nil, child = child.canonicalized)
  override lazy val deterministic: Boolean = {
    function.deterministic.getOrElse(true) && children.forall(_.deterministic)
  }
}

/**
 * Provide a way to keep state during analysis for resolving nested SQL functions.
 *
 * @param nestedSQLFunctionDepth The nested depth in the SQL function resolution. A SQL function
 *                               expression should only be expanded as a [[SQLScalarFunction]] if
 *                               the nested depth is 0.
 */
case class SQLFunctionContext(nestedSQLFunctionDepth: Int = 0)

object SQLFunctionContext {

  private val value = new ThreadLocal[SQLFunctionContext]() {
    override def initialValue: SQLFunctionContext = SQLFunctionContext()
  }

  def get: SQLFunctionContext = value.get()

  def reset(): Unit = value.remove()

  private def set(context: SQLFunctionContext): Unit = value.set(context)

  def withSQLFunction[A](f: => A): A = {
    val originContext = value.get()
    val context = originContext.copy(
      nestedSQLFunctionDepth = originContext.nestedSQLFunctionDepth + 1)
    set(context)
    try f finally { set(originContext) }
  }
}
