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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETER, TreePattern}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * The expression represents a named parameter that should be bound later
 * to a literal with concrete value and type.
 *
 * @param name The identifier of the parameter without the marker '@'.
 */
case class NamedParameter(name: String) extends LeafExpression {
  override def dataType: DataType = NullType
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(PARAMETER)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw SparkException.internalError(s"Found the unbound parameter: $name.")
  }

  def eval(input: InternalRow): Any = {
    throw SparkException.internalError(s"Found the unbound parameter: $name.")
  }
}
