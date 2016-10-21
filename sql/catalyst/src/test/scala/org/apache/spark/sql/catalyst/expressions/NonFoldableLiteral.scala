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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._


/**
 * A literal value that is not foldable. Used in expression codegen testing to test code path
 * that behave differently based on foldable values.
 */
case class NonFoldableLiteral(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def toString: String = if (value != null) value.toString else "null"

  override def eval(input: InternalRow): Any = value

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    Literal.create(value, dataType).doGenCode(ctx, ev)
  }
}


object NonFoldableLiteral {
  def apply(value: Any): NonFoldableLiteral = {
    val lit = Literal(value)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
  def create(value: Any, dataType: DataType): NonFoldableLiteral = {
    val lit = Literal.create(value, dataType)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
}
