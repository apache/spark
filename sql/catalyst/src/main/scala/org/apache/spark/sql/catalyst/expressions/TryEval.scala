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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.DataType

case class TryEval(child: Expression) extends UnaryExpression with NullIntolerant {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ev.copy(code = code"""
      boolean ${ev.isNull} = true;
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      try {
        ${childGen.code}
        ${ev.isNull} = ${childGen.isNull};
        ${ev.value} = ${childGen.value};
      } catch (Exception e) {
      }"""
    )
  }

  override def eval(input: InternalRow): Any =
    try {
      child.eval(input)
    } catch {
      case _: Exception =>
        null
    }

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr1`+`expr2` and the result is null on overflow.",
  examples = """
    Examples:
      > SELECT _FUNC_(1, 2);
       3
      > SELECT _FUNC_(2147483647, 1);
       NULL
  """,
  since = "3.2.0",
  group = "math_funcs")
case class TryAdd(left: Expression, right: Expression, child: Expression)
    extends RuntimeReplaceable {
  def this(left: Expression, right: Expression) =
    this(left, right, TryEval(Add(left, right, failOnError = true)))

  override def flatArguments: Iterator[Any] = Iterator(left, right)

  override def exprsReplaced: Seq[Expression] = Seq(left, right)

  override def prettyName: String = "try_add"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr1`/`expr2`. It always performs floating point division. Its result is always null if `expr2` is 0.",
  examples = """
    Examples:
      > SELECT _FUNC_(3, 2);
       1.5
      > SELECT _FUNC_(2L, 2L);
       1.0
      > SELECT _FUNC_(1, 0);
       NULL
  """,
  since = "3.2.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryDivide(left: Expression, right: Expression, child: Expression)
    extends RuntimeReplaceable {
  def this(left: Expression, right: Expression) =
    this(left, right, TryEval(Divide(left, right, failOnError = true)))

  override def flatArguments: Iterator[Any] = Iterator(left, right)

  override def exprsReplaced: Seq[Expression] = Seq(left, right)

  override def prettyName: String = "try_divide"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(child = newChild)
}
