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
import org.apache.spark.sql.types.{DataType, NumericType}

case class TryEval(child: Expression) extends UnaryExpression {
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
  override def nullIntolerant: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the sum of `expr1`and `expr2` and the result is null on overflow. " +
    "The acceptable input types are the same with the `+` operator.",
  examples = """
    Examples:
      > SELECT _FUNC_(1, 2);
       3
      > SELECT _FUNC_(2147483647, 1);
       NULL
      > SELECT _FUNC_(date'2021-01-01', 1);
       2021-01-02
      > SELECT _FUNC_(date'2021-01-01', interval 1 year);
       2022-01-01
      > SELECT _FUNC_(timestamp'2021-01-01 00:00:00', interval 1 day);
       2021-01-02 00:00:00
      > SELECT _FUNC_(interval 1 year, interval 2 year);
       3-0
  """,
  since = "3.2.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryAdd(left: Expression, right: Expression, replacement: Expression)
    extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) = this(left, right,
    (left.dataType, right.dataType) match {
      case (_: NumericType, _: NumericType) => Add(left, right, EvalMode.TRY)
      // TODO: support TRY eval mode on datetime arithmetic expressions.
      case _ => TryEval(Add(left, right, EvalMode.ANSI))
    }
  )

  override def prettyName: String = "try_add"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(dividend, divisor) - Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `expr2` is 0. " +
    "`dividend` must be a numeric or an interval. `divisor` must be a numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_(3, 2);
       1.5
      > SELECT _FUNC_(2L, 2L);
       1.0
      > SELECT _FUNC_(1, 0);
       NULL
      > SELECT _FUNC_(interval 2 month, 2);
       0-1
      > SELECT _FUNC_(interval 2 month, 0);
       NULL
  """,
  since = "3.2.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryDivide(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) = this(left, right,
    (left.dataType, right.dataType) match {
      case (_: NumericType, _: NumericType) => Divide(left, right, EvalMode.TRY)
      // TODO: support TRY eval mode on datetime arithmetic expressions.
      case _ => TryEval(Divide(left, right, EvalMode.ANSI))
    }
  )

  override def prettyName: String = "try_divide"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(dividend, divisor) - Returns the remainder after `expr1`/`expr2`. " +
    "`dividend` must be a numeric. `divisor` must be a numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_(3, 2);
       1
      > SELECT _FUNC_(2L, 2L);
       0
      > SELECT _FUNC_(3.0, 2.0);
       1.0
      > SELECT _FUNC_(1, 0);
       NULL
  """,
  since = "4.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryMod(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) = this(left, right,
    (left.dataType, right.dataType) match {
      case (_: NumericType, _: NumericType) => Remainder(left, right, EvalMode.TRY)
      // TODO: support TRY eval mode on datetime arithmetic expressions.
      case _ => TryEval(Remainder(left, right, EvalMode.ANSI))
    }
  )

  override def prettyName: String = "try_mod"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr1`-`expr2` and the result is null on overflow. " +
    "The acceptable input types are the same with the `-` operator.",
  examples = """
    Examples:
      > SELECT _FUNC_(2, 1);
       1
      > SELECT _FUNC_(-2147483648, 1);
       NULL
      > SELECT _FUNC_(date'2021-01-02', 1);
       2021-01-01
      > SELECT _FUNC_(date'2021-01-01', interval 1 year);
       2020-01-01
      > SELECT _FUNC_(timestamp'2021-01-02 00:00:00', interval 1 day);
       2021-01-01 00:00:00
      > SELECT _FUNC_(interval 2 year, interval 1 year);
       1-0
  """,
  since = "3.3.0",
  group = "math_funcs")
case class TrySubtract(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) = this(left, right,
    (left.dataType, right.dataType) match {
      case (_: NumericType, _: NumericType) => Subtract(left, right, EvalMode.TRY)
      // TODO: support TRY eval mode on datetime arithmetic expressions.
      case _ => TryEval(Subtract(left, right, EvalMode.ANSI))
    }
  )

  override def prettyName: String = "try_subtract"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr1`*`expr2` and the result is null on overflow. " +
    "The acceptable input types are the same with the `*` operator.",
  examples = """
    Examples:
      > SELECT _FUNC_(2, 3);
       6
      > SELECT _FUNC_(-2147483648, 10);
       NULL
      > SELECT _FUNC_(interval 2 year, 3);
       6-0
  """,
  since = "3.3.0",
  group = "math_funcs")
case class TryMultiply(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) = this(left, right,
    (left.dataType, right.dataType) match {
      case (_: NumericType, _: NumericType) => Multiply(left, right, EvalMode.TRY)
      // TODO: support TRY eval mode on datetime arithmetic expressions.
      case _ => TryEval(Multiply(left, right, EvalMode.ANSI))
    }
  )

  override def prettyName: String = "try_multiply"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, fmt]) - This is a special version of `to_binary` that performs the same operation, but returns a NULL value instead of raising an error if the conversion cannot be performed.",
  examples = """
    Examples:
      > SELECT _FUNC_('abc', 'utf-8');
       abc
      > select _FUNC_('a!', 'base64');
       NULL
      > select _FUNC_('abc', 'invalidFormat');
       NULL
  """,
  since = "3.3.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class TryToBinary(
    expr: Expression,
    format: Option[Expression],
    replacement: Expression) extends RuntimeReplaceable
  with InheritAnalysisRules {
  def this(expr: Expression) =
    this(expr, None, TryEval(ToBinary(expr, None, nullOnInvalidFormat = true)))

  def this(expr: Expression, formatExpression: Expression) =
    this(expr, Some(formatExpression),
      TryEval(ToBinary(expr, Some(formatExpression), nullOnInvalidFormat = true)))

  override def prettyName: String = "try_to_binary"

  override def parameters: Seq[Expression] = expr +: format.toSeq

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(class, method[, arg1[, arg2 ..]]) - This is a special version of `reflect` that" +
    " performs the same operation, but returns a NULL value instead of raising an error if the invoke method thrown exception.",
  examples = """
    Examples:
      > SELECT _FUNC_('java.util.UUID', 'randomUUID');
       c33fb387-8500-4bfa-81d2-6e0e3e930df2
      > SELECT _FUNC_('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2');
       a5cf6c42-0c85-418f-af6c-3e4e5b1328f2
      > SELECT _FUNC_('java.net.URLDecoder', 'decode', '%');
       NULL
  """,
  since = "4.0.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class TryReflect(params: Seq[Expression], replacement: Expression) extends RuntimeReplaceable
  with InheritAnalysisRules {

  def this(params: Seq[Expression]) = this(params,
    CallMethodViaReflection(params, failOnError = false))

  override def prettyName: String = "try_reflect"

  override def parameters: Seq[Expression] = params

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }
}

