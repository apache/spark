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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


/**
 * An expression that is evaluated to the first non-null input.
 *
 * {{{
 *   coalesce(1, 2) => 1
 *   coalesce(null, 1, 2) => 1
 *   coalesce(null, null, 2) => 2
 *   coalesce(null, null, null) => null
 * }}}
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, ...) - Returns the first non-null argument if exists. Otherwise, null.",
  extended = """
    Examples:
      > SELECT _FUNC_(NULL, 1, NULL);
       1
  """)
// scalastyle:on line.size.limit
case class Coalesce(children: Seq[Expression]) extends Expression {

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  override def nullable: Boolean = children.forall(_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children == Nil) {
      TypeCheckResult.TypeCheckFailure("input to function coalesce cannot be empty")
    } else {
      TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function coalesce")
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    var result: Any = null
    val childIterator = children.iterator
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val first = children(0)
    val rest = children.drop(1)
    val firstEval = first.genCode(ctx)
    ev.copy(code = s"""
      ${firstEval.code}
      boolean ${ev.isNull} = ${firstEval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${firstEval.value};""" +
      rest.map { e =>
      val eval = e.genCode(ctx)
      s"""
        if (${ev.isNull}) {
          ${eval.code}
          if (!${eval.isNull}) {
            ${ev.isNull} = false;
            ${ev.value} = ${eval.value};
          }
        }
      """
    }.mkString("\n"))
  }
}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr2` if `expr1` is null, or `expr1` otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(NULL, array('2'));
       ["2"]
  """)
case class IfNull(left: Expression, right: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, right: Expression) = {
    this(left, right, Coalesce(Seq(left, right)))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns null if `expr1` equals to `expr2`, or `expr1` otherwise.",
  extended = """
   Examples:
     > SELECT _FUNC_(2, 2);
      NULL
  """)
case class NullIf(left: Expression, right: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, right: Expression) = {
    this(left, right, If(EqualTo(left, right), Literal.create(null, left.dataType), left))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr2` if `expr1` is null, or `expr1` otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(NULL, array('2'));
       ["2"]
  """)
case class Nvl(left: Expression, right: Expression, child: Expression) extends RuntimeReplaceable {

  def this(left: Expression, right: Expression) = {
    this(left, right, Coalesce(Seq(left, right)))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3) - Returns `expr2` if `expr1` is not null, or `expr3` otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(NULL, 2, 1);
       1
  """)
// scalastyle:on line.size.limit
case class Nvl2(expr1: Expression, expr2: Expression, expr3: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(expr1: Expression, expr2: Expression, expr3: Expression) = {
    this(expr1, expr2, expr3, If(IsNotNull(expr1), expr2, expr3))
  }

  override def flatArguments: Iterator[Any] = Iterator(expr1, expr2, expr3)
  override def sql: String = s"$prettyName(${expr1.sql}, ${expr2.sql}, ${expr3.sql})"
}


/**
 * Evaluates to `true` iff it's NaN.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if `expr` is NaN, or false otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(cast('NaN' as double));
       true
  """)
case class IsNaN(child: Expression) extends UnaryExpression
  with Predicate with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      false
    } else {
      child.dataType match {
        case DoubleType => value.asInstanceOf[Double].isNaN
        case FloatType => value.asInstanceOf[Float].isNaN
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    child.dataType match {
      case DoubleType | FloatType =>
        ev.copy(code = s"""
          ${eval.code}
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          ${ev.value} = !${eval.isNull} && Double.isNaN(${eval.value});""", isNull = "false")
    }
  }
}

/**
 * An Expression evaluates to `left` iff it's not NaN, or evaluates to `right` otherwise.
 * This Expression is useful for mapping NaN values to null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns `expr1` if it's not NaN, or `expr2` otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(cast('NaN' as double), 123);
       123.0
  """)
case class NaNvl(left: Expression, right: Expression)
    extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, FloatType), TypeCollection(DoubleType, FloatType))

  override def eval(input: InternalRow): Any = {
    val value = left.eval(input)
    if (value == null) {
      null
    } else {
      left.dataType match {
        case DoubleType =>
          if (!value.asInstanceOf[Double].isNaN) value else right.eval(input)
        case FloatType =>
          if (!value.asInstanceOf[Float].isNaN) value else right.eval(input)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    left.dataType match {
      case DoubleType | FloatType =>
        ev.copy(code = s"""
          ${leftGen.code}
          boolean ${ev.isNull} = false;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (${leftGen.isNull}) {
            ${ev.isNull} = true;
          } else {
            if (!Double.isNaN(${leftGen.value})) {
              ${ev.value} = ${leftGen.value};
            } else {
              ${rightGen.code}
              if (${rightGen.isNull}) {
                ${ev.isNull} = true;
              } else {
                ${ev.value} = ${rightGen.value};
              }
            }
          }""")
    }
  }
}


/**
 * An expression that is evaluated to true if the input is null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if `expr` is null, or false otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(1);
       false
  """)
case class IsNull(child: Expression) extends UnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    child.eval(input) == null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(code = eval.code, isNull = "false", value = eval.isNull)
  }

  override def sql: String = s"(${child.sql} IS NULL)"
}


/**
 * An expression that is evaluated to true if the input is not null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if `expr` is not null, or false otherwise.",
  extended = """
    Examples:
      > SELECT _FUNC_(1);
       true
  """)
case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    child.eval(input) != null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(code = eval.code, isNull = "false", value = s"(!(${eval.isNull}))")
  }

  override def sql: String = s"(${child.sql} IS NOT NULL)"
}


/**
 * A predicate that is evaluated to be true if there are at least `n` non-null and non-NaN values.
 */
case class AtLeastNNonNulls(n: Int, children: Seq[Expression]) extends Predicate {
  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.foldable)
  override def toString: String = s"AtLeastNNulls(n, ${children.mkString(",")})"

  private[this] val childrenArray = children.toArray

  override def eval(input: InternalRow): Boolean = {
    var numNonNulls = 0
    var i = 0
    while (i < childrenArray.length && numNonNulls < n) {
      val evalC = childrenArray(i).eval(input)
      if (evalC != null) {
        childrenArray(i).dataType match {
          case DoubleType =>
            if (!evalC.asInstanceOf[Double].isNaN) numNonNulls += 1
          case FloatType =>
            if (!evalC.asInstanceOf[Float].isNaN) numNonNulls += 1
          case _ => numNonNulls += 1
        }
      }
      i += 1
    }
    numNonNulls >= n
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val nonnull = ctx.freshName("nonnull")
    val code = children.map { e =>
      val eval = e.genCode(ctx)
      e.dataType match {
        case DoubleType | FloatType =>
          s"""
            if ($nonnull < $n) {
              ${eval.code}
              if (!${eval.isNull} && !Double.isNaN(${eval.value})) {
                $nonnull += 1;
              }
            }
          """
        case _ =>
          s"""
            if ($nonnull < $n) {
              ${eval.code}
              if (!${eval.isNull}) {
                $nonnull += 1;
              }
            }
          """
      }
    }.mkString("\n")
    ev.copy(code = s"""
      int $nonnull = 0;
      $code
      boolean ${ev.value} = $nonnull >= $n;""", isNull = "false")
  }
}
