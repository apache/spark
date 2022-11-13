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

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._


/**
 * A function that calculates bitwise and(&) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise AND of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       1
  """,
  since = "1.4.0",
  group = "bitwise_funcs")
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic
  with CommutativeExpression {

  protected override val evalMode: EvalMode.Value = EvalMode.LEGACY

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "&"

  private lazy val and: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = and(input1, input2)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BitwiseAnd = copy(left = newLeft, right = newRight)

  override lazy val canonicalized: Expression = {
    orderCommutative({ case BitwiseAnd(l, r) => Seq(l, r) }).reduce(BitwiseAnd)
  }
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise OR of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       7
  """,
  since = "1.4.0",
  group = "bitwise_funcs")
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic
  with CommutativeExpression {

  protected override val evalMode: EvalMode.Value = EvalMode.LEGACY

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "|"

  private lazy val or: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = or(input1, input2)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BitwiseOr = copy(left = newLeft, right = newRight)

  override lazy val canonicalized: Expression = {
    orderCommutative({ case BitwiseOr(l, r) => Seq(l, r) }).reduce(BitwiseOr)
  }
}

/**
 * A function that calculates bitwise xor({@literal ^}) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise exclusive OR of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       6
  """,
  since = "1.4.0",
  group = "bitwise_funcs")
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic
  with CommutativeExpression {

  protected override val evalMode: EvalMode.Value = EvalMode.LEGACY

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "^"

  private lazy val xor: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = xor(input1, input2)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BitwiseXor = copy(left = newLeft, right = newRight)

  override lazy val canonicalized: Expression = {
    orderCommutative({ case BitwiseXor(l, r) => Seq(l, r) }).reduce(BitwiseXor)
  }
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
@ExpressionDescription(
  usage = "_FUNC_ expr - Returns the result of bitwise NOT of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_ 0;
       -1
  """,
  since = "1.4.0",
  group = "bitwise_funcs")
case class BitwiseNot(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def dataType: DataType = child.dataType

  override def toString: String = s"~$child"

  private lazy val not: (Any) => Any = dataType match {
    case ByteType =>
      ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
    case ShortType =>
      ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
    case IntegerType =>
      ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
    case LongType =>
      ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"(${CodeGenerator.javaType(dataType)}) ~($c)")
  }

  protected override def nullSafeEval(input: Any): Any = not(input)

  override def sql: String = s"~${child.sql}"

  override protected def withNewChildInternal(newChild: Expression): BitwiseNot =
    copy(child = newChild)
}


@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the number of bits that are set in the argument expr as an" +
    " unsigned 64-bit integer, or NULL if the argument is NULL.",
  examples = """
    Examples:
      > SELECT _FUNC_(2);
       1
      > SELECT _FUNC_(2, 0);
       31
      > SELECT _FUNC_(5, 1);
       2
      > SELECT _FUNC_(5, 0);
       30
  """,
  since = "3.0.0",
  group = "bitwise_funcs")
case class BitwiseCount(child: Expression, bitExpr: Expression)
    extends BinaryExpression
    with ExpectsInputTypes
    with QueryErrorsBase
    with NullIntolerant {

  def this(child: Expression) = this(child, Literal(1))

  lazy val bitSize = BitwiseUtil.getBitSize(left.dataType)

  lazy val bitInt = bitExpr.eval()

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegralType, BooleanType), IntegerType)

  override def dataType: DataType = IntegerType

  override def toString: String = s"bit_count($child)"

  override def prettyName: String = "bit_count"

  override def left: Expression = child

  override def right: Expression = bitExpr

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheckResult = super.checkInputDataTypes()
    if (defaultCheckResult.isSuccess) {
      if (!bitExpr.foldable) {
        DataTypeMismatch(
          errorSubClass = "NON_FOLDABLE_INPUT",
          messageParameters = Map(
            "inputName" -> "bitExpr",
            "inputType" -> toSQLType(bitExpr.dataType),
            "inputExpr" -> toSQLExpr(bitExpr)))
      } else {
        val bit = bitExpr.eval()
        if (bit == null) {
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_NULL",
            messageParameters = Map("exprName" -> "bitExpr"))
        } else {
          val bitValue = bit.asInstanceOf[Int]
          // either 1 or 0 are supported
          if (bitValue < 0 || bitValue > 1) {
            DataTypeMismatch(errorSubClass = "INVALID_BIT_VALUE")
          } else {
            defaultCheckResult
          }
        }
      }
    } else {
      defaultCheckResult
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): BitwiseCount = copy(child = newLeft, bitExpr = newRight)

  /**
   * Called by default [[eval]] implementation. If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code. If we need full control
   * of evaluation process, we should override [[eval]].
   */
  override protected def nullSafeEval(input: Any, bit: Any): Any = {
    val bitCount = child.dataType match {
      case BooleanType => if (input.asInstanceOf[Boolean]) 1 else 0
      case ByteType => java.lang.Long.bitCount(input.asInstanceOf[Byte])
      case ShortType => java.lang.Long.bitCount(input.asInstanceOf[Short])
      case IntegerType => java.lang.Long.bitCount(input.asInstanceOf[Int])
      case LongType => java.lang.Long.bitCount(input.asInstanceOf[Long])
    }
    if (bit.asInstanceOf[Int] == 1) bitCount else bitSize - bitCount
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression. The default
   * behavior is to call the eval method of the expression. Concrete expression implementations
   * should override this to do actual code generation.
   *
   * @param ctx
   *   a [[CodegenContext]]
   * @param ev
   *   an [[ExprCode]] with unique terms.
   * @return
   *   an [[ExprCode]] containing the Java source code to generate the given expression
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (input, bit) => {
        child.dataType match {
          case BooleanType => s"if ($input) 1 else 0"
          case _ => s"""if ($bit == 1) {
            ${ev.value} = java.lang.Long.bitCount($input);
          } else {
            ${ev.value} = $bitSize - java.lang.Long.bitCount($input);
          }"""
        }
      })
  }
}

object BitwiseUtil {
  def checkPosition(pos: Int, size: Int): Unit = {
    if (pos < 0) {
      throw new IllegalArgumentException(s"Invalid bit position: $pos is less than zero")
    } else if (size <= pos) {
      throw new IllegalArgumentException(
        s"Invalid bit position: $pos exceeds the bit upper limit")
    }
  }

  def getBitSize(dataType: DataType): Int = dataType match {
    case BooleanType => 1
    case ByteType => java.lang.Byte.SIZE
    case ShortType => java.lang.Short.SIZE
    case IntegerType => java.lang.Integer.SIZE
    case LongType => java.lang.Long.SIZE
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr, pos) - Returns the value of the bit (0 or 1) at the specified position.
      The positions are numbered from right to left, starting at zero.
      The position argument cannot be negative.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(11, 0);
       1
      > SELECT _FUNC_(11, 2);
       0
  """,
  since = "3.2.0",
  group = "bitwise_funcs")
case class BitwiseGet(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType, IntegerType)

  override def dataType: DataType = ByteType

  lazy val bitSize = left.dataType match {
    case ByteType => java.lang.Byte.SIZE
    case ShortType => java.lang.Short.SIZE
    case IntegerType => java.lang.Integer.SIZE
    case LongType => java.lang.Long.SIZE
  }

  override def nullSafeEval(target: Any, pos: Any): Any = {
    val posInt = pos.asInstanceOf[Int]
    BitwiseUtil.checkPosition(posInt, bitSize)
    ((target.asInstanceOf[Number].longValue() >> posInt) & 1).toByte
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (target, pos) => {
      s"""
         |org.apache.spark.sql.catalyst.expressions.BitwiseUtil.checkPosition($pos, $bitSize);
         |${ev.value} = (byte) ((((long) $target) >> $pos) & 1);
       """.stripMargin
    })
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("bit_get")

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BitwiseGet = copy(left = newLeft, right = newRight)
}
