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

import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
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

  override def evalMode: EvalMode.Value = EvalMode.LEGACY

  override val evalContext: NumericEvalContext = NumericEvalContext(evalMode)

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
    buildCanonicalizedPlan(
      { case BitwiseAnd(l, r) => Seq(l, r) },
      { case (l: Expression, r: Expression) => BitwiseAnd(l, r)}
    )
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

  override def evalMode: EvalMode.Value = EvalMode.LEGACY

  override val evalContext: NumericEvalContext = NumericEvalContext(evalMode)

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
    buildCanonicalizedPlan(
      { case BitwiseOr(l, r) => Seq(l, r) },
      { case (l: Expression, r: Expression) => BitwiseOr(l, r)}
    )
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

  override def evalMode: EvalMode.Value = EvalMode.LEGACY

  override val evalContext: NumericEvalContext = NumericEvalContext(evalMode)

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
    buildCanonicalizedPlan(
      { case BitwiseXor(l, r) => Seq(l, r) },
      { case (l: Expression, r: Expression) => BitwiseXor(l, r)}
    )
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
  extends UnaryExpression with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true
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
  usage = "_FUNC_(expr[, bits]) - Returns the number of bits that are set in the argument expr." +
    " If bits is specified, treats expr as a bits-bit signed integer in 2's complement." +
    " If bits is not specified, uses the natural bit width of the input type.",
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0
      > SELECT _FUNC_(9, 64);
       2
      > SELECT _FUNC_(-7, 8);
       6
      > SELECT _FUNC_(-7, 64);
       62
  """,
  since = "3.0.0",
  group = "bitwise_funcs")
object BitCountExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    expressions.length match {
      case 1 => new BitwiseCount(expressions.head)
      case 2 =>
        val bitsExpr = expressions(1)
        if (!bitsExpr.foldable || bitsExpr.dataType != IntegerType) {
          throw QueryCompilationErrors.nonFoldableArgumentError(funcName, "bits", IntegerType)
        }
        val bitsVal = bitsExpr.eval()
        if (bitsVal == null) {
          throw QueryCompilationErrors.nonFoldableArgumentError(funcName, "bits", IntegerType)
        }
        val bits = bitsVal.asInstanceOf[Int]
        if (bits < 1 || bits > 64) {
          throw QueryCompilationErrors.bitsRangeError(funcName, bits)
        }
        BitwiseCount(expressions.head, bits)
      case n =>
        throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1, 2), n)
    }
  }
}

case class BitwiseCount(child: Expression, bits: Int)
  extends UnaryExpression with ExpectsInputTypes {
  require(bits >= 1 && bits <= 64, s"bits must be in [1, 64], got $bits")

  def this(child: Expression) = this(child, child.dataType match {
    case BooleanType => 1
    case ByteType => 8
    case ShortType => 16
    case IntegerType => 32
    case _ => 64 // LongType
  })

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegralType, BooleanType))

  override def dataType: DataType = IntegerType

  private lazy val isNaturalWidth: Boolean = bits == (child.dataType match {
    case BooleanType => 1
    case ByteType => 8
    case ShortType => 16
    case IntegerType => 32
    case _ => 64
  })

  override def toString: String =
    if (isNaturalWidth) s"bit_count($child)" else s"bit_count($child, $bits)"

  override def prettyName: String = "bit_count"

  override def sql: String =
    if (isNaturalWidth) s"${prettyName}(${child.sql})"
    else s"${prettyName}(${child.sql}, $bits)"

  private lazy val needsRangeCheck: Boolean = !isNaturalWidth

  private def checkRange(longVal: Long): Unit = {
    if (needsRangeCheck) {
      val lower = -(1L << (bits - 1))
      val upper = (1L << (bits - 1)) - 1
      if (longVal < lower || longVal > upper) {
        throw QueryExecutionErrors.bitsValueOutOfRangeError("bit_count", longVal, bits)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val maskStr = if (bits == 64) "-1L" else s"((1L << $bits) - 1)"
    if (needsRangeCheck) {
      val lower = -(1L << (bits - 1))
      val upper = (1L << (bits - 1)) - 1
      val checkClass =
        "org.apache.spark.sql.errors.QueryExecutionErrors"
      child.dataType match {
        case BooleanType => defineCodeGen(ctx, ev, c =>
          s"""(int) java.lang.Long.bitCount(($c ? 1L : 0L) & $maskStr)""")
        case ByteType | ShortType | IntegerType => nullSafeCodeGen(ctx, ev, c =>
          s"""
             |long ${ev.value}_long = (long) $c;
             |if (${ev.value}_long < ${lower}L || ${ev.value}_long > ${upper}L) {
             |  throw (RuntimeException) $checkClass.bitsValueOutOfRangeError(
             |    "bit_count", ${ev.value}_long, $bits);
             |}
             |${ev.value} = (int) java.lang.Long.bitCount(${ev.value}_long & $maskStr);
           """.stripMargin)
        case LongType => nullSafeCodeGen(ctx, ev, c =>
          s"""
             |if ($c < ${lower}L || $c > ${upper}L) {
             |  throw (RuntimeException) $checkClass.bitsValueOutOfRangeError(
             |    "bit_count", $c, $bits);
             |}
             |${ev.value} = (int) java.lang.Long.bitCount($c & $maskStr);
           """.stripMargin)
      }
    } else {
      child.dataType match {
        case BooleanType => defineCodeGen(ctx, ev,
          c => s"(int) java.lang.Long.bitCount(($c ? 1L : 0L) & $maskStr)")
        case ByteType | ShortType | IntegerType => defineCodeGen(ctx, ev,
          c => s"(int) java.lang.Long.bitCount(((long) $c) & $maskStr)")
        case LongType => defineCodeGen(ctx, ev,
          c => s"(int) java.lang.Long.bitCount($c & $maskStr)")
      }
    }
  }

  protected override def nullSafeEval(input: Any): Any = {
    val mask = if (bits == 64) -1L else (1L << bits) - 1
    val longVal = child.dataType match {
      case BooleanType => if (input.asInstanceOf[Boolean]) 1L else 0L
      case ByteType => input.asInstanceOf[Byte].toLong
      case ShortType => input.asInstanceOf[Short].toLong
      case IntegerType => input.asInstanceOf[Int].toLong
      case LongType => input.asInstanceOf[Long]
    }
    checkRange(longVal)
    java.lang.Long.bitCount(longVal & mask).toInt
  }

  override protected def withNewChildInternal(newChild: Expression): BitwiseCount =
    copy(child = newChild)
}

object BitwiseGetUtil {
  def checkPosition(funcName: String, pos: Int, size: Int): Unit = {
    if (pos < 0 || pos >= size) {
      throw QueryExecutionErrors.bitPositionRangeError(funcName, pos, size)
    }
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
  extends BinaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

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
    BitwiseGetUtil.checkPosition(prettyName, posInt, bitSize)
    ((target.asInstanceOf[Number].longValue() >> posInt) & 1).toByte
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (target, pos) => {
      s"""
         |org.apache.spark.sql.catalyst.expressions.BitwiseGetUtil.checkPosition(
         |  "$prettyName", $pos, $bitSize);
         |${ev.value} = (byte) ((((long) $target) >> $pos) & 1);
       """.stripMargin
    })
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("bit_get")

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BitwiseGet = copy(left = newLeft, right = newRight)
}
