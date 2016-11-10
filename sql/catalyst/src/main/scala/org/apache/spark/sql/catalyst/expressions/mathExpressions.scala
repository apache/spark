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

import java.{lang => jl}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckSuccess, TypeCheckFailure}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A leaf expression specifically for math constants. Math constants expect no input.
 *
 * There is no code generation because they should get constant folded by the optimizer.
 *
 * @param c The math constant.
 * @param name The short name of the function
 */
abstract class LeafMathExpression(c: Double, name: String)
  extends LeafExpression with CodegenFallback {

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def toString: String = s"$name()"

  override def eval(input: InternalRow): Any = c
}

/**
 * A unary expression specifically for math functions. Math Functions expect a specific type of
 * input format, therefore these functions extend `ExpectsInputTypes`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class UnaryMathExpression(val f: Double => Double, name: String)
  extends UnaryExpression with Serializable with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  protected override def nullSafeEval(input: Any): Any = {
    f(input.asInstanceOf[Double])
  }

  // name of function in java.lang.Math
  def funcName: String = name.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"java.lang.Math.${funcName}($c)")
  }
}

abstract class UnaryLogExpression(f: Double => Double, name: String)
    extends UnaryMathExpression(f, name) {

  // values less than or equal to yAsymptote eval to null in Hive, instead of NaN or -Infinity
  protected val yAsymptote: Double = 0.0

  protected override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Double]
    if (d <= yAsymptote) null else f(d)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, c =>
      s"""
        if ($c <= $yAsymptote) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = java.lang.Math.${funcName}($c);
        }
      """
    )
  }
}

/**
 * A binary expression specifically for math functions that take two `Double`s as input and returns
 * a `Double`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String)
  extends BinaryExpression with Serializable with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType)

  override def toString: String = s"$name($left, $right)"

  override def dataType: DataType = DoubleType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    f(input1.asInstanceOf[Double], input2.asInstanceOf[Double])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.${name.toLowerCase}($c1, $c2)")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Leaf math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Euler's number. Note that there is no code generation because this is only
 * evaluated by the optimizer during constant folding.
 */
case class EulerNumber() extends LeafMathExpression(math.E, "E")

/**
 * Pi. Note that there is no code generation because this is only
 * evaluated by the optimizer during constant folding.
 */
case class Pi() extends LeafMathExpression(math.Pi, "PI")

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Unary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

case class Acos(child: Expression) extends UnaryMathExpression(math.acos, "ACOS")

case class Asin(child: Expression) extends UnaryMathExpression(math.asin, "ASIN")

case class Atan(child: Expression) extends UnaryMathExpression(math.atan, "ATAN")

case class Cbrt(child: Expression) extends UnaryMathExpression(math.cbrt, "CBRT")

case class Ceil(child: Expression) extends UnaryMathExpression(math.ceil, "CEIL") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType))

  protected override def nullSafeEval(input: Any): Any = child.dataType match {
    case DoubleType => f(input.asInstanceOf[Double]).toLong
    case DecimalType.Fixed(precision, scale) => input.asInstanceOf[Decimal].ceil
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    child.dataType match {
      case DecimalType.Fixed(_, 0) => defineCodeGen(ctx, ev, c => s"$c")
      case DecimalType.Fixed(precision, scale) =>
        defineCodeGen(ctx, ev, c => s"$c.ceil()")
      case _ => defineCodeGen(ctx, ev, c => s"(long)(java.lang.Math.${funcName}($c))")
    }
  }
}

case class Cos(child: Expression) extends UnaryMathExpression(math.cos, "COS")

case class Cosh(child: Expression) extends UnaryMathExpression(math.cosh, "COSH")

/**
 * Convert a num from one base to another
 * @param numExpr the number to be converted
 * @param fromBaseExpr from which base
 * @param toBaseExpr to which base
 */
case class Conv(numExpr: Expression, fromBaseExpr: Expression, toBaseExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = Seq(numExpr, fromBaseExpr, toBaseExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType, IntegerType)
  override def dataType: DataType = StringType

  override def nullSafeEval(num: Any, fromBase: Any, toBase: Any): Any = {
    NumberConverter.convert(
      num.asInstanceOf[UTF8String].getBytes,
      fromBase.asInstanceOf[Int],
      toBase.asInstanceOf[Int])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val numconv = NumberConverter.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (num, from, to) =>
      s"""
       ${ev.value} = $numconv.convert($num.getBytes(), $from, $to);
       if (${ev.value} == null) {
         ${ev.isNull} = true;
       }
       """
    )
  }
}

case class Exp(child: Expression) extends UnaryMathExpression(math.exp, "EXP")

case class Expm1(child: Expression) extends UnaryMathExpression(math.expm1, "EXPM1")

case class Floor(child: Expression) extends UnaryMathExpression(math.floor, "FLOOR") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType))

  protected override def nullSafeEval(input: Any): Any = child.dataType match {
    case DoubleType => f(input.asInstanceOf[Double]).toLong
    case DecimalType.Fixed(precision, scale) => input.asInstanceOf[Decimal].floor
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    child.dataType match {
      case DecimalType.Fixed(_, 0) => defineCodeGen(ctx, ev, c => s"$c")
      case DecimalType.Fixed(precision, scale) =>
        defineCodeGen(ctx, ev, c => s"$c.floor()")
      case _ => defineCodeGen(ctx, ev, c => s"(long)(java.lang.Math.${funcName}($c))")
    }
  }
}

object Factorial {

  def factorial(n: Int): Long = {
    if (n < factorials.length) factorials(n) else Long.MaxValue
  }

  private val factorials: Array[Long] = Array[Long](
    1,
    1,
    2,
    6,
    24,
    120,
    720,
    5040,
    40320,
    362880,
    3628800,
    39916800,
    479001600,
    6227020800L,
    87178291200L,
    1307674368000L,
    20922789888000L,
    355687428096000L,
    6402373705728000L,
    121645100408832000L,
    2432902008176640000L
  )
}

case class Factorial(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def dataType: DataType = LongType

  // If the value not in the range of [0, 20], it still will be null, so set it to be true here.
  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any): Any = {
    val value = input.asInstanceOf[jl.Integer]
    if (value > 20 || value < 0) {
      null
    } else {
      Factorial.factorial(value)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        if ($eval > 20 || $eval < 0) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} =
            org.apache.spark.sql.catalyst.expressions.Factorial.factorial($eval);
        }
      """
    })
  }
}

case class Log(child: Expression) extends UnaryLogExpression(math.log, "LOG")

case class Log2(child: Expression)
  extends UnaryLogExpression((x: Double) => math.log(x) / math.log(2), "LOG2") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, c =>
      s"""
        if ($c <= $yAsymptote) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = java.lang.Math.log($c) / java.lang.Math.log(2);
        }
      """
    )
  }
}

case class Log10(child: Expression) extends UnaryLogExpression(math.log10, "LOG10")

case class Log1p(child: Expression) extends UnaryLogExpression(math.log1p, "LOG1P") {
  protected override val yAsymptote: Double = -1.0
}

case class Rint(child: Expression) extends UnaryMathExpression(math.rint, "ROUND") {
  override def funcName: String = "rint"
}

case class Signum(child: Expression) extends UnaryMathExpression(math.signum, "SIGNUM")

case class Sin(child: Expression) extends UnaryMathExpression(math.sin, "SIN")

case class Sinh(child: Expression) extends UnaryMathExpression(math.sinh, "SINH")

case class Sqrt(child: Expression) extends UnaryMathExpression(math.sqrt, "SQRT")

case class Tan(child: Expression) extends UnaryMathExpression(math.tan, "TAN")

case class Tanh(child: Expression) extends UnaryMathExpression(math.tanh, "TANH")

case class ToDegrees(child: Expression) extends UnaryMathExpression(math.toDegrees, "DEGREES") {
  override def funcName: String = "toDegrees"
}

case class ToRadians(child: Expression) extends UnaryMathExpression(math.toRadians, "RADIANS") {
  override def funcName: String = "toRadians"
}

case class Bin(child: Expression)
  extends UnaryExpression with Serializable with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = StringType

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(jl.Long.toBinaryString(input.asInstanceOf[Long]))

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c) =>
      s"UTF8String.fromString(java.lang.Long.toBinaryString($c))")
  }
}

object Hex {
  val hexDigits = Array[Char](
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  ).map(_.toByte)

  // lookup table to translate '0' -> 0 ... 'F'/'f' -> 15
  val unhexDigits = {
    val array = Array.fill[Byte](128)(-1)
    (0 to 9).foreach(i => array('0' + i) = i.toByte)
    (0 to 5).foreach(i => array('A' + i) = (i + 10).toByte)
    (0 to 5).foreach(i => array('a' + i) = (i + 10).toByte)
    array
  }

  def hex(bytes: Array[Byte]): UTF8String = {
    val length = bytes.length
    val value = new Array[Byte](length * 2)
    var i = 0
    while (i < length) {
      value(i * 2) = Hex.hexDigits((bytes(i) & 0xF0) >> 4)
      value(i * 2 + 1) = Hex.hexDigits(bytes(i) & 0x0F)
      i += 1
    }
    UTF8String.fromBytes(value)
  }

  def hex(num: Long): UTF8String = {
    // Extract the hex digits of num into value[] from right to left
    val value = new Array[Byte](16)
    var numBuf = num
    var len = 0
    do {
      len += 1
      value(value.length - len) = Hex.hexDigits((numBuf & 0xF).toInt)
      numBuf >>>= 4
    } while (numBuf != 0)
    UTF8String.fromBytes(java.util.Arrays.copyOfRange(value, value.length - len, value.length))
  }

  def unhex(bytes: Array[Byte]): Array[Byte] = {
    val out = new Array[Byte]((bytes.length + 1) >> 1)
    var i = 0
    if ((bytes.length & 0x01) != 0) {
      // padding with '0'
      if (bytes(0) < 0) {
        return null
      }
      val v = Hex.unhexDigits(bytes(0))
      if (v == -1) {
        return null
      }
      out(0) = v
      i += 1
    }
    // two characters form the hex value.
    while (i < bytes.length) {
      if (bytes(i) < 0 || bytes(i + 1) < 0) {
        return null
      }
      val first = Hex.unhexDigits(bytes(i))
      val second = Hex.unhexDigits(bytes(i + 1))
      if (first == -1 || second == -1) {
        return null
      }
      out(i / 2) = (((first << 4) | second) & 0xFF).toByte
      i += 2
    }
    out
  }
}

/**
 * If the argument is an INT or binary, hex returns the number as a STRING in hexadecimal format.
 * Otherwise if the number is a STRING, it converts each character into its hex representation
 * and returns the resulting STRING. Negative numbers would be treated as two's complement.
 */
case class Hex(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, BinaryType, StringType))

  override def dataType: DataType = StringType

  protected override def nullSafeEval(num: Any): Any = child.dataType match {
    case LongType => Hex.hex(num.asInstanceOf[Long])
    case BinaryType => Hex.hex(num.asInstanceOf[Array[Byte]])
    case StringType => Hex.hex(num.asInstanceOf[UTF8String].getBytes)
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (c) => {
      val hex = Hex.getClass.getName.stripSuffix("$")
      s"${ev.value} = " + (child.dataType match {
        case StringType => s"""$hex.hex($c.getBytes());"""
        case _ => s"""$hex.hex($c);"""
      })
    })
  }
}

/**
 * Performs the inverse operation of HEX.
 * Resulting characters are returned as a byte array.
 */
case class Unhex(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(num: Any): Any =
    Hex.unhex(num.asInstanceOf[UTF8String].getBytes)

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (c) => {
      val hex = Hex.getClass.getName.stripSuffix("$")
      s"""
        ${ev.value} = $hex.unhex($c.getBytes());
        ${ev.isNull} = ${ev.value} == null;
       """
    })
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Binary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////


case class Atan2(left: Expression, right: Expression)
  extends BinaryMathExpression(math.atan2, "ATAN2") {

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    // With codegen, the values returned by -0.0 and 0.0 are different. Handled with +0.0
    math.atan2(input1.asInstanceOf[Double] + 0.0, input2.asInstanceOf[Double] + 0.0)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.atan2($c1 + 0.0, $c2 + 0.0)")
  }
}

case class Pow(left: Expression, right: Expression)
  extends BinaryMathExpression(math.pow, "POWER") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.pow($c1, $c2)")
  }
}


/**
 * Bitwise unsigned left shift.
 * @param left the base number to shift.
 * @param right number of bits to left shift.
 */
case class ShiftLeft(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def dataType: DataType = left.dataType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    input1 match {
      case l: jl.Long => l << input2.asInstanceOf[jl.Integer]
      case i: jl.Integer => i << input2.asInstanceOf[jl.Integer]
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (left, right) => s"$left << $right")
  }
}


/**
 * Bitwise unsigned left shift.
 * @param left the base number to shift.
 * @param right number of bits to left shift.
 */
case class ShiftRight(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def dataType: DataType = left.dataType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    input1 match {
      case l: jl.Long => l >> input2.asInstanceOf[jl.Integer]
      case i: jl.Integer => i >> input2.asInstanceOf[jl.Integer]
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (left, right) => s"$left >> $right")
  }
}


/**
 * Bitwise unsigned right shift, for integer and long data type.
 * @param left the base number.
 * @param right the number of bits to right shift.
 */
case class ShiftRightUnsigned(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def dataType: DataType = left.dataType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    input1 match {
      case l: jl.Long => l >>> input2.asInstanceOf[jl.Integer]
      case i: jl.Integer => i >>> input2.asInstanceOf[jl.Integer]
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (left, right) => s"$left >>> $right")
  }
}


case class Hypot(left: Expression, right: Expression)
  extends BinaryMathExpression(math.hypot, "HYPOT")


/**
 * Computes the logarithm of a number.
 * @param left the logarithm base, default to e.
 * @param right the number to compute the logarithm of.
 */
case class Logarithm(left: Expression, right: Expression)
  extends BinaryMathExpression((c1, c2) => math.log(c2) / math.log(c1), "LOG") {

  /**
   * Natural log, i.e. using e as the base.
   */
  def this(child: Expression) = {
    this(EulerNumber(), child)
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dLeft = input1.asInstanceOf[Double]
    val dRight = input2.asInstanceOf[Double]
    // Unlike Hive, we support Log base in (0.0, 1.0]
    if (dLeft <= 0.0 || dRight <= 0.0) null else math.log(dRight) / math.log(dLeft)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (left.isInstanceOf[EulerNumber]) {
      nullSafeCodeGen(ctx, ev, (c1, c2) =>
        s"""
          if ($c2 <= 0.0) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = java.lang.Math.log($c2);
          }
        """)
    } else {
      nullSafeCodeGen(ctx, ev, (c1, c2) =>
        s"""
          if ($c1 <= 0.0 || $c2 <= 0.0) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = java.lang.Math.log($c2) / java.lang.Math.log($c1);
          }
        """)
    }
  }
}

/**
 * Round the `child`'s result to `scale` decimal place when `scale` >= 0
 * or round at integral part when `scale` < 0.
 * For example, round(31.415, 2) = 31.42 and round(31.415, -1) = 30.
 *
 * Child of IntegralType would round to itself when `scale` >= 0.
 * Child of FractionalType whose value is NaN or Infinite would always round to itself.
 *
 * Round's dataType would always equal to `child`'s dataType except for DecimalType,
 * which would lead scale decrease from the origin DecimalType.
 *
 * @param child expr to be round, all [[NumericType]] is allowed as Input
 * @param scale new scale to be round to, this should be a constant int at runtime
 */
case class Round(child: Expression, scale: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  import BigDecimal.RoundingMode.HALF_UP

  def this(child: Expression) = this(child, Literal(0))

  override def left: Expression = child
  override def right: Expression = scale

  // round of Decimal would eval to null if it fails to `changePrecision`
  override def nullable: Boolean = true

  override def foldable: Boolean = child.foldable

  override lazy val dataType: DataType = child.dataType match {
    // if the new scale is bigger which means we are scaling up,
    // keep the original scale as `Decimal` does
    case DecimalType.Fixed(p, s) => DecimalType(p, if (_scale > s) s else _scale)
    case t => t
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case TypeCheckSuccess =>
        if (scale.foldable) {
          TypeCheckSuccess
        } else {
          TypeCheckFailure("Only foldable Expression is allowed for scale arguments")
        }
      case f => f
    }
  }

  // Avoid repeated evaluation since `scale` is a constant int,
  // avoid unnecessary `child` evaluation in both codegen and non-codegen eval
  // by checking if scaleV == null as well.
  private lazy val scaleV: Any = scale.eval(EmptyRow)
  private lazy val _scale: Int = scaleV.asInstanceOf[Int]

  override def eval(input: InternalRow): Any = {
    if (scaleV == null) { // if scale is null, no need to eval its child at all
      null
    } else {
      val evalE = child.eval(input)
      if (evalE == null) {
        null
      } else {
        nullSafeEval(evalE)
      }
    }
  }

  // not overriding since _scale is a constant int at runtime
  def nullSafeEval(input1: Any): Any = {
    child.dataType match {
      case _: DecimalType =>
        val decimal = input1.asInstanceOf[Decimal]
        if (decimal.changePrecision(decimal.precision, _scale)) decimal else null
      case ByteType =>
        BigDecimal(input1.asInstanceOf[Byte]).setScale(_scale, HALF_UP).toByte
      case ShortType =>
        BigDecimal(input1.asInstanceOf[Short]).setScale(_scale, HALF_UP).toShort
      case IntegerType =>
        BigDecimal(input1.asInstanceOf[Int]).setScale(_scale, HALF_UP).toInt
      case LongType =>
        BigDecimal(input1.asInstanceOf[Long]).setScale(_scale, HALF_UP).toLong
      case FloatType =>
        val f = input1.asInstanceOf[Float]
        if (f.isNaN || f.isInfinite) {
          f
        } else {
          BigDecimal(f.toDouble).setScale(_scale, HALF_UP).toFloat
        }
      case DoubleType =>
        val d = input1.asInstanceOf[Double]
        if (d.isNaN || d.isInfinite) {
          d
        } else {
          BigDecimal(d).setScale(_scale, HALF_UP).toDouble
        }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val ce = child.gen(ctx)

    val evaluationCode = child.dataType match {
      case _: DecimalType =>
        s"""
        if (${ce.value}.changePrecision(${ce.value}.precision(), ${_scale})) {
          ${ev.value} = ${ce.value};
        } else {
          ${ev.isNull} = true;
        }"""
      case ByteType =>
        if (_scale < 0) {
          s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).byteValue();"""
        } else {
          s"${ev.value} = ${ce.value};"
        }
      case ShortType =>
        if (_scale < 0) {
          s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).shortValue();"""
        } else {
          s"${ev.value} = ${ce.value};"
        }
      case IntegerType =>
        if (_scale < 0) {
          s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).intValue();"""
        } else {
          s"${ev.value} = ${ce.value};"
        }
      case LongType =>
        if (_scale < 0) {
          s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).longValue();"""
        } else {
          s"${ev.value} = ${ce.value};"
        }
      case FloatType => // if child eval to NaN or Infinity, just return it.
        s"""
          if (Float.isNaN(${ce.value}) || Float.isInfinite(${ce.value})){
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).floatValue();
          }"""
      case DoubleType => // if child eval to NaN or Infinity, just return it.
        s"""
          if (Double.isNaN(${ce.value}) || Double.isInfinite(${ce.value})){
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.ROUND_HALF_UP).doubleValue();
          }"""
    }

    if (scaleV == null) { // if scale is null, no need to eval its child at all
      s"""
        boolean ${ev.isNull} = true;
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      """
    } else {
      s"""
        ${ce.code}
        boolean ${ev.isNull} = ${ce.isNull};
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        if (!${ev.isNull}) {
          $evaluationCode
        }
      """
    }
  }
}
