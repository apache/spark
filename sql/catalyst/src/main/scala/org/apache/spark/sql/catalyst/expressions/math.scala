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

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A leaf expression specifically for math constants. Math constants expect no input.
 * @param c The math constant.
 * @param name The short name of the function
 */
abstract class LeafMathExpression(c: Double, name: String)
  extends LeafExpression with Serializable {
  self: Product =>

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def toString: String = s"$name()"

  override def eval(input: InternalRow): Any = c

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(dataType)} ${ev.primitive} = java.lang.Math.$name;
    """
  }
}

/**
 * A unary expression specifically for math functions. Math Functions expect a specific type of
 * input format, therefore these functions extend `ExpectsInputTypes`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class UnaryMathExpression(f: Double => Double, name: String)
  extends UnaryExpression with Serializable with ExpectsInputTypes { self: Product =>

  override def inputTypes: Seq[DataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  protected override def nullSafeEval(input: Any): Any = {
    val result = f(input.asInstanceOf[Double])
    if (result.isNaN) null else result
  }

  // name of function in java.lang.Math
  def funcName: String = name.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        ${ev.primitive} = java.lang.Math.${funcName}($eval);
        if (Double.valueOf(${ev.primitive}).isNaN()) {
          ${ev.isNull} = true;
        }
      """
    })
  }
}

/**
 * A binary expression specifically for math functions that take two `Double`s as input and returns
 * a `Double`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String)
  extends BinaryExpression with Serializable with ExpectsInputTypes { self: Product =>

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType)

  override def toString: String = s"$name($left, $right)"

  override def dataType: DataType = DoubleType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val result = f(input1.asInstanceOf[Double], input2.asInstanceOf[Double])
    if (result.isNaN) null else result
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

case class EulerNumber() extends LeafMathExpression(math.E, "E")

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

case class Ceil(child: Expression) extends UnaryMathExpression(math.ceil, "CEIL")

case class Cos(child: Expression) extends UnaryMathExpression(math.cos, "COS")

case class Cosh(child: Expression) extends UnaryMathExpression(math.cosh, "COSH")

case class Exp(child: Expression) extends UnaryMathExpression(math.exp, "EXP")

case class Expm1(child: Expression) extends UnaryMathExpression(math.expm1, "EXPM1")

case class Floor(child: Expression) extends UnaryMathExpression(math.floor, "FLOOR")

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

case class Factorial(child: Expression) extends UnaryExpression with ExpectsInputTypes {

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
          ${ev.primitive} =
            org.apache.spark.sql.catalyst.expressions.Factorial.factorial($eval);
        }
      """
    })
  }
}

case class Log(child: Expression) extends UnaryMathExpression(math.log, "LOG")

case class Log2(child: Expression)
  extends UnaryMathExpression((x: Double) => math.log(x) / math.log(2), "LOG2") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        ${ev.primitive} = java.lang.Math.log($eval) / java.lang.Math.log(2);
        if (Double.valueOf(${ev.primitive}).isNaN()) {
          ${ev.isNull} = true;
        }
      """
    })
  }
}

case class Log10(child: Expression) extends UnaryMathExpression(math.log10, "LOG10")

case class Log1p(child: Expression) extends UnaryMathExpression(math.log1p, "LOG1P")

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
  extends UnaryExpression with Serializable with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = StringType

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(jl.Long.toBinaryString(input.asInstanceOf[Long]))

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c) =>
      s"${ctx.stringType}.fromString(java.lang.Long.toBinaryString($c))")
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
}

/**
 * If the argument is an INT or binary, hex returns the number as a STRING in hexadecimal format.
 * Otherwise if the number is a STRING, it converts each character into its hex representation
 * and returns the resulting STRING. Negative numbers would be treated as two's complement.
 */
case class Hex(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  // TODO: Create code-gen version.

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, BinaryType, StringType))

  override def dataType: DataType = StringType

  protected override def nullSafeEval(num: Any): Any = child.dataType match {
    case LongType => hex(num.asInstanceOf[Long])
    case BinaryType => hex(num.asInstanceOf[Array[Byte]])
    case StringType => hex(num.asInstanceOf[UTF8String].getBytes)
  }

  private[this] def hex(bytes: Array[Byte]): UTF8String = {
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

  private def hex(num: Long): UTF8String = {
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
}

/**
 * Performs the inverse operation of HEX.
 * Resulting characters are returned as a byte array.
 */
case class Unhex(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  // TODO: Create code-gen version.

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(num: Any): Any =
    unhex(num.asInstanceOf[UTF8String].getBytes)

  private[this] def unhex(bytes: Array[Byte]): Array[Byte] = {
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


////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Binary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////


case class Atan2(left: Expression, right: Expression)
  extends BinaryMathExpression(math.atan2, "ATAN2") {

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    // With codegen, the values returned by -0.0 and 0.0 are different. Handled with +0.0
    val result = math.atan2(input1.asInstanceOf[Double] + 0.0, input2.asInstanceOf[Double] + 0.0)
    if (result.isNaN) null else result
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.atan2($c1 + 0.0, $c2 + 0.0)") + s"""
      if (Double.valueOf(${ev.primitive}).isNaN()) {
        ${ev.isNull} = true;
      }
      """
  }
}

case class Pow(left: Expression, right: Expression)
  extends BinaryMathExpression(math.pow, "POWER") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.pow($c1, $c2)") + s"""
      if (Double.valueOf(${ev.primitive}).isNaN()) {
        ${ev.isNull} = true;
      }
      """
  }
}


/**
 * Bitwise unsigned left shift.
 * @param left the base number to shift.
 * @param right number of bits to left shift.
 */
case class ShiftLeft(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

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
  extends BinaryExpression with ExpectsInputTypes {

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
  extends BinaryExpression with ExpectsInputTypes {

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val logCode = if (left.isInstanceOf[EulerNumber]) {
      defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.log($c2)")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.log($c2) / java.lang.Math.log($c1)")
    }
    logCode + s"""
      if (Double.isNaN(${ev.primitive})) {
        ${ev.isNull} = true;
      }
    """
  }
}
