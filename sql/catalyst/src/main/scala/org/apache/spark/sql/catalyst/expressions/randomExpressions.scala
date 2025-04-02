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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedSeed}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes.{toSQLExpr, toSQLId}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXPRESSION_WITH_RANDOM_SEED, RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types._
import org.apache.spark.util.random.XORShiftRandom

/**
 * A Random distribution generating expression.
 * TODO: This can be made generic to generate any type of random distribution, or any type of
 * StructType.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
trait RDG extends Expression with ExpressionWithRandomSeed {
  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  override def stateful: Boolean = true

  @transient protected lazy val seed: Long = seedExpression match {
    case e if e.dataType == IntegerType => e.eval().asInstanceOf[Int]
    case e if e.dataType == LongType => e.eval().asInstanceOf[Long]
  }

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType
}

abstract class NondeterministicUnaryRDG
  extends RDG with UnaryLike[Expression] with Nondeterministic with ExpectsInputTypes {
  override def seedExpression: Expression = child

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegerType, LongType))
}

/**
 * Represents the behavior of expressions which have a random seed and can renew the seed.
 * Usually the random seed needs to be renewed at each execution under streaming queries.
 */
trait ExpressionWithRandomSeed extends Expression {
  override val nodePatterns: Seq[TreePattern] = Seq(EXPRESSION_WITH_RANDOM_SEED)

  def seedExpression: Expression
  def withNewSeed(seed: Long): Expression
}

private[catalyst] object ExpressionWithRandomSeed {
  def expressionToSeed(e: Expression, source: String): Option[Long] = e match {
    case IntegerLiteral(seed) => Some(seed)
    case LongLiteral(seed) => Some(seed)
    case Literal(null, _) => None
    case _ => throw QueryCompilationErrors.invalidRandomSeedParameter(source, e)
  }
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) uniformly distributed values in [0, 1).",
  examples = """
    Examples:
      > SELECT _FUNC_();
       0.9629742951434543
      > SELECT _FUNC_(0);
       0.7604953758285915
      > SELECT _FUNC_(null);
       0.7604953758285915
  """,
  note = """
    The function is non-deterministic in general case.
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Rand(child: Expression, hideSeed: Boolean = false) extends NondeterministicUnaryRDG {

  def this() = this(UnresolvedSeed, true)

  def this(child: Expression) = this(child, false)

  override def withNewSeed(seed: Long): Rand = Rand(Literal(seed, LongType), hideSeed)

  override protected def evalInternal(input: InternalRow): Double = rng.nextDouble()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $rngTerm.nextDouble();""",
      isNull = FalseLiteral)
  }

  override def flatArguments: Iterator[Any] = Iterator(child)
  override def sql: String = {
    s"rand(${if (hideSeed) "" else child.sql})"
  }

  override protected def withNewChildInternal(newChild: Expression): Rand = copy(child = newChild)
}

object Rand {
  def apply(seed: Long): Rand = Rand(Literal(seed, LongType))
}

/** Generate a random column with i.i.d. values drawn from the standard normal distribution. */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) values drawn from the standard normal distribution.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       -0.3254147983080288
      > SELECT _FUNC_(0);
       1.6034991609278433
      > SELECT _FUNC_(null);
       1.6034991609278433
  """,
  note = """
    The function is non-deterministic in general case.
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Randn(child: Expression, hideSeed: Boolean = false) extends NondeterministicUnaryRDG {

  def this() = this(UnresolvedSeed, true)

  def this(child: Expression) = this(child, false)

  override def withNewSeed(seed: Long): Randn = Randn(Literal(seed, LongType), hideSeed)

  override protected def evalInternal(input: InternalRow): Double = rng.nextGaussian()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $rngTerm.nextGaussian();""",
      isNull = FalseLiteral)
  }

  override def flatArguments: Iterator[Any] = Iterator(child)
  override def sql: String = {
    s"randn(${if (hideSeed) "" else child.sql})"
  }

  override protected def withNewChildInternal(newChild: Expression): Randn = copy(child = newChild)
}

object Randn {
  def apply(seed: Long): Randn = Randn(Literal(seed, LongType))
}

@ExpressionDescription(
  usage = """
    _FUNC_(min, max[, seed]) - Returns a random value with independent and identically
      distributed (i.i.d.) values with the specified range of numbers. The random seed is optional.
      The provided numbers specifying the minimum and maximum values of the range must be constant.
      If both of these numbers are integers, then the result will also be an integer. Otherwise if
      one or both of these are floating-point numbers, then the result will also be a floating-point
      number.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(10, 20, 0) > 0 AS result;
      true
  """,
  since = "4.0.0",
  group = "math_funcs")
case class Uniform(min: Expression, max: Expression, seedExpression: Expression, hideSeed: Boolean)
  extends RuntimeReplaceable with TernaryLike[Expression] with RDG with ExpectsInputTypes {
  def this(min: Expression, max: Expression) =
    this(min, max, UnresolvedSeed, hideSeed = true)
  def this(min: Expression, max: Expression, seedExpression: Expression) =
    this(min, max, seedExpression, hideSeed = false)

  final override lazy val deterministic: Boolean = false
  override val nodePatterns: Seq[TreePattern] =
    Seq(RUNTIME_REPLACEABLE, EXPRESSION_WITH_RANDOM_SEED)

  override def inputTypes: Seq[AbstractDataType] = {
    val randomSeedTypes = TypeCollection(IntegerType, LongType)
    Seq(NumericType, NumericType, randomSeedTypes)
  }

  override def dataType: DataType = {
    (min.dataType, max.dataType) match {
      case _ if !seedExpression.resolved || seedExpression.dataType == NullType =>
        NullType
      case (left: IntegralType, right: IntegralType) =>
        if (UpCastRule.legalNumericPrecedence(left, right)) right else left
      case (_: NumericType, DoubleType) | (DoubleType, _: NumericType) => DoubleType
      case (_: NumericType, FloatType) | (FloatType, _: NumericType) => FloatType
      case (lhs: DecimalType, rhs: DecimalType) => if (lhs.isWiderThan(rhs)) lhs else rhs
      case (_, d: DecimalType) => d
      case (d: DecimalType, _) => d
      case _ =>
        throw SparkException.internalError(
          s"Unexpected argument data types: ${min.dataType}, ${max.dataType}")
    }
  }

  override def sql: String = {
    s"uniform(${min.sql}, ${max.sql}${if (hideSeed) "" else s", ${seedExpression.sql}"})"
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    var result: TypeCheckResult = super.checkInputDataTypes()
    def requiredType = "integer or floating-point"
    Seq((min, "min"),
      (max, "max"),
      (seedExpression, "seed")).foreach {
      case (expr: Expression, name: String) =>
        if (result == TypeCheckResult.TypeCheckSuccess && !expr.foldable) {
          result = DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId(name),
              "inputType" -> requiredType,
              "inputExpr" -> toSQLExpr(expr)))
        }
    }
    result
  }

  override def first: Expression = min
  override def second: Expression = max
  override def third: Expression = seedExpression

  override def withNewSeed(newSeed: Long): Expression =
    Uniform(min, max, Literal(newSeed, LongType), hideSeed)

  override def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    Uniform(newFirst, newSecond, newThird, hideSeed)

  override def replacement: Expression = {
    if (Seq(min, max, seedExpression).exists(_.dataType == NullType)) {
      Literal(null)
    } else {
      def cast(e: Expression, to: DataType): Expression = if (e.dataType == to) e else Cast(e, to)
      cast(Add(
        cast(min, DoubleType),
        Multiply(
          Subtract(
            cast(max, DoubleType),
            cast(min, DoubleType)),
          Rand(seed))),
        dataType)
    }
  }
}

object Uniform {
  def apply(min: Expression, max: Expression): Uniform =
    Uniform(min, max, UnresolvedSeed, hideSeed = true)
  def apply(min: Expression, max: Expression, seedExpression: Expression): Uniform =
    Uniform(min, max, seedExpression, hideSeed = false)
}

@ExpressionDescription(
  usage = """
    _FUNC_(length[, seed]) - Returns a string of the specified length whose characters are chosen
      uniformly at random from the following pool of characters: 0-9, a-z, A-Z. The random seed is
      optional. The string length must be a constant two-byte or four-byte integer (SMALLINT or INT,
      respectively).
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_(3, 0) AS result;
       ceV
  """,
  since = "4.0.0",
  group = "string_funcs")
case class RandStr(
    length: Expression, override val seedExpression: Expression, hideSeed: Boolean)
  extends ExpressionWithRandomSeed
  with BinaryLike[Expression]
  with DefaultStringProducingExpression
  with Nondeterministic
  with ExpectsInputTypes {
  def this(length: Expression) =
    this(length, UnresolvedSeed, hideSeed = true)
  def this(length: Expression, seedExpression: Expression) =
    this(length, seedExpression, hideSeed = false)

  override def nullable: Boolean = false
  override def stateful: Boolean = true
  override def left: Expression = length
  override def right: Expression = seedExpression

  override def inputTypes: Seq[AbstractDataType] = Seq(
    IntegerType,
    TypeCollection(IntegerType, LongType))

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  @transient protected lazy val seed: Long = seedExpression match {
    case e if e.dataType == IntegerType => e.eval().asInstanceOf[Int]
    case e if e.dataType == LongType => e.eval().asInstanceOf[Long]
  }
  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
  }

  override def withNewSeed(newSeed: Long): Expression =
    RandStr(length, Literal(newSeed, LongType), hideSeed)
  override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
    RandStr(newFirst, newSecond, hideSeed)

  override def sql: String = {
    s"randstr(${length.sql}${if (hideSeed) "" else s", ${seedExpression.sql}"})"
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    var result: TypeCheckResult = super.checkInputDataTypes()
    Seq((length, "length"),
      (seedExpression, "seed")).foreach {
      case (expr: Expression, name: String) =>
        if (result == TypeCheckResult.TypeCheckSuccess && !expr.foldable) {
          result = DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId(name),
              "inputType" -> "integer",
              "inputExpr" -> toSQLExpr(expr)))
        }
    }
    result
  }

  override def evalInternal(input: InternalRow): Any = {
    val numChars = lengthInteger()
    ExpressionImplUtils.randStr(rng, numChars)
  }

  private def lengthInteger(): Int = {
    // We should have already added a cast to IntegerType (if necessary) in
    // FunctionArgumentTypeCoercion.
    assert(length.dataType == IntegerType, s"Expected IntegerType, got ${length.dataType}")
    val result = length.eval().asInstanceOf[Int]
    if (result < 0) {
      throw QueryExecutionErrors.unexpectedValueForLengthInFunctionError(prettyName, result)
    }
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    val numChars = lengthInteger()
    ev.copy(code =
      code"""
        |UTF8String ${ev.value} =
        |  ${classOf[ExpressionImplUtils].getName}.randStr($rngTerm, $numChars);
        |boolean ${ev.isNull} = false;
        |""".stripMargin,
      isNull = FalseLiteral)
  }
}

object RandStr {
  def apply(length: Expression): RandStr =
    RandStr(length, UnresolvedSeed, hideSeed = true)
  def apply(length: Expression, seedExpression: Expression): RandStr =
    RandStr(length, seedExpression, hideSeed = false)
}
