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
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes.{toSQLExpr, toSQLId, toSQLValue}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXPRESSION_WITH_RANDOM_SEED, RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
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
  def withShiftedSeed(shift: Long): Expression
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
  arguments = """
    Arguments:
      * seed - The seed for the random number generator.
        An expression that evaluates to an integer or long. Must be a constant.
  """,
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

  override def withShiftedSeed(shift: Long): Rand =
    Rand(Add(child, Literal(shift), evalMode = EvalMode.LEGACY), hideSeed)

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
  arguments = """
    Arguments:
      * seed - The seed used to produce reproducible random results.
        An expression that evaluates to an integer or long. Must be a constant.
  """,
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

  override def withShiftedSeed(shift: Long): Randn =
    Randn(Add(child, Literal(shift), evalMode = EvalMode.LEGACY), hideSeed)

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
      If both of these numbers have integral types, then the result will be an integer drawn from
      the closed range between them, given in either order, with each integer equally likely.
      Otherwise the result lies between them and, after floating-point or decimal rounding, can
      equal max; a DOUBLE result is not finite when the difference of the bounds overflows.
  """,
  arguments = """
    Arguments:
      * min - The lower bound of the range of random values.
        An expression that evaluates to a numeric. Must be a constant.
      * max - The upper bound of the range of random values.
        An expression that evaluates to a numeric. Must be a constant.
      * seed - The seed used to produce reproducible random results.
        An expression that evaluates to an integer or long. Must be a constant.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(10, 20, 0) > 0 AS result;
      true
  """,
  since = "4.0.0",
  group = "math_funcs")
case class Uniform(
    min: Expression,
    max: Expression,
    seedExpression: Expression,
    hideSeed: Boolean,
    timeZoneId: Option[String] = None,
    legacyIntegralTruncate: Boolean =
      SQLConf.get.getConf(SQLConf.LEGACY_UNIFORM_INTEGRAL_TRUNCATE))
  extends RuntimeReplaceable
    with TernaryLike[Expression]
    with RDG
    with ExpectsInputTypes
    with TimeZoneAwareExpression {
  def this(min: Expression, max: Expression) =
    this(min, max, UnresolvedSeed, hideSeed = true)
  def this(min: Expression, max: Expression, seedExpression: Expression) =
    this(min, max, seedExpression, hideSeed = false)

  final override lazy val deterministic: Boolean = false
  override def nodePatternsInternal(): Seq[TreePattern] =
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
    copy(seedExpression = Literal(newSeed, LongType))

  override def withShiftedSeed(shift: Long): Expression =
    copy(seedExpression = Literal(seed + shift, LongType))

  override def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    copy(min = newFirst, max = newSecond, seedExpression = newThird)

  override def replacement: Expression = {
    if (Seq(min, max, seedExpression).exists(_.dataType == NullType)) {
      Literal(null)
    } else {
      def cast(e: Expression, to: DataType): Expression = {
        if (e.dataType == to) e else Cast(e, to, timeZoneId)
      }
      dataType match {
        case _: IntegralType if !legacyIntegralTruncate =>
          integralReplacement(cast)
        case _ =>
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

  /**
   * Draws each integer in the closed range [min, max] with equal probability. Truncating a double
   * drawn from [min, max) toward zero would never return a positive `max` or a negative `min`, and
   * would return zero twice as often as any other value when the range spans it.
   *
   * The bounds stay children of the draw instead of being evaluated here, so that the optimizer
   * gives current-time and special datetime bounds their query values and folds each bound once.
   */
  private def integralReplacement(cast: (Expression, DataType) => Expression): Expression = {
    cast(
      UniformIntegralDraw(cast(min, LongType), cast(max, LongType), Literal(seed, LongType)),
      dataType)
  }

  /** Returns a copy of this expression with the specified timeZoneId. */
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Some(timeZoneId))
  }
}

object Uniform {
  def apply(min: Expression, max: Expression): Uniform =
    Uniform(min, max, UnresolvedSeed, hideSeed = true)
  def apply(min: Expression, max: Expression, seedExpression: Expression): Uniform =
    Uniform(min, max, seedExpression, hideSeed = false)
}

/**
 * Draws a LONG uniformly from the closed range between `lower` and `upper`, given in either order,
 * for [[Uniform]] with integral bounds. Returns NULL when either bound is NULL.
 *
 * The bounds are constant, so they are read on the first row of each partition rather than when
 * the partition is initialized, so that a draw in a branch that never runs cannot fail the query.
 * A range of n values takes `floor(n * nextDouble())` when n is at most
 * [[UniformIntegralDraw.MaxDoubleDrawRangeSize]], which keeps every offset's share within about
 * 2^-21 of the others, and otherwise the unsigned remainder by n of a 64-bit word, redrawing words
 * below 2^64 mod n so that every remainder is equally likely. The offset is added to the lower
 * bound with wrapping LONG arithmetic, which stays in range at the BIGINT limits.
 */
case class UniformIntegralDraw(lower: Expression, upper: Expression, seedExpression: Expression)
  extends RDG with TernaryLike[Expression] with Nondeterministic with ExpectsInputTypes {

  override def first: Expression = lower
  override def second: Expression = upper
  override def third: Expression = seedExpression

  override def dataType: DataType = LongType
  override def nullable: Boolean = lower.nullable || upper.nullable

  override def inputTypes: Seq[AbstractDataType] =
    Seq(LongType, LongType, TypeCollection(IntegerType, LongType))

  @transient private var boundsRead: Boolean = _
  @transient private var nullRange: Boolean = _
  @transient private var lo: Long = _
  // The number of values in the range, read as unsigned; zero stands for 2^64.
  @transient private var rangeSize: Long = _
  @transient private var rejectBelow: Long = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
    boundsRead = false
  }

  override protected def evalInternal(input: InternalRow): Any = {
    if (!boundsRead) {
      val a = lower.eval(input)
      val b = upper.eval(input)
      nullRange = a == null || b == null
      if (!nullRange) {
        val (x, y) = (a.asInstanceOf[Long], b.asInstanceOf[Long])
        lo = math.min(x, y)
        rangeSize = math.max(x, y) - lo + 1L
        rejectBelow = UniformIntegralDraw.rejectionThreshold(rangeSize)
      }
      boundsRead = true
    }
    if (nullRange) null else lo + UniformIntegralDraw.drawOffset(rng, rangeSize, rejectBelow)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rngClass = classOf[XORShiftRandom].getName
    val drawClass = classOf[UniformIntegralDraw].getName
    val rngTerm = ctx.addMutableState(rngClass, "rng")
    val boundsReadTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "boundsRead")
    val nullRangeTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "nullRange")
    val loTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "lo")
    val rangeSizeTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "rangeSize")
    val rejectBelowTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "rejectBelow")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $rngClass(${seed}L + partitionIndex); $boundsReadTerm = false;")
    val lowerGen = lower.genCode(ctx)
    val upperGen = upper.genCode(ctx)
    ev.copy(code = code"""
      if (!$boundsReadTerm) {
        ${lowerGen.code}
        ${upperGen.code}
        $nullRangeTerm = ${lowerGen.isNull} || ${upperGen.isNull};
        if (!$nullRangeTerm) {
          $loTerm = java.lang.Math.min(${lowerGen.value}, ${upperGen.value});
          $rangeSizeTerm = java.lang.Math.max(${lowerGen.value}, ${upperGen.value}) - $loTerm + 1L;
          $rejectBelowTerm = $drawClass.rejectionThreshold($rangeSizeTerm);
        }
        $boundsReadTerm = true;
      }
      boolean ${ev.isNull} = $nullRangeTerm;
      long ${ev.value} = -1L;
      if (!${ev.isNull}) {
        ${ev.value} = $loTerm + $drawClass.drawOffset($rngTerm, $rangeSizeTerm, $rejectBelowTerm);
      }""")
  }

  override def withNewSeed(newSeed: Long): UniformIntegralDraw =
    copy(seedExpression = Literal(newSeed, LongType))

  override def withShiftedSeed(shift: Long): UniformIntegralDraw =
    copy(seedExpression = Add(seedExpression, Literal(shift), evalMode = EvalMode.LEGACY))

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): UniformIntegralDraw =
    copy(lower = newFirst, upper = newSecond, seedExpression = newThird)
}

object UniformIntegralDraw {
  /** The largest range, in values, that takes one DOUBLE draw instead of a 64-bit draw. */
  val MaxDoubleDrawRangeSize: Long = 1L << 32

  /** Unsigned 2^64 mod `rangeSize`: words below it would bias the unsigned remainder. */
  def rejectionThreshold(rangeSize: Long): Long =
    if (rangeSize == 0L) 0L else java.lang.Long.remainderUnsigned(-rangeSize, rangeSize)

  /** Draws an offset in [0, rangeSize), reading `rangeSize` as unsigned with zero for 2^64. */
  def drawOffset(rng: XORShiftRandom, rangeSize: Long, rejectBelow: Long): Long = {
    if (rangeSize != 0L && java.lang.Long.compareUnsigned(rangeSize, MaxDoubleDrawRangeSize) <= 0) {
      (rangeSize.toDouble * rng.nextDouble()).toLong
    } else {
      var word = rng.nextLong()
      if (rangeSize == 0L) {
        word
      } else {
        while (java.lang.Long.compareUnsigned(word, rejectBelow) < 0) {
          word = rng.nextLong()
        }
        java.lang.Long.remainderUnsigned(word, rangeSize)
      }
    }
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(length[, seed]) - Returns a string of the specified length whose characters are chosen
      uniformly at random from the following pool of characters: 0-9, a-z, A-Z. The random seed is
      optional. The string length must be a non-negative constant two-byte or four-byte integer
      (SMALLINT or INT, respectively).
  """,
  arguments = """
    Arguments:
      * length - The length of the random string to generate.
        An expression that evaluates to a non-negative integer. Must be a constant.
      * seed - The seed used to produce reproducible random results.
        An expression that evaluates to an integer or long. Must be a constant.
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

  override def withShiftedSeed(shift: Long): Expression =
    RandStr(length, Literal(seed + shift, LongType), hideSeed)

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
    if (result == TypeCheckResult.TypeCheckSuccess) {
      val lengthValue = length.eval()
      // randstr(NULL, 0) is valid (treated as 0), so only reject a negative length.
      if (lengthValue != null && lengthValue.asInstanceOf[Int] < 0) {
        result = DataTypeMismatch(
          errorSubClass = "VALUE_OUT_OF_RANGE",
          messageParameters = Map(
            "exprName" -> toSQLId("length"),
            "valueRange" -> s"[0, ${Int.MaxValue}]",
            "currentValue" -> toSQLValue(lengthValue, IntegerType)))
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
