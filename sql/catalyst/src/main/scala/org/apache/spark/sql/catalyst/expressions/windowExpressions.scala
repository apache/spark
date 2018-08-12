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

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, NoOp}
import org.apache.spark.sql.types._

/**
 * The trait of the Window Specification (specified in the OVER clause or WINDOW clause) for
 * Window Functions.
 */
sealed trait WindowSpec

/**
 * The specification for a window function.
 *
 * @param partitionSpec It defines the way that input rows are partitioned.
 * @param orderSpec It defines the ordering of rows in a partition.
 * @param frameSpecification It defines the window frame in a partition.
 */
case class WindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: WindowFrame) extends Expression with WindowSpec with Unevaluable {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec :+ frameSpecification

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess &&
      frameSpecification.isInstanceOf[SpecifiedWindowFrame]

  override def nullable: Boolean = true
  override def foldable: Boolean = false
  override def dataType: DataType = throw new UnsupportedOperationException("dataType")

  override def checkInputDataTypes(): TypeCheckResult = {
    frameSpecification match {
      case UnspecifiedFrame =>
        TypeCheckFailure(
          "Cannot use an UnspecifiedFrame. This should have been converted during analysis. " +
            "Please file a bug report.")
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && !f.isUnbounded &&
          orderSpec.isEmpty =>
        TypeCheckFailure(
          "A range window frame cannot be used in an unordered window specification.")
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
          orderSpec.size > 1 =>
        TypeCheckFailure(
          s"A range window frame with value boundaries cannot be used in a window specification " +
            s"with multiple order by expressions: ${orderSpec.mkString(",")}")
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
          !isValidFrameType(f.valueBoundary.head.dataType) =>
        TypeCheckFailure(
          s"The data type '${orderSpec.head.dataType.catalogString}' used in the order " +
            "specification does not match the data type " +
            s"'${f.valueBoundary.head.dataType.catalogString}' which is used in the range frame.")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    def toSql(exprs: Seq[Expression], prefix: String): Seq[String] = {
      Seq(exprs).filter(_.nonEmpty).map(_.map(_.sql).mkString(prefix, ", ", ""))
    }

    val elements =
      toSql(partitionSpec, "PARTITION BY ") ++
        toSql(orderSpec, "ORDER BY ") ++
        Seq(frameSpecification.sql)
    elements.mkString("(", " ", ")")
  }

  private def isValidFrameType(ft: DataType): Boolean = (orderSpec.head.dataType, ft) match {
    case (DateType, IntegerType) => true
    case (TimestampType, CalendarIntervalType) => true
    case (a, b) => a == b
  }
}

/**
 * A Window specification reference that refers to the [[WindowSpecDefinition]] defined
 * under the name `name`.
 */
case class WindowSpecReference(name: String) extends WindowSpec

/**
 * The trait used to represent the type of a Window Frame.
 */
sealed trait FrameType {
  def inputType: AbstractDataType
  def sql: String
}

/**
 * RowFrame treats rows in a partition individually. Values used in a row frame are considered
 * to be physical offsets.
 * For example, `ROW BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a 3-row frame,
 * from the row that precedes the current row to the row that follows the current row.
 */
case object RowFrame extends FrameType {
  override def inputType: AbstractDataType = IntegerType
  override def sql: String = "ROWS"
}

/**
 * RangeFrame treats rows in a partition as groups of peers. All rows having the same `ORDER BY`
 * ordering are considered as peers. Values used in a range frame are considered to be logical
 * offsets.
 * For example, assuming the value of the current row's `ORDER BY` expression `expr` is `v`,
 * `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a frame containing rows whose values
 * `expr` are in the range of [v-1, v+1].
 *
 * If `ORDER BY` clause is not defined, all rows in the partition are considered as peers
 * of the current row.
 */
case object RangeFrame extends FrameType {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval
  override def sql: String = "RANGE"
}

/**
 * The trait used to represent special boundaries used in a window frame.
 */
sealed trait SpecialFrameBoundary extends Expression with Unevaluable {
  override def children: Seq[Expression] = Nil
  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = false
}

/** UNBOUNDED boundary. */
case object UnboundedPreceding extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED PRECEDING"
}

case object UnboundedFollowing extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED FOLLOWING"
}

/** CURRENT ROW boundary. */
case object CurrentRow extends SpecialFrameBoundary {
  override def sql: String = "CURRENT ROW"
}

/**
 * Represents a window frame.
 */
sealed trait WindowFrame extends Expression with Unevaluable {
  override def children: Seq[Expression] = Nil
  override def dataType: DataType = throw new UnsupportedOperationException("dataType")
  override def foldable: Boolean = false
  override def nullable: Boolean = false
}

/** Used as a placeholder when a frame specification is not defined. */
case object UnspecifiedFrame extends WindowFrame

/**
 * A specified Window Frame. The val lower/uppper can be either a foldable [[Expression]] or a
 * [[SpecialFrameBoundary]].
 */
case class SpecifiedWindowFrame(
    frameType: FrameType,
    lower: Expression,
    upper: Expression)
  extends WindowFrame {

  override def children: Seq[Expression] = lower :: upper :: Nil

  lazy val valueBoundary: Seq[Expression] =
    children.filterNot(_.isInstanceOf[SpecialFrameBoundary])

  override def checkInputDataTypes(): TypeCheckResult = {
    // Check lower value.
    val lowerCheck = checkBoundary(lower, "lower")
    if (lowerCheck.isFailure) {
      return lowerCheck
    }

    // Check upper value.
    val upperCheck = checkBoundary(upper, "upper")
    if (upperCheck.isFailure) {
      return upperCheck
    }

    // Check combination (of expressions).
    (lower, upper) match {
      case (l: Expression, u: Expression) if !isValidFrameBoundary(l, u) =>
        TypeCheckFailure(s"Window frame upper bound '$upper' does not followes the lower bound " +
          s"'$lower'.")
      case (l: SpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, u: SpecialFrameBoundary) => TypeCheckSuccess
      case (l: Expression, u: Expression) if l.dataType != u.dataType =>
        TypeCheckFailure(
          s"Window frame bounds '$lower' and '$upper' do no not have the same data type: " +
            s"'${l.dataType.catalogString}' <> '${u.dataType.catalogString}'")
      case (l: Expression, u: Expression) if isGreaterThan(l, u) =>
        TypeCheckFailure(
          "The lower bound of a window frame must be less than or equal to the upper bound")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    val lowerSql = boundarySql(lower)
    val upperSql = boundarySql(upper)
    s"${frameType.sql} BETWEEN $lowerSql AND $upperSql"
  }

  def isUnbounded: Boolean = lower == UnboundedPreceding && upper == UnboundedFollowing

  def isValueBound: Boolean = valueBoundary.nonEmpty

  def isOffset: Boolean = (lower, upper) match {
    case (l: Expression, u: Expression) => frameType == RowFrame && l == u
    case _ => false
  }

  private def boundarySql(expr: Expression): String = expr match {
    case e: SpecialFrameBoundary => e.sql
    case UnaryMinus(n) => n.sql + " PRECEDING"
    case e: Expression => e.sql + " FOLLOWING"
  }

  private def isGreaterThan(l: Expression, r: Expression): Boolean = {
    GreaterThan(l, r).eval().asInstanceOf[Boolean]
  }

  private def checkBoundary(b: Expression, location: String): TypeCheckResult = b match {
    case _: SpecialFrameBoundary => TypeCheckSuccess
    case e: Expression if !e.foldable =>
      TypeCheckFailure(s"Window frame $location bound '$e' is not a literal.")
    case e: Expression if !frameType.inputType.acceptsType(e.dataType) =>
      TypeCheckFailure(
        s"The data type of the $location bound '${e.dataType.catalogString}' does not match " +
          s"the expected data type '${frameType.inputType.simpleString}'.")
    case _ => TypeCheckSuccess
  }

  private def isValidFrameBoundary(l: Expression, u: Expression): Boolean = {
    (l, u) match {
      case (UnboundedFollowing, _) => false
      case (_, UnboundedPreceding) => false
      case _ => true
    }
  }
}

case class UnresolvedWindowExpression(
    child: Expression,
    windowSpec: WindowSpecReference) extends UnaryExpression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}

case class WindowExpression(
    windowFunction: Expression,
    windowSpec: WindowSpecDefinition) extends Expression with Unevaluable {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  override def dataType: DataType = windowFunction.dataType
  override def foldable: Boolean = windowFunction.foldable
  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"
  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql
}

/**
 * A window function is a function that can only be evaluated in the context of a window operator.
 */
trait WindowFunction extends Expression {
  /** Frame in which the window operator must be executed. */
  def frame: WindowFrame = UnspecifiedFrame
}

/**
 * Case objects that describe whether a window function is a SQL window function or a Python
 * user-defined window function.
 */
sealed trait WindowFunctionType

object WindowFunctionType {
  case object SQL extends WindowFunctionType
  case object Python extends WindowFunctionType

  def functionType(windowExpression: NamedExpression): WindowFunctionType = {
    val t = windowExpression.collectFirst {
      case _: WindowFunction | _: AggregateFunction => SQL
      case udf: PythonUDF if PythonUDF.isWindowPandasUDF(udf) => Python
    }

    // Normally a window expression would either have a SQL window function, a SQL
    // aggregate function or a python window UDF. However, sometimes the optimizer will replace
    // the window function if the value of the window function can be predetermined.
    // For example, for query:
    //
    // select count(NULL) over () from values 1.0, 2.0, 3.0 T(a)
    //
    // The window function will be replaced by expression literal(0)
    // To handle this case, if a window expression doesn't have a regular window function, we
    // consider its type to be SQL as literal(0) is also a SQL expression.
    t.getOrElse(SQL)
  }
}


/**
 * An offset window function is a window function that returns the value of the input column offset
 * by a number of rows within the partition. For instance: an OffsetWindowfunction for value x with
 * offset -2, will get the value of x 2 rows back in the partition.
 */
abstract class OffsetWindowFunction
  extends Expression with WindowFunction with Unevaluable with ImplicitCastInputTypes {
  /**
   * Input expression to evaluate against a row which a number of rows below or above (depending on
   * the value and sign of the offset) the current row.
   */
  val input: Expression

  /**
   * Default result value for the function when the `offset`th row does not exist.
   */
  val default: Expression

  /**
   * (Foldable) expression that contains the number of rows between the current row and the row
   * where the input expression is evaluated.
   */
  val offset: Expression

  /**
   * Direction of the number of rows between the current row and the row where the input expression
   * is evaluated.
   */
  val direction: SortDirection

  override def children: Seq[Expression] = Seq(input, offset, default)

  /*
   * The result of an OffsetWindowFunction is dependent on the frame in which the
   * OffsetWindowFunction is executed, the input expression and the default expression. Even when
   * both the input and the default expression are foldable, the result is still not foldable due to
   * the frame.
   */
  override def foldable: Boolean = false

  override def nullable: Boolean = default == null || default.nullable || input.nullable

  override lazy val frame: WindowFrame = {
    val boundary = direction match {
      case Ascending => offset
      case Descending => UnaryMinus(offset) match {
          case e: Expression if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
          case o => o
      }
    }
    SpecifiedWindowFrame(RowFrame, boundary, boundary)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val check = super.checkInputDataTypes()
    if (check.isFailure) {
      check
    } else if (!offset.foldable) {
      TypeCheckFailure(s"Offset expression '$offset' must be a literal.")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyDataType, IntegerType, TypeCollection(input.dataType, NullType))

  override def toString: String = s"$prettyName($input, $offset, $default)"
}

/**
 * The Lead function returns the value of `input` at the `offset`th row after the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of `input` is null at the `offset`th row,
 * null is returned. If there is no such offset row, the `default` expression is evaluated.
 *
 * @param input expression to evaluate `offset` rows after the current row.
 * @param offset rows to jump ahead in the partition.
 * @param default to use when the offset is larger than the window. The default value is null.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
      after the current row in the window. The default value of `offset` is 1 and the default
      value of `default` is null. If the value of `input` at the `offset`th row is null,
      null is returned. If there is no such an offset row (e.g., when the offset is 1, the last
      row of the window does not have any subsequent row), `default` is returned.
  """)
case class Lead(input: Expression, offset: Expression, default: Expression)
    extends OffsetWindowFunction {

  def this(input: Expression, offset: Expression) = this(input, offset, Literal(null))

  def this(input: Expression) = this(input, Literal(1))

  def this() = this(Literal(null))

  override val direction = Ascending
}

/**
 * The Lag function returns the value of `input` at the `offset`th row before the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of `input` is null at the `offset`th row,
 * null is returned. If there is no such offset row, the `default` expression is evaluated.
 *
 * @param input expression to evaluate `offset` rows before the current row.
 * @param offset rows to jump back in the partition.
 * @param default to use when the offset row does not exist.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
      before the current row in the window. The default value of `offset` is 1 and the default
      value of `default` is null. If the value of `input` at the `offset`th row is null,
      null is returned. If there is no such offset row (e.g., when the offset is 1, the first
      row of the window does not have any previous row), `default` is returned.
  """)
case class Lag(input: Expression, offset: Expression, default: Expression)
    extends OffsetWindowFunction {

  def this(input: Expression, offset: Expression) = this(input, offset, Literal(null))

  def this(input: Expression) = this(input, Literal(1))

  def this() = this(Literal(null))

  override val direction = Descending
}

abstract class AggregateWindowFunction extends DeclarativeAggregate with WindowFunction {
  self: Product =>
  override val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override lazy val mergeExpressions =
    throw new UnsupportedOperationException("Window Functions do not support merging.")
}

abstract class RowNumberLike extends AggregateWindowFunction {
  override def children: Seq[Expression] = Nil
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, nullable = false)()
  override val aggBufferAttributes: Seq[AttributeReference] = rowNumber :: Nil
  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = rowNumber + one :: Nil
}

/**
 * A [[SizeBasedWindowFunction]] needs the size of the current window for its calculation.
 */
trait SizeBasedWindowFunction extends AggregateWindowFunction {
  // It's made a val so that the attribute created on driver side is serialized to executor side.
  // Otherwise, if it's defined as a function, when it's called on executor side, it actually
  // returns the singleton value instantiated on executor side, which has different expression ID
  // from the one created on driver side.
  val n: AttributeReference = SizeBasedWindowFunction.n
}

object SizeBasedWindowFunction {
  val n = AttributeReference("window__partition__size", IntegerType, nullable = false)()
}

/**
 * The RowNumber function computes a unique, sequential number to each row, starting with one,
 * according to the ordering of rows within the window partition.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Assigns a unique, sequential number to each row, starting with one,
      according to the ordering of rows within the window partition.
  """)
case class RowNumber() extends RowNumberLike {
  override val evaluateExpression = rowNumber
  override def prettyName: String = "row_number"
}

/**
 * The CumeDist function computes the position of a value relative to all values in the partition.
 * The result is the number of rows preceding or equal to the current row in the ordering of the
 * partition divided by the total number of rows in the window partition. Any tie values in the
 * ordering will evaluate to the same position.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the position of a value relative to all values in the partition.
  """)
case class CumeDist() extends RowNumberLike with SizeBasedWindowFunction {
  override def dataType: DataType = DoubleType
  // The frame for CUME_DIST is Range based instead of Row based, because CUME_DIST must
  // return the same value for equal values in the partition.
  override val frame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
  override val evaluateExpression = rowNumber.cast(DoubleType) / n.cast(DoubleType)
  override def prettyName: String = "cume_dist"
}

/**
 * The NTile function divides the rows for each window partition into `n` buckets ranging from 1 to
 * at most `n`. Bucket values will differ by at most 1. If the number of rows in the partition does
 * not divide evenly into the number of buckets, then the remainder values are distributed one per
 * bucket, starting with the first bucket.
 *
 * The NTile function is particularly useful for the calculation of tertiles, quartiles, deciles and
 * other common summary statistics
 *
 * The function calculates two variables during initialization: The size of a regular bucket, and
 * the number of buckets that will have one extra row added to it (when the rows do not evenly fit
 * into the number of buckets); both variables are based on the size of the current partition.
 * During the calculation process the function keeps track of the current row number, the current
 * bucket number, and the row number at which the bucket will change (bucketThreshold). When the
 * current row number reaches bucket threshold, the bucket value is increased by one and the
 * threshold is increased by the bucket size (plus one extra if the current bucket is padded).
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param buckets number of buckets to divide the rows in. Default value is 1.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(n) - Divides the rows for each window partition into `n` buckets ranging
      from 1 to at most `n`.
  """)
case class NTile(buckets: Expression) extends RowNumberLike with SizeBasedWindowFunction {
  def this() = this(Literal(1))

  override def children: Seq[Expression] = Seq(buckets)

  // Validate buckets. Note that this could be relaxed, the bucket value only needs to constant
  // for each partition.
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!buckets.foldable) {
      return TypeCheckFailure(s"Buckets expression must be foldable, but got $buckets")
    }

    if (buckets.dataType != IntegerType) {
      return TypeCheckFailure(s"Buckets expression must be integer type, but got $buckets")
    }

    val i = buckets.eval().asInstanceOf[Int]
    if (i > 0) {
      TypeCheckSuccess
    } else {
      TypeCheckFailure(s"Buckets expression must be positive, but got: $i")
    }
  }

  private val bucket = AttributeReference("bucket", IntegerType, nullable = false)()
  private val bucketThreshold =
    AttributeReference("bucketThreshold", IntegerType, nullable = false)()
  private val bucketSize = AttributeReference("bucketSize", IntegerType, nullable = false)()
  private val bucketsWithPadding =
    AttributeReference("bucketsWithPadding", IntegerType, nullable = false)()
  private def bucketOverflow(e: Expression) = If(rowNumber >= bucketThreshold, e, zero)

  override val aggBufferAttributes = Seq(
    rowNumber,
    bucket,
    bucketThreshold,
    bucketSize,
    bucketsWithPadding
  )

  override val initialValues = Seq(
    zero,
    zero,
    zero,
    (n / buckets).cast(IntegerType),
    (n % buckets).cast(IntegerType)
  )

  override val updateExpressions = Seq(
    rowNumber + one,
    bucket + bucketOverflow(one),
    bucketThreshold + bucketOverflow(bucketSize + If(bucket < bucketsWithPadding, one, zero)),
    NoOp,
    NoOp
  )

  override val evaluateExpression = bucket
}

/**
 * A RankLike function is a WindowFunction that changes its value based on a change in the value of
 * the order of the window in which is processed. For instance, when the value of `input` changes
 * in a window ordered by `input` the rank function also changes. The size of the change of the
 * rank function is (typically) not dependent on the size of the change in `input`.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
abstract class RankLike extends AggregateWindowFunction {

  /** Store the values of the window 'order' expressions. */
  protected val orderAttrs = children.map { expr =>
    AttributeReference(expr.sql, expr.dataType)()
  }

  /** Predicate that detects if the order attributes have changed. */
  protected val orderEquals = children.zip(orderAttrs)
    .map(EqualNullSafe.tupled)
    .reduceOption(And)
    .getOrElse(Literal(true))

  protected val orderInit = children.map(e => Literal.create(null, e.dataType))
  protected val rank = AttributeReference("rank", IntegerType, nullable = false)()
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, nullable = false)()
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val increaseRowNumber = rowNumber + one

  /**
   * Different RankLike implementations use different source expressions to update their rank value.
   * Rank for instance uses the number of rows seen, whereas DenseRank uses the number of changes.
   */
  protected def rankSource: Expression = rowNumber

  /** Increase the rank when the current rank == 0 or when the one of order attributes changes. */
  protected val increaseRank = If(orderEquals && rank =!= zero, rank, rankSource)

  override val aggBufferAttributes: Seq[AttributeReference] = rank +: rowNumber +: orderAttrs
  override val initialValues = zero +: one +: orderInit
  override val updateExpressions = increaseRank +: increaseRowNumber +: children
  override val evaluateExpression: Expression = rank

  override def sql: String = s"${prettyName.toUpperCase(Locale.ROOT)}()"

  def withOrder(order: Seq[Expression]): RankLike
}

/**
 * The Rank function computes the rank of a value in a group of values. The result is one plus the
 * number of rows preceding or equal to the current row in the ordering of the partition. The values
 * will produce gaps in the sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param children to base the rank on; a change in the value of one the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the rank of a value in a group of values. The result is one plus the number
      of rows preceding or equal to the current row in the ordering of the partition. The values
      will produce gaps in the sequence.
  """)
case class Rank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): Rank = Rank(order)
}

/**
 * The DenseRank function computes the rank of a value in a group of values. The result is one plus
 * the previously assigned rank value. Unlike [[Rank]], [[DenseRank]] will not produce gaps in the
 * ranking sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param children to base the rank on; a change in the value of one the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the rank of a value in a group of values. The result is one plus the
      previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps
      in the ranking sequence.
  """)
case class DenseRank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): DenseRank = DenseRank(order)
  override protected def rankSource = rank + one
  override val updateExpressions = increaseRank +: children
  override val aggBufferAttributes = rank +: orderAttrs
  override val initialValues = zero +: orderInit
  override def prettyName: String = "dense_rank"
}

/**
 * The PercentRank function computes the percentage ranking of a value in a group of values. The
 * result the rank of the minus one divided by the total number of rows in the partition minus one:
 * (r - 1) / (n - 1). If a partition only contains one row, the function will return 0.
 *
 * The PercentRank function is similar to the CumeDist function, but it uses rank values instead of
 * row counts in the its numerator.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param children to base the rank on; a change in the value of one of the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the percentage ranking of a value in a group of values.
  """)
case class PercentRank(children: Seq[Expression]) extends RankLike with SizeBasedWindowFunction {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): PercentRank = PercentRank(order)
  override def dataType: DataType = DoubleType
  override val evaluateExpression =
    If(n > one, (rank - one).cast(DoubleType) / (n - one).cast(DoubleType), 0.0d)
  override def prettyName: String = "percent_rank"
}
