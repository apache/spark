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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, NoOp}
import org.apache.spark.sql.catalyst.InternalRow
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
    frameSpecification: WindowFrame,
    excludeSpec: ExcludeClause = null) extends Expression with WindowSpec with Unevaluable {

  def validate: Option[String] = frameSpecification match {
    case UnspecifiedFrame =>
      Some("Found a UnspecifiedFrame. It should be converted to a SpecifiedWindowFrame " +
        "during analysis. Please file a bug report.")
    case frame: SpecifiedWindowFrame => frame.validate.orElse {
      def checkValueBasedBoundaryForRangeFrame(): Option[String] = {
        if (orderSpec.length > 1)  {
          // It is not allowed to have a value-based PRECEDING and FOLLOWING
          // as the boundary of a Range Window Frame.
          Some("This Range Window Frame only accepts at most one ORDER BY expression.")
        } else if (orderSpec.nonEmpty && !orderSpec.head.dataType.isInstanceOf[NumericType]) {
          Some("The data type of the expression in the ORDER BY clause should be a numeric type.")
        } else {
          None
        }
      }

      (frame.frameType, frame.frameStart, frame.frameEnd) match {
        case (RangeFrame, vp: ValuePreceding, _) => checkValueBasedBoundaryForRangeFrame()
        case (RangeFrame, vf: ValueFollowing, _) => checkValueBasedBoundaryForRangeFrame()
        case (RangeFrame, _, vp: ValuePreceding) => checkValueBasedBoundaryForRangeFrame()
        case (RangeFrame, _, vf: ValueFollowing) => checkValueBasedBoundaryForRangeFrame()
        case (_, _, _) => None
      }
    }
  }

  override def children: Seq[Expression] = partitionSpec ++ orderSpec

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess &&
      frameSpecification.isInstanceOf[SpecifiedWindowFrame]

  override def nullable: Boolean = true
  override def foldable: Boolean = false
  override def dataType: DataType = throw new UnsupportedOperationException

  override def sql: String = {
    val partition = if (partitionSpec.isEmpty) {
      ""
    } else {
      "PARTITION BY " + partitionSpec.map(_.sql).mkString(", ") + " "
    }

    val order = if (orderSpec.isEmpty) {
      ""
    } else {
      "ORDER BY " + orderSpec.map(_.sql).mkString(", ") + " "
    }

    s"($partition$order${frameSpecification.toString})"
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
sealed trait FrameType

/**
 * RowFrame treats rows in a partition individually. When a [[ValuePreceding]]
 * or a [[ValueFollowing]] is used as its [[FrameBoundary]], the value is considered
 * as a physical offset.
 * For example, `ROW BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a 3-row frame,
 * from the row precedes the current row to the row follows the current row.
 */
case object RowFrame extends FrameType

/**
 * RangeFrame treats rows in a partition as groups of peers.
 * All rows having the same `ORDER BY` ordering are considered as peers.
 * When a [[ValuePreceding]] or a [[ValueFollowing]] is used as its [[FrameBoundary]],
 * the value is considered as a logical offset.
 * For example, assuming the value of the current row's `ORDER BY` expression `expr` is `v`,
 * `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a frame containing rows whose values
 * `expr` are in the range of [v-1, v+1].
 *
 * If `ORDER BY` clause is not defined, all rows in the partition is considered as peers
 * of the current row.
 */
case object RangeFrame extends FrameType

/**
 * The trait used to represent the type of a Window Frame Boundary.
 */
sealed trait FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean
}

/**
 * Extractor for making working with frame boundaries easier.
 */
object FrameBoundary {
  def apply(boundary: FrameBoundary): Option[Int] = unapply(boundary)
  def unapply(boundary: FrameBoundary): Option[Int] = boundary match {
    case CurrentRow => Some(0)
    case ValuePreceding(offset) => Some(-offset)
    case ValueFollowing(offset) => Some(offset)
    case _ => None
  }
}

/** UNBOUNDED PRECEDING boundary. */
case object UnboundedPreceding extends FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean = other match {
    case UnboundedPreceding => true
    case vp: ValuePreceding => true
    case CurrentRow => true
    case vf: ValueFollowing => true
    case UnboundedFollowing => true
  }

  override def toString: String = "UNBOUNDED PRECEDING"
}

/** <value> PRECEDING boundary. */
case class ValuePreceding(value: Int) extends FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean = other match {
    case UnboundedPreceding => false
    case ValuePreceding(anotherValue) => value >= anotherValue
    case CurrentRow => true
    case vf: ValueFollowing => true
    case UnboundedFollowing => true
  }

  override def toString: String = s"$value PRECEDING"
}

/** CURRENT ROW boundary. */
case object CurrentRow extends FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean = other match {
    case UnboundedPreceding => false
    case vp: ValuePreceding => false
    case CurrentRow => true
    case vf: ValueFollowing => true
    case UnboundedFollowing => true
  }

  override def toString: String = "CURRENT ROW"
}

/** <value> FOLLOWING boundary. */
case class ValueFollowing(value: Int) extends FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean = other match {
    case UnboundedPreceding => false
    case vp: ValuePreceding => false
    case CurrentRow => false
    case ValueFollowing(anotherValue) => value <= anotherValue
    case UnboundedFollowing => true
  }

  override def toString: String = s"$value FOLLOWING"
}

/** UNBOUNDED FOLLOWING boundary. */
case object UnboundedFollowing extends FrameBoundary {
  def notFollows(other: FrameBoundary): Boolean = other match {
    case UnboundedPreceding => false
    case vp: ValuePreceding => false
    case CurrentRow => false
    case vf: ValueFollowing => false
    case UnboundedFollowing => true
  }

  override def toString: String = "UNBOUNDED FOLLOWING"
}

/**
 * The trait used to represent the a Window Frame.
 */
sealed trait WindowFrame

/** Used as a place holder when a frame specification is not defined.  */
case object UnspecifiedFrame extends WindowFrame

/** A specified Window Frame. */
case class SpecifiedWindowFrame(
    frameType: FrameType,
    frameStart: FrameBoundary,
    frameEnd: FrameBoundary) extends WindowFrame {

  /** If this WindowFrame is valid or not. */
  def validate: Option[String] = (frameType, frameStart, frameEnd) match {
    case (_, UnboundedFollowing, _) =>
      Some(s"$UnboundedFollowing is not allowed as the start of a Window Frame.")
    case (_, _, UnboundedPreceding) =>
      Some(s"$UnboundedPreceding is not allowed as the end of a Window Frame.")
    // case (RowFrame, start, end) => ??? RowFrame specific rule
    // case (RangeFrame, start, end) => ??? RangeFrame specific rule
    case (_, start, end) =>
      if (start.notFollows(end)) {
        None
      } else {
        val reason =
          s"The end of this Window Frame $end is smaller than the start of " +
          s"this Window Frame $start."
        Some(reason)
      }
  }

  override def toString: String = frameType match {
    case RowFrame => s"ROWS BETWEEN $frameStart AND $frameEnd"
    case RangeFrame => s"RANGE BETWEEN $frameStart AND $frameEnd"
  }
}

object SpecifiedWindowFrame {
  /**
   *
   * @param hasOrderSpecification If the window spec has order by expressions.
   * @param acceptWindowFrame If the window function accepts user-specified frame.
   * @return
   */
  def defaultWindowFrame(
      hasOrderSpecification: Boolean,
      acceptWindowFrame: Boolean): SpecifiedWindowFrame = {
    if (hasOrderSpecification && acceptWindowFrame) {
      // If order spec is defined and the window function supports user specified window frames,
      // the default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
      SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
    } else {
      // Otherwise, the default frame is
      // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
    }
  }
}

/**
 * The trait used to represent an Exclude type .
 */
sealed trait ExcludeType

/** Represent the type of Excluding Current Row  */
case object ExcludeCurrentRow extends ExcludeType

/** Specifies excluding the current row and all rows that are tied with it.
  *  Ties occur when there is a match on the order column or columns*/
case object ExcludeGroup extends ExcludeType

/** Specifies excluding all rows that are tied with the current row (peer rows),
  * but retaining the current row.*/
case object ExcludeTies extends ExcludeType

/** Specifies not excluding any rows. This value is the default if you specify no exclusion. */
case object ExcludeNoOthers extends ExcludeType

case class ExcludeClause (
    excludeType: ExcludeType,
    valueOrdering: Ordering[InternalRow] = null, // compare order by column value with current row
    toBeCompare: Projection = null) {

  override def toString: String = excludeType match {
    case ExcludeCurrentRow => s"EXCLUDE CURRENT ROW"
    case ExcludeGroup => s"EXCLUDE GROUP"
    case ExcludeTies => s"EXCLUDE TIES"
    case ExcludeNoOthers => s"EXCLUDDE NO OTHERS"
  }
}
object ExcludeClause {
  def defaultExclude: ExcludeClause = {
    ExcludeClause(ExcludeNoOthers)
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
   * Default result value for the function when the 'offset'th row does not exist.
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

  override lazy val frame = {
    // This will be triggered by the Analyzer.
    val offsetValue = offset.eval() match {
      case o: Int => o
      case x => throw new AnalysisException(
        s"Offset expression must be a foldable integer expression: $x")
    }
    val boundary = direction match {
      case Ascending => ValueFollowing(offsetValue)
      case Descending => ValuePreceding(offsetValue)
    }
    SpecifiedWindowFrame(RowFrame, boundary, boundary)
  }

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyDataType, IntegerType, TypeCollection(input.dataType, NullType))

  override def toString: String = s"$prettyName($input, $offset, $default)"
}

/**
 * The Lead function returns the value of 'x' at the 'offset'th row after the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of 'x' is null at the 'offset'th row,
 * null is returned. If there is no such offset row, the default expression is evaluated.
 *
 * @param input expression to evaluate 'offset' rows after the current row.
 * @param offset rows to jump ahead in the partition.
 * @param default to use when the offset is larger than the window. The default value is null.
 */
@ExpressionDescription(usage =
  """_FUNC_(input, offset, default) - LEAD returns the value of 'x' at the 'offset'th row
     after the current row in the window.
     The default value of 'offset' is 1 and the default value of 'default' is null.
     If the value of 'x' at the 'offset'th row is null, null is returned.
     If there is no such offset row (e.g. when the offset is 1, the last row of the window
     does not have any subsequent row), 'default' is returned.""")
case class Lead(input: Expression, offset: Expression, default: Expression)
    extends OffsetWindowFunction {

  def this(input: Expression, offset: Expression) = this(input, offset, Literal(null))

  def this(input: Expression) = this(input, Literal(1))

  def this() = this(Literal(null))

  override val direction = Ascending
}

/**
 * The Lag function returns the value of 'x' at the 'offset'th row before the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of 'x' is null at the 'offset'th row,
 * null is returned. If there is no such offset row, the default expression is evaluated.
 *
 * @param input expression to evaluate 'offset' rows before the current row.
 * @param offset rows to jump back in the partition.
 * @param default to use when the offset row does not exist.
 */
@ExpressionDescription(usage =
  """_FUNC_(input, offset, default) - LAG returns the value of 'x' at the 'offset'th row
     before the current row in the window.
     The default value of 'offset' is 1 and the default value of 'default' is null.
     If the value of 'x' at the 'offset'th row is null, null is returned.
     If there is no such offset row (e.g. when the offset is 1, the first row of the window
     does not have any previous row), 'default' is returned.""")
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
  override def supportsPartial: Boolean = false
  override lazy val mergeExpressions =
    throw new UnsupportedOperationException("Window Functions do not support merging.")
}

abstract class RowNumberLike extends AggregateWindowFunction {
  override def children: Seq[Expression] = Nil
  override def inputTypes: Seq[AbstractDataType] = Nil
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, nullable = false)()
  override val aggBufferAttributes: Seq[AttributeReference] = rowNumber :: Nil
  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = Add(rowNumber, one) :: Nil
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
@ExpressionDescription(usage =
  """_FUNC_() - The ROW_NUMBER() function assigns a unique, sequential number to
     each row, starting with one, according to the ordering of rows within
     the window partition.""")
case class RowNumber() extends RowNumberLike {
  override val evaluateExpression = rowNumber
  override def prettyName: String = "row_number"
}

/**
 * The CumeDist function computes the position of a value relative to a all values in the partition.
 * The result is the number of rows preceding or equal to the current row in the ordering of the
 * partition divided by the total number of rows in the window partition. Any tie values in the
 * ordering will evaluate to the same position.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
@ExpressionDescription(usage =
  """_FUNC_() - The CUME_DIST() function computes the position of a value relative to
     a all values in the partition.""")
case class CumeDist() extends RowNumberLike with SizeBasedWindowFunction {
  override def dataType: DataType = DoubleType
  // The frame for CUME_DIST is Range based instead of Row based, because CUME_DIST must
  // return the same value for equal values in the partition.
  override val frame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
  override val evaluateExpression = Divide(Cast(rowNumber, DoubleType), Cast(n, DoubleType))
  override def prettyName: String = "cume_dist"
}

/**
 * The NTile function divides the rows for each window partition into 'n' buckets ranging from 1 to
 * at most 'n'. Bucket values will differ by at most 1. If the number of rows in the partition does
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
 * current row number reaches bucket threshold, the bucket value is increased by one and the the
 * threshold is increased by the bucket size (plus one extra if the current bucket is padded).
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param buckets number of buckets to divide the rows in. Default value is 1.
 */
@ExpressionDescription(usage =
  """_FUNC_(x) - The NTILE(n) function divides the rows for each window partition
     into 'n' buckets ranging from 1 to at most 'n'.""")
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
  private def bucketOverflow(e: Expression) =
    If(GreaterThanOrEqual(rowNumber, bucketThreshold), e, zero)

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
    Cast(Divide(n, buckets), IntegerType),
    Cast(Remainder(n, buckets), IntegerType)
  )

  override val updateExpressions = Seq(
    Add(rowNumber, one),
    Add(bucket, bucketOverflow(one)),
    Add(bucketThreshold, bucketOverflow(
      Add(bucketSize, If(LessThan(bucket, bucketsWithPadding), one, zero)))),
    NoOp,
    NoOp
  )

  override val evaluateExpression = bucket
}

/**
 * A RankLike function is a WindowFunction that changes its value based on a change in the value of
 * the order of the window in which is processed. For instance, when the value of 'x' changes in a
 * window ordered by 'x' the rank function also changes. The size of the change of the rank function
 * is (typically) not dependent on the size of the change in 'x'.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
abstract class RankLike extends AggregateWindowFunction {
  override def inputTypes: Seq[AbstractDataType] = children.map(_ => AnyDataType)

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
  protected val increaseRowNumber = Add(rowNumber, one)

  /**
   * Different RankLike implementations use different source expressions to update their rank value.
   * Rank for instance uses the number of rows seen, whereas DenseRank uses the number of changes.
   */
  protected def rankSource: Expression = rowNumber

  /** Increase the rank when the current rank == 0 or when the one of order attributes changes. */
  protected val increaseRank = If(And(orderEquals, Not(EqualTo(rank, zero))), rank, rankSource)

  override val aggBufferAttributes: Seq[AttributeReference] = rank +: rowNumber +: orderAttrs
  override val initialValues = zero +: one +: orderInit
  override val updateExpressions = increaseRank +: increaseRowNumber +: children
  override val evaluateExpression: Expression = rank

  override def sql: String = s"${prettyName.toUpperCase}()"

  def withOrder(order: Seq[Expression]): RankLike
}

/**
 * The Rank function computes the rank of a value in a group of values. The result is one plus the
 * number of rows preceding or equal to the current row in the ordering of the partition. Tie values
 * will produce gaps in the sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param children to base the rank on; a change in the value of one the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(usage =
  """_FUNC_() -  RANK() computes the rank of a value in a group of values. The result
     is one plus the number of rows preceding or equal to the current row in the
     ordering of the partition. Tie values will produce gaps in the sequence.""")
case class Rank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): Rank = Rank(order)
}

/**
 * The DenseRank function computes the rank of a value in a group of values. The result is one plus
 * the previously assigned rank value. Unlike Rank, DenseRank will not produce gaps in the ranking
 * sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 *
 * @param children to base the rank on; a change in the value of one the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(usage =
  """_FUNC_() - The DENSE_RANK() function computes the rank of a value in a group of
     values. The result is one plus the previously assigned rank value. Unlike Rank,
     DenseRank will not produce gaps in the ranking sequence.""")
case class DenseRank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): DenseRank = DenseRank(order)
  override protected def rankSource = Add(rank, one)
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
 * @param children to base the rank on; a change in the value of one the children will trigger a
 *                 change in rank. This is an internal parameter and will be assigned by the
 *                 Analyser.
 */
@ExpressionDescription(usage =
  """_FUNC_() - PERCENT_RANK() The PercentRank function computes the percentage
     ranking of a value in a group of values.""")
case class PercentRank(children: Seq[Expression]) extends RankLike with SizeBasedWindowFunction {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): PercentRank = PercentRank(order)
  override def dataType: DataType = DoubleType
  override val evaluateExpression = If(GreaterThan(n, one),
      Divide(Cast(Subtract(rank, one), DoubleType), Cast(Subtract(n, one), DoubleType)),
      Literal(0.0d))
  override def prettyName: String = "percent_rank"
}
