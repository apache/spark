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
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types._

/**
 * The trait of the Window Specification (specified in the OVER clause or WINDOW clause) for
 * Window Functions.
 */
sealed trait WindowSpec

/**
 * The specification for a window function.
 * @param partitionSpec It defines the way that input rows are partitioned.
 * @param orderSpec It defines the ordering of rows in a partition.
 * @param frameSpecification It defines the window frame in a partition.
 */
case class WindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: WindowFrame) extends Expression with WindowSpec with Unevaluable {

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


  override def toString: String = simpleString

  override def nullable: Boolean = true
  override def foldable: Boolean = false
  override def dataType: DataType = throw new UnsupportedOperationException
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
}

/**
 * Extractor for making working with frame boundaries easier.
 */
object FrameBoundaryExtractor {
  def unapply(boundary: FrameBoundary): Option[Int] = boundary match {
    case CurrentRow => Some(0)
    case ValuePreceding(offset) => Some(-offset)
    case ValueFollowing(offset) => Some(offset)
    case _ => None
  }
}

/**
  * A window function is a function that can only be evaluated in the context of a window operator.
  */
trait WindowFunction extends Expression {
  /** Frame in which the window operator must be executed. */
  def frame: WindowFrame = UnspecifiedFrame
}

abstract class OffsetWindowFunction
  extends Expression with WindowFunction with Unevaluable with ImplicitCastInputTypes {
  val input: Expression
  val default: Expression
  val offset: Expression
  val offsetSign: Int


  override def children: Seq[Expression] = Seq(input, offset, default)

  override def foldable: Boolean = input.foldable && (default == null || default.foldable)

  override def nullable: Boolean = input.nullable && (default == null || default.nullable)

  override lazy val frame = {
    // This will be triggered by the Analyzer.
    val offsetValue = offset.eval() match {
      case o: Int => o
      case x => throw new AnalysisException(
        "Offset expression must be a foldable integer expression: $x")
    }
    val boundary = ValueFollowing(offsetSign * offsetValue)
    SpecifiedWindowFrame(RowFrame, boundary, boundary)
  }

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyDataType, IntegerType, TypeCollection(input.dataType, NullType))

  override def toString: String = s"$prettyName($input, $offset, $default)"
}

case class Lead(input: Expression, offset: Expression, default: Expression)
  extends OffsetWindowFunction {

  def this(input: Expression, offset: Expression) =
    this(input, offset, Literal(null))

  def this(input: Expression) =
    this(input, Literal(1), Literal(null))

  def this() = this(Literal(null), Literal(1), Literal(null))

  val offsetSign = 1
}

case class Lag(input: Expression, offset: Expression, default: Expression)
  extends OffsetWindowFunction {

  def this(input: Expression, offset: Expression) =
    this(input, offset, Literal(null))

  def this(input: Expression) =
    this(input, Literal(1), Literal(null))

  def this() = this(Literal(null), Literal(1), Literal(null))

  val offsetSign = -1
}

abstract class AggregateWindowFunction extends DeclarativeAggregate with WindowFunction {
  self: Product =>
  override val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override val mergeExpressions = Nil // TODO how to deal with this?
}

abstract class RowNumberLike extends AggregateWindowFunction {
  override def children: Seq[Expression] = Nil
  override def inputTypes: Seq[AbstractDataType] = Nil
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, false)()
  override val aggBufferAttributes: Seq[AttributeReference] = rowNumber :: Nil
  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = Add(rowNumber, one) :: Nil
}

/**
  * A [[SizeBasedWindowFunction]] needs the size of the current window for its calculation.
  */
trait SizeBasedWindowFunction extends AggregateWindowFunction {
  /** The partition size attribute, this is here to prevent us from creating an attribute on the
    * executor side. */
  val size = AttributeReference("size", IntegerType, false)()

  /** Set the window size expression. */
  def withSize(n: Expression): SizeBasedWindowFunction
}

case class RowNumber() extends RowNumberLike {
  override val evaluateExpression = Cast(rowNumber, IntegerType)
}

case class CumeDist(n: Expression)
    extends RowNumberLike with SizeBasedWindowFunction {
  def this() = this(Literal(0))
  override def dataType: DataType = DoubleType
  override def withSize(n: Expression): CumeDist = CumeDist(n)
  override val frame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
  override val evaluateExpression = Divide(Cast(rowNumber, DoubleType), Cast(n, DoubleType))
}

case class NTile(buckets: Expression, n: Expression)
    extends RowNumberLike with SizeBasedWindowFunction {
  def this() = this(Literal(1), Literal(0))
  def this(buckets: Expression) = this(buckets, Literal(0))

  // Validate buckets.
  buckets.eval() match {
    case b: Int if b > 1 => // Ok
    case x => throw new AnalysisException(
      "Buckets expression must be a foldable positive integer expression: $x")
  }

  override def withSize(n: Expression): NTile = NTile(buckets, n)
  private val bucket = AttributeReference("bucket", IntegerType, false)()
  private val bucketThreshold = AttributeReference("bucketThreshold", IntegerType, false)()
  private val bucketSize = AttributeReference("bucketSize", IntegerType, false)()
  private val bucketsWithPadding = AttributeReference("bucketsWithPadding", IntegerType, false)()
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
    bucketSize,
    bucketsWithPadding
  )

  override val evaluateExpression = bucket
}

abstract class RankLike extends AggregateWindowFunction {
  override def children: Seq[Expression] = order
  override def inputTypes: Seq[AbstractDataType] = children.map(_ => AnyDataType)

  val order: Seq[Expression]
  protected val orderAttrs = order.map{ expr =>
    AttributeReference(expr.prettyString, expr.dataType)()
  }

  protected val orderEquals = order.zip(orderAttrs)
    .map(EqualNullSafe.tupled)
    .reduceOption(And)
    .getOrElse(Literal(true))
  protected val orderInit = order.map(e => Literal.create(null, e.dataType))
  protected val rank = AttributeReference("rank", IntegerType, false)()
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, false)()
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val increaseRowNumber = Add(rowNumber, one)

  /** Source of the rank value. */
  protected def rankSource: Expression = rowNumber

  /** Increase the rank when the current rank == 0 or when the one of order attributes changes. */
  protected val increaseRank = If(And(orderEquals, Not(EqualTo(rank, zero))), rank, rankSource)

  override val aggBufferAttributes: Seq[AttributeReference] = rank +: rowNumber +: orderAttrs
  override val initialValues = zero +: one +: orderInit
  override val updateExpressions = increaseRank +: increaseRowNumber +: order
  override val evaluateExpression: Expression = rank

  def withOrder(order: Seq[Expression]): RankLike
}

case class Rank(order: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): Rank = Rank(order)
}

case class DenseRank(order: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): DenseRank = DenseRank(order)
  override protected def rankSource = Add(rank, one)
  override val updateExpressions = increaseRank +: order
  override val aggBufferAttributes = rank +: orderAttrs
  override val initialValues = zero +: orderInit
}

case class PercentRank(order: Seq[Expression], n: Expression) extends RankLike
with SizeBasedWindowFunction {
  def this() = this(Nil, Literal(0))
  override def withOrder(order: Seq[Expression]): PercentRank = PercentRank(order, n)
  override def withSize(n: Expression): PercentRank = PercentRank(order, n)
  override def dataType: DataType = DoubleType
  override val evaluateExpression = If(GreaterThan(n, one),
      Divide(Cast(Subtract(rank, one), DoubleType), Cast(Subtract(n, one), DoubleType)),
      Literal(0.0d))
}
