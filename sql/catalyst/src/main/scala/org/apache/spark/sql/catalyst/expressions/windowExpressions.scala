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

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{LeafNode, UnaryNode}
import org.apache.spark.sql.types.{NumericType, DataType, LongType, IntegerType}

/**
 * The base class for all Window Specification for Window Functions.
 */
abstract class WindowSpec extends Expression {
  self: Product =>
  override def eval(input: Row): Any = throw new UnsupportedOperationException
  override def nullable: Boolean = true
  override def foldable: Boolean = false
  override def dataType: DataType = throw new UnsupportedOperationException
}

/**
 * The specification for a window function.
 * @param partitionSpec It defines the way that input rows are partitioned.
 * @param orderSpec It defines the ordering of rows in a partition.
 * @param frameSpecification It defines the window frame in a partition.
 */
case class WindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: WindowFrame) extends WindowSpec {

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

  override def toString: String = {
    val builder = new StringBuilder
    builder.append("OVER (")
    if (!partitionSpec.isEmpty) {
      builder.append("PARTITION BY ")
      builder.append(partitionSpec.mkString(","))
    }
    if (!partitionSpec.isEmpty && !orderSpec.isEmpty) {
      builder.append(" ")
    }
    if (!orderSpec.isEmpty) {
      builder.append("ORDER BY ")
      builder.append(orderSpec.mkString(","))
    }
    if (!orderSpec.isEmpty && frameSpecification != UnspecifiedFrame) {
      builder.append(" ")
    }
    if (frameSpecification != UnspecifiedFrame) {
      builder.append(frameSpecification)
    }
    builder.append(")")
    builder.toString
  }
}

object WindowSpecDefinition {
  val empty = WindowSpecDefinition(Nil, Nil, UnspecifiedFrame)
}

/**
 * A Window specification reference that refers to the [[WindowSpecDefinition]] defined
 * under the name `name`.
 */
case class WindowSpecReference(name: String) extends WindowSpec with LeafNode[Expression] {
  override lazy val resolved: Boolean = false
}

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
 * Helper extractor for making working with frame boundaries easier.
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
  def shift(offset: Int): SpecifiedWindowFrame = SpecifiedWindowFrame(RowFrame,
    ValuePreceding(-offset), ValueFollowing(offset))
  val rowRunning = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  val rangeRunning = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
  val unbounded = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
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
      unbounded
    }
  }
}

case class WindowExpression(
    windowFunction: Expression,
    windowSpec: WindowSpec = WindowSpecDefinition.empty,
    fixedWindowFrame: WindowFrame = UnspecifiedFrame,
    pivot: Boolean = false) extends Expression {

  override def children: Seq[Expression] =
    windowFunction :: windowSpec :: Nil

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def dataType: DataType = windowFunction.dataType
  override def foldable: Boolean = windowFunction.foldable
  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"
}

/**
 * A number of window functions imply the use of the window order specification. The Implied Order
 * Spec allows the analyzer to define the order specification in such cases.
 */
trait ImpliedOrderSpec {
  self: Expression =>
  def defineOrderSpec(orderSpec: Seq[Expression]): Expression
}

/**
 * A few of the more advanced window functions can be defined as a combination of two window
 * functions with essentially different frame specifications, examples are: PERCENT_RANK, NTILE
 * and CUME_DIST. The ComposedWindowFunction allows the analyzer to recognize such a situation.
 */
case class ComposedWindowFunction(child: Expression) extends Expression
    with UnaryNode[Expression] {
  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = child.toString
  override def eval(input: Row): Any = child.eval(input)
}

/**
 * Base class for ranking expressions.
 */
abstract class RankLikeExpression extends AggregateExpression with ImpliedOrderSpec{
  self: Product =>
  override def dataType: DataType = LongType
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def toString: String = s"${this.nodeName}()"
}

/**
 * Base class for a rank function. This function should always be evaluated in a running fashion:
 * i.e. in a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW frame.
 */
abstract class RankLikeFunction extends AggregateFunction {
  self: Product =>
  var counter: Long = 0L
  var value: Long = 0L
  var last: Row = EmptyRow
  val extractor = new InterpretedProjection(children)
  override def eval(input: Row): Any = value
}

/**
 * Same as:
 * COALESCE(COUNT(1) OVER (PARTITION BY ... ORDER BY ... RANGE
 *   BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ROW), 0) + 1
 */
case class Rank(children: Seq[Expression]) extends RankLikeExpression {
  override def defineOrderSpec(children: Seq[Expression]): Rank = new Rank(children)
  override def newInstance(): AggregateFunction = RankFunction(children, this)
}

case class RankFunction(override val children: Seq[Expression], base: AggregateExpression)
    extends RankLikeFunction {
  def update(input: Row): Unit = {
    val current = extractor(input)
    counter += 1
    if (current != last) {
      last = current
      value = counter
    }
  }
}

case class DenseRank(children: Seq[Expression]) extends RankLikeExpression {
  override def defineOrderSpec(children: Seq[Expression]): DenseRank = new DenseRank(children)
  override def newInstance(): AggregateFunction = DenseRankFunction(children, this)
}

case class DenseRankFunction(override val children: Seq[Expression], base: AggregateExpression)
    extends RankLikeFunction {
  def update(input: Row): Unit = {
    val current = extractor(input)
    if (current != last) {
      counter += 1
      last = current
      value = counter
    }
  }
}

object WindowFunction {
  def count(spec: WindowSpec = WindowSpecDefinition.empty): WindowExpression =
    WindowExpression(Count(Literal(1)), spec, SpecifiedWindowFrame.unbounded)

  def rowNumber(spec: WindowSpec = WindowSpecDefinition.empty): WindowExpression =
    WindowExpression(Count(Literal(1)), spec, SpecifiedWindowFrame.rowRunning)

  def rank(spec: WindowSpec = WindowSpecDefinition.empty): WindowExpression =
      WindowExpression(Rank(Nil), spec, SpecifiedWindowFrame.rowRunning)

  def denseRank(spec: WindowSpec = WindowSpecDefinition.empty): WindowExpression =
      WindowExpression(DenseRank(Nil), spec, SpecifiedWindowFrame.rowRunning)

  def percentRank(spec: WindowSpec = WindowSpecDefinition.empty): ComposedWindowFunction = {
    val rankMinusOne = Subtract(rank(spec), Literal(1))
    val countMinusOne = Subtract(count(spec), Literal(1))
    ComposedWindowFunction(Divide(rankMinusOne, countMinusOne))
  }

  def cumeDist(spec: WindowSpec = WindowSpecDefinition.empty): ComposedWindowFunction = {
    val rankInclCurrentValue = WindowExpression(Count(Literal(1)), spec,
      SpecifiedWindowFrame.rangeRunning)
    ComposedWindowFunction(Divide(rankInclCurrentValue, count(spec)))
  }

  def ntile(buckets: Int,
    spec: WindowSpec = WindowSpecDefinition.empty): ComposedWindowFunction = {
    val n = count(spec)
    val bucketSize = Cast(Divide(n, Literal(buckets)), IntegerType)
    val bucketSizePlusOne = Add(bucketSize, Literal(1))
    val remainder = Remainder(n, Literal(buckets))
    val threshold = Multiply(remainder, bucketSizePlusOne)
    val rowNumberMinusOne = Subtract(rowNumber(spec), Literal(1))
    ComposedWindowFunction(Add(Cast(If(LessThan(rowNumberMinusOne, threshold),
      Divide(rowNumberMinusOne, bucketSizePlusOne),
      Add(Divide(Subtract(rowNumberMinusOne, threshold), bucketSize), remainder)
    ), IntegerType), Literal(1)))
  }

  def lead(expr: Expression, offset: Int = 1, default: Expression = null,
    spec: WindowSpec = WindowSpecDefinition.empty): Expression = {
    val we = WindowExpression(expr, spec, SpecifiedWindowFrame.shift(offset))
    addDefaultToShift(we, default)
  }

  def lag(expr: Expression, offset: Int = 1, default: Expression = null,
    spec: WindowSpec = WindowSpecDefinition.empty): Expression = {
    val we = WindowExpression(expr, spec, SpecifiedWindowFrame.shift(-offset))
    addDefaultToShift(we, default)
  }

  private def addDefaultToShift(we: WindowExpression, default: Expression): Expression = {
    default match {
      case null | Literal(null, _) => we
      case _: Expression => ComposedWindowFunction(Coalesce(we :: default :: Nil))
    }
  }
}
