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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.math.{BigDecimal => JBigDecimal, RoundingMode}
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult, UnresolvedWithinGroup}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.TypeCollection.NumericAndAnsiInterval
import org.apache.spark.unsafe.types.TimestampNanosVal
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.OpenHashMap

abstract class PercentileBase
  extends TypedAggregateWithHashMapAsBuffer with ImplicitCastInputTypes {

  val child: Expression
  val percentageExpression: Expression
  val frequencyExpression : Expression

  // Whether to reverse calculate percentile value
  val reverse: Boolean

  // Whether the value is discrete
  protected def discrete: Boolean

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  @transient
  private lazy val returnPercentileArray = percentageExpression.dataType.isInstanceOf[ArrayType]

  @transient
  protected lazy val percentages = percentageExpression.eval() match {
    case null => null
    case num: Double => Array(num)
    case arrayData: ArrayData => arrayData.toDoubleArray()
  }

  // Nanosecond timestamps are interpolated exactly, at an exact position.
  @transient
  private lazy val isNanos = child.dataType.isInstanceOf[AnyTimestampNanoType]

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = {
    val resultType = child.dataType match {
      case _: YearMonthIntervalType => YearMonthIntervalType()
      case _: DayTimeIntervalType => DayTimeIntervalType()
      case t: TimeType => t
      case t: AnyTimestampNanoType => t
      case _ => DoubleType
    }
    if (returnPercentileArray) ArrayType(resultType, false) else resultType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    val percentageExpType = percentageExpression.dataType match {
      case _: ArrayType => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
    // AnyTimestampNanoType goes last, so the implicit cast fallback for other inputs is unchanged.
    Seq(
      TypeCollection(NumericAndAnsiInterval, AnyTimeType, AnyTimestampNanoType),
      percentageExpType,
      IntegralType)
  }

  // Check the inputTypes are valid, and the percentageExpression satisfies:
  // 1. percentageExpression must be foldable;
  // 2. percentages(s) must be in the range [0.0, 1.0].
  override def checkInputDataTypes(): TypeCheckResult = {
    // Validate the inputTypes
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!percentageExpression.foldable) {
      // percentageExpression must be foldable
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("percentage"),
          "inputType" -> toSQLType(percentageExpression.dataType),
          "inputExpr" -> toSQLExpr(percentageExpression))
      )
    } else if (percentages == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "percentage")
      )
    } else if (percentages.exists(percentage => percentage < 0.0 || percentage > 1.0)) {
      // percentages(s) must be in the range [0.0, 1.0]
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "percentage",
          "valueRange" -> "[0.0, 1.0]",
          "currentValue" -> percentages.map(toSQLValue(_, DoubleType)).mkString(",")
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  protected def toDoubleValue(d: Any): Double = d match {
    case d: Decimal => d.toDouble
    case n: Number => n.doubleValue
  }

  // Nanosecond timestamps are kept as is, so that no sub-microsecond digit is lost.
  protected def toPercentileValue(key: AnyRef): Any = if (isNanos) key else toDoubleValue(key)

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input).asInstanceOf[AnyRef]
    val frqValue = frequencyExpression.eval(input)

    // Null values are ignored in counts map.
    if (key != null && frqValue != null) {
      val frqLong = frqValue.asInstanceOf[Number].longValue()
      // add only when frequency is positive
      if (frqLong > 0) {
        buffer.changeValue(key, frqLong, _ + frqLong)
      } else if (frqLong < 0) {
        throw QueryExecutionErrors.negativeValueUnexpectedError(
          frequencyExpression, frqLong)
      }
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    generateOutput(getPercentiles(buffer))
  }

  private def getPercentiles(buffer: OpenHashMap[AnyRef, Long]): Seq[Any] = {
    if (buffer.isEmpty) {
      return Seq.empty
    }

    val ordering = PhysicalDataType.ordering(child.dataType)
    val sortedCounts = if (reverse) {
      buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]].reverse)
    } else {
      buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]])
    }
    val accumulatedCounts = sortedCounts.scanLeft((sortedCounts.head._1, 0L)) {
      case ((key1, count1), (key2, count2)) => (key2, count1 + count2)
    }.tail

    percentages.map(getPercentile(accumulatedCounts, _)).toImmutableArraySeq
  }

  private def generateOutput(percentiles: Seq[Any]): Any = {
    val results = child.dataType match {
      case _: YearMonthIntervalType => percentiles.map(_.asInstanceOf[Double].toInt)
      case _: DayTimeIntervalType => percentiles.map(_.asInstanceOf[Double].toLong)
      case _: TimeType => percentiles.map(_.asInstanceOf[Double].toLong)
      case _ => percentiles
    }
    if (percentiles.isEmpty) {
      null
    } else if (returnPercentileArray) {
      new GenericArrayData(results)
    } else {
      results.head
    }
  }

  /**
   * Get the percentile value.
   * This function has been based upon similar function from HIVE
   * `org.apache.hadoop.hive.ql.udf.UDAFPercentile.getPercentile()`.
   */
  protected def getPercentile(
      accumulatedCounts: Seq[(AnyRef, Long)],
      percentile: Double): Any = {
    val position = (accumulatedCounts.last._2 - 1) * percentile
    // A double position can land just below a whole number (e.g. 90 * 0.7), which flooring a
    // nanosecond result would turn into a whole step.
    lazy val exactPosition = JBigDecimal.valueOf(accumulatedCounts.last._2 - 1)
      .multiply(JBigDecimal.valueOf(percentile))

    // We may need to do linear interpolation to get the exact percentile
    val (lower, higher) = if (isNanos) {
      (exactPosition.setScale(0, RoundingMode.FLOOR).longValueExact(),
        exactPosition.setScale(0, RoundingMode.CEILING).longValueExact())
    } else {
      (position.floor.toLong, position.ceil.toLong)
    }

    // Use binary search to find the lower and the higher position.
    val countsArray = accumulatedCounts.map(_._2).toArray[Long]
    val lowerIndex = binarySearchCount(countsArray, 0, accumulatedCounts.size, lower + 1)
    val higherIndex = binarySearchCount(countsArray, 0, accumulatedCounts.size, higher + 1)

    val lowerKey = accumulatedCounts(lowerIndex)._1
    if (higher == lower) {
      // no interpolation needed because position does not have a fraction
      return toPercentileValue(lowerKey)
    }

    val higherKey = accumulatedCounts(higherIndex)._1
    if (higherKey == lowerKey) {
      // no interpolation needed because lower position and higher position has the same key
      return toPercentileValue(lowerKey)
    }

    if (discrete) {
      // We end up here only if spark.sql.legacy.percentileDiscCalculation=true
      toPercentileValue(lowerKey)
    } else if (isNanos) {
      interpolateNanos(lowerKey.asInstanceOf[TimestampNanosVal],
        higherKey.asInstanceOf[TimestampNanosVal],
        exactPosition.subtract(JBigDecimal.valueOf(lower)))
    } else {
      // Linear interpolation to get the exact percentile. Computed as lower + fraction *
      // (higher - lower) to stay monotonic; the delta can overflow to Infinity only for adjacent
      // values straddling zero near Double.MaxValue, an extreme case where the old form was also
      // inaccurate.
      toDoubleValue(lowerKey) +
        (position - lower) * (toDoubleValue(higherKey) - toDoubleValue(lowerKey))
    }
  }

  // Exact `lower + fraction * (higher - lower)` in epoch nanoseconds (a double can't hold them),
  // floored to the input precision.
  private def interpolateNanos(
      lower: TimestampNanosVal,
      higher: TimestampNanosVal,
      fraction: JBigDecimal): TimestampNanosVal = {
    import PercentileBase.NANOS_PER_MICRO
    def toEpochNanos(v: TimestampNanosVal): JBigDecimal =
      JBigDecimal.valueOf(v.epochMicros).multiply(NANOS_PER_MICRO).add(
        JBigDecimal.valueOf(v.nanosWithinMicro.toLong))
    val lowerNanos = toEpochNanos(lower)
    val nanos = toEpochNanos(higher).subtract(lowerNanos)
      .multiply(fraction)
      .add(lowerNanos)
      .setScale(0, RoundingMode.FLOOR)
    val micros = nanos.divide(NANOS_PER_MICRO, 0, RoundingMode.FLOOR)
    val nanosWithinMicro = nanos.subtract(micros.multiply(NANOS_PER_MICRO)).intValueExact()
    DateTimeUtils.truncateTimestampNanosToPrecision(
      TimestampNanosVal.fromParts(micros.longValueExact(), nanosWithinMicro.toShort),
      TimestampFamily.fractionalPrecision(child.dataType).get)
  }

  /**
   * use a binary search to find the index of the position closest to the current value.
   */
  protected def binarySearchCount(
      countsArray: Array[Long], start: Int, end: Int, value: Long): Int = {
    util.Arrays.binarySearch(countsArray, 0, end, value) match {
      case ix if ix < 0 => -(ix + 1)
      case ix => ix
    }
  }
}

object PercentileBase {
  private val NANOS_PER_MICRO = JBigDecimal.valueOf(DateTimeConstants.NANOS_PER_MICROS)
}

/**
 * The Percentile aggregate function returns the exact percentile(s) of numeric column `expr` at
 * the given percentage(s) with value range in [0.0, 1.0].
 *
 * Because the number of elements and their partial order cannot be determined in advance.
 * Therefore we have to store all the elements in memory, and so notice that too many elements can
 * cause GC paused and eventually OutOfMemory Errors.
 *
 * @param child child expression that produce numeric column value with `child.eval(inputRow)`
 * @param percentageExpression Expression that represents a single percentage value or an array of
 *                             percentage values. Each percentage value must be in the range
 *                             [0.0, 1.0].
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """
      _FUNC_(col, percentage [, frequency]) - Returns the exact percentile value of numeric,
       ANSI interval, TIME or nanosecond-precision timestamp column `col` at the given percentage.
       The value of percentage must be between 0.0 and 1.0. The value of frequency should be
       positive integral

      _FUNC_(col, array(percentage1 [, percentage2]...) [, frequency]) - Returns the exact
      percentile value array of numeric column `col` at the given percentage(s). Each value
      of the percentage array must be between 0.0 and 1.0. The value of frequency should be
      positive integral

      """,
  arguments = """
    Arguments:
      * col - The column to compute the percentile of.
          An expression that evaluates to a numeric, ANSI interval, time, or nanosecond-precision
          timestamp (TIMESTAMP_LTZ(p) / TIMESTAMP_NTZ(p), p in [7, 9]).
      * percentage - The percentile(s) to compute, each between 0.0 and 1.0. Either a
          single numeric value or an array of numeric values. Must be foldable.
      * frequency - Optional. The number of times each value should be counted.
          An expression that evaluates to a positive integral value. Defaults to 1.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 0.3) FROM VALUES (0), (10) AS tab(col);
       3.0
      > SELECT _FUNC_(col, array(0.25, 0.75)) FROM VALUES (0), (10) AS tab(col);
       [2.5,7.5]
      > SELECT _FUNC_(col, 0.5) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-5
      > SELECT _FUNC_(col, array(0.2, 0.5)) FROM VALUES (INTERVAL '0' SECOND), (INTERVAL '10' SECOND) AS tab(col);
       [0 00:00:02.000000000,0 00:00:05.000000000]
  """,
  group = "agg_funcs",
  since = "2.1.0")
// scalastyle:on line.size.limit
case class Percentile(
    child: Expression,
    percentageExpression: Expression,
    frequencyExpression : Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    reverse: Boolean = false) extends PercentileBase with TernaryLike[Expression] {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, Literal(1L), 0, 0)
  }

  def this(child: Expression, percentageExpression: Expression, frequency: Expression) = {
    this(child, percentageExpression, frequency, 0, 0)
  }

  def this(child: Expression, percentageExpression: Expression, reverse: Boolean) = {
    this(child, percentageExpression, Literal(1L), reverse = reverse)
  }

  override def first: Expression = child
  override def second: Expression = percentageExpression
  override def third: Expression = frequencyExpression

  override def prettyName: String = "percentile"

  override def discrete: Boolean = false

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): Percentile =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): Percentile =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def stringArgs: Iterator[Any] = if (discrete) {
    super.stringArgs ++ Some(discrete)
  } else {
    super.stringArgs
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Percentile = copy(
    child = newFirst,
    percentageExpression = newSecond,
    frequencyExpression = newThird
  )
}

@ExpressionDescription(
  usage = "_FUNC_(col) - Returns the median of numeric, ANSI interval, TIME or " +
    "nanosecond-precision timestamp column `col`.",
  arguments = """
    Arguments:
      * col - The column to compute the median of.
        An expression that evaluates to a numeric, interval, time, or nanosecond-precision
        timestamp (TIMESTAMP_LTZ(p) / TIMESTAMP_NTZ(p), p in [7, 9]).
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (0), (10) AS tab(col);
       5.0
      > SELECT _FUNC_(col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-5
  """,
  group = "agg_funcs",
  since = "3.4.0")
case class Median(child: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with UnaryLike[Expression] {
  private lazy val percentile = new Percentile(child, Literal(0.5, DoubleType))
  override lazy val replacement: Expression = percentile
  override def nodeName: String = "median"
  override def inputTypes: Seq[AbstractDataType] = percentile.inputTypes.take(1)
  override protected def withNewChildInternal(
      newChild: Expression): Median = this.copy(child = newChild)
}

/**
 * Return a percentile value based on a continuous distribution of numeric, ANSI interval, TIME or
 * nanosecond-precision timestamp column at the given percentage (specified in ORDER BY clause).
 * The value of percentage must be between 0.0 and 1.0.
 */
case class PercentileCont(left: Expression, right: Expression, reverse: Boolean = false)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with SupportsOrderingWithinGroup
  with BinaryLike[Expression] {
  private lazy val percentile = new Percentile(left, right, reverse)
  override lazy val replacement: Expression = percentile
  override def nodeName: String = "percentile_cont"
  override def inputTypes: Seq[AbstractDataType] = percentile.inputTypes
  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    val direction = if (reverse) " DESC" else ""
    s"$prettyName($distinct${right.sql}) WITHIN GROUP (ORDER BY ${left.sql}$direction)"
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    percentile.checkInputDataTypes()
  }

  override def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction = {
    if (orderingWithinGroup.length != 1) {
      throw QueryCompilationErrors.wrongNumOrderingsForFunctionError(
        nodeName, 1, orderingWithinGroup.length)
    }
    orderingWithinGroup.head match {
      case SortOrder(child, Ascending, _, _) => this.copy(left = child)
      case SortOrder(child, Descending, _, _) => this.copy(left = child, reverse = true)
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PercentileCont =
    this.copy(left = newLeft, right = newRight)

  override def orderingFilled: Boolean = left != UnresolvedWithinGroup
  override def isOrderingMandatory: Boolean = true
  override def isDistinctSupported: Boolean = false
}

/**
 * The Percentile aggregate function returns the percentile(s) based on a discrete distribution of
 * numeric column `expr` at the given percentage(s) with value range in [0.0, 1.0].
 *
 * Because the number of elements and their partial order cannot be determined in advance.
 * Therefore we have to store all the elements in memory, and so notice that too many elements can
 * cause GC paused and eventually OutOfMemory Errors.
 */
case class PercentileDisc(
    child: Expression,
    percentageExpression: Expression,
    reverse: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    legacyCalculation: Boolean = SQLConf.get.getConf(SQLConf.LEGACY_PERCENTILE_DISC_CALCULATION))
  extends PercentileBase with SupportsOrderingWithinGroup with BinaryLike[Expression] {

  val frequencyExpression: Expression = Literal(1L)

  override def left: Expression = child
  override def right: Expression = percentageExpression

  override def prettyName: String = "percentile_disc"

  override def discrete: Boolean = true

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): PercentileDisc =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): PercentileDisc =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    val direction = if (reverse) " DESC" else ""
    s"$prettyName($distinct${right.sql}) WITHIN GROUP (ORDER BY ${left.sql}$direction)"
  }

  override def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction = {
    if (orderingWithinGroup.length != 1) {
      throw QueryCompilationErrors.wrongNumOrderingsForFunctionError(
        nodeName, 1, orderingWithinGroup.length)
    }
    orderingWithinGroup.head match {
      case SortOrder(expr, Ascending, _, _) => this.copy(child = expr)
      case SortOrder(expr, Descending, _, _) => this.copy(child = expr, reverse = true)
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PercentileDisc = copy(
    child = newLeft,
    percentageExpression = newRight
  )

  override protected def getPercentile(
      accumulatedCounts: Seq[(AnyRef, Long)],
      percentile: Double): Any = {
    if (legacyCalculation) {
      super.getPercentile(accumulatedCounts, percentile)
    } else {
      // `percentile_disc(p)` returns the value with the smallest `cume_dist()` value given that is
      // greater than or equal to `p` so `position` here is `p` adjusted by max position.
      val position = accumulatedCounts.last._2 * percentile

      val higher = position.ceil.toLong

      // Use binary search to find the higher position.
      val countsArray = accumulatedCounts.map(_._2).toArray[Long]
      val higherIndex = binarySearchCount(countsArray, 0, accumulatedCounts.size, higher)
      val higherKey = accumulatedCounts(higherIndex)._1

      toPercentileValue(higherKey)
    }
  }

  override def orderingFilled: Boolean = left != UnresolvedWithinGroup
  override def isOrderingMandatory: Boolean = true
  override def isDistinctSupported: Boolean = false
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(percentage) WITHIN GROUP (ORDER BY col) - Return a percentile value based on " +
    "a continuous distribution of numeric, ANSI interval, TIME or nanosecond-precision timestamp " +
    "column `col` at the given `percentage` (specified in ORDER BY clause).",
  arguments = """
    Arguments:
      * percentage - The percentile to compute, between 0.0 and 1.0. Must be foldable.
      * col - The column to compute the percentile of, specified in the ORDER BY clause.
          An expression that evaluates to a numeric, ANSI interval, time, or nanosecond-precision
          timestamp (TIMESTAMP_LTZ(p) / TIMESTAMP_NTZ(p), p in [7, 9]).
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0.25) WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10) AS tab(col);
       2.5
      > SELECT _FUNC_(0.25) WITHIN GROUP (ORDER BY col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-2
  """,
  group = "agg_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
object PercentileContBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1) {
      PercentileCont(UnresolvedWithinGroup, expressions(0))
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(percentage) WITHIN GROUP (ORDER BY col) - Return a percentile value based on " +
    "a discrete distribution of numeric, ANSI interval, TIME or nanosecond-precision timestamp " +
    "column `col` at the given `percentage` (specified in ORDER BY clause).",
  arguments = """
    Arguments:
      * percentage - The percentile to compute, between 0.0 and 1.0. Must be foldable.
      * col - The column to compute the percentile of, specified in the ORDER BY clause.
          An expression that evaluates to a numeric, ANSI interval, time, or nanosecond-precision
          timestamp (TIMESTAMP_LTZ(p) / TIMESTAMP_NTZ(p), p in [7, 9]).
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0.25) WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10) AS tab(col);
       0.0
      > SELECT _FUNC_(0.25) WITHIN GROUP (ORDER BY col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-0
  """,
  group = "agg_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
object PercentileDiscBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1) {
      PercentileDisc(UnresolvedWithinGroup, expressions(0))
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1), numArgs)
    }
  }
}
