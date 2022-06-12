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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

abstract class PercentileBase extends TypedImperativeAggregate[OpenHashMap[AnyRef, Long]]
  with ImplicitCastInputTypes {

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

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override lazy val dataType: DataType = percentageExpression.dataType match {
    case _: ArrayType => ArrayType(DoubleType, false)
    case _ => DoubleType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    val percentageExpType = percentageExpression.dataType match {
      case _: ArrayType => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
    Seq(NumericType, percentageExpType, IntegralType)
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
      TypeCheckFailure("The percentage(s) must be a constant literal, " +
        s"but got $percentageExpression")
    } else if (percentages == null) {
      TypeCheckFailure("Percentage value must not be null")
    } else if (percentages.exists(percentage => percentage < 0.0 || percentage > 1.0)) {
      // percentages(s) must be in the range [0.0, 1.0]
      TypeCheckFailure("Percentage(s) must be between 0.0 and 1.0, " +
        s"but got $percentageExpression")
    } else {
      TypeCheckSuccess
    }
  }

  protected def toDoubleValue(d: Any): Double = d match {
    case d: Decimal => d.toDouble
    case n: Number => n.doubleValue
  }

  override def createAggregationBuffer(): OpenHashMap[AnyRef, Long] = {
    // Initialize new counts map instance here.
    new OpenHashMap[AnyRef, Long]()
  }

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
        throw QueryExecutionErrors.negativeValueUnexpectedError(frequencyExpression)
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

  private def getPercentiles(buffer: OpenHashMap[AnyRef, Long]): Seq[Double] = {
    if (buffer.isEmpty) {
      return Seq.empty
    }

    val ordering = child.dataType match {
      case numericType: NumericType => numericType.ordering
      case intervalType: YearMonthIntervalType => intervalType.ordering
      case intervalType: DayTimeIntervalType => intervalType.ordering
      case otherType => QueryExecutionErrors.unsupportedTypeError(otherType)
    }
    val sortedCounts = if (reverse) {
      buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]].reverse)
    } else {
      buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]])
    }
    val accumulatedCounts = sortedCounts.scanLeft((sortedCounts.head._1, 0L)) {
      case ((key1, count1), (key2, count2)) => (key2, count1 + count2)
    }.tail
    val maxPosition = accumulatedCounts.last._2 - 1

    percentages.map { percentile =>
      getPercentile(accumulatedCounts, maxPosition * percentile)
    }
  }

  private def generateOutput(results: Seq[Double]): Any = {
    if (results.isEmpty) {
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
  private def getPercentile(
      accumulatedCounts: Seq[(AnyRef, Long)], position: Double): Double = {
    // We may need to do linear interpolation to get the exact percentile
    val lower = position.floor.toLong
    val higher = position.ceil.toLong

    // Use binary search to find the lower and the higher position.
    val countsArray = accumulatedCounts.map(_._2).toArray[Long]
    val lowerIndex = binarySearchCount(countsArray, 0, accumulatedCounts.size, lower + 1)
    val higherIndex = binarySearchCount(countsArray, 0, accumulatedCounts.size, higher + 1)

    val lowerKey = accumulatedCounts(lowerIndex)._1
    if (higher == lower) {
      // no interpolation needed because position does not have a fraction
      return toDoubleValue(lowerKey)
    }

    val higherKey = accumulatedCounts(higherIndex)._1
    if (higherKey == lowerKey) {
      // no interpolation needed because lower position and higher position has the same key
      return toDoubleValue(lowerKey)
    }

    if (discrete) {
      toDoubleValue(lowerKey)
    } else {
      // Linear interpolation to get the exact percentile
      (higher - position) * toDoubleValue(lowerKey) + (position - lower) * toDoubleValue(higherKey)
    }
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

  private lazy val projection = UnsafeProjection.create(Array[DataType](child.dataType, LongType))

  override def serialize(obj: OpenHashMap[AnyRef, Long]): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10)  // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    try {
      // Write pairs in counts map to byte buffer.
      obj.foreach { case (key, count) =>
        val row = InternalRow.apply(key, count)
        val unsafeRow = projection.apply(row)
        out.writeInt(unsafeRow.getSizeInBytes)
        unsafeRow.writeToStream(out, buffer)
      }
      out.writeInt(-1)
      out.flush()

      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }

  override def deserialize(bytes: Array[Byte]): OpenHashMap[AnyRef, Long] = {
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)
    try {
      val counts = new OpenHashMap[AnyRef, Long]
      // Read unsafeRow size and content in bytes.
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(2)
        row.pointTo(bs, sizeOfNextRow)
        // Insert the pairs into counts map.
        val key = row.get(0, child.dataType)
        val count = row.get(1, LongType).asInstanceOf[Long]
        counts.update(key, count)
        sizeOfNextRow = ins.readInt()
      }

      counts
    } finally {
      ins.close()
      bis.close()
    }
  }
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
      _FUNC_(col, percentage [, frequency]) - Returns the exact percentile value of numeric
       column `col` at the given percentage. The value of percentage must be
       between 0.0 and 1.0. The value of frequency should be positive integral

      _FUNC_(col, array(percentage1 [, percentage2]...) [, frequency]) - Returns the exact
      percentile value array of numeric column `col` at the given percentage(s). Each value
      of the percentage array must be between 0.0 and 1.0. The value of frequency should be
      positive integral

      """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 0.3) FROM VALUES (0), (10) AS tab(col);
       3.0
      > SELECT _FUNC_(col, array(0.25, 0.75)) FROM VALUES (0), (10) AS tab(col);
       [2.5,7.5]
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
  usage = "_FUNC_(col) - Returns the median of numeric column `col`.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (0), (10) AS tab(col);
       5.0
  """,
  group = "agg_funcs",
  since = "3.4.0")
case class Median(child: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with UnaryLike[Expression] {
  private lazy val percentile = new Percentile(child, Literal(0.5, DoubleType))
  override def replacement: Expression = percentile
  override def nodeName: String = "median"
  override def inputTypes: Seq[AbstractDataType] = percentile.inputTypes.take(1)
  override protected def withNewChildInternal(
      newChild: Expression): Median = this.copy(child = newChild)
}

/**
 * Return a percentile value based on a continuous distribution of
 * numeric column at the given percentage (specified in ORDER BY clause).
 * The value of percentage must be between 0.0 and 1.0.
 */
case class PercentileCont(left: Expression, right: Expression, reverse: Boolean = false)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  private lazy val percentile = new Percentile(left, right, reverse)
  override def replacement: Expression = percentile
  override def nodeName: String = "percentile_cont"
  override def inputTypes: Seq[AbstractDataType] = percentile.inputTypes
  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    val direction = if (reverse) " DESC" else ""
    s"$prettyName($distinct${right.sql}) WITHIN GROUP (ORDER BY v$direction)"
  }
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PercentileCont =
    this.copy(left = newLeft, right = newRight)
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
    inputAggBufferOffset: Int = 0) extends PercentileBase with BinaryLike[Expression] {

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
    s"$prettyName($distinct${right.sql}) WITHIN GROUP (ORDER BY v$direction)"
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PercentileDisc = copy(
    child = newLeft,
    percentageExpression = newRight
  )
}
