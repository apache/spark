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

import org.apache.datasketches.tuple.{AnotB, Intersection, Summary, TupleSketchIterator, Union}
import org.apache.datasketches.tuple.adouble.DoubleSummary
import org.apache.datasketches.tuple.aninteger.IntegerSummary

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ThetaSketchUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, DoubleType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, summaryType]) - Returns the estimated number of unique values
    given the binary representation of a Datasketches TupleSketch. The summaryType
    parameter can be 'double', 'integer', or 'string' (defaults to 'double'). """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg(struct(col, value))) FROM VALUES (1, 1.0), (1, 2.0), (2, 3.0) tab(col, value);
       2
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchEstimate(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_estimate"

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleSketchEstimate =
    copy(left = newLeft, right = newRight)

  override def nullSafeEval(input: Any, summaryTypeInput: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val summaryType = summaryTypeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkSummaryType(summaryType, prettyName)

    val sketch = ThetaSketchUtils.heapifyTupleSketch(buffer, summaryType, prettyName)
    sketch.getEstimate()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, summaryType, mode]) - Aggregates the summary values from a
    Datasketches TupleSketch. The summaryType can be 'double', 'integer', or 'string'
    (defaults to 'double'). The mode can be 'sum', 'min', 'max', or 'alwaysone'
    (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg(struct(col, value))) FROM VALUES (1, 1.0), (1, 2.0), (2, 3.0) tab(col, value);
       6.0
      > SELECT _FUNC_(tuple_sketch_agg(struct(col, value))) FROM VALUES (1, 1.0), (1, 2.0), (2, 3.0) tab(col, value);
       3.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchSummary(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  def this(first: Expression) = {
    this(first, Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE), Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_summary"

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleSketchSummary =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def nullSafeEval(input: Any, summaryTypeInput: Any, modeInput: Any): Any = {

    val buffer = input.asInstanceOf[Array[Byte]]
    val summaryType = summaryTypeInput.asInstanceOf[UTF8String].toString
    val mode = modeInput.asInstanceOf[UTF8String].toString

    ThetaSketchUtils.checkSummaryType(summaryType, prettyName)
    ThetaSketchUtils.checkMode(mode, prettyName)

    val sketch = ThetaSketchUtils.heapifyTupleSketch(buffer, summaryType, prettyName)

    summaryType match {
      case ThetaSketchUtils.SUMMARY_TYPE_DOUBLE =>
        aggregateNumericSummaries(
          sketch.iterator().asInstanceOf[TupleSketchIterator[DoubleSummary]],
          mode,
          (it: TupleSketchIterator[DoubleSummary]) => it.getSummary.getValue)

      case ThetaSketchUtils.SUMMARY_TYPE_INTEGER =>
        aggregateNumericSummaries(
          sketch.iterator().asInstanceOf[TupleSketchIterator[IntegerSummary]],
          mode,
          (it: TupleSketchIterator[IntegerSummary]) => it.getSummary.getValue.toDouble)

      case ThetaSketchUtils.SUMMARY_TYPE_STRING =>
        throw QueryExecutionErrors.tupleInvalidOperationForSummaryType(prettyName, summaryType)
    }
  }

  /**
   * Generic aggregation function that works for any numeric summary type.
   *
   * @param iterator
   *   The sketch iterator
   * @param mode
   *   The aggregation mode (sum, min, max, alwaysone)
   * @param getValue
   *   Function to extract the numeric value from the summary
   * @return
   *   The aggregated result
   */
  private def aggregateNumericSummaries[S <: Summary](
      iterator: TupleSketchIterator[S],
      mode: String,
      getValue: TupleSketchIterator[S] => Double): Double = {

    mode match {
      case ThetaSketchUtils.MODE_SUM =>
        var sum = 0.0
        while (iterator.next()) {
          sum += getValue(iterator)
        }
        sum

      case ThetaSketchUtils.MODE_MIN =>
        var min = Double.MaxValue
        while (iterator.next()) {
          min = Math.min(min, getValue(iterator))
        }
        if (min == Double.MaxValue) 0.0 else min

      case ThetaSketchUtils.MODE_MAX =>
        var max = Double.MinValue
        while (iterator.next()) {
          max = Math.max(max, getValue(iterator))
        }
        if (max == Double.MinValue) 0.0 else max

      case ThetaSketchUtils.MODE_ALWAYSONE =>
        var count = 0.0
        while (iterator.next()) {
          count += 1.0
        }
        count
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second[, lgNomEntries, summaryType, mode]) - Merges two binary representations of
    Datasketches TupleSketch objects using a TupleSketch Union object. Users can set lgNomEntries
    to a value between 4 and 26 (defaults to 12), summaryType to 'double', 'integer', or 'string'
    (defaults to 'double'), and mode to 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), tuple_sketch_agg(struct(col2, val2)))) FROM VALUES (1, 1.0, 4, 4.0), (2, 2.0, 5, 5.0), (3, 3.0, 6, 6.0) tab(col1, val1, col2, val2);
       6
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleUnion(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression,
    fifth: Expression)
    extends TupleUnionBase {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(
      first,
      second,
      third,
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression, fourth: Expression) = {
    this(first, second, third, fourth, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TupleUnion =
    copy(
      first = newChildren(0),
      second = newChildren(1),
      third = newChildren(2),
      fourth = newChildren(3),
      fifth = newChildren(4))

  override def prettyName: String = "tuple_union"

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[Summary],
      summaryType: String,
      mode: String): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)

    val tupleSketch2 = ThetaSketchUtils.heapifyTupleSketch(sketch2Bytes, summaryType, prettyName)

    union.union(tupleSketch1)
    union.union(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch[, lgNomEntries, summaryType, mode]) - Merges a TupleSketch with
    a ThetaSketch using a TupleSketch Union object. The ThetaSketch entries are assigned a default
    summary value. Users can set lgNomEntries to a value between 4 and 26 (defaults to 12),
    summaryType to 'double', 'integer', or 'string' (defaults to 'double'), and mode to 'sum', 'min',
    'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), theta_sketch_agg(col2))) FROM VALUES (1, 1.0, 4), (2, 2.0, 5), (3, 3.0, 6) tab(col1, val1, col2);
       6
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleUnionTheta(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression,
    fifth: Expression)
    extends TupleUnionBase {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(
      first,
      second,
      third,
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression, fourth: Expression) = {
    this(first, second, third, fourth, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TupleUnionTheta =
    copy(
      first = newChildren(0),
      second = newChildren(1),
      third = newChildren(2),
      fourth = newChildren(3),
      fifth = newChildren(4))

  override def prettyName: String = "tuple_union_theta"

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[Summary],
      summaryType: String,
      mode: String): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)
    val summaryFactory = ThetaSketchUtils.getSummaryFactory(summaryType, mode)

    val defaultSummary = summaryFactory.newSummary()

    union.union(tupleSketch)
    union.union(thetaSketch, defaultSummary)
  }
}

abstract class TupleUnionBase
    extends QuinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  def first: Expression
  def second: Expression
  def third: Expression
  def fourth: Expression
  def fifth: Expression

  override def nullIntolerant: Boolean = true

  override def children: Seq[Expression] =
    Seq(first, second, third, fourth, fifth)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      BinaryType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  /**
   * Performs the union operation on two sketches. Subclasses implement this to handle Tuple-Tuple
   * or Tuple-Theta union.
   *
   * @param sketch1Bytes
   *   The first sketch (always a Tuple sketch)
   * @param sketch2Bytes
   *   The second sketch bytes (may be Tuple or Theta)
   * @param union
   *   The union object to use
   * @param summaryType
   *   The summary type
   * @param mode
   *   The aggregation mode
   * @param prettyName
   *   The function name for error messages
   */
  protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[Summary],
      summaryType: String,
      mode: String): Unit

  override def nullSafeEval(
      sketch1Binary: Any,
      sketch2Binary: Any,
      lgNomEntries: Any,
      summaryTypeInput: Any,
      modeInput: Any): Any = {

    // Validate parameters
    val logNominalEntries = lgNomEntries.asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(logNominalEntries, prettyName)

    val summaryType = summaryTypeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkSummaryType(summaryType, prettyName)

    val mode = modeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkMode(mode, prettyName)

    // Deserialize first sketch
    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    // Create union and add first sketch
    val union = createUnion(logNominalEntries, summaryType, mode)

    unionSketches(sketch1Bytes, sketch2Bytes, union, summaryType, mode)

    union.getResult.toByteArray
  }

  /**
   * Creates a Union instance configured with the appropriate summary operations for the given
   * summary type and aggregation mode.
   */
  protected def createUnion(
      logNominalEntries: Int,
      summaryType: String,
      mode: String): Union[Summary] = {
    val nominalEntries = 1 << logNominalEntries
    val summarySetOps = ThetaSketchUtils.getSummarySetOperations(summaryType, mode)
    new Union(nominalEntries, summarySetOps).asInstanceOf[Union[Summary]]
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second[, summaryType, mode]) - Intersects two binary representations of
    Datasketches TupleSketch objects using a TupleSketch Intersection object. The summaryType
    can be 'double', 'integer', or 'string' (defaults to 'double'), and mode can be 'sum', 'min',
    'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), tuple_sketch_agg(struct(col2, val2)))) FROM VALUES (1, 1.0, 1, 4.0), (2, 2.0, 2, 5.0), (3, 3.0, 4, 6.0) tab(col1, val1, col2, val2);
       2
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersection(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleIntersectionBase {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleIntersection =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_intersection"

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[Summary],
      summaryType: String,
      mode: String): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)

    val tupleSketch2 = ThetaSketchUtils.heapifyTupleSketch(sketch2Bytes, summaryType, prettyName)

    intersection.intersect(tupleSketch1)
    intersection.intersect(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch[, summaryType, mode]) - Intersects a TupleSketch with
    a ThetaSketch using a TupleSketch Intersection object. The ThetaSketch entries are assigned
    a default summary value. The summaryType can be 'double', 'integer', or 'string' (defaults to
    'double'), and mode can be 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), theta_sketch_agg(col2))) FROM VALUES (1, 1.0, 1), (2, 2.0, 2), (3, 3.0, 4) tab(col1, val1, col2);
       2
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionTheta(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleIntersectionBase {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleIntersectionTheta =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_intersection_theta"

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[Summary],
      summaryType: String,
      mode: String): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)
    val summaryFactory = ThetaSketchUtils.getSummaryFactory(summaryType, mode)

    val defaultSummary = summaryFactory.newSummary()

    intersection.intersect(tupleSketch)
    intersection.intersect(thetaSketch, defaultSummary)
  }
}

abstract class TupleIntersectionBase
    extends QuaternaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  /**
   * Performs the intersection operation on two sketches. Subclasses implement this to handle
   * Tuple-Tuple or Tuple-Theta intersection.
   *
   * @param sketch1Bytes
   *   The first sketch (always a Tuple sketch)
   * @param sketch2Bytes
   *   The second sketch bytes (may be Tuple or Theta)
   * @param intersection
   *   The intersection object to use
   * @param summaryType
   *   The summary type
   * @param mode
   *   The aggregation mode
   */
  protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[Summary],
      summaryType: String,
      mode: String): Unit

  override def nullSafeEval(
      sketch1Binary: Any,
      sketch2Binary: Any,
      summaryTypeInput: Any,
      modeInput: Any): Any = {

    // Validate parameters (no logNominalEntries needed for Intersection)
    val summaryType = summaryTypeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkSummaryType(summaryType, prettyName)

    val mode = modeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkMode(mode, prettyName)

    // Deserialize sketches
    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    // Create intersection
    val intersection = createIntersection(summaryType, mode)

    intersectSketches(sketch1Bytes, sketch2Bytes, intersection, summaryType, mode)

    intersection.getResult.toByteArray
  }

  /**
   * Creates an Intersection instance configured with the appropriate summary operations for the
   * given summary type and aggregation mode.
   */
  protected def createIntersection(summaryType: String, mode: String): Intersection[Summary] = {
    val summarySetOps = ThetaSketchUtils.getSummarySetOperations(summaryType, mode)
    new Intersection(summarySetOps).asInstanceOf[Intersection[Summary]]
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second[, summaryType]) - Subtracts two binary representations of
    Datasketches TupleSketch objects using a TupleSketch AnotB object. The summaryType
    can be 'double', 'integer', or 'string' (defaults to 'double'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), tuple_sketch_agg(struct(col2, val2)))) FROM VALUES (5, 5.0, 4, 4.0), (1, 1.0, 4, 4.0), (2, 2.0, 5, 5.0), (3, 3.0, 1, 1.0) tab(col1, val1, col2, val2);
       2
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifference(first: Expression, second: Expression, third: Expression)
    extends TupleDifferenceBase {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleDifference =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_difference"

  override protected def aNotBSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[Summary],
      summaryType: String): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)

    val tupleSketch2 = ThetaSketchUtils.heapifyTupleSketch(sketch2Bytes, summaryType, prettyName)

    aNotB.setA(tupleSketch1)
    aNotB.notB(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch[, summaryType]) - Subtracts a ThetaSketch from a
    TupleSketch using a TupleSketch AnotB object. The summaryType can be 'double',
    'integer', or 'string' (defaults to 'double'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate(_FUNC_(tuple_sketch_agg(struct(col1, val1)), theta_sketch_agg(col2))) FROM VALUES (5, 5.0, 4), (1, 1.0, 4), (2, 2.0, 5), (3, 3.0, 1) tab(col1, val1, col2);
       2
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifferenceTheta(first: Expression, second: Expression, third: Expression)
    extends TupleDifferenceBase {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaSketchUtils.SUMMARY_TYPE_DOUBLE))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleDifferenceTheta =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_difference_theta"

  override protected def aNotBSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[Summary],
      summaryType: String): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyTupleSketch(sketch1Bytes, summaryType, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch)
    aNotB.notB(thetaSketch)
  }
}

abstract class TupleDifferenceBase
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  /**
   * Performs the A-NOT-B operation on two sketches. Subclasses implement this to handle
   * Tuple-Tuple or Tuple-Theta A-NOT-B.
   *
   * @param sketch1Bytes
   *   The first sketch (A)
   * @param sketch2Bytes
   *   The second sketch (B)
   * @param aNotB
   *   The AnotB operator to use
   * @param summaryType
   *   The summary type
   */
  protected def aNotBSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[Summary],
      summaryType: String): Unit

  override def nullSafeEval(
      sketch1Binary: Any,
      sketch2Binary: Any,
      summaryTypeInput: Any): Any = {

    // Validate parameters (no logNominalEntries needed for AnotB)
    val summaryType = summaryTypeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkSummaryType(summaryType, prettyName)

    // Deserialize sketches
    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    // Create AnotB operation
    val aNotB = new AnotB[Summary]()

    aNotBSketches(sketch1Bytes, sketch2Bytes, aNotB, summaryType)

    aNotB.getResult(true).toByteArray
  }

}
