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

import org.apache.datasketches.tuple.{Intersection, Summary, SummarySetOperations}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory, IntegerSummarySetOperations}

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ThetaSketchUtils, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2, mode) - Intersects two binary representations of Datasketches
    TupleSketch objects with double summary data type using a TupleSketch Intersection object.
    Users can set mode to 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), tuple_sketch_agg_double(col2, val2))) FROM VALUES (1, 1.0D, 1, 4.0D), (2, 2.0D, 2, 5.0D), (3, 3.0D, 4, 6.0D) tab(col1, val1, col2, val2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionDouble(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase[DoubleSummary, DoubleSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionDouble =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_double"

  override protected def convertMode(tupleSummaryMode: TupleSummaryMode): DoubleSummary.Mode =
    tupleSummaryMode.toDoubleSummaryMode

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch1 = TupleSketchUtils.heapifyDoubleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyDoubleSketch(sketch2Bytes, prettyName)

    intersection.intersect(tupleSketch1)
    intersection.intersect(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch, mode) - Intersects the binary representation of a
    Datasketches TupleSketch with double summary data type with the binary representation of a
    Datasketches ThetaSketch using a TupleSketch Intersection object. The ThetaSketch entries are
    assigned a default double summary value. Users can set mode to 'sum', 'min', 'max', or 'alwaysone'
    (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), theta_sketch_agg(col2))) FROM VALUES (1, 1.0D, 1), (2, 2.0D, 2), (3, 3.0D, 4) tab(col1, val1, col2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionThetaDouble(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase[DoubleSummary, DoubleSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionThetaDouble =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_theta_double"

  override protected def convertMode(tupleSummaryMode: TupleSummaryMode): DoubleSummary.Mode =
    tupleSummaryMode.toDoubleSummaryMode

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch = TupleSketchUtils.heapifyDoubleSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    val defaultSummary = new DoubleSummaryFactory(mode).newSummary()

    intersection.intersect(tupleSketch)
    intersection.intersect(thetaSketch, defaultSummary)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2, mode) - Intersects two binary representations of Datasketches
    TupleSketch objects with integer summary data type using a TupleSketch Intersection object.
    Users can set mode to 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), tuple_sketch_agg_integer(col2, val2))) FROM VALUES (1, 1, 1, 4), (2, 2, 2, 5), (3, 3, 4, 6) tab(col1, val1, col2, val2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionInteger(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase[IntegerSummary, IntegerSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionInteger =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_integer"

  override protected def convertMode(tupleSummaryMode: TupleSummaryMode): IntegerSummary.Mode =
    tupleSummaryMode.toIntegerSummaryMode

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch1 = TupleSketchUtils.heapifyIntegerSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyIntegerSketch(sketch2Bytes, prettyName)

    intersection.intersect(tupleSketch1)
    intersection.intersect(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch, mode) - Intersects the binary representation of a
    Datasketches TupleSketch with integer summary data type with the binary representation of a
    Datasketches ThetaSketch using a TupleSketch Intersection object. The ThetaSketch entries are
    assigned a default integer summary value. Users can set mode to 'sum', 'min', 'max', or 'alwaysone'
    (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), theta_sketch_agg(col2))) FROM VALUES (1, 1, 1), (2, 2, 2), (3, 3, 4) tab(col1, val1, col2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleIntersectionThetaInteger(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase[IntegerSummary, IntegerSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionThetaInteger =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_theta_integer"

  override protected def convertMode(tupleSummaryMode: TupleSummaryMode): IntegerSummary.Mode =
    tupleSummaryMode.toIntegerSummaryMode

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch = TupleSketchUtils.heapifyIntegerSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    val defaultSummary = new IntegerSummaryFactory(mode).newSummary()

    intersection.intersect(tupleSketch)
    intersection.intersect(thetaSketch, defaultSummary)
  }
}

abstract class TupleIntersectionBase[S <: Summary, M]
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  protected def convertMode(tupleSummaryMode: TupleSummaryMode): M

  protected def createSummarySetOperations(mode: M): SummarySetOperations[S]

  protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[S],
      mode: M): Unit

  override def nullSafeEval(sketch1Binary: Any, sketch2Binary: Any, modeInput: Any): Any = {

    val modeStr = modeInput.asInstanceOf[UTF8String].toString
    val tupleSummaryMode = TupleSummaryMode.fromString(modeStr, prettyName)
    val mode = convertMode(tupleSummaryMode)

    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val summarySetOps = createSummarySetOperations(mode)
    val intersection = new Intersection(summarySetOps)

    intersectSketches(sketch1Bytes, sketch2Bytes, intersection, mode)

    intersection.getResult.toByteArray
  }
}
