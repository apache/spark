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

import org.apache.datasketches.tuple.{AnotB, Intersection, Summary, SummarySetOperations, TupleSketchIterator, Union}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummaryFactory, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummaryFactory, IntegerSummarySetOperations}

import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.util.ThetaSketchUtils
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, DoubleType, IntegerType, LongType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns the estimated number of unique values
    given the binary representation of a Datasketches TupleSketch. The sketch's
    summary type must be a double. """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_double(key, summary)) FROM VALUES (1, 1.0D), (1, 2.0D), (2, 3.0D) tab(key, summary);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchEstimateDouble(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_estimate_double"

  override protected def withNewChildInternal(newChild: Expression): TupleSketchEstimateDouble =
    copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val sketch = ThetaSketchUtils.heapifyDoubleTupleSketch(buffer, prettyName)
    sketch.getEstimate()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns the estimated number of unique values
    given the binary representation of a Datasketches TupleSketch. The sketch's
    summary type must be an integer. """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_integer(key, summary)) FROM VALUES (1, 1), (1, 2), (2, 3) tab(key, summary);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchEstimateInteger(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_estimate_integer"

  override protected def withNewChildInternal(newChild: Expression): TupleSketchEstimateInteger =
    copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val sketch = ThetaSketchUtils.heapifyIntegerTupleSketch(buffer, prettyName)
    sketch.getEstimate()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, mode) - Aggregates the summary values from a double summary type
    Datasketches TupleSketch. The mode can be 'sum', 'min', 'max', or 'alwaysone'
    (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_double(key, summary)) FROM VALUES (1, 1.0D), (1, 2.0D), (2, 3.0D) tab(key, summary);
       6.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchSummaryDouble(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_summary_double"

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): TupleSketchSummaryDouble =
    copy(left = newFirst, right = newSecond)

  override def nullSafeEval(input: Any, modeInput: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val mode = modeInput.asInstanceOf[UTF8String].toString

    ThetaSketchUtils.checkMode(mode, prettyName)

    val sketch = ThetaSketchUtils.heapifyDoubleTupleSketch(buffer, prettyName)

    ThetaSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
      sketch.iterator(),
      mode,
      (it: TupleSketchIterator[DoubleSummary]) => it.getSummary.getValue)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, mode) - Aggregates the summary values from a integer summary type
    Datasketches TupleSketch. The mode can be 'sum', 'min', 'max', or 'alwaysone'
    (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_integer(key, summary)) FROM VALUES (1, 1), (1, 2), (2, 3) tab(key, summary);
       6
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchSummaryInteger(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = LongType

  override def prettyName: String = "tuple_sketch_summary_integer"

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): TupleSketchSummaryInteger =
    copy(left = newFirst, right = newSecond)

  override def nullSafeEval(input: Any, modeInput: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val mode = modeInput.asInstanceOf[UTF8String].toString

    ThetaSketchUtils.checkMode(mode, prettyName)

    val sketch = ThetaSketchUtils.heapifyIntegerTupleSketch(buffer, prettyName)

    ThetaSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      sketch.iterator(),
      mode,
      (it: TupleSketchIterator[IntegerSummary]) => it.getSummary.getValue.toLong)
  }
}

case class TupleUnionDouble(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[DoubleSummary, DoubleSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionDouble =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_double"

  override protected def getModeFromString(modeString: String): DoubleSummary.Mode =
    ThetaSketchUtils.getDoubleSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch2Bytes, prettyName)

    union.union(tupleSketch1)
    union.union(tupleSketch2)
  }
}

case class TupleUnionThetaDouble(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[DoubleSummary, DoubleSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionThetaDouble =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_theta_double"

  override protected def getModeFromString(modeString: String): DoubleSummary.Mode =
    ThetaSketchUtils.getDoubleSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    val defaultSummary = new DoubleSummaryFactory(mode).newSummary()

    union.union(tupleSketch)
    union.union(thetaSketch, defaultSummary)
  }
}

case class TupleUnionInteger(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[IntegerSummary, IntegerSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionInteger =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_integer"

  override protected def getModeFromString(modeString: String): IntegerSummary.Mode =
    ThetaSketchUtils.getIntegerSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch2Bytes, prettyName)

    union.union(tupleSketch1)
    union.union(tupleSketch2)
  }
}

case class TupleUnionThetaInteger(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[IntegerSummary, IntegerSummary.Mode] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(ThetaSketchUtils.MODE_SUM))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionThetaInteger =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_theta_integer"

  override protected def getModeFromString(modeString: String): IntegerSummary.Mode =
    ThetaSketchUtils.getIntegerSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    val defaultSummary = new IntegerSummaryFactory(mode).newSummary()

    union.union(tupleSketch)
    union.union(thetaSketch, defaultSummary)
  }
}

abstract class TupleUnionBase[S <: Summary, M]
    extends QuaternaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      BinaryType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  protected def getModeFromString(modeString: String): M

  protected def createSummarySetOperations(mode: M): SummarySetOperations[S]

  protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[S],
      mode: M): Unit

  override def nullSafeEval(
      sketch1Binary: Any,
      sketch2Binary: Any,
      lgNomEntries: Any,
      modeInput: Any): Any = {

    val logNominalEntries = lgNomEntries.asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(logNominalEntries, prettyName)

    val modeString = modeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkMode(modeString, prettyName)

    val mode = getModeFromString(modeString)
    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val nominalEntries = 1 << logNominalEntries
    val summarySetOps = createSummarySetOperations(mode)
    val union = new Union(nominalEntries, summarySetOps)

    unionSketches(sketch1Bytes, sketch2Bytes, union, mode)

    union.getResult.toByteArray
  }
}

abstract class TupleUnionNoModeBase[S <: Summary]
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  protected def createSummarySetOperations(): SummarySetOperations[S]

  protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[S]): Unit

  override def nullSafeEval(sketch1Binary: Any, sketch2Binary: Any, lgNomEntries: Any): Any = {

    val logNominalEntries = lgNomEntries.asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(logNominalEntries, prettyName)

    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val nominalEntries = 1 << logNominalEntries
    val summarySetOps = createSummarySetOperations()
    val union = new Union(nominalEntries, summarySetOps)

    unionSketches(sketch1Bytes, sketch2Bytes, union)

    union.getResult.toByteArray
  }
}

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
    this(first, second, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionDouble =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_double"

  override protected def getModeFromString(modeString: String): DoubleSummary.Mode =
    ThetaSketchUtils.getDoubleSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch2Bytes, prettyName)

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
    this(first, second, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionThetaDouble =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_theta_double"

  override protected def getModeFromString(modeString: String): DoubleSummary.Mode =
    ThetaSketchUtils.getDoubleSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: DoubleSummary.Mode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[DoubleSummary],
      mode: DoubleSummary.Mode): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
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
    this(first, second, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionInteger =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_integer"

  override protected def getModeFromString(modeString: String): IntegerSummary.Mode =
    ThetaSketchUtils.getIntegerSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch2Bytes, prettyName)

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
    this(first, second, Literal(ThetaSketchUtils.MODE_SUM))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): TupleIntersectionThetaInteger =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "tuple_intersection_theta_integer"

  override protected def getModeFromString(modeString: String): IntegerSummary.Mode =
    ThetaSketchUtils.getIntegerSummaryMode(modeString)

  override protected def createSummarySetOperations(
      mode: IntegerSummary.Mode): SummarySetOperations[IntegerSummary] =
    new IntegerSummarySetOperations(mode, mode)

  override protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[IntegerSummary],
      mode: IntegerSummary.Mode): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
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

  protected def getModeFromString(modeString: String): M

  protected def createSummarySetOperations(mode: M): SummarySetOperations[S]

  protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[S],
      mode: M): Unit

  override def nullSafeEval(sketch1Binary: Any, sketch2Binary: Any, modeInput: Any): Any = {

    val modeString = modeInput.asInstanceOf[UTF8String].toString
    ThetaSketchUtils.checkMode(modeString, prettyName)

    val mode = getModeFromString(modeString)
    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val summarySetOps = createSummarySetOperations(mode)
    val intersection = new Intersection(summarySetOps)

    intersectSketches(sketch1Bytes, sketch2Bytes, intersection, mode)

    intersection.getResult.toByteArray
  }
}

abstract class TupleIntersectionNoModeBase[S <: Summary]
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  protected def intersectSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      intersection: Intersection[S]): Unit

  protected def createSummarySetOperations(): SummarySetOperations[S]

  override def nullSafeEval(sketch1Binary: Any, sketch2Binary: Any): Any = {

    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val summarySetOps = createSummarySetOperations()
    val intersection = new Intersection(summarySetOps)

    intersectSketches(sketch1Bytes, sketch2Bytes, intersection)

    intersection.getResult.toByteArray
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2) - Subtracts two binary representations of Datasketches
    TupleSketch objects with double summary data type using a TupleSketch AnotB object.
    Returns elements in the first sketch that are not in the second sketch. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), tuple_sketch_agg_double(col2, val2))) FROM VALUES (5, 5.0D, 4, 4.0D), (1, 1.0D, 4, 4.0D), (2, 2.0D, 5, 5.0D), (3, 3.0D, 1, 1.0D) tab(col1, val1, col2, val2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifferenceDouble(left: Expression, right: Expression)
    extends TupleDifferenceBase[DoubleSummary] {

  override def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleDifferenceDouble =
    copy(left = newLeft, right = newRight)

  override def prettyName: String = "tuple_difference_double"

  override protected def differenceSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[DoubleSummary]): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch1)
    aNotB.notB(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch) - Subtracts the binary representation of a
    Datasketches ThetaSketch from a TupleSketch with double summary data type using a TupleSketch
    AnotB object. Returns elements in the TupleSketch that are not in the ThetaSketch. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), theta_sketch_agg(col2))) FROM VALUES (5, 5.0D, 4), (1, 1.0D, 4), (2, 2.0D, 5), (3, 3.0D, 1) tab(col1, val1, col2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifferenceThetaDouble(left: Expression, right: Expression)
    extends TupleDifferenceBase[DoubleSummary] {

  override def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleDifferenceThetaDouble =
    copy(left = newLeft, right = newRight)

  override def prettyName: String = "tuple_difference_theta_double"

  override protected def differenceSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[DoubleSummary]): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyDoubleTupleSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch)
    aNotB.notB(thetaSketch)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2) - Subtracts two binary representations of Datasketches
    TupleSketch objects with integer summary data type using a TupleSketch AnotB object.
    Returns elements in the first sketch that are not in the second sketch. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), tuple_sketch_agg_integer(col2, val2))) FROM VALUES (5, 5, 4, 4), (1, 1, 4, 4), (2, 2, 5, 5), (3, 3, 1, 1) tab(col1, val1, col2, val2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifferenceInteger(left: Expression, right: Expression)
    extends TupleDifferenceBase[IntegerSummary] {

  override def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleDifferenceInteger =
    copy(left = newLeft, right = newRight)

  override def prettyName: String = "tuple_difference_integer"

  override protected def differenceSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[IntegerSummary]): Unit = {
    val tupleSketch1 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch1)
    aNotB.notB(tupleSketch2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch) - Subtracts the binary representation of a
    Datasketches ThetaSketch from a TupleSketch with integer summary data type using a TupleSketch
    AnotB object. Returns elements in the TupleSketch that are not in the ThetaSketch. """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), theta_sketch_agg(col2))) FROM VALUES (5, 5, 4), (1, 1, 4), (2, 2, 5), (3, 3, 1) tab(col1, val1, col2);
       2.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleDifferenceThetaInteger(left: Expression, right: Expression)
    extends TupleDifferenceBase[IntegerSummary] {

  override def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TupleDifferenceThetaInteger =
    copy(left = newLeft, right = newRight)

  override def prettyName: String = "tuple_difference_theta_integer"

  override protected def differenceSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[IntegerSummary]): Unit = {
    val tupleSketch = ThetaSketchUtils.heapifyIntegerTupleSketch(sketch1Bytes, prettyName)
    val thetaSketch = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch)
    aNotB.notB(thetaSketch)
  }
}

abstract class TupleDifferenceBase[S <: Summary]
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  protected def differenceSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      aNotB: AnotB[S]): Unit

  override def nullSafeEval(sketch1Binary: Any, sketch2Binary: Any): Any = {

    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val aNotB = new AnotB[S]()

    differenceSketches(sketch1Bytes, sketch2Bytes, aNotB)

    aNotB.getResult(true).toByteArray
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2, lgNomEntries, mode) - Merges two binary representations of Datasketches
    TupleSketch objects with double summary data type using a TupleSketch Union object. Users can
    set lgNomEntries to a value between 4 and 26 (defaults to 12) and mode to 'sum', 'min', 'max',
    or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), tuple_sketch_agg_double(col2, val2))) FROM VALUES (1, 1.0D, 4, 4.0D), (2, 2.0D, 5, 5.0D), (3, 3.0D, 6, 6.0D) tab(col1, val1, col2, val2);
       6.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("first"),
    InputParameter("second"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleUnionDouble(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch, lgNomEntries, mode) - Merges the binary representation of a
    Datasketches TupleSketch with double summary data type with the binary representation of a
    Datasketches ThetaSketch using a TupleSketch Union object. The ThetaSketch entries are assigned
    a default double summary value. Users can set lgNomEntries to a value between 4 and 26 (defaults to 12),
    and mode to 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_double(_FUNC_(tuple_sketch_agg_double(col1, val1), theta_sketch_agg(col2))) FROM VALUES (1, 1.0D, 4), (2, 2.0D, 5), (3, 3.0D, 6) tab(col1, val1, col2);
       6.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionThetaDoubleExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("first"),
    InputParameter("second"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleUnionThetaDouble(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch1, tupleSketch2, lgNomEntries, mode) - Merges two binary representations of Datasketches
    TupleSketch objects with integer summary data type using a TupleSketch Union object. Users can
    set lgNomEntries to a value between 4 and 26 (defaults to 12) and mode to 'sum', 'min', 'max',
    or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), tuple_sketch_agg_integer(col2, val2))) FROM VALUES (1, 1, 4, 4), (2, 2, 5, 5), (3, 3, 6, 6) tab(col1, val1, col2, val2);
       6.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("first"),
    InputParameter("second"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleUnionInteger(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(tupleSketch, thetaSketch, lgNomEntries, mode) - Merges the binary representation of a
    Datasketches TupleSketch with integer summary data type with the binary representation of a
    Datasketches ThetaSketch using a TupleSketch Union object. The ThetaSketch entries are assigned
    a default integer summary value. Users can set lgNomEntries to a value between 4 and 26 (defaults to 12),
    and mode to 'sum', 'min', 'max', or 'alwaysone' (defaults to 'sum'). """,
  examples = """
    Examples:
      > SELECT tuple_sketch_estimate_integer(_FUNC_(tuple_sketch_agg_integer(col1, val1), theta_sketch_agg(col2))) FROM VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6) tab(col1, val1, col2);
       6.0
  """,
  group = "misc_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
object TupleUnionThetaIntegerExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("first"),
    InputParameter("second"),
    InputParameter("lgNomEntries", Some(Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))),
    InputParameter("mode", Some(Literal(ThetaSketchUtils.MODE_SUM)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleUnionThetaInteger(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}
