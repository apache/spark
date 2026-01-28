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

import org.apache.datasketches.tuple.{Summary, SummarySetOperations, Union}
import org.apache.datasketches.tuple.adouble.{DoubleSummary, DoubleSummarySetOperations}
import org.apache.datasketches.tuple.aninteger.{IntegerSummary, IntegerSummarySetOperations}

import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.util.{SketchSize, SummaryAggregateMode, ThetaSketchUtils, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The TupleUnionDouble function merges two binary representations of Datasketches TupleSketch
 * objects with double summary data type using a TupleSketch Union object. When duplicate keys
 * appear across the two sketches, their summary values are combined according to the specified
 * mode (sum, min, max, or alwaysone).
 *
 * Keys are hashed internally based on their type and value - the same logical value in different
 * types (e.g., String("123") and Int(123)) will be treated as distinct keys. Summary value types
 * must be consistent across all sketches being merged.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param first
 *   first TupleSketch binary representation with double summaries
 * @param second
 *   second TupleSketch binary representation with double summaries
 * @param third
 *   lgNomEntries - the log-base-2 of nominal entries that determines the result sketch size
 * @param fourth
 *   mode - the aggregation mode for combining duplicate key summaries (sum, min, max, alwaysone)
 */
case class TupleUnionDouble(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[DoubleSummary] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionDouble =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_double"

  override protected def createSummarySetOperations(
      mode: TupleSummaryMode): SummarySetOperations[DoubleSummary] =
    new DoubleSummarySetOperations(mode.toDoubleSummaryMode)

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[DoubleSummary]): Unit = {
    val tupleSketch1 = TupleSketchUtils.heapifyDoubleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyDoubleSketch(sketch2Bytes, prettyName)

    union.union(tupleSketch1)
    union.union(tupleSketch2)
  }
}

/**
 * The TupleUnionInteger function merges two binary representations of Datasketches TupleSketch
 * objects with integer summary data type using a TupleSketch Union object. When duplicate keys
 * appear across the two sketches, their summary values are combined according to the specified
 * mode (sum, min, max, or alwaysone).
 *
 * Keys are hashed internally based on their type and value - the same logical value in different
 * types (e.g., String("123") and Int(123)) will be treated as distinct keys. Summary value types
 * must be consistent across all sketches being merged.
 *
 * See [[https://datasketches.apache.org/docs/Tuple/TupleSketches.html]] for more information.
 *
 * @param first
 *   first TupleSketch binary representation with integer summaries
 * @param second
 *   second TupleSketch binary representation with integer summaries
 * @param third
 *   lgNomEntries - the log-base-2 of nominal entries that determines the result sketch size
 * @param fourth
 *   mode - the aggregation mode for combining duplicate key summaries (sum, min, max, alwaysone)
 */
case class TupleUnionInteger(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression)
    extends TupleUnionBase[IntegerSummary] {

  def this(first: Expression, second: Expression) = {
    this(
      first,
      second,
      Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS),
      Literal(TupleSummaryMode.Sum.toString))
  }

  def this(first: Expression, second: Expression, third: Expression) = {
    this(first, second, third, Literal(TupleSummaryMode.Sum.toString))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): TupleUnionInteger =
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)

  override def prettyName: String = "tuple_union_integer"

  override protected def createSummarySetOperations(
      mode: TupleSummaryMode): SummarySetOperations[IntegerSummary] = {
    val integerMode = mode.toIntegerSummaryMode
    new IntegerSummarySetOperations(integerMode, integerMode)
  }

  override protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[IntegerSummary]): Unit = {
    val tupleSketch1 = TupleSketchUtils.heapifyIntegerSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyIntegerSketch(sketch2Bytes, prettyName)

    union.union(tupleSketch1)
    union.union(tupleSketch2)
  }
}

abstract class TupleUnionBase[S <: Summary]
    extends QuaternaryExpression
    with CodegenFallback
    with ExpectsInputTypes
    with SketchSize
    with SummaryAggregateMode {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      BinaryType,
      IntegerType,
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = BinaryType

  // Implement SketchSize trait
  override def lgNomEntries: Expression = third

  // Implement SummaryAggregateMode trait
  override def mode: Expression = fourth

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    val lgCheck = checkLgNomEntriesParameter()

    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (lgCheck.isFailure) {
      lgCheck
    } else {
      checkModeParameter()
    }
  }

  protected def createSummarySetOperations(mode: TupleSummaryMode): SummarySetOperations[S]

  protected def unionSketches(
      sketch1Bytes: Array[Byte],
      sketch2Bytes: Array[Byte],
      union: Union[S]): Unit

  override def nullSafeEval(
      sketch1Binary: Any,
      sketch2Binary: Any,
      lgNomEntries: Any,
      modeInput: Any): Any = {

    val logNominalEntries = lgNomEntries.asInstanceOf[Int]

    val modeStr = modeInput.asInstanceOf[UTF8String].toString
    val tupleSummaryMode = TupleSummaryMode.fromString(modeStr, prettyName)

    val sketch1Bytes = sketch1Binary.asInstanceOf[Array[Byte]]
    val sketch2Bytes = sketch2Binary.asInstanceOf[Array[Byte]]

    val nominalEntries = 1 << logNominalEntries
    val summarySetOps = createSummarySetOperations(tupleSummaryMode)
    val union = new Union(nominalEntries, summarySetOps)

    unionSketches(sketch1Bytes, sketch2Bytes, union)

    union.getResult.toByteArray
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
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
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
    InputParameter("mode", Some(Literal(TupleSummaryMode.Sum.toString)))
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // The rearrange method ensures expressions.size == 4 with defaults filled in
    assert(expressions.size == 4)
    new TupleUnionInteger(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}
