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

import org.apache.datasketches.tuple.TupleSketchIterator
import org.apache.datasketches.tuple.adouble.DoubleSummary
import org.apache.datasketches.tuple.aninteger.IntegerSummary

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{SummaryAggregateMode, TupleSketchUtils, TupleSummaryMode}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, DoubleType, LongType}
import org.apache.spark.unsafe.types.UTF8String

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
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchSummaryDouble(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes
    with SummaryAggregateMode {

  def this(child: Expression) = {
    this(child, Literal(TupleSummaryMode.Sum.toString))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_summary_double"

  override def mode: Expression = right

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkModeParameter()
    }
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): TupleSketchSummaryDouble =
    copy(left = newFirst, right = newSecond)

  override def nullSafeEval(input: Any, modeInput: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val modeStr = modeInput.asInstanceOf[UTF8String].toString

    val mode = TupleSummaryMode.fromString(modeStr, prettyName)

    val sketch = TupleSketchUtils.heapifyDoubleSketch(buffer, prettyName)

    TupleSketchUtils.aggregateNumericSummaries[DoubleSummary, Double](
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
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchSummaryInteger(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes
    with SummaryAggregateMode {

  def this(child: Expression) = {
    this(child, Literal(TupleSummaryMode.Sum.toString))
  }

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = LongType

  override def prettyName: String = "tuple_sketch_summary_integer"

  override def mode: Expression = right

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      checkModeParameter()
    }
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): TupleSketchSummaryInteger =
    copy(left = newFirst, right = newSecond)

  override def nullSafeEval(input: Any, modeInput: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val modeStr = modeInput.asInstanceOf[UTF8String].toString

    val mode = TupleSummaryMode.fromString(modeStr, prettyName)

    val sketch = TupleSketchUtils.heapifyIntegerSketch(buffer, prettyName)

    TupleSketchUtils.aggregateNumericSummaries[IntegerSummary, Long](
      sketch.iterator(),
      mode,
      (it: TupleSketchIterator[IntegerSummary]) => it.getSummary.getValue.toLong)
  }
}
