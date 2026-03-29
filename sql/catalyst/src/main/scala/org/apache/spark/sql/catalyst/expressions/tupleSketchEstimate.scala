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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.TupleSketchUtils
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, DoubleType}

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
  group = "sketch_funcs",
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
    val sketch = TupleSketchUtils.heapifyDoubleSketch(buffer, prettyName)
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
  group = "sketch_funcs",
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
    val sketch = TupleSketchUtils.heapifyIntegerSketch(buffer, prettyName)
    sketch.getEstimate()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns the theta value (sampling rate) from a Datasketches TupleSketch.
    The theta value represents the effective sampling rate of the sketch, between 0.0 and 1.0.
    The sketch's summary type must be a double. """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_double(key, summary)) FROM VALUES (1, 1.0D), (2, 2.0D), (3, 3.0D) tab(key, summary);
       1.0
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchThetaDouble(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_theta_double"

  override protected def withNewChildInternal(newChild: Expression): TupleSketchThetaDouble =
    copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val sketch = TupleSketchUtils.heapifyDoubleSketch(buffer, prettyName)
    sketch.getTheta()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child) - Returns the theta value (sampling rate) from a Datasketches TupleSketch.
    The theta value represents the effective sampling rate of the sketch, between 0.0 and 1.0.
    The sketch's summary type must be an integer. """,
  examples = """
    Examples:
      > SELECT _FUNC_(tuple_sketch_agg_integer(key, summary)) FROM VALUES (1, 1), (2, 2), (3, 3) tab(key, summary);
       1.0
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class TupleSketchThetaInteger(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = DoubleType

  override def prettyName: String = "tuple_sketch_theta_integer"

  override protected def withNewChildInternal(newChild: Expression): TupleSketchThetaInteger =
    copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    val sketch = TupleSketchUtils.heapifyIntegerSketch(buffer, prettyName)
    sketch.getTheta()
  }
}
