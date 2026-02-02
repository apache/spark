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

import org.apache.datasketches.tuple.{AnotB, Summary}
import org.apache.datasketches.tuple.adouble.DoubleSummary
import org.apache.datasketches.tuple.aninteger.IntegerSummary

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.TupleSketchUtils
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

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
  group = "sketch_funcs",
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
    val tupleSketch1 = TupleSketchUtils.heapifyDoubleSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyDoubleSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch1)
    aNotB.notB(tupleSketch2)
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
  group = "sketch_funcs",
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
    val tupleSketch1 = TupleSketchUtils.heapifyIntegerSketch(sketch1Bytes, prettyName)
    val tupleSketch2 = TupleSketchUtils.heapifyIntegerSketch(sketch2Bytes, prettyName)

    aNotB.setA(tupleSketch1)
    aNotB.notB(tupleSketch2)
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
