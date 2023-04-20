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

import org.apache.datasketches.hll.{HllSketch, Union}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, LongType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated number of unique values given the binary representation
    of a Datasketches HllSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(hll_sketch_agg(col1))
      FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "misc_funcs",
  since = "3.5.0")
case class HllSketchEstimate(child: Expression)
  extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes
    with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): HllSketchEstimate =
    copy(child = newChild)

  override def prettyName: String = "hll_sketch_estimate"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Any = {
    val buffer = input.asInstanceOf[Array[Byte]]
    Math.round(HllSketch.heapify(Memory.wrap(buffer)).getEstimate)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two binary representations of Datasketches HllSketch objects,
    using a Datasketches Union object configured with an lgMaxK equal to the min of the
    HllSketch object's lgConfigK values """,
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(hll_sketch_agg(col1), hll_sketch_agg(col1)))
      FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
       6
  """,
  group = "misc_funcs",
  since = "3.5.0")
case class HllUnion(left: Expression, right: Expression)
  extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes
    with NullIntolerant {

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
  HllUnion = copy(left = newLeft, right = newRight)

  override def prettyName: String = "hll_union"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    val sketch1 = HllSketch.heapify(Memory.wrap(value1.asInstanceOf[Array[Byte]]))
    val sketch2 = HllSketch.heapify(Memory.wrap(value2.asInstanceOf[Array[Byte]]))
    val union = new Union(Math.min(sketch1.getLgConfigK, sketch2.getLgConfigK))
    union.update(sketch1)
    union.update(sketch2)
    union.getResult.toUpdatableByteArray
  }
}
