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

package org.apache.spark.sql.catalyst.expressions.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, ExpressionEvalHelper, Literal, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

class SparseVectorToArraySuite extends SparkFunSuite with ExpressionEvalHelper {
  private val indicesType = ArrayType(IntegerType, containsNull = false)
  private val valuesType = ArrayType(DoubleType, containsNull = false)

  private def sparseToArray(
      size: Int,
      indices: Array[Int],
      values: Array[Double],
      elementType: DataType): SparseVectorToArray = {
    SparseVectorToArray(
      Literal(size),
      Literal(UnsafeArrayData.fromPrimitiveArray(indices), indicesType),
      Literal(UnsafeArrayData.fromPrimitiveArray(values), valuesType),
      elementType)
  }

  test("sparse vector to array interpreted and code-generated evaluation") {
    val vectors = Seq(
      (0, Array.emptyIntArray, Array.emptyDoubleArray, Seq.empty[Double]),
      (4, Array.emptyIntArray, Array.emptyDoubleArray, Seq(0.0, 0.0, 0.0, 0.0)),
      (5, Array(1, 4), Array(1.1, -2.5), Seq(0.0, 1.1, 0.0, 0.0, -2.5)),
      (3, Array(0, 1, 2), Array(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)))

    for ((size, indices, values, expected) <- vectors;
        elementType <- Seq(DoubleType, FloatType)) {
      val expression = sparseToArray(size, indices, values, elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      assert(expression.prettyName === "sparse_vector_to_array")
      assert(expression.dataType === ArrayType(elementType, containsNull = false))
      assert(!expression.nullable)
      val result = if (elementType == FloatType) expected.map(_.toFloat) else expected
      checkEvaluation(expression, result)
      checkEvaluation(
        SparseVectorToArray(
          BoundReference(0, IntegerType, nullable = true),
          BoundReference(1, indicesType, nullable = true),
          BoundReference(2, valuesType, nullable = true),
          elementType),
        result,
        InternalRow(size,
          UnsafeArrayData.fromPrimitiveArray(indices),
          UnsafeArrayData.fromPrimitiveArray(values)))
    }
  }

  test("sparse vector to array propagates null inputs") {
    val size = Literal(3)
    val indices = Literal(UnsafeArrayData.fromPrimitiveArray(Array(1)), indicesType)
    val values = Literal(UnsafeArrayData.fromPrimitiveArray(Array(2.5)), valuesType)
    for (elementType <- Seq(DoubleType, FloatType)) {
      Seq(
        SparseVectorToArray(Literal(null, IntegerType), indices, values, elementType),
        SparseVectorToArray(size, Literal(null, indicesType), values, elementType),
        SparseVectorToArray(size, indices, Literal(null, valuesType), elementType))
        .foreach { expression =>
          assert(expression.checkInputDataTypes().isSuccess)
          assert(expression.nullable)
          checkEvaluation(expression, null)
        }
    }
  }

  test("sparse vector to array requires integer indices and double values without null elements") {
    val inputTypes = Seq(IntegerType, indicesType, valuesType)
    val mismatches = Seq(
      (0, LongType),
      (1, ArrayType(LongType, containsNull = false)),
      (1, ArrayType(IntegerType, containsNull = true)),
      (2, ArrayType(FloatType, containsNull = false)),
      (2, ArrayType(DoubleType, containsNull = true)))
    for ((index, invalidType) <- mismatches; elementType <- Seq(DoubleType, FloatType)) {
      val types = inputTypes.updated(index, invalidType)
      val expression = SparseVectorToArray(
        BoundReference(0, types(0), nullable = true),
        BoundReference(1, types(1), nullable = true),
        BoundReference(2, types(2), nullable = true),
        elementType)
      assert(expression.checkInputDataTypes().isFailure)
    }
  }

  test("sparse vector to array preserves signed zero") {
    checkEvaluation(sparseToArray(3, Array(1), Array(-0.0), DoubleType), Seq(0.0, -0.0, 0.0))
    checkEvaluation(sparseToArray(3, Array(1), Array(-0.0), FloatType), Seq(0.0f, -0.0f, 0.0f))
    val doubles = sparseToArray(3, Array(1), Array(-0.0), DoubleType)
      .eval().asInstanceOf[ArrayData]
    val floats = sparseToArray(3, Array(1), Array(-0.0), FloatType)
      .eval().asInstanceOf[ArrayData]
    assert(java.lang.Double.doubleToRawLongBits(doubles.getDouble(1)) === Long.MinValue)
    assert(java.lang.Float.floatToRawIntBits(floats.getFloat(1)) === Int.MinValue)
  }

  test("sparse vector to array rejects unsupported output types") {
    intercept[IllegalArgumentException] {
      sparseToArray(1, Array(0), Array(1.0), IntegerType)
    }
  }
}
