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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, GenericInternalRow, Literal, UnsafeArrayData}

class VectorAffineTransformSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorAffineTransform.vectorSqlType

  private def denseRow(values: Double*): InternalRow = {
    new GenericInternalRow(Array[Any](
      1.toByte,
      null,
      null,
      UnsafeArrayData.fromPrimitiveArray(values.toArray)))
  }

  private def dense(values: Double*): Literal = Literal(denseRow(values: _*), vectorSqlType)

  private def sparseRow(size: Int, indices: Array[Int], values: Array[Double]): InternalRow = {
    new GenericInternalRow(Array[Any](
      0.toByte,
      size,
      UnsafeArrayData.fromPrimitiveArray(indices),
      UnsafeArrayData.fromPrimitiveArray(values)))
  }

  private def sparse(size: Int, indices: Array[Int], values: Array[Double]): Literal = {
    Literal(sparseRow(size, indices, values), vectorSqlType)
  }

  test("vector affine transform interpreted and code-generated evaluation") {
    val expression = VectorAffineTransform(
      dense(1.0, 2.0, 3.0),
      dense(2.0, 3.0, 4.0),
      dense(5.0, 6.0, 7.0))
    assert(expression.prettyName === "ml_vector_affine_transform")
    checkEvaluation(expression, denseRow(7.0, 12.0, 19.0))

    checkEvaluation(
      VectorAffineTransform(
        dense(1.0, 2.0, 3.0),
        sparse(3, Array(0, 2), Array(2.0, 4.0)),
        sparse(3, Array(1), Array(1.0))),
      denseRow(2.0, 1.0, 12.0))
  }

  test("vector affine transform preserves sparse vectors for a zero shift") {
    val vector = sparse(3, Array(0, 2), Array(1.0, 3.0))
    val expected = sparseRow(3, Array(0, 2), Array(2.0, 12.0))

    checkEvaluation(
      VectorAffineTransform(vector, dense(2.0, 3.0, 4.0), dense(0.0, 0.0, 0.0)),
      expected)
    checkEvaluation(
      VectorAffineTransform(
        vector,
        sparse(3, Array(0, 2), Array(2.0, 4.0)),
        sparse(3, Array.emptyIntArray, Array.emptyDoubleArray)),
      expected)
  }

  test("vector affine transform produces a dense vector for a nonzero shift") {
    checkEvaluation(
      VectorAffineTransform(
        sparse(3, Array(0, 2), Array(1.0, 3.0)),
        dense(2.0, 3.0, 4.0),
        sparse(3, Array(1), Array(1.0))),
      denseRow(2.0, 1.0, 12.0))
  }

  test("vector affine transform with null vectors") {
    val nullVector = Literal(null, vectorSqlType)
    val vector = dense(1.0)

    checkEvaluation(VectorAffineTransform(nullVector, vector, vector), null)
    checkEvaluation(VectorAffineTransform(vector, nullVector, vector), null)
    checkEvaluation(VectorAffineTransform(vector, vector, nullVector), null)
  }

  test("vector affine transform with empty vectors") {
    val emptyDense = dense()
    val emptySparse = sparse(0, Array.emptyIntArray, Array.emptyDoubleArray)

    checkEvaluation(
      VectorAffineTransform(emptyDense, emptyDense, emptyDense),
      denseRow())
    checkEvaluation(
      VectorAffineTransform(emptySparse, emptySparse, emptySparse),
      sparseRow(0, Array.emptyIntArray, Array.emptyDoubleArray))
  }

  test("vector affine transform with infinite and NaN values") {
    Seq(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN).foreach { value =>
      checkEvaluation(
        VectorAffineTransform(dense(value), dense(1.0), dense(0.0)),
        denseRow(value))
      checkEvaluation(
        VectorAffineTransform(dense(1.0), dense(value), dense(0.0)),
        denseRow(value))
      checkEvaluation(
        VectorAffineTransform(dense(1.0), dense(1.0), dense(value)),
        denseRow(value))
    }
  }

  test("vector affine transform rejects vectors with different sizes") {
    checkExceptionInExpression[IllegalArgumentException](
      VectorAffineTransform(dense(1.0), dense(1.0, 2.0), dense(1.0)),
      "vectors with non-matching sizes")
    checkExceptionInExpression[IllegalArgumentException](
      VectorAffineTransform(dense(1.0), dense(1.0), dense(1.0, 2.0)),
      "vectors with non-matching sizes")
  }
}
