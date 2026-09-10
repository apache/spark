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
import org.apache.spark.sql.types.{ArrayType, DoubleType}

class VectorAffineTransformSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorAffineTransform.vectorSqlType
  private val doubleArraySqlType = VectorAffineTransform.doubleArraySqlType

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

  private def array(values: Double*): Literal = {
    Literal(UnsafeArrayData.fromPrimitiveArray(values.toArray), doubleArraySqlType)
  }

  test("vector affine transform interpreted and code-generated evaluation") {
    val expression = VectorAffineTransform(
      dense(1.0, 2.0, 3.0),
      array(2.0, 3.0, 4.0),
      array(5.0, 6.0, 7.0))
    assert(expression.prettyName === "ml_vector_affine_transform")
    checkEvaluation(expression, denseRow(7.0, 12.0, 19.0))

    checkEvaluation(
      VectorAffineTransform(
        dense(1.0, 2.0, 3.0),
        array(2.0, 0.0, 4.0),
        array(0.0, 1.0, 0.0)),
      denseRow(2.0, 1.0, 12.0))
  }

  test("vector affine transform produces a dense vector for a non-null shift") {
    val vector = sparse(3, Array(0, 2), Array(1.0, 3.0))

    checkEvaluation(
      VectorAffineTransform(vector, array(2.0, 3.0, 4.0), array(0.0, 0.0, 0.0)),
      denseRow(2.0, 0.0, 12.0))
    checkEvaluation(
      VectorAffineTransform(
        vector,
        array(2.0, 3.0, 4.0),
        array(0.0, 1.0, 0.0)),
      denseRow(2.0, 1.0, 12.0))
  }

  test("vector affine transform with a null vector") {
    val nullVector = Literal(null, vectorSqlType)
    val values = array(1.0)

    checkEvaluation(VectorAffineTransform(nullVector, values, values), null)
  }

  test("vector affine transform with a null scale") {
    val nullArray = Literal(null, doubleArraySqlType)

    checkEvaluation(
      VectorAffineTransform(dense(1.0, 2.0), nullArray, array(3.0, 4.0)),
      denseRow(4.0, 6.0))
    checkEvaluation(
      VectorAffineTransform(
        sparse(3, Array(0, 2), Array(1.0, 3.0)),
        nullArray,
        array(0.0, 2.0, 0.0)),
      denseRow(1.0, 2.0, 3.0))
  }

  test("vector affine transform with a null shift") {
    val nullArray = Literal(null, doubleArraySqlType)

    checkEvaluation(
      VectorAffineTransform(dense(1.0, 2.0), array(3.0, 4.0), nullArray),
      denseRow(3.0, 8.0))
    checkEvaluation(
      VectorAffineTransform(
        sparse(3, Array(0, 2), Array(1.0, 3.0)),
        array(2.0, 3.0, 4.0),
        nullArray),
      sparseRow(3, Array(0, 2), Array(2.0, 12.0)))
  }

  test("vector affine transform with a null scale and shift") {
    val nullArray = Literal(null, doubleArraySqlType)

    checkEvaluation(
      VectorAffineTransform(dense(1.0, 2.0), nullArray, nullArray),
      denseRow(1.0, 2.0))
    checkEvaluation(
      VectorAffineTransform(
        sparse(3, Array(0, 2), Array(1.0, 3.0)),
        nullArray,
        nullArray),
      sparseRow(3, Array(0, 2), Array(1.0, 3.0)))
  }

  test("vector affine transform with empty vectors") {
    val emptySparse = sparse(0, Array.emptyIntArray, Array.emptyDoubleArray)
    val emptyArray = array()

    checkEvaluation(
      VectorAffineTransform(dense(), emptyArray, emptyArray),
      denseRow())
    checkEvaluation(
      VectorAffineTransform(emptySparse, emptyArray, emptyArray),
      denseRow())
  }

  test("vector affine transform with infinite and NaN values") {
    Seq(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN).foreach { value =>
      checkEvaluation(
        VectorAffineTransform(dense(value), array(1.0), array(0.0)),
        denseRow(value))
      checkEvaluation(
        VectorAffineTransform(dense(1.0), array(value), array(0.0)),
        denseRow(value))
      checkEvaluation(
        VectorAffineTransform(dense(1.0), array(1.0), array(value)),
        denseRow(value))
    }
  }

  test("vector affine transform rejects inputs with different sizes") {
    checkExceptionInExpression[IllegalArgumentException](
      VectorAffineTransform(dense(1.0), array(1.0, 2.0), array(1.0)),
      "inputs with non-matching sizes")
    checkExceptionInExpression[IllegalArgumentException](
      VectorAffineTransform(dense(1.0), array(1.0), array(1.0, 2.0)),
      "inputs with non-matching sizes")
  }

  test("vector affine transform requires arrays without null elements") {
    val nullableArray = Literal(
      UnsafeArrayData.fromPrimitiveArray(Array(1.0)),
      ArrayType(DoubleType, containsNull = true))

    assert(VectorAffineTransform(dense(1.0), nullableArray, array(0.0))
      .checkInputDataTypes().isFailure)
    assert(VectorAffineTransform(dense(1.0), array(1.0), nullableArray)
      .checkInputDataTypes().isFailure)
  }
}
