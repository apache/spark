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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, GenericInternalRow, Literal, UnsafeArrayData}

class VectorDotProductSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorDotProduct.vectorSqlType

  private def dense(values: Double*): Literal = {
    val row = new GenericInternalRow(Array[Any](
      1.toByte,
      null,
      null,
      UnsafeArrayData.fromPrimitiveArray(values.toArray)))
    Literal(row, vectorSqlType)
  }

  private def sparse(size: Int, indices: Array[Int], values: Array[Double]): Literal = {
    val row = new GenericInternalRow(Array[Any](
      0.toByte,
      size,
      UnsafeArrayData.fromPrimitiveArray(indices),
      UnsafeArrayData.fromPrimitiveArray(values)))
    Literal(row, vectorSqlType)
  }

  test("vector dot product interpreted and code-generated evaluation") {
    val denseVector = dense(1.0, 2.0, 3.0)
    val sparseVector = sparse(3, Array(0, 2), Array(1.0, 3.0))
    val denseWeights = dense(4.0, 5.0, 6.0)
    val sparseWeights = sparse(3, Array(0, 2), Array(4.0, 6.0))

    val expression = VectorDotProduct(denseVector, denseWeights)
    assert(expression.prettyName === "ml_vector_dot_product")
    checkEvaluation(expression, 32.0)
    checkEvaluation(VectorDotProduct(denseVector, sparseWeights), 22.0)
    checkEvaluation(VectorDotProduct(sparseVector, denseWeights), 22.0)
    checkEvaluation(VectorDotProduct(sparseVector, sparseWeights), 22.0)
  }

  test("vector dot product with null vectors") {
    val nullVector = Literal(null, vectorSqlType)
    val vector = dense(1.0)

    checkEvaluation(VectorDotProduct(nullVector, vector), null)
    checkEvaluation(VectorDotProduct(vector, nullVector), null)
  }

  test("vector dot product with empty vectors") {
    val emptyDense = dense()
    val emptySparse = sparse(0, Array.emptyIntArray, Array.emptyDoubleArray)

    checkEvaluation(VectorDotProduct(emptyDense, emptyDense), 0.0)
    checkEvaluation(VectorDotProduct(emptyDense, emptySparse), 0.0)
    checkEvaluation(VectorDotProduct(emptySparse, emptyDense), 0.0)
    checkEvaluation(VectorDotProduct(emptySparse, emptySparse), 0.0)
  }

  test("vector dot product rejects vectors with different sizes") {
    checkExceptionInExpression[IllegalArgumentException](
      VectorDotProduct(dense(1.0), dense(1.0, 2.0)),
      "vectors with non-matching sizes")
  }
}
