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

package org.apache.spark.mllib.linalg.distance

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.scalatest.{FunSuite, Matchers}

private[distance]
trait GeneralDistanceMetricSuite extends FunSuite with Matchers {
  def distanceFactory: DistanceMetric

  test("the length of two vectors should be same") {
    val vector1 = Vectors.dense(1, 1, 1)
    val vector2 = Vectors.dense(1, 1, 1, 1)

    intercept[IllegalArgumentException] {
      distanceFactory(vector1, vector2)
    }
  }

  test("ditances are required to satisfy the conditions for distance function") {
    val vectors = Array(
      Vectors.dense(1, 1, 1, 1, 1, 1),
      Vectors.dense(2, 2, 2, 2, 2, 2),
      Vectors.dense(6, 6, 6, 6, 6, 6),
      Vectors.dense(-1, -1, -1, -1, -1, -1),
      Vectors.dense(0, 0, 0, 0, 0, 0),
      Vectors.dense(0.1, 0.1, -0.1, 0.1, 0.1, 0.1),
      Vectors.dense(-0.9, 0.8, 0.7, -0.6, 0.5, -0.4)
    )

    val distanceMatrix = GeneralDistanceMetricSuite.calcDistanceMatrix(distanceFactory, vectors)

    assert(distanceMatrix(0, 0) <= distanceMatrix(0, 1))
    assert(distanceMatrix(0, 1) <= distanceMatrix(0, 2))

    assert(distanceMatrix(1, 0) >= distanceMatrix(1, 1))
    assert(distanceMatrix(1, 2) >= distanceMatrix(1, 0))

    assert(distanceMatrix(2, 0) >= distanceMatrix(2, 1))
    assert(distanceMatrix(2, 1) >= distanceMatrix(2, 2))

    // non-negative
    assert(NonNegativeValidator(distanceMatrix), "not non-negative")

    // identity of indiscernibles
    assert(IdentityOfIndiscerniblesValidator(distanceMatrix), "not identity of indiscernibles")

    // symmetry
    assert(SymmetryValidator(distanceMatrix), "not symmetry")

    //  triangle inequality
    assert(TriangleInequalityValidator(distanceMatrix), "not triangle inequality")
  }
}

private[distance]
object GeneralDistanceMetricSuite {

  def calcDistanceMatrix(distanceMeasure: DistanceMeasure, vectors: Array[Vector]): Matrix = {
    val denseMatrixElements = for (v1 <- vectors; v2 <- vectors) yield {
      distanceMeasure(v2, v1)
    }
    Matrices.dense(vectors.size, vectors.size, denseMatrixElements)
  }

  def roundValue(value: Double, numDigits: Int): Double = {
    Math.round(value * Math.pow(10, numDigits)) / Math.pow(10, numDigits)
  }
}
