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
import org.scalatest.{FunSuite, ShouldMatchers}

private[distance]
abstract class GeneralDistanceMeasureSuite extends FunSuite with ShouldMatchers {
  def distanceMeasureFactory: DistanceMeasure

  test("the length of two vectors should be same") {
    val vector1 = Vectors.dense(1, 1, 1)
    val vector2 = Vectors.dense(1, 1, 1, 1)

    intercept[IllegalArgumentException] {
      distanceMeasureFactory.distance(vector1, vector2)
    }
  }

  test("measure the distances between two vector") {
    val vectors = Array(
      Vectors.dense(1, 1, 1, 1, 1, 1),
      Vectors.dense(2, 2, 2, 2, 2, 2),
      Vectors.dense(6, 6, 6, 6, 6, 6),
      Vectors.dense(-1, -1, -1, -1, -1, -1)
    )

    val distanceMatrix = GeneralDistanceMeasureSuite.calcDistanceMatrix(distanceMeasureFactory, vectors)

    assert(distanceMatrix(0, 0) <= distanceMatrix(0, 1))
    assert(distanceMatrix(0, 1) <= distanceMatrix(0, 2))

    assert(distanceMatrix(1, 0) >= distanceMatrix(1, 1))
    assert(distanceMatrix(1, 2) >= distanceMatrix(1, 0))

    assert(distanceMatrix(2, 0) >= distanceMatrix(2, 1))
    assert(distanceMatrix(2, 1) >= distanceMatrix(2, 2))

    for (i <- 0 to (vectors.size - 1); j <- 0 to (vectors.size - 1)) {
      if(i.equals(j)) {
        assert(distanceMatrix(i, i) == 0.0, "Diagonal elements must be equal to zero")
      }
      else {
        assert(distanceMatrix(i, j) >= 0, "Distance between vectors greater than zero")
      }
    }
  }

  test("the distance of a long vector should be greater than that of a small vector") {
    val vectors = Array(
      Vectors.dense(1, 1, 1, 1, 1, 1),
      Vectors.dense(2, 3, 4, 5, 6, 7),
      Vectors.dense(20, 30, 40, 50, 60, 70)
    )
    val distanceMatrix = GeneralDistanceMeasureSuite.calcDistanceMatrix(distanceMeasureFactory, vectors)

    assert(distanceMatrix(0, 1) < distanceMatrix(0, 2),
      s"${distanceMatrix(0, 1)} should be greater than ${distanceMatrix(0, 2)}")
  }
}

private[distance]
object GeneralDistanceMeasureSuite {

  def calcDistanceMatrix(distanceMeasure: DistanceMeasure, vectors: Array[Vector]): Matrix = {
    val denseMatrixElements = for (v1 <- vectors; v2 <- vectors) yield {
      distanceMeasure.distance(v2, v1)
    }
    Matrices.dense(vectors.size, vectors.size, denseMatrixElements)
  }

  def roundValue(value: Double, numDigits: Int): Double = {
    Math.round(value * Math.pow(10, numDigits)) / Math.pow(10, numDigits)
  }
}
