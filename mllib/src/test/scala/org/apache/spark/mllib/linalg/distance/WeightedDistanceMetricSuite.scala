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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.TestingUtils._

class WeightedChebyshevDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.1, 0.1, 0.1, 0.1, 0.5).toBreeze // size should be 6
    new WeightedChebyshevDistanceMetric(weights)
  }

  test("called by companion object") {
    val weights = Vectors.dense(0.5, 0.5)
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val distance = WeightedChebyshevDistanceMetric(weights)(vector1, vector2)
    assert(distance ~== 1.0 absTol 1.0E-10)
  }
}

class WeightedEuclideanDistanceMetricSuite extends GeneralDistanceMetricSuite {

  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.1, 0.1, 0.1, 0.1, 0.5).toBreeze // size should be 6
    new WeightedEuclideanDistanceMetric(weights)
  }

  test("the distance should be 4.419841626121913") {
    val v1 = Vectors.dense(1, 1, 1, 1, 1, 1).toBreeze
    val v2 = Vectors.dense(1.1, 2.2, 3.3, 4.4, 5.5, 6.6).toBreeze

    val distance = distanceFactory(v1, v2)
    assert(distance ~== 4.419841626121913 absTol 1.0E-10)
  }

  test("called by companion object") {
    val weights = Vectors.dense(0.5, 0.5)
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val distance = WeightedEuclideanDistanceMetric(weights)(vector1, vector2)
    assert(distance ~== 2.0 absTol 1.0E-10)
  }
}

class WeightedManhattanDistanceMetricSuite extends GeneralDistanceMetricSuite {

  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.1, 0.1, 0.1, 0.1, 0.5).toBreeze // size should be 6
    new WeightedManhattanDistanceMetric(weights)
  }

  test("the distance should be 3.95") {
    val v1 = Vectors.dense(1, 1, 1, 1, 1, 1).toBreeze
    val v2 = Vectors.dense(1.1, 2.2, 3.3, 4.4, 5.5, 6.6).toBreeze

    val distance = distanceFactory(v1, v2)
    val expected = 3.95
    assert(distance ~== 3.95 absTol 1.0E-10)
  }

  test("called by companion object") {
    val weights = Vectors.dense(0.5, 0.5)
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val distance = WeightedManhattanDistanceMetric(weights)(vector1, vector2)
    assert(distance ~== 2.0 absTol 1.0E-10)
  }
}

