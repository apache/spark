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
import org.scalatest.FunSuite

class ChebyshevDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = new ChebyshevDistanceMetric

  test("the distance should be 6") {
    val vector1 = Vectors.dense(1, -1, 1, -1)
    val vector2 = Vectors.dense(2, -3, 4, 5)
    val distance = distanceFactory(vector1, vector2)
    assert(distance == 6, s"the distance should be 6, but ${distance}")
  }

  test("the distance should be 100") {
    val vector1 = Vectors.dense(1, -1, 1, -1)
    val vector2 = Vectors.dense(101, -3, 4, 5)
    val distance = distanceFactory(vector1, vector2)
    assert(distance == 100, s"the distance should be 100, but ${distance}")
  }
}

class WeightedChebyshevDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.2, 0.3, 0.4, 0.5, 0.6) // size should be 6
    new WeightedChebyshevDistanceMetric(weights)
  }
}

class EuclideanDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory = new EuclideanDistanceMetric()

  test("the distance should be 6.7082039325") {
    val v1 = Vectors.dense(2, 3)
    val v2 = Vectors.dense(5, 9)

    val distance = distanceFactory(v1, v2)
    val expected = 6.7082039325
    val isNear = GeneralDistanceMetricSuite.isNearlyEqual(distance, expected)
    assert(isNear, s"the distance should be nearly equal to ${expected}, actual ${distance}")
  }
}

class WeightedEuclideanDistanceMetricSuite extends GeneralDistanceMetricSuite {


  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.2, 0.3, 0.4, 0.5, 0.6) // size should be 6
    new WeightedEuclideanDistanceMetric(weights)
  }

  test("the distance should be 5.9532344150") {
    val v1 = Vectors.dense(1, 1, 1, 1, 1, 1)
    val v2 = Vectors.dense(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

    val distance = distanceFactory(v1, v2)
    val expected = 5.9532344150
    val isNear = GeneralDistanceMetricSuite.isNearlyEqual(distance, expected)
    assert(isNear, s"the distance should be nearly equal to 5.9532344150, but ${distance}")
  }
}

class ManhattanDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory = new ManhattanDistanceMetric()
}

class WeightedManhattanDistanceMetricSuite extends GeneralDistanceMetricSuite {


  override def distanceFactory: DistanceMetric = {
    val weights = Vectors.dense(0.1, 0.2, 0.3, 0.4, 0.5, 0.6) // size should be 6
    new WeightedManhattanDistanceMetric(weights)
  }

  test("the distance should be 7.91") {
    val v1 = Vectors.dense(1, 1, 1, 1, 1, 1)
    val v2 = Vectors.dense(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

    val distance = distanceFactory(v1, v2)
    val expected = 7.91
    val isNear = GeneralDistanceMetricSuite.isNearlyEqual(distance, expected)
    assert(isNear, s"the distance should be nearly equal to 7.91, actual ${distance}")
  }
}

class MinkowskiDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = new MinkowskiDistanceMetric(4.0)

  test("the distance between the vectors should be expected") {
    val vector1 = Vectors.dense(0, 0, 0)
    val vector2 = Vectors.dense(2, 3, 4)

    val measure = new MinkowskiDistanceMetric
    assert(measure.exponent == 3.0, s"the default value for exponent should be ${measure.exponent}")

    val distance = measure(vector1, vector2)
    val expected = 4.6260650092
    val isNear = GeneralDistanceMetricSuite.isNearlyEqual(distance, expected)
    assert(isNear, s"the distance between the vectors should be ${expected}")
  }
}
