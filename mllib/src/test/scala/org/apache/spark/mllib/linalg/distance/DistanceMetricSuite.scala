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

import breeze.linalg.{Vector => BV}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.TestingUtils._

class ChebyshevDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = new ChebyshevDistanceMetric

  test("the distance should be 6") {
    val vector1 = Vectors.dense(1, -1, 1, -1).toBreeze
    val vector2 = Vectors.dense(2, -3, 4, 5).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance == 6, s"the distance should be 6, actual ${distance}")
  }

  test("the distance should be 100") {
    val vector1 = Vectors.dense(1, -1, 1, -1).toBreeze
    val vector2 = Vectors.dense(101, -3, 4, 5).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance == 100, s"the distance should be 100, actual ${distance}")
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = ChebyshevDistanceMetric(vector1, vector2)
    assert(distance ~== 2.0 absTol 1.0E-10)
  }
}

class EuclideanDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory = new EuclideanDistanceMetric

  test("the distance should be 6.7082039325") {
    val v1 = Vectors.dense(2, 3).toBreeze
    val v2 = Vectors.dense(5, 9).toBreeze
    val distance = distanceFactory(v1, v2)
    assert(distance ~== 6.7082039325 absTol 1.0E-10)
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = EuclideanDistanceMetric(vector1, vector2)
    assert(distance ~== 2.8284271247461903 absTol 1.0E-10)
  }
}

class ManhattanDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory = new ManhattanDistanceMetric()

  test("the distance should be 6.7082039325") {
    val v1 = Vectors.dense(2, 3, 6, 8).toBreeze
    val v2 = Vectors.dense(5, 9, 1, 4).toBreeze
    val distance = distanceFactory(v1, v2)
    assert(distance ~== 18.0 absTol 1.0E-10)
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = ManhattanDistanceMetric(vector1, vector2)
    assert(distance ~== 4.0 absTol 1.0E-10)
  }
}

class MinkowskiDistanceMetricSuite extends GeneralDistanceMetricSuite {
  override def distanceFactory: DistanceMetric = new MinkowskiDistanceMetric(4.0)

  test("the distance between the vectors should be expected") {
    val vector1 = Vectors.dense(0, 0, 0).toBreeze
    val vector2 = Vectors.dense(2, 3, 4).toBreeze
    val measure = new MinkowskiDistanceMetric(3.0)
    val distance = measure(vector1, vector2)
    assert(distance ~== 4.6260650092 absTol 1.0E-10)
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = MinkowskiDistanceMetric(4.0)(vector1, vector2)
    assert(distance ~== 2.378414230005442 absTol 1.0E-10)
  }

  test("called by the companion object without the parameters for vectors") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val measure = MinkowskiDistanceMetric(4.0) _
    val distance = measure(vector1, vector2)
    assert(distance ~==  2.378414230005442 absTol 1.0E-10)
  }
}

