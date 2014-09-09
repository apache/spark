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
import org.apache.spark.mllib.util.TestingUtils._

class DistanceMeasureFactorySuite extends FunSuite {
  val v1 = Vectors.dense(1.0, 1.0, 1.0).toBreeze
  val v2 = Vectors.dense(-3.0, -2.0, -2.0).toBreeze

  test("should throw NoSuchElementException with illegal distance function type") {
    intercept[NoSuchElementException] {
      DistanceMeasureFactory("no-such-a-type")
    }
  }

  test("apply(type) shouldn't accept Minkowski distance") {
    intercept[NoSuchElementException] {
      DistanceMeasureFactory("minkowski")
    }

    intercept[NoSuchElementException] {
      DistanceMeasureFactory(DistanceType.minkowski)
    }
  }

  test("euclidean distance metric should be generated") {
    val measure = DistanceMeasureFactory(DistanceType.euclidean)
    assert(measure(v1, v2) ~== 5.830951894845301 absTol 1.0E-10)

    val measure2 = DistanceMeasureFactory("euclidean")
    assert(measure2(v1, v2) ~== 5.830951894845301 absTol 1.0E-10)
  }

  test("manhattan distance measure should be generated") {
    val measure = DistanceMeasureFactory(DistanceType.manhattan)
    assert(measure(v1, v2) ~== 10.0 absTol 1.0E-10)

    val measure2 = DistanceMeasureFactory("manhattan")
    assert(measure(v1, v2) ~== 10.0 absTol 1.0E-10)
  }

  test("chebyshev distance metr should be generated") {
    val measure = DistanceMeasureFactory(DistanceType.chebyshev)
    assert(measure(v1, v2) ~== 4.0 absTol 1.0E-10)

    val measure2 = DistanceMeasureFactory("chebyshev")
    assert(measure(v1, v2) ~== 4.0 absTol 1.0E-10)
  }

  test("cosine distance measure should be generated") {
    val measure = DistanceMeasureFactory(DistanceType.cosine)
    assert(measure(v1, v2) ~== 1.9801960588196068 absTol 1.0E-10)

    val measure2 = DistanceMeasureFactory("cosine")
    assert(measure2(v1, v2) ~== 1.9801960588196068 absTol 1.0E-10)
  }

  test("tanimoto distance measure should be generated") {
    val measure = DistanceMeasureFactory(DistanceType.tanimoto)
    assert(measure(v1, v2) ~== 1.2592592592592593 absTol 1.0E-10)

    val measure2 = DistanceMeasureFactory("tanimoto")
    assert(measure2(v1, v2) ~== 1.2592592592592593 absTol 1.0E-10)
  }
}

class WeightedDistanceFactorySuite extends FunSuite {
  val weights = Vectors.dense(0.1, 0.9).toBreeze
  val v1 = Vectors.dense(1.0, 2.0).toBreeze
  val v2 = Vectors.dense(3.0, 4.0).toBreeze

  test("should throw NoSuchElementException with illegal distance function type") {
    intercept[NoSuchElementException] {
      WeightedDistanceFactory("no-such-a-type", weights)
    }
  }

  test("euclidean distance metric should be generated") {
    val measure = WeightedDistanceFactory(DistanceType.euclidean, weights)
    assert(measure(v1, v2) ~== 2.0 absTol 1.0E-10)

    val measure2 = WeightedDistanceFactory("euclidean", weights)
    assert(measure(v1, v2) ~== 2.0 absTol 1.0E-10)
  }

  test("manhattan distance measure should be generated") {
    val measure = WeightedDistanceFactory(DistanceType.manhattan, weights)
    assert(measure(v1, v2) ~== 2.0 absTol 1.0E-10)

    val measure2 = WeightedDistanceFactory("manhattan", weights)
    assert(measure(v1, v2) ~== 2.0 absTol 1.0E-10)
  }

  test("chebyshev distance metr should be generated") {
    val measure = WeightedDistanceFactory(DistanceType.chebyshev, weights)
    assert(measure(v1, v2) ~== 1.8 absTol 1.0E-10)

    val measure2 = WeightedDistanceFactory("chebyshev", weights)
    assert(measure(v1, v2) ~== 1.8 absTol 1.0E-10)
  }

  test("cosine distance measure should be generated") {
    val measure = WeightedDistanceFactory(DistanceType.cosine, weights)
    assert(measure(v1, v2) ~== 0.003184721463875051 absTol 1.0E-10)

    val measure2 = WeightedDistanceFactory("cosine", weights)
    assert(measure(v1, v2) ~== 0.003184721463875051 absTol 1.0E-10)
  }
}
