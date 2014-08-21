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
