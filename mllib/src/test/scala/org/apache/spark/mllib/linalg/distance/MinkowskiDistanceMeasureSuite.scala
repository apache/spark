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

class MinkowskiDistanceMeasureSuite extends GeneralDistanceMeasureSuite {
  override def distanceMeasureFactory: DistanceMeasure = new MinkowskiDistanceMeasure(4.0)

  test("the distance between the vectors should be expected") {
    val vector1 = Vectors.dense(0, 0, 0)
    val vector2 = Vectors.dense(2, 3, 4)

    val measure = new MinkowskiDistanceMeasure
    assert(measure.exponent == 3.0, s"the default value for exponent should be ${measure.exponent}")

    val distance = measure(vector1, vector2)
    val roundDistance = GeneralDistanceMeasureSuite.roundValue(distance, 10)
    val expected = 4.6260650092
    assert(roundDistance == expected, s"the distance between the vectors should be ${expected}")
  }
}
