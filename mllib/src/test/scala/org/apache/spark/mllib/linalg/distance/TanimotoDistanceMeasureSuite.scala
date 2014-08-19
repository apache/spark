/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg.distance

import org.apache.spark.mllib.linalg.Vectors

class TanimotoDistanceMeasureSuite extends GeneralDistanceMeasureSuite {
  override def distanceMeasureFactory = new TanimotoDistanceMeasure

  test("calculate tanimoto distance for 2-dimension") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = distanceMeasureFactory(vector1, vector2)

    val coefficient = 100000
    Math.floor(distance * coefficient) / coefficient should be(0.42105)
  }

  test("calculate tanimoto distance for 3-dimension") {
    val vector1 = Vectors.dense(1.0, 2.0, 3.0)
    val vector2 = Vectors.dense(4.0, 5.0, 6.0)
    val distance = distanceMeasureFactory(vector1, vector2)

    val coefficient = 100000
    Math.floor(distance * coefficient) / coefficient should be(0.45762)
  }

  test("calculate tanimoto distance for 6-dimension") {
    val vector1 = Vectors.dense(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val vector2 = Vectors.dense(-1.0, -1.0, -1.0, -1.0, -1.0, -1.0)
    val distance = distanceMeasureFactory(vector1, vector2)

    val coefficient = 100000
    Math.floor(distance * coefficient) / coefficient should be(1.33333)
  }
}
