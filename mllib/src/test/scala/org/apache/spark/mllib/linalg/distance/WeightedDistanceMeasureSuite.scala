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

class WeightedCosineDistanceMeasureSuite extends GeneralDistanceMeasureSuite {

  override def distanceFactory: DistanceMeasure = {
    val weights = Vectors.dense(0.1, 0.1, 0.1, 0.1, 0.1, 0.5).toBreeze // size should be 6
    new WeightedCosineDistanceMeasure(weights)
  }

  test("concreate distance check") {
    val vector1 = Vectors.dense(1.0, 2.0).toBreeze
    val vector2 = Vectors.dense(3.0, 4.0).toBreeze

    val measure = new WeightedCosineDistanceMeasure(Vectors.dense(0.5, 0.5).toBreeze)
    val distance = measure(vector1, vector2)
    assert(distance ~== 0.01613008990009257 absTol 1.0E-10)
  }

  test("two vectors have the same magnitude") {
    val weights = Vectors.dense(0.5, 0.5).toBreeze
    val vector1 = Vectors.dense(1.0, 1.0).toBreeze
    val vector2 = Vectors.dense(2.0, 2.0).toBreeze

    val measure = new WeightedCosineDistanceMeasure(weights)
    val distance = measure(vector1, vector2)
    assert(distance ~== 0.0 absTol 1.0E-10)
  }

  test("called by companion object") {
    val weights = Vectors.dense(0.5, 0.5)
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val distance = WeightedCosineDistanceMeasure(weights)(vector1, vector2)
    assert(distance ~== 0.01613008990009257 absTol 1.0E-10)
  }

  test("called by companion object as curry-like") {
    val weights = Vectors.dense(0.5, 0.5)
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val measure = WeightedCosineDistanceMeasure(weights) _
    val distance = measure(vector1, vector2)
    assert(distance ~== 0.01613008990009257 absTol 1.0E-10)
  }
}
