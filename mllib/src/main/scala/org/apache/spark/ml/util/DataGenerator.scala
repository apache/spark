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

package org.apache.spark.ml.util

import scala.util.Random

import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors

/**
 * Helper class for test value generation.
 */
object DataGenerator {
  /**
   * @param seed random generator seed
   * @param classification whether this should have classification {0.0, 1.0} or regression
   *                       [0.0, 1.0) labels
   * @param numFeatures total dimensionality
   * @param categoricalFeatures the mapping from categorical feature index to the number
   *                            of categories for that index
   * @param numPoints number of points to generate
   * @return array of random labeled points for testing
   */
  @Since("2.1")
  def randomLabeledPoints(seed: Int,
                          classification: Boolean,
                          numFeatures: Int,
                          categoricalFeatures: Map[Int, Int],
                          numPoints: Int): Array[LabeledPoint] = {
    assert(categoricalFeatures.keys.forall(0 until numFeatures contains _))
    assert(categoricalFeatures.values.forall(_ > 0))

    val continuousFeatures = (0 until numFeatures).toSet -- categoricalFeatures.keys

    val rng = new Random(seed)
    val arr = new Array[LabeledPoint](numPoints)

    for (i <- 0 until numPoints) {
      val label = if (classification) {
        if (rng.nextBoolean()) 1.0 else 0.0
      } else {
        rng.nextDouble()
      }

      val features = new Array[Double](numFeatures)
      for ((idx, numCategories) <- categoricalFeatures) {
        features(idx) = Math.abs(rng.nextInt() % numCategories)
      }
      for (idx <- continuousFeatures) {
        features(idx) = rng.nextDouble()
      }

      arr(i) = LabeledPoint(label, Vectors.dense(features))
    }
    arr
  }
}
