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

package org.apache.spark.mllib.tree

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.tree.impurity._

/**
 * Test suites for `GiniAggregator` and `EntropyAggregator`.
 */
class ImpuritySuite extends SparkFunSuite {

  private val seed = 42

  test("Gini impurity does not support negative labels") {
    val gini = new GiniAggregator(2)
    intercept[IllegalArgumentException] {
      gini.update(Array(0.0, 1.0, 2.0), 0, -1, 3, 0.0)
    }
  }

  test("Entropy does not support negative labels") {
    val entropy = new EntropyAggregator(2)
    intercept[IllegalArgumentException] {
      entropy.update(Array(0.0, 1.0, 2.0), 0, -1, 3, 0.0)
    }
  }

  test("Classification impurities are insensitive to scaling") {
    val rng = new scala.util.Random(seed)
    val weightedCounts = Array.fill(5)(rng.nextDouble())
    val smallWeightedCounts = weightedCounts.map(_ * 0.0001)
    val largeWeightedCounts = weightedCounts.map(_ * 10000)
    Seq(Gini, Entropy).foreach { impurity =>
      val impurity1 = impurity.calculate(weightedCounts, weightedCounts.sum)
      assert(impurity.calculate(smallWeightedCounts, smallWeightedCounts.sum)
        ~== impurity1 relTol 0.005)
      assert(impurity.calculate(largeWeightedCounts, largeWeightedCounts.sum)
        ~== impurity1 relTol 0.005)
    }
  }

  test("Regression impurities are insensitive to scaling") {
    def computeStats(samples: Seq[Double], weights: Seq[Double]): (Double, Double, Double) = {
      samples.zip(weights).foldLeft((0.0, 0.0, 0.0)) { case ((wn, wy, wyy), (y, w)) =>
        (wn + w, wy + w * y, wyy + w * y * y)
      }
    }
    val rng = new scala.util.Random(seed)
    val samples = Array.fill(10)(rng.nextDouble())
    val _weights = Array.fill(10)(rng.nextDouble())
    val smallWeights = _weights.map(_ * 0.0001)
    val largeWeights = _weights.map(_ * 10000)
    val (count, sum, sumSquared) = computeStats(samples, _weights)
    Seq(Variance).foreach { impurity =>
      val impurity1 = impurity.calculate(count, sum, sumSquared)
      val (smallCount, smallSum, smallSumSquared) = computeStats(samples, smallWeights)
      val (largeCount, largeSum, largeSumSquared) = computeStats(samples, largeWeights)
      assert(impurity.calculate(smallCount, smallSum, smallSumSquared) ~== impurity1 relTol 0.005)
      assert(impurity.calculate(largeCount, largeSum, largeSumSquared) ~== impurity1 relTol 0.005)
    }
  }

}
