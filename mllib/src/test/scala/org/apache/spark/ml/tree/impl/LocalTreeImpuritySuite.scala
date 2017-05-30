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

package org.apache.spark.ml.tree.impl

import breeze.numerics.log2

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.util.MLlibTestSparkContext

/** Test single-node impurity aggregators used for local tree training */
class LocalTreeImpuritySuite extends SparkFunSuite with MLlibTestSparkContext {

  /** Computes the impurity of the passed-in values using the specified impurity metric */
  def getImpurity(labels: Array[Double], impurity: Impurity): Double = {
    val metadata = TreeTests.getMetadata(numExamples = 0,
      numFeatures = 0, numClasses = 2, featureArity = Map.empty, impurity = impurity)
    val agg = metadata.createImpurityAggregator()
    labels.foreach(agg.update)
    agg.getCalculator.calculate()
  }

  def getFreqs(labels: Array[Double]): Array[Double] = {
    // Get a map of unique labels to counts
    val counts = labels.groupBy(identity).mapValues(_.length).values.toArray
    val totalCount = counts.sum.toDouble
    counts.map(_ / totalCount)
  }

  /**
   * Given a function that returns the impurity of a single label class given its frequency,
   * tests single-node impurity calculations on a variety of test arrays.
   */
  def testImpurities(impurityFunc: (Double => Double), impurity: Impurity): Unit = {
    def testHelper(labels: Array[Double]): Unit = {
      val computedImpurity = getImpurity(labels, impurity)
      val freqs = getFreqs(labels)
      val expectedImpurity = freqs.map(impurityFunc).sum
      assert(computedImpurity == expectedImpurity)
    }

    // Test impurity calculations on a variety of label arrays
    testHelper(Array(1.0, 1.0, 0.0, 1.0, 0.0))
    testHelper(Array.empty)
    testHelper(Array(1.0))
    testHelper(Array(0.0))
    testHelper(Array.fill[Double](100)(1.0))
    testHelper(Array.fill[Double](100)(0.0))
  }

  test("Single node impurity: Entropy") {
    def entropyFromFreq(freq: Double): Double = {
      -freq * log2(freq)
    }
    testImpurities(entropyFromFreq, Entropy)
  }

  test("Single node impurity: Gini") {
    def giniFromFreq(freq: Double): Double = {
      freq * (1.0 - freq)
    }
    testImpurities(giniFromFreq, Gini)
  }

  test("Single node impurity: Variance") {
    def testHelper(labels: Array[Double]): Unit = {
      val impurity = getImpurity(labels, Variance)
      val count = labels.length.toDouble
      if (count == 0) {
        assert(impurity == 0)
      } else {
        val ex = labels.sum / count
        val ex2 = labels.map(label => label * label).sum / count
        val expectedImpurity = ex2 - ex * ex
        assert(impurity == expectedImpurity)
      }
    }

    // Test impurity calculations on a variety of label arrays
    testHelper(Array(1.0, 1.0, 0.0, 1.0, 0.0))
    testHelper(Array.empty)
    testHelper(Array(1.0))
    testHelper(Array(0.0))
    testHelper(Array.fill[Double](100)(1.0))
    testHelper(Array.fill[Double](100)(0.0))
  }

}
