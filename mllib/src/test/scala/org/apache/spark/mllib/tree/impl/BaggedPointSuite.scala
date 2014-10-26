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

package org.apache.spark.mllib.tree.impl

import org.scalatest.FunSuite

import org.apache.spark.mllib.tree.RandomForestSuite

import org.apache.spark.mllib.util.LocalSparkContext

/**
 * Test suite for [[BaggedPoint]].
 */
class BaggedPointSuite extends FunSuite with LocalSparkContext  {

  test("BaggedPoint RDD: without subsampling") {
    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 1)
    val rdd = sc.parallelize(arr)
    val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, 1.0, 1, false)
    baggedRDD.collect().foreach { baggedPoint =>
      assert(baggedPoint.subsampleWeights.size == 1 && baggedPoint.subsampleWeights(0) == 1)
    }
  }

  test("BaggedPoint RDD: with subsampling") {
    val numSubsamples = 100
    val (expectedMean, expectedStddev) = (1.0, 1.0)

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 1)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, 1.0, numSubsamples, true)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      RandomForestSuite.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }

}