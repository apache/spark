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

package org.apache.spark.mllib.stat

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.stat.test.{StreamingTest, StreamingTestResult, StudentTTest,
  WelchTTest, BinarySample}
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter
import org.apache.spark.util.random.XORShiftRandom

class StreamingTestSuite extends SparkFunSuite with TestSuiteBase {

  override def maxWaitTimeMillis : Int = 30000

  test("accuracy for null hypothesis using welch t-test") {
    // set parameters
    val testMethod = "welch"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue > 0.05 && res.method == WelchTTest.methodName))
  }

  test("accuracy for alternative hypothesis using welch t-test") {
    // set parameters
    val testMethod = "welch"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue < 0.05 && res.method == WelchTTest.methodName))
  }

  test("accuracy for null hypothesis using student t-test") {
    // set parameters
    val testMethod = "student"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)


    assert(outputBatches.flatten.forall(res =>
      res.pValue > 0.05 && res.method == StudentTTest.methodName))
  }

  test("accuracy for alternative hypothesis using student t-test") {
    // set parameters
    val testMethod = "student"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue < 0.05 && res.method == StudentTTest.methodName))
  }

  test("batches within same test window are grouped") {
    // set parameters
    val testWindow = 3
    val numBatches = 5
    val pointsPerBatch = 100
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(testWindow)
      .setPeacePeriod(0)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input,
      (inputDStream: DStream[BinarySample]) => model.summarizeByKeyAndWindow(inputDStream))
    val outputBatches = runStreams[(Boolean, StatCounter)](ssc, numBatches, numBatches)
    val outputCounts = outputBatches.flatten.map(_._2.count)

    // number of batches seen so far does not exceed testWindow, expect counts to continue growing
    for (i <- 0 until testWindow) {
      assert(outputCounts.drop(2 * i).take(2).forall(_ == (i + 1) * pointsPerBatch / 2))
    }

    // number of batches seen exceeds testWindow, expect counts to be constant
    assert(outputCounts.drop(2 * (testWindow - 1)).forall(_ == testWindow * pointsPerBatch / 2))
  }


  test("entries in peace period are dropped") {
    // set parameters
    val peacePeriod = 3
    val numBatches = 7
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(peacePeriod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.dropPeacePeriod(inputDStream))
    val outputBatches = runStreams[(Boolean, Double)](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.length == (numBatches - peacePeriod) * pointsPerBatch)
  }

  test("null hypothesis when only data from one group is present") {
    // set parameters
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)

    val input = generateTestData(numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)
      .map(batch => batch.filter(_.isExperiment)) // only keep one test group

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(result => (result.pValue - 1.0).abs < 0.001))
  }

  // Generate testing input with half of the entries in group A and half in group B
  private def generateTestData(
      numBatches: Int,
      pointsPerBatch: Int,
      meanA: Double,
      stdevA: Double,
      meanB: Double,
      stdevB: Double,
      seed: Int): (IndexedSeq[IndexedSeq[BinarySample]]) = {
    val rand = new XORShiftRandom(seed)
    val numTrues = pointsPerBatch / 2
    val data = (0 until numBatches).map { i =>
      (0 until numTrues).map { idx => BinarySample(true, meanA + stdevA * rand.nextGaussian())} ++
        (pointsPerBatch / 2 until pointsPerBatch).map { idx =>
          BinarySample(false, meanB + stdevB * rand.nextGaussian())
        }
    }

    data
  }
}
