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

package org.apache.spark.mllib.regression

import java.io.File

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.mllib.util.{MLStreamingUtils, LinearDataGenerator, LocalSparkContext}

import scala.collection.mutable.ArrayBuffer

class StreamingLinearRegressionSuite extends FunSuite {

  // Assert that two values are equal within tolerance epsilon
  def assertEqual(v1: Double, v2: Double, epsilon: Double) {
    def errorMessage = v1.toString + " did not equal " + v2.toString
    assert(math.abs(v1-v2) <= epsilon, errorMessage)
  }

  // Assert that model predictions are correct
  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  // Test if we can accurately learn Y = 10*X1 + 10*X2 on streaming data
  test("streaming linear regression parameter accuracy") {

    val conf = new SparkConf().setMaster("local").setAppName("streaming test")
    val testDir = Files.createTempDir()
    val numBatches = 10
    val ssc = new StreamingContext(conf, Seconds(1))
    val data = MLStreamingUtils.loadLabeledPointsFromText(ssc, testDir.toString)
    val model = StreamingLinearRegressionWithSGD.start(numFeatures=2, numIterations=50)

    model.trainOn(data)

    ssc.start()

    // write data to a file stream
    Thread.sleep(5000)
    for (i <- 0 until numBatches) {
      val samples = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42 * (i + 1))
      val file = new File(testDir, i.toString)
      FileUtils.writeStringToFile(file, samples.map(x => x.toString).mkString("\n"))
      Thread.sleep(Milliseconds(1000).milliseconds)
    }
    Thread.sleep(Milliseconds(5000).milliseconds)

    ssc.stop()

    System.clearProperty("spark.driver.port")
    FileUtils.deleteDirectory(testDir)

    // check accuracy of final parameter estimates
    assertEqual(model.latest().intercept, 0.0, 0.1)
    assertEqual(model.latest().weights(0), 10.0, 0.1)
    assertEqual(model.latest().weights(1), 10.0, 0.1)

    // check accuracy of predictions
    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    validatePrediction(validationData.map(row => model.latest().predict(row.features)), validationData)
  }

  // Test that parameter estimates improve when learning Y = 10*X1 on streaming data
  test("streaming linear regression parameter convergence") {

    val conf = new SparkConf().setMaster("local").setAppName("streaming test")
    val testDir = Files.createTempDir()
    val ssc = new StreamingContext(conf, Seconds(1))
    val numBatches = 5
    val data = MLStreamingUtils.loadLabeledPointsFromText(ssc, testDir.toString)
    val model = StreamingLinearRegressionWithSGD.start(numFeatures=1, numIterations=50)

    model.trainOn(data)

    ssc.start()

    // write data to a file stream
    val history = new ArrayBuffer[Double](numBatches)
    Thread.sleep(5000)
    for (i <- 0 until numBatches) {
      val samples = LinearDataGenerator.generateLinearInput(0.0, Array(10.0), 100, 42 * (i + 1))
      val file = new File(testDir, i.toString)
      FileUtils.writeStringToFile(file, samples.map(x => x.toString).mkString("\n"))
      Thread.sleep(Milliseconds(1000).milliseconds)
      history.append(math.abs(model.latest().weights(0) - 10.0))
    }
    Thread.sleep(Milliseconds(5000).milliseconds)

    ssc.stop()

    System.clearProperty("spark.driver.port")
    FileUtils.deleteDirectory(testDir)

    // check that error is always getting smaller
    assert(history.drop(1).zip(history.dropRight(1)).forall(x => (x._1 - x._2) < 0))

  }

}
