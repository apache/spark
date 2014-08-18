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
import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LinearDataGenerator, LocalSparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils

class StreamingLinearRegressionSuite extends FunSuite with LocalSparkContext {

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

    val testDir = Files.createTempDir()
    val numBatches = 10
    val batchDuration = Milliseconds(1000)
    val ssc = new StreamingContext(sc, batchDuration)
    val data = ssc.textFileStream(testDir.toString).map(LabeledPoint.parse)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.1)
      .setNumIterations(50)

    model.trainOn(data)

    ssc.start()

    // write data to a file stream
    for (i <- 0 until numBatches) {
      val samples = LinearDataGenerator.generateLinearInput(
        0.0, Array(10.0, 10.0), 100, 42 * (i + 1))
      val file = new File(testDir, i.toString)
      Files.write(samples.map(x => x.toString).mkString("\n"), file, Charset.forName("UTF-8"))
      Thread.sleep(batchDuration.milliseconds)
    }

    ssc.stop(stopSparkContext=false)

    System.clearProperty("spark.driver.port")
    Utils.deleteRecursively(testDir)

    // check accuracy of final parameter estimates
    assertEqual(model.latestModel().intercept, 0.0, 0.1)
    assertEqual(model.latestModel().weights(0), 10.0, 0.1)
    assertEqual(model.latestModel().weights(1), 10.0, 0.1)

    // check accuracy of predictions
    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    validatePrediction(validationData.map(row => model.latestModel().predict(row.features)),
      validationData)
  }

  // Test that parameter estimates improve when learning Y = 10*X1 on streaming data
  test("streaming linear regression parameter convergence") {

    val testDir = Files.createTempDir()
    val batchDuration = Milliseconds(2000)
    val ssc = new StreamingContext(sc, batchDuration)
    val numBatches = 5
    val data = ssc.textFileStream(testDir.toString()).map(LabeledPoint.parse)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0))
      .setStepSize(0.1)
      .setNumIterations(50)

    model.trainOn(data)

    ssc.start()

    // write data to a file stream
    val history = new ArrayBuffer[Double](numBatches)
    for (i <- 0 until numBatches) {
      val samples = LinearDataGenerator.generateLinearInput(0.0, Array(10.0), 100, 42 * (i + 1))
      val file = new File(testDir, i.toString)
      Files.write(samples.map(x => x.toString).mkString("\n"), file, Charset.forName("UTF-8"))
      Thread.sleep(batchDuration.milliseconds)
      // wait an extra few seconds to make sure the update finishes before new data arrive
      Thread.sleep(4000)
      history.append(math.abs(model.latestModel().weights(0) - 10.0))
    }

    ssc.stop(stopSparkContext=false)

    System.clearProperty("spark.driver.port")
    Utils.deleteRecursively(testDir)

    val deltas = history.drop(1).zip(history.dropRight(1))
    // check error stability (it always either shrinks, or increases with small tol)
    assert(deltas.forall(x => (x._1 - x._2) <= 0.1))
    // check that error shrunk on at least 2 batches
    assert(deltas.map(x => if ((x._1 - x._2) < 0) 1 else 0).sum > 1)

  }

}
