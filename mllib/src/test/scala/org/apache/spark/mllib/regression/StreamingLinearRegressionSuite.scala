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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase

class StreamingLinearRegressionSuite extends SparkFunSuite with TestSuiteBase {

  // use longer wait time to ensure job completion
  override def maxWaitTimeMillis: Int = 20000

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
  test("parameter accuracy") {
    // create model
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data
    val numBatches = 10
    val input = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42 * (i + 1))
    }

    // apply model training to input stream
    val ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

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
  test("parameter convergence") {
    // create model
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data
    val numBatches = 10
    val input = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0), 100, 42 * (i + 1))
    }

    // create buffer to store intermediate fits
    val history = new ArrayBuffer[Double](numBatches)

    // apply model training to input stream, storing the intermediate results
    // (we add a count to ensure the result is a DStream)
    val ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.foreachRDD(x => history.append(math.abs(model.latestModel().weights(0) - 10.0)))
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // compute change in error
    val deltas = history.drop(1).zip(history.dropRight(1))
    // check error stability (it always either shrinks, or increases with small tol)
    assert(deltas.forall(x => (x._1 - x._2) <= 0.1))
    // check that error shrunk on at least 2 batches
    assert(deltas.map(x => if ((x._1 - x._2) < 0) 1 else 0).sum > 1)
  }

  // Test predictions on a stream
  test("predictions") {
    // create model initialized with true weights
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(10.0, 10.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), nPoints, 42 * (i + 1))
    }

    // apply model predictions to test stream
    val ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })
    // collect the output as (true, estimated) tuples
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // compute the mean absolute error and check that it's always less than 0.1
    val errors = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints)
    assert(errors.forall(x => x <= 0.1))
  }

  // Test training combined with prediction
  test("training and prediction") {
    // create model initialized with zero weights
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), nPoints, 42 * (i + 1))
    }

    // train and predict
    val ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })

    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // assert that prediction error improves, ensuring that the updated model is being used
    val error = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints).toList
    assert((error.head - error.last) > 2)
  }

  // Test empty RDDs in a stream
  test("handling empty RDDs in a stream") {
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)
      .setNumIterations(25)
    val numBatches = 10
    val nPoints = 100
    val emptyInput = Seq.empty[Seq[LabeledPoint]]
    val ssc = setupStreams(emptyInput,
      (inputDStream: DStream[LabeledPoint]) => {
        model.trainOn(inputDStream)
        model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
      }
    )
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)
  }
}
