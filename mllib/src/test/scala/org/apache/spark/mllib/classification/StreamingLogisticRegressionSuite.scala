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

package org.apache.spark.mllib.classification

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase

class StreamingLogisticRegressionSuite extends SparkFunSuite with TestSuiteBase {

  // use longer wait time to ensure job completion
  override def maxWaitTimeMillis: Int = 30000

  // Test if we can accurately learn B for Y = logistic(BX) on streaming data
  test("parameter accuracy") {

    val nPoints = 100
    val B = 1.5

    // create model
    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data
    val numBatches = 20
    val input = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // apply model training to input stream
    val ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check accuracy of final parameter estimates
    assert(model.latestModel().weights(0) ~== B relTol 0.1)

  }

  // Test that parameter estimates improve when learning Y = logistic(BX) on streaming data
  test("parameter convergence") {

    val B = 1.5
    val nPoints = 100

    // create model
    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0.0))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data
    val numBatches = 20
    val input = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // create buffer to store intermediate fits
    val history = new ArrayBuffer[Double](numBatches)

    // apply model training to input stream, storing the intermediate results
    // (we add a count to ensure the result is a DStream)
    val ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.foreachRDD(x => history.append(math.abs(model.latestModel().weights(0) - B)))
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

    val B = 1.5
    val nPoints = 100

    // create model initialized with true weights
    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.dense(1.5))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    val numBatches = 10
    val testInput = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // apply model predictions to test stream
    val ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })

    // collect the output as (true, estimated) tuples
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // check that at least 60% of predictions are correct on all batches
    val errors = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints)

    assert(errors.forall(x => x <= 0.4))
  }

  // Test training combined with prediction
  test("training and prediction") {
    // create model initialized with zero weights
    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.dense(-0.1))
      .setStepSize(0.01)
      .setNumIterations(10)

    // generate sequence of simulated data for testing
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, 5.0, nPoints, 42 * (i + 1))
    }

    // train and predict
    val ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })

    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // assert that prediction error improves, ensuring that the updated model is being used
    val error = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints).toList
    assert(error.head > 0.8 & error.last < 0.2)
  }

  // Test empty RDDs in a stream
  test("handling empty RDDs in a stream") {
    val model = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.dense(-0.1))
      .setStepSize(0.01)
      .setNumIterations(10)
    val numBatches = 10
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
