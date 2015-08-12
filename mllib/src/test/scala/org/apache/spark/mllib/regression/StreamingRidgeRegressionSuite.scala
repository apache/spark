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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase

class StreamingRidgeRegressionSuite extends FunSuite with TestSuiteBase {

  // Test predictions on a stream
  test("predictions") {
    // create model initialized with true weights
    val model = new StreamingRidgeRegressionWithSGD()
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
}
