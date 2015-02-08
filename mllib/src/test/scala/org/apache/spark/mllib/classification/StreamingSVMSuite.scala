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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase

class StreamingSVMSuite extends FunSuite with TestSuiteBase {

  // use longer wait time to ensure job completion
  override def maxWaitTimeMillis = 40000

  // Test only predictions on a stream
  test("predictions") {

    val A = 0.0
    val B = -1.5
    val C = 1.0
    val nPoints = 100

    // create model initialized with true weights
    val model = new StreamingSVMWithSGD()
      .setInitialWeights(Vectors.dense(B, C))
      .setStepSize(0.2)
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    val numBatches = 10
    val testInput = (0 until numBatches).map { i =>
      SVMSuite.generateSVMInput(A, Array(B, C), nPoints, 42 * (i + 1))
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

}
