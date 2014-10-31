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

package org.apache.spark.mllib.clustering

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase

class StreamingKMeansSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 30000

  test("accuracy for single center and equivalence to grand average") {

    // set parameters
    val numBatches = 10
    val numPoints = 50
    val k = 1
    val d = 5
    val r = 0.1

    // create model with one cluster
    val model = new StreamingKMeans()
      .setK(1)
      .setDecayFactor(1.0)
      .setInitialCenters(Array(Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0)))

    // generate random data for kmeans
    val (input, centers) = StreamingKMeansDataGenerator(numPoints, numBatches, k, d, r, 42)

    // setup and run the model training
    val ssc = setupStreams(input, (inputDStream: DStream[Vector]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // estimated center should be close to true center
    assert(centers(0) ~== model.latestModel().clusterCenters(0) absTol 1E-1)

    // estimated center from streaming should exactly match the arithmetic mean of all data points
    // because the decay factor is set to 1.0
    val grandMean = input.flatten.map(x => x.toBreeze).reduce(_+_) / (numBatches * numPoints).toDouble
    assert(model.latestModel().clusterCenters(0) ~== Vectors.dense(grandMean.toArray) absTol 1E-5)

  }

  test("accuracy for two centers") {

    val numBatches = 10
    val numPoints = 5
    val k = 2
    val d = 5
    val r = 0.1

    // create model with two clusters
    val model = new StreamingKMeans()
      .setK(2)
      .setDecayFactor(1.0)
      .setInitialCenters(Array(Vectors.dense(-0.1, 0.1, -0.2, -0.3, -0.1),
                               Vectors.dense(0.1, -0.2, 0.0, 0.2, 0.1)))

    // generate random data for kmeans
    val (input, centers) = StreamingKMeansDataGenerator(numPoints, numBatches, k, d, r, 42)

    // setup and run the model training
    val ssc = setupStreams(input, (inputDStream: DStream[Vector]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check that estimated centers are close to true centers
    // NOTE exact assignment depends on the initialization!
    assert(centers(0) ~== model.latestModel().clusterCenters(0) absTol 1E-1)
    assert(centers(1) ~== model.latestModel().clusterCenters(1) absTol 1E-1)

  }

  def StreamingKMeansDataGenerator(
      numPoints: Int,
      numBatches: Int,
      k: Int,
      d: Int,
      r: Double,
      seed: Int,
      initCenters: Array[Vector] = null): (IndexedSeq[IndexedSeq[Vector]], Array[Vector]) = {
    val rand = new Random(seed)
    val centers = initCenters match {
      case null => Array.fill(k)(Vectors.dense(Array.fill(d)(rand.nextGaussian())))
      case _ => initCenters
    }
    val data = (0 until numBatches).map { i =>
      (0 until numPoints).map { idx =>
        val center = centers(idx % k)
        Vectors.dense(Array.tabulate(d)(x => center(x) + rand.nextGaussian() * r))
      }
    }
    (data, centers)
  }


}
