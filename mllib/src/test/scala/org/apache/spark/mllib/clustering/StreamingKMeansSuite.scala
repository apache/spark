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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.random.XORShiftRandom

class StreamingKMeansSuite extends SparkFunSuite with TestSuiteBase {

  override def maxWaitTimeMillis: Int = 30000

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

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
      .setInitialCenters(Array(Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0)), Array(0.0))

    // generate random data for k-means
    val (input, centers) = StreamingKMeansDataGenerator(numPoints, numBatches, k, d, r, 42)

    // setup and run the model training
    ssc = setupStreams(input, (inputDStream: DStream[Vector]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // estimated center should be close to true center
    assert(centers(0) ~== model.latestModel().clusterCenters(0) absTol 1E-1)

    // estimated center from streaming should exactly match the arithmetic mean of all data points
    // because the decay factor is set to 1.0
    val grandMean =
      input.flatten.map(x => x.asBreeze).reduce(_ + _) / (numBatches * numPoints).toDouble
    assert(model.latestModel().clusterCenters(0) ~== Vectors.dense(grandMean.toArray) absTol 1E-5)
  }

  test("accuracy for two centers") {
    val numBatches = 10
    val numPoints = 5
    val k = 2
    val d = 5
    val r = 0.1

    // create model with two clusters
    val kMeans = new StreamingKMeans()
      .setK(2)
      .setHalfLife(2, "batches")
      .setInitialCenters(
        Array(Vectors.dense(-0.1, 0.1, -0.2, -0.3, -0.1),
          Vectors.dense(0.1, -0.2, 0.0, 0.2, 0.1)),
        Array(5.0, 5.0))

    // generate random data for k-means
    val (input, centers) = StreamingKMeansDataGenerator(numPoints, numBatches, k, d, r, 42)

    // setup and run the model training
    ssc = setupStreams(input, (inputDStream: DStream[Vector]) => {
      kMeans.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check that estimated centers are close to true centers
    // cluster ordering is arbitrary, so choose closest cluster
    val d0 = Vectors.sqdist(kMeans.latestModel().clusterCenters(0), centers(0))
    val d1 = Vectors.sqdist(kMeans.latestModel().clusterCenters(0), centers(1))
    val (c0, c1) = if (d0 < d1) {
      (centers(0), centers(1))
    } else {
      (centers(1), centers(0))
    }
    assert(c0 ~== kMeans.latestModel().clusterCenters(0) absTol 1E-1)
    assert(c1 ~== kMeans.latestModel().clusterCenters(1) absTol 1E-1)
  }

  test("detecting dying clusters") {
    val numBatches = 10
    val numPoints = 5
    val k = 1
    val d = 1
    val r = 1.0

    // create model with two clusters
    val kMeans = new StreamingKMeans()
      .setK(2)
      .setHalfLife(0.5, "points")
      .setInitialCenters(
        Array(Vectors.dense(0.0), Vectors.dense(1000.0)),
        Array(1.0, 1.0))

    // new data are all around the first cluster 0.0
    val (input, _) =
      StreamingKMeansDataGenerator(numPoints, numBatches, k, d, r, 42, Array(Vectors.dense(0.0)))

    // setup and run the model training
    ssc = setupStreams(input, (inputDStream: DStream[Vector]) => {
      kMeans.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check that estimated centers are close to true centers
    // NOTE exact assignment depends on the initialization!
    val model = kMeans.latestModel()
    val c0 = model.clusterCenters(0)(0)
    val c1 = model.clusterCenters(1)(0)

    assert(c0 * c1 < 0.0, "should have one positive center and one negative center")
    // 0.8 is the mean of half-normal distribution
    assert(math.abs(c0) ~== 0.8 absTol 0.6)
    assert(math.abs(c1) ~== 0.8 absTol 0.6)
  }

  test("SPARK-7946 setDecayFactor") {
    val kMeans = new StreamingKMeans()
    assert(kMeans.decayFactor === 1.0)
    kMeans.setDecayFactor(2.0)
    assert(kMeans.decayFactor === 2.0)
  }

  def StreamingKMeansDataGenerator(
      numPoints: Int,
      numBatches: Int,
      k: Int,
      d: Int,
      r: Double,
      seed: Int,
      initCenters: Array[Vector] = null): (IndexedSeq[IndexedSeq[Vector]], Array[Vector]) = {
    val rand = new XORShiftRandom(seed)
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
