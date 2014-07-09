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

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.linalg.Vectors

class KMeansMiniBatchSuite extends FunSuite with LocalSparkContext {

  import KMeans.{RANDOM, K_MEANS_PARALLEL}
  
  val epsilon = 1e-01

  test("single cluster with big dataset") {
    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    
    // produce 300 data points
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)
    
    data.persist()

    // result should converge to given center after a few iterations
    val center = Vectors.dense(1.0, 3.0, 4.0).toBreeze

    var model = KMeansMiniBatch.train(data, k=1, batchSize=50, maxIterations=50, runs=1, initializationMode=RANDOM)
    var error = breezeNorm(model.clusterCenters.head.toBreeze - center, 2.0)
    assert(error < epsilon, "Error (" + error + ") was larger than expected value of " + epsilon)

    model = KMeansMiniBatch.train(data, k=1, batchSize=50, maxIterations=50, runs=1, initializationMode=K_MEANS_PARALLEL)
    error = breezeNorm(model.clusterCenters.head.toBreeze - center, 2.0)
    assert(error < epsilon, "Error (" + error + ") was larger than expected value of " + epsilon)

  }

  test("single cluster with sparse data") {

    val n = 10000
    // 600 data points
    val data = sc.parallelize((1 to 100).flatMap { i =>
      val x = i / 1000.0
      Array(
        Vectors.sparse(n, Seq((0, 1.0 + x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0 - x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 - x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 - x)))
      )
    }, 4)

    data.persist()
    
    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0))).toBreeze

    var model = KMeansMiniBatch.train(data, k=1, batchSize=100, maxIterations=50, runs=1, initializationMode=RANDOM)
    var error = breezeNorm(model.clusterCenters.head.toBreeze - center, 2.0)
    assert(error < epsilon, "Error (" + error + ") was larger than expected value of " + epsilon)


    model = KMeansMiniBatch.train(data, k=1, batchSize=100, maxIterations=50, runs=1, initializationMode=K_MEANS_PARALLEL)
    error = breezeNorm(model.clusterCenters.head.toBreeze - center, 2.0)
    assert(error < epsilon, "Error (" + error + ") was larger than expected value of " + epsilon)


    data.unpersist()
  }

  test("two clusters") {
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0)
    )
    
    val rdd = sc.parallelize((1 to 10000).flatMap(_ => points), 3)
    
    rdd.persist()

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {
      val model = KMeansMiniBatch.train(rdd, k = 2, batchSize=10, maxIterations = 10, runs = 1, initMode)

      val predicts = model.predict(rdd).collect()

      assert(predicts(0) === predicts(1))
      assert(predicts(0) === predicts(2))
      assert(predicts(3) === predicts(4))
      assert(predicts(3) === predicts(5))
      assert(predicts(0) != predicts(3))
    }
  }
}
