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

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.linalg.Vectors

class KMeansSuite extends FunSuite with LocalSparkContext {

  import KMeans.{RANDOM, K_MEANS_PARALLEL}

  test("single cluster") {
    val data = sc.parallelize(Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    ))

    val center = Vectors.dense(1.0, 3.0, 4.0)

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    var model = KMeans.train(data, k=1, maxIterations=1)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=2)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=1, initializationMode=RANDOM)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(
      data, k=1, maxIterations=1, runs=1, initializationMode=K_MEANS_PARALLEL)
    assert(model.clusterCenters.head === center)
  }

  test("single cluster with big dataset") {
    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.dense(1.0, 3.0, 4.0)

    var model = KMeans.train(data, k=1, maxIterations=1)
    assert(model.clusterCenters.size === 1)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=2)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=1, initializationMode=RANDOM)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=1, initializationMode=K_MEANS_PARALLEL)
    assert(model.clusterCenters.head === center)
  }

  test("single cluster with sparse data") {

    val n = 10000
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

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0)))

    var model = KMeans.train(data, k=1, maxIterations=1)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=2)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=5)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=1, initializationMode=RANDOM)
    assert(model.clusterCenters.head === center)

    model = KMeans.train(data, k=1, maxIterations=1, runs=1, initializationMode=K_MEANS_PARALLEL)
    assert(model.clusterCenters.head === center)

    data.unpersist()
  }

  test("k-means|| initialization") {
    val points = Seq(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0),
      Vectors.dense(1.0, 0.0, 1.0),
      Vectors.dense(1.0, 1.0, 1.0)
    )
    val rdd = sc.parallelize(points)

    // K-means|| initialization should place all clusters into distinct centers because
    // it will make at least five passes, and it will give non-zero probability to each
    // unselected point as long as it hasn't yet selected all of them

    var model = KMeans.train(rdd, k=5, maxIterations=1)
    assert(Set(model.clusterCenters: _*) === Set(points: _*))

    // Iterations of Lloyd's should not change the answer either
    model = KMeans.train(rdd, k=5, maxIterations=10)
    assert(Set(model.clusterCenters: _*) === Set(points: _*))

    // Neither should more runs
    model = KMeans.train(rdd, k=5, maxIterations=10, runs=5)
    assert(Set(model.clusterCenters: _*) === Set(points: _*))
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
    val rdd = sc.parallelize(points, 3)

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {
      // Two iterations are sufficient no matter where the initial centers are.
      val model = KMeans.train(rdd, k = 2, maxIterations = 2, runs = 1, initMode)

      val predicts = model.predict(rdd).collect()

      assert(predicts(0) === predicts(1))
      assert(predicts(0) === predicts(2))
      assert(predicts(3) === predicts(4))
      assert(predicts(3) === predicts(5))
      assert(predicts(0) != predicts(3))
    }
  }
}
