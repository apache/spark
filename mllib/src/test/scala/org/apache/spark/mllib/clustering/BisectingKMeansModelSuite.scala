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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class BisectingKMeansModelSuite
    extends SparkFunSuite with MLlibTestSparkContext with BeforeAndAfterEach {

  test("clustering dense vectors") {
    val app = new BisectingKMeans().setK(5).setSeed(1)

    val localData = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    val data = sc.parallelize(localData.map(_._2))
    val model = app.run(data)

    val clusters = model.getClusters
    assert(clusters.isInstanceOf[Array[BisectingClusterNode]])
    assert(clusters.length === 5)

    val centers = model.getCenters.sortBy(_.toArray.sum)
    assert(centers.length === 5)
    assert(centers(0) === Vectors.dense(0.0, 0.0, 0.0))
    assert(centers(1) === Vectors.dense(1.0, 1.0, 1.0))
    assert(centers(2) === Vectors.dense(2.0, 2.0, 2.0))
    assert(centers(3) === Vectors.dense(3.0, 3.0, 3.0))
    assert(centers(4) === Vectors.dense(4.0, 4.0, 4.0))

    // predict with one vector
    assert(model.predict(Vectors.dense(0.0, 0.0, 0.0)) === 0)
    assert(model.predict(Vectors.dense(0.5, 0.5, 0.5)) === 0)
    assert(model.predict(Vectors.dense(1.0, 1.0, 1.0)) === 1)
    assert(model.predict(Vectors.dense(2.0, 2.0, 2.0)) === 2)
    assert(model.predict(Vectors.dense(3.0, 3.0, 3.0)) === 3)
    assert(model.predict(Vectors.dense(4.0, 4.0, 4.0)) === 4)

    // predict with a RDD
    val predicted = model.predict(data).collect()
    assert(predicted === localData.map(_._1))

    // compute WSSSE
    assert(model.WSSSE(data) === 0.0)
  }

  test("clustering sparse vectors") {
    val app = new BisectingKMeans().setK(5).setSeed(1)

    val localData = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.sparse(5, Seq((label, label.toDouble)))
      (label, vector)
    }
    val data = sc.parallelize(localData.map(_._2))
    val model = app.run(data)

    val clusters = model.getClusters
    assert(clusters.isInstanceOf[Array[BisectingClusterNode]])
    assert(clusters.length === 5)

    val centers = model.getCenters.sortBy(_.toArray.sum)
    assert(centers.length === 5)
    assert(centers(0) === Vectors.sparse(5, Array(), Array()))
    assert(centers(1) === Vectors.sparse(5, Array(1), Array(1.0)))
    assert(centers(2) === Vectors.sparse(5, Array(2), Array(2.0)))
    assert(centers(3) === Vectors.sparse(5, Array(3), Array(3.0)))
    assert(centers(4) === Vectors.sparse(5, Array(4), Array(4.0)))

    // predict with one vector
    assert(model.predict(Vectors.sparse(5, Array(0), Array(0.0))) === 0)
    assert(model.predict(Vectors.sparse(5, Array(1), Array(1.0))) === 1)
    assert(model.predict(Vectors.sparse(5, Array(2), Array(2.0))) === 2)
    assert(model.predict(Vectors.sparse(5, Array(3), Array(3.0))) === 3)
    assert(model.predict(Vectors.sparse(5, Array(4), Array(4.0))) === 4)

    // predict with a RDD
    val predicted = model.predict(data).collect()
    assert(predicted === localData.map(_._1))

    // compute WSSSE
    assert(model.WSSSE(data) === 0.0)
  }

  test("clustering should be done correctly") {
    for (numClusters <- Array(9, 19)) {
      val app = new BisectingKMeans().setK(numClusters).setSeed(1)
      val localData = (1 to 19).toSeq.map { i =>
        val label = i % numClusters
        val sparseVector = Vectors.sparse(numClusters, Seq((label, label.toDouble)))
        val denseVector = Vectors.fromBreeze(sparseVector.toBreeze.toDenseVector)
        (label, denseVector, sparseVector)
      }

      // dense version
      val denseData = sc.parallelize(localData.map(_._2), 2)
      val denseModel = app.run(denseData)
      assert(denseModel.getCenters.length === numClusters)
      assert(denseModel.getClusters.forall(_.cost == 0.0))

      // sparse version
      val sparseData = sc.parallelize(localData.map(_._3), 2)
      val sparseModel = app.run(sparseData)
      assert(sparseModel.getCenters.length === numClusters)
      assert(sparseModel.getClusters.forall(_.cost == 0.0))
    }
  }
}
