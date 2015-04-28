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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class HierarchicalClusteringModelSuite
    extends FunSuite with MLlibTestSparkContext with BeforeAndAfterEach {

  test("clustering dense vectors") {
    val app = new HierarchicalClustering().setNumClusters(5).setSeed(1)

    val localData = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    val data = sc.parallelize(localData.map(_._2))
    val model = app.run(data)

    val clusters = model.getClusters
    assert(clusters.isInstanceOf[Array[ClusterTree]])
    assert(clusters.size === 5)

    val centers = model.getCenters.sortBy(_.toArray.sum)
    assert(centers.size === 5)
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

    // adjacency list
    val adjacencyList = model.toAdjacencyList()
        .map(x => (x._1, x._2, math.round(10E3 * x._3) / 10E3))
    assert(adjacencyList.size === 8)
    assert(adjacencyList(0) === (0, 1, 2.5981))
    assert(adjacencyList(1) === (0, 6, 2.5981))
    assert(adjacencyList(2) === (1, 2, 1.7321))
    assert(adjacencyList(3) === (1, 5, 1.7321))
    assert(adjacencyList(4) === (2, 3, 0.866))
    assert(adjacencyList(5) === (2, 4, 0.866))
    assert(adjacencyList(6) === (6, 7, 0.866))
    assert(adjacencyList(7) === (6, 8, 0.866))

    // linkage matrix
    val linkageMatrix = model.toLinkageMatrix()
        .map(x => (x._1, x._2, math.round(10E3 * x._3) / 10E3, x._4))
    assert(linkageMatrix.size === 4)
    assert(linkageMatrix(0) === (0, 1, 0.866, 2))
    assert(linkageMatrix(1) === (3, 4, 0.866, 2))
    assert(linkageMatrix(2) === (5, 2, 2.5981, 3))
    assert(linkageMatrix(3) === (7, 6, 5.1962, 5))
  }

  test("clustering sparse vectors") {
    val app = new HierarchicalClustering().setNumClusters(5).setSeed(1)

    val localData = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.sparse(5, Seq((label, label.toDouble)))
      (label, vector)
    }
    val data = sc.parallelize(localData.map(_._2))
    val model = app.run(data)

    val clusters = model.getClusters
    assert(clusters.isInstanceOf[Array[ClusterTree]])
    assert(clusters.size === 5)

    val centers = model.getCenters.sortBy(_.toArray.sum)
    assert(centers.size === 5)
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

    // adjacency list
    val adjacencyList = model.toAdjacencyList()
        .map(x => (x._1, x._2, math.round(10E3 * x._3) / 10E3))
    assert(adjacencyList.size === 8)
    assert(adjacencyList(0) === (0, 1, 1.5652))
    assert(adjacencyList(1) === (0, 6, 1.5652))
    assert(adjacencyList(2) === (1, 2, 1.3744))
    assert(adjacencyList(3) === (1, 5, 1.3744))
    assert(adjacencyList(4) === (2, 3, 0.5))
    assert(adjacencyList(5) === (2, 4, 0.5))
    assert(adjacencyList(6) === (6, 7, 2.5))
    assert(adjacencyList(7) === (6, 8, 2.5))

    // linkage matrix
    val linkageMatrix = model.toLinkageMatrix()
        .map(x => (x._1, x._2, math.round(10E3 * x._3) / 10E3, x._4))
    assert(linkageMatrix.size === 4)
    assert(linkageMatrix(0) === (0, 1, 0.5, 2))
    assert(linkageMatrix(1) === (5, 2, 1.8744, 3))
    assert(linkageMatrix(2) === (3, 4, 2.5, 2))
    assert(linkageMatrix(3) === (6, 7, 4.0652, 5))
  }

  test("clustering should be done correctly") {
    for (numClusters <- Array(9, 99, 999)) {
      val app = new HierarchicalClustering().setNumClusters(numClusters).setSeed(1)
      val localData = (1 to 1000).toSeq.map { i =>
        val label = i % numClusters
        val sparseVector = Vectors.sparse(numClusters, Seq((label, label.toDouble)))
        val denseVector = Vectors.fromBreeze(sparseVector.toBreeze.toDenseVector)
        (label, denseVector, sparseVector)
      }
      // dense version
      val denseData = sc.parallelize(localData.map(_._2), 2)
      val denseModel = app.run(denseData)
      assert(denseModel.getCenters.size === numClusters)
      assert(denseModel.getClusters.forall(_.variancesNorm == 0.0))

      // sparse version
      val sparseData = sc.parallelize(localData.map(_._3), 2)
      val sparseModel = app.run(sparseData)
      assert(sparseModel.getCenters.size === numClusters)
      assert(sparseModel.getClusters.forall(_.variancesNorm == 0.0))
    }
  }

  test("save a model, and then load the model") {
    val app = new HierarchicalClustering().setNumClusters(5).setSeed(1)

    val localData = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    val data = sc.parallelize(localData.map(_._2))
    val model = app.run(data)

    val tmpFile = java.io.File.createTempFile("hierarchical-clustering", "save-load")
    model.save(sc, tmpFile.getAbsolutePath)

    val sameModel = HierarchicalClusteringModel.load(sc, tmpFile.getAbsolutePath)
    assert(sameModel.getClass.getSimpleName.toString === "HierarchicalClusteringModel")
    localData.foreach { case (label, vector) =>
        assert(model.predict(vector) === sameModel.predict(vector))
    }
  }
}
