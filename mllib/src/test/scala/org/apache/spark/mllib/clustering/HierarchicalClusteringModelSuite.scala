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

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class HierarchicalClusteringModelSuite
    extends FunSuite with LocalSparkContext with BeforeAndAfterEach {

  var denseSeed: Seq[(Int, Vector)] = _
  var sparseSeed: Seq[(Int, Vector)] = _
  var denseData: RDD[Vector] = _
  var sparseData: RDD[Vector] = _
  var app: HierarchicalClustering = _
  var denseModel: HierarchicalClusteringModel = _
  var sparseModel: HierarchicalClusteringModel = _

  override def beforeEach() {
    denseSeed = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    denseData = sc.parallelize(denseSeed.map(_._2))
    app = new HierarchicalClustering().setNumClusters(5).setRandomSeed(1)
    denseModel = app.run(denseData)

    sparseSeed = (1 to 100).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.sparse(5, Seq((label, label.toDouble)))
      (label, vector)
    }
    sparseData = sc.parallelize(sparseSeed.map(_._2))
    sparseModel = app.run(sparseData)
  }

  test("should get the array of ClusterTree") {
    val clusters = denseModel.getClusters()
    assert(clusters.isInstanceOf[Array[ClusterTree]])
    assert(clusters.size === 5)
  }

  test("should get the array of ClusterTree with sparse vectors") {
    val clusters2 = sparseModel.getClusters()
    assert(clusters2.isInstanceOf[Array[ClusterTree]])
    assert(clusters2.size === 5)
  }

  test("the number of predicted clusters should be equal to their origin") {
    val predictedData = denseModel.predict(denseData)
    // the number of contained vectors in each cluster is 10
    predictedData.map { case (i, vector) => (i, 1)}.reduceByKey(_ + _)
        .collect().foreach { case (idx, n) => assert(n === 20)}

    val predictedData2 = sparseModel.predict(sparseData)
    predictedData2.map { case (i, vector) => (i, 1)}.reduceByKey(_ + _)
        .collect().foreach { case (idx, n) => assert(n === 20)}
  }

  test("predicted result should be 1") {
    assert(denseModel.predict(Vectors.dense(1.0, 1.0, 1.0)) === 1)

    val sparseVector = Vectors.sparse(5, Seq((1, 1.0)))
    assert(sparseModel.predict(sparseVector) === 1)
  }

  test("predicted result should be same the seed data") {
    val densePredictedData = denseModel.predict(denseData).collect()
    assert(densePredictedData(0) === densePredictedData(5))
    assert(densePredictedData(1) === densePredictedData(6))
    assert(densePredictedData(2) === densePredictedData(7))
    assert(densePredictedData(3) === densePredictedData(8))
    assert(densePredictedData(4) === densePredictedData(9))

    val sparsePredictedData = sparseModel.predict(sparseData).collect()
    assert(sparsePredictedData(0) === sparsePredictedData(5))
    assert(sparsePredictedData(1) === sparsePredictedData(6))
    assert(sparsePredictedData(2) === sparsePredictedData(7))
    assert(sparsePredictedData(3) === sparsePredictedData(8))
    assert(sparsePredictedData(4) === sparsePredictedData(9))
  }

  test("sum of the total variance") {
    assert(denseModel.getSumOfVariance() === 0.0)
    assert(sparseModel.getSumOfVariance() === 0.0)
  }

  test("a model should be clonable") {
    val denseClone = denseModel.clone()
    val sparseClone = sparseModel.clone()
    assert(denseModel.clusterTree.hashCode() != denseClone.clusterTree.hashCode())
    assert(sparseModel.clusterTree.hashCode() != sparseClone.clusterTree.hashCode())
    assert(denseModel.clusterTree.getTreeSize() === denseClone.clusterTree.getTreeSize())
    assert(sparseModel.clusterTree.getTreeSize() === sparseClone.clusterTree.getTreeSize())
  }

  test("a model can be cut by height") {
    val cutDenseModel = denseModel.cut(6.0)
    val cutSparseModel = sparseModel.cut(8.0)

    assert(cutDenseModel.hashCode() !== denseModel.hashCode())
    assert(cutSparseModel.hashCode() !== sparseModel.hashCode())

    assert(cutDenseModel.getClusters().size === 2)
    assert(cutSparseModel.getClusters().size === 2)
    assert(denseModel.getClusters().size === 5)
    assert(sparseModel.getClusters().size === 5)

    assert(denseModel.predict(denseData).collect().map(_._1).distinct.sorted === (0 to 4).toArray)
    assert(cutDenseModel.predict(denseData).collect().map(_._1).distinct.sorted === Array(0, 1))
    assert(sparseModel.predict(sparseData).collect().map(_._1).distinct.sorted === (0 to 4).toArray)
    assert(cutSparseModel.predict(sparseData).collect().map(_._1).distinct.sorted === Array(0, 1))
  }

  test("A cluster tree should be converted a merging nodes list") {
    assert(denseModel.toMergeList().size === 4)
    assert(denseModel.cut(4.0).toMergeList().size === 2)
    assert(sparseModel.toMergeList().size === 4)
    assert(sparseModel.cut(8.0).toMergeList().size === 1)
  }
}
