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
    denseSeed = (1 to 99).toSeq.map { i =>
      val label = i % 3
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    denseData = sc.parallelize(denseSeed.map(_._2))
    app = new HierarchicalClustering().setNumClusters(3).setRandomSeed(1)
    denseModel = app.run(denseData)

    sparseSeed = (1 to 99).toSeq.map { i =>
      val label = i % 3
      val vector = Vectors.sparse(3, Seq((label, label.toDouble)))
      (label, vector)
    }
    sparseData = sc.parallelize(sparseSeed.map(_._2))
    sparseModel = app.run(sparseData)
  }

  test("should get the array of ClusterTree") {
    val clusters = denseModel.getClusters()
    assert(clusters.isInstanceOf[Array[ClusterTree]])
    assert(clusters.size === 3)
  }

  test("should get the array of ClusterTree with sparse vectors") {
    val clusters2 = sparseModel.getClusters()
    assert(clusters2.isInstanceOf[Array[ClusterTree]])
    assert(clusters2.size === 3)
  }

  test("the number of predicted clusters should be equal to their origin") {
    val predictedData = denseModel.predict(denseData)
    // the number of contained vectors in each cluster is 10
    predictedData.map { case (i, vector) => (i, 1)}.reduceByKey(_ + _)
        .collect().foreach { case (idx, n) => assert(n === 33)}

    val predictedData2 = sparseModel.predict(sparseData)
    predictedData2.map { case (i, vector) => (i, 1)}.reduceByKey(_ + _)
        .collect().foreach { case (idx, n) => assert(n === 33)}
  }

  test("predicted result should be 1") {
    assert(denseModel.predict(Vectors.dense(1.0, 1.0, 1.0)) === 1)
    assert(sparseModel.predict(Vectors.sparse(3, Seq((1, 1.0)))) === 1)
  }

  test("predicted result should be same the seed data") {
    val predictedData = denseModel.predict(denseData).collect()
    assert(predictedData === denseSeed)

    val predictedData2 = sparseModel.predict(sparseData).collect()
    assert(predictedData2 === sparseSeed)
  }

  test("sum of the total variance") {
    assert(denseModel.computeCost() === 0.0)
    assert(sparseModel.computeCost() === 0.0)
  }
}
