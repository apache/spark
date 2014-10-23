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

  var seed: Seq[(Int, Vector)] = _
  var data: RDD[Vector] = _
  var app: HierarchicalClustering = _
  var model: HierarchicalClusteringModel = _

  override def beforeEach() {
    seed = (0 to 49).toSeq.map { i =>
      val label = i % 5
      val vector = Vectors.dense(label, label, label)
      (label, vector)
    }
    data = sc.parallelize(seed.map(_._2))

    val conf = new HierarchicalClusteringConf().setNumClusters(10).setRandomSeed(1)
    app = new HierarchicalClustering(conf)
    model = app.run(data)
  }

  test("should get the array of ClusterTree") {
    val clusters = model.getClusters()
    assert(clusters.isInstanceOf[Array[ClusterTree]])
    assert(clusters.size === 5)
  }

  test("the number of predicted clusters should be equal to their origin") {
    val predictedData = model.predict(data)
    // the number of contained vectors in each cluster is 10
    predictedData.map { case (i, vector) => (i, 1)}.reduceByKey(_ + _)
        .collect().foreach { case (idx, n) => assert(n === 10)}
  }

  test("predicted result should be same the seed data") {
    val predictedData = model.predict(data).collect()
    assert(predictedData === seed)
  }

  test("sum of the total variance") {
    assert(model.computeCost() === 0.0)
  }
}
