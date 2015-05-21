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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => breezeNorm}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.scalatest.FunSuite


class HierarchicalClusteringSuite extends FunSuite with MLlibTestSparkContext {

  test("the root index is equal to 1") {
    assert(HierarchicalClustering.ROOT_INDEX_KEY === 1)
  }

  test("findClosestCenter") {
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val centers = Seq(
      Vectors.sparse(5, Array(0, 1, 2), Array(0.0, 1.0, 2.0)).toBreeze,
      Vectors.sparse(5, Array(1, 2, 3), Array(1.0, 2.0, 3.0)).toBreeze,
      Vectors.sparse(5, Array(2, 3, 4), Array(2.0, 3.0, 4.0)).toBreeze
    )

    for (i <- 0 to (centers.size - 1)) {
      val point = centers(i)
      val closestIndex = HierarchicalClustering.findClosestCenter(metric)(centers)(point)
      assert(closestIndex === i)
    }
  }

  test("run") {
    val algo = new HierarchicalClustering().setNumClusters(123)
    val localSeed: Seq[Vector] = (0 to 999).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val data = sc.parallelize(localSeed, 2)
    val model = algo.run(data)
    assert(model.getClusters.size == 123)
    assert(model.tree.getHeight ~== 702.8641 absTol 10E-4)

    // check the relations between a parent cluster and its children
    assert(model.tree.getParent === None)
    assert(model.tree.getChildren.apply(0).getParent.get === model.tree)
    assert(model.tree.getChildren.apply(1).getParent.get === model.tree)
    assert(model.getClusters.forall(_.getParent != None))
  }

  test("run with too many cluster size than the records") {
    val algo = new HierarchicalClustering().setNumClusters(123)
    val localSeed: Seq[Vector] = (0 to 99).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val data = sc.parallelize(localSeed, 2)
    val model = algo.run(data)
    assert(model.getClusters.size == 100)
    assert(model.tree.getHeight ~== 72.12489 absTol 10E-4)
  }

  test("initializeData") {
    val algo = new HierarchicalClustering
    val localSeed: Seq[Vector] = (0 to 99).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val seed = sc.parallelize(localSeed)
    val data = algo.initData(seed)
    assert(data.map(_._1).collect().distinct === Array(1))
  }

  test("get center stats") {
    val algo = new HierarchicalClustering
    val localSeed: Seq[Vector] = (0 to 99).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val seed = sc.parallelize(localSeed)
    val data = algo.initData(seed)

    val clusters = algo.summarizeAsClusters(data)
    val center = clusters(1).center
    assert(clusters.size === 1)
    assert(clusters(1).center === Vectors.dense(49.5, 49.5))
    assert(clusters(1).records === 100)

    val data2 = seed.map(v => ((v.apply(0) / 25).toLong + 1L, v.toBreeze))
    val clusters2 = algo.summarizeAsClusters(data2)
    assert(clusters2.size === 4)
    assert(clusters2(1).center === Vectors.dense(12.0, 12.0))
    assert(clusters2(1).records === 25)
    assert(clusters2(2).center === Vectors.dense(37.0, 37.0))
    assert(clusters2(2).records === 25)
    assert(clusters2(3).center === Vectors.dense(62.0, 62.0))
    assert(clusters2(3).records === 25)
    assert(clusters2(4).center === Vectors.dense(87.0, 87.0))
    assert(clusters2(4).records === 25)
  }

  test("getChildrenCenter") {
    val algo = new HierarchicalClustering
    val centers = Map(
      2L -> Vectors.dense(1.0, 1.0).toBreeze,
      3L -> Vectors.dense(2.0, 2.0).toBreeze
    )
    val initNextCenters = algo.initChildrenCenter(centers)
    assert(initNextCenters.size === 4)
    assert(initNextCenters.keySet === Set(4, 5, 6, 7))
  }

  test("should divide clusters") {
    val algo = new HierarchicalClustering
    val seed = (0 to 99).map(i => ((i / 50) + 2L, Vectors.dense(i, i).toBreeze))
    val data = sc.parallelize(seed)
    val clusters = algo.summarizeAsClusters(data)
    val newClusters = algo.getDividedClusters(data, clusters)

    assert(newClusters.size === 4)
    assert(newClusters(4).center === Vectors.dense(12.0, 12.0))
    assert(newClusters(4).records === 25)
    assert(newClusters(5).center === Vectors.dense(37.0, 37.0))
    assert(newClusters(5).records === 25)
    assert(newClusters(6).center === Vectors.dense(62.0, 62.0))
    assert(newClusters(6).records === 25)
    assert(newClusters(7).center === Vectors.dense(87.0, 87.0))
    assert(newClusters(7).records === 25)
  }

  test("should assign each data to new clusters") {
    val algo = new HierarchicalClustering
    val seed = Seq(
      (2L, Vectors.dense(0.0, 0.0)), (2L, Vectors.dense(1.0, 1.0)), (2L, Vectors.dense(2.0, 2.0)),
      (2L, Vectors.dense(3.0, 3.0)), (2L, Vectors.dense(4.0, 4.0)), (2L, Vectors.dense(5.0, 5.0)),
      (3L, Vectors.dense(6.0, 6.0)), (3L, Vectors.dense(7.0, 7.0)), (3L, Vectors.dense(8.0, 8.0)),
      (3L, Vectors.dense(9.0, 9.0)), (3L, Vectors.dense(10.0, 10.0)), (3L, Vectors.dense(11.0, 11.0))
    ).map { case (idx, vector) => (idx, vector.toBreeze)}
    val newClusters = Map(
      4L -> new ClusterTree(Vectors.dense(1.0, 1.0), 3, Vectors.dense(1.0, 1.0)),
      5L -> new ClusterTree(Vectors.dense(4.0, 4.0), 3, Vectors.dense(1.0, 1.0)),
      6L -> new ClusterTree(Vectors.dense(7.0, 7.0), 3, Vectors.dense(1.0, 1.0)),
      7L -> new ClusterTree(Vectors.dense(10.0, 10.0), 3, Vectors.dense(1.0, 1.0))
    )
    val data = sc.parallelize(seed)
    val result = algo.updateClusterIndex(data, newClusters).collect().toSeq

    val expected = Seq(
      (4, Vectors.dense(0.0, 0.0)), (4, Vectors.dense(1.0, 1.0)), (4, Vectors.dense(2.0, 2.0)),
      (5, Vectors.dense(3.0, 3.0)), (5, Vectors.dense(4.0, 4.0)), (5, Vectors.dense(5.0, 5.0)),
      (6, Vectors.dense(6.0, 6.0)), (6, Vectors.dense(7.0, 7.0)), (6, Vectors.dense(8.0, 8.0)),
      (7, Vectors.dense(9.0, 9.0)), (7, Vectors.dense(10.0, 10.0)), (7, Vectors.dense(11.0, 11.0))
    ).map { case (idx, vector) => (idx, vector.toBreeze)}
    assert(result === expected)
  }

  test("setNumClusters") {
    val algo = new HierarchicalClustering()
    assert(algo.getNumClusters == 20)
    algo.setNumClusters(1000)
    assert(algo.getNumClusters == 1000)
  }

  test("setSubIterations") {
    val algo = new HierarchicalClustering()
    assert(algo.getMaxIterations == 20)
    algo.setMaxIterations(15)
    assert(algo.getMaxIterations == 15)
  }

  test("setNumRetries") {
    val algo = new HierarchicalClustering()
    assert(algo.getMaxRetries == 10)
    algo.setMaxRetries(15)
    assert(algo.getMaxRetries == 15)
  }

  test("setSeed") {
    val algo = new HierarchicalClustering()
    assert(algo.getSeed == 1)
    algo.setSeed(987)
    assert(algo.getSeed == 987)
  }
}
