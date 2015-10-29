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

import breeze.linalg.{Vector => BV, norm => breezeNorm}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._


class BisectingKMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("run") {
    val k = 123
    val algo = new BisectingKMeans().setK(k).setSeed(1)
    val localSeed: Seq[Vector] = (0 to 999).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val data = sc.parallelize(localSeed, 2)
    val model = algo.run(data)
    assert(model.getClusters.length == 123)
    assert(model.node.getHeight ~== 702.8641 absTol 10E-4)

    // check the relations between a parent cluster and its children
    assert(model.node.getParent === None)
    assert(model.node.getChildren.head.getParent.get === model.node)
    assert(model.node.getChildren.apply(1).getParent.get === model.node)
    assert(model.getClusters.forall(_.getParent.isDefined))

    val predicted = model.predict(data)
    assert(predicted.distinct.count() === k)
  }

  test("run with too many cluster size than the records") {
    val algo = new BisectingKMeans().setK(123).setSeed(1)
    val localSeed: Seq[Vector] = (0 to 99).map(i => Vectors.dense(i.toDouble, i.toDouble)).toSeq
    val data = sc.parallelize(localSeed)
    val model = algo.run(data)
    assert(model.getClusters.length == 100)
    assert(model.node.getHeight ~== 72.12489 absTol 10E-4)
  }

  test("setNumClusters") {
    val algo = new BisectingKMeans()
    assert(algo.getK == 2)
    algo.setK(1000)
    assert(algo.getK == 1000)
  }

  test("setSubIterations") {
    val algo = new BisectingKMeans()
    assert(algo.getMaxIterations == 20)
    algo.setMaxIterations(15)
    assert(algo.getMaxIterations == 15)
  }

  test("setSeed") {
    val algo = new BisectingKMeans()
    assert(algo.getSeed == 1)
    algo.setSeed(987)
    assert(algo.getSeed == 987)
  }

  test("summarize center stats") {
    val algo = new BisectingKMeans
    val local = Seq(
      (4L, Vectors.dense(1.5, 1.5).toBreeze),
      (4L, Vectors.dense(2.5, 2.5).toBreeze),
      (5L, Vectors.dense(11.5, 11.5).toBreeze),
      (5L, Vectors.dense(12.5, 12.5).toBreeze),
      (6L, Vectors.dense(21.5, 21.5).toBreeze),
      (6L, Vectors.dense(22.5, 22.5).toBreeze),
      (7L, Vectors.dense(31.5, 31.5).toBreeze),
      (7L, Vectors.dense(32.5, 32.5).toBreeze)
    )
    val data = sc.parallelize(local)

    val clusterStats = BisectingKMeans.summarizeClusters(data)
    assert(clusterStats.size === 4)
    assert(clusterStats(4).mean === Vectors.dense(2.0, 2.0).toBreeze)
    assert(clusterStats(4).sumOfSquares ~== 1.0 absTol 10e-4)
    assert(clusterStats(4).rows === 2)
    assert(clusterStats(5).mean === Vectors.dense(12.0, 12.0).toBreeze)
    assert(clusterStats(5).sumOfSquares ~== 1.0 absTol 10e-4)
    assert(clusterStats(5).rows === 2)
    assert(clusterStats(6).mean === Vectors.dense(22.0, 22.0).toBreeze)
    assert(clusterStats(6).sumOfSquares ~== 1.0 absTol 10e-4)
    assert(clusterStats(6).rows === 2)
    assert(clusterStats(7).mean === Vectors.dense(32.0, 32.0).toBreeze)
    assert(clusterStats(7).sumOfSquares ~== 1.0 absTol 10e-4)
    assert(clusterStats(7).rows === 2)
  }

  test("initialize centers at next step") {
    val local = Seq(
      (2L, BV[Double](0.9, 0.9)), (2L, BV[Double](1.1, 1.1)),
      (3L, BV[Double](1.9, 1.9)), (2L, BV[Double](2.1, 2.1))
    )
    val data = sc.parallelize(local)
    val stats = Map[Long, BisectingClusterStat](
      2L -> new BisectingClusterStat(2, BV[Double](1.0, 1.0) * 2.0, 0.0),
      3L -> new BisectingClusterStat(2, BV[Double](2.0, 2.0) * 2.0, 0.0)
    )
    val initNextCenters = BisectingKMeans.initNextCenters(data, stats)
    assert(initNextCenters.size === 4)
    assert(initNextCenters.keySet === Set(4, 5, 6, 7))
  }

  test("should assign each data to new clusters") {
    val seed = Seq(
      (2L, Vectors.dense(0.0, 0.0)), (2L, Vectors.dense(1.0, 1.0)),
      (2L, Vectors.dense(2.0, 2.0)), (2L, Vectors.dense(3.0, 3.0)),
      (2L, Vectors.dense(4.0, 4.0)), (2L, Vectors.dense(5.0, 5.0)),
      (3L, Vectors.dense(6.0, 6.0)), (3L, Vectors.dense(7.0, 7.0)),
      (3L, Vectors.dense(8.0, 8.0)), (3L, Vectors.dense(9.0, 9.0)),
      (3L, Vectors.dense(10.0, 10.0)), (3L, Vectors.dense(11.0, 11.0))
    ).map { case (idx, vector) => (idx, vector.toBreeze) }
    val variance = breezeNorm(Vectors.dense(1.0, 1.0).toBreeze, 2.0)
    val newClusterStats = Map(
      4L -> new BisectingClusterStat(3L, BV[Double](1.0, 1.0) :* 3.0, variance),
      5L -> new BisectingClusterStat(3L, BV[Double](4.0, 4.0) :* 3.0, variance),
      6L -> new BisectingClusterStat(3L, BV[Double](7.0, 7.0) :* 3.0, variance),
      7L -> new BisectingClusterStat(3L, BV[Double](10.0, 10.0) :* 3.0, variance)
    )
    val data = sc.parallelize(seed, 1)
    val leafClusterStats = BisectingKMeans.summarizeClusters(data)
    val dividableLeafClusters = leafClusterStats.filter(_._2.isDividable)
    val result = BisectingKMeans.divideClusters(data, dividableLeafClusters, 20).collect()

    val expected = Seq(
      (4, Vectors.dense(0.0, 0.0)), (4, Vectors.dense(1.0, 1.0)), (4, Vectors.dense(2.0, 2.0)),
      (5, Vectors.dense(3.0, 3.0)), (5, Vectors.dense(4.0, 4.0)), (5, Vectors.dense(5.0, 5.0)),
      (6, Vectors.dense(6.0, 6.0)), (6, Vectors.dense(7.0, 7.0)), (6, Vectors.dense(8.0, 8.0)),
      (7, Vectors.dense(9.0, 9.0)), (7, Vectors.dense(10.0, 10.0)), (7, Vectors.dense(11.0, 11.0))
    ).map { case (idx, vector) => (idx, vector.toBreeze) }
    assert(result === expected)
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
      val closestIndex = BisectingKMeans.findClosestCenter(metric)(centers)(point)
      assert(closestIndex === i)
    }
  }

  test("should be equal to math.pow") {
    (1 to 1000).foreach { k =>
      // the minimum number of nodes of a binary tree by given parameter
      val multiplier = math.ceil(math.log(k) / math.log(2.0)) + 1
      val expected = math.pow(2, multiplier).toInt
      val result = BisectingKMeans.getMinimumNumNodesInTree(k)
      assert(result === expected)
    }
  }

  test("should divide clusters correctly") {
    val local = Seq(
      (2L, BV[Double](0.9, 0.9)), (2L, BV[Double](1.1, 1.1)),
      (2L, BV[Double](9.9, 9.9)), (2L, BV[Double](10.1, 10.1)),
      (3L, BV[Double](99.9, 99.9)), (3L, BV[Double](100.1, 100.1)),
      (3L, BV[Double](109.9, 109.9)), (3L, BV[Double](110.1, 110.1))
    )
    val data = sc.parallelize(local, 1)
    val stats = BisectingKMeans.summarizeClusters(data)
    val dividedData = BisectingKMeans.divideClusters(data, stats, 20).collect()

    assert(dividedData(0) == (4L, BV[Double](0.9, 0.9)))
    assert(dividedData(1) == (4L, BV[Double](1.1, 1.1)))
    assert(dividedData(2) == (5L, BV[Double](9.9, 9.9)))
    assert(dividedData(3) == (5L, BV[Double](10.1, 10.1)))
    assert(dividedData(4) == (6L, BV[Double](99.9, 99.9)))
    assert(dividedData(5) == (6L, BV[Double](100.1, 100.1)))
    assert(dividedData(6) == (7L, BV[Double](109.9, 109.9)))
    assert(dividedData(7) == (7L, BV[Double](110.1, 110.1)))
  }

}
