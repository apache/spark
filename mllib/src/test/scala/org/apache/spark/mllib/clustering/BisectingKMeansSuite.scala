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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.TestingUtils._
import org.apache.spark.util.Utils

class BisectingKMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("default values") {
    val bkm0 = new BisectingKMeans()
    assert(bkm0.getK === 4)
    assert(bkm0.getMaxIterations === 20)
    assert(bkm0.getMinDivisibleClusterSize === 1.0)
    val bkm1 = new BisectingKMeans()
    assert(bkm0.getSeed === bkm1.getSeed, "The default seed should be constant.")
  }

  test("setter/getter") {
    val bkm = new BisectingKMeans()

    val k = 10
    assert(bkm.getK !== k)
    assert(bkm.setK(k).getK === k)
    val maxIter = 100
    assert(bkm.getMaxIterations !== maxIter)
    assert(bkm.setMaxIterations(maxIter).getMaxIterations === maxIter)
    val minSize = 2.0
    assert(bkm.getMinDivisibleClusterSize !== minSize)
    assert(bkm.setMinDivisibleClusterSize(minSize).getMinDivisibleClusterSize === minSize)
    val seed = 10L
    assert(bkm.getSeed !== seed)
    assert(bkm.setSeed(seed).getSeed === seed)

    intercept[IllegalArgumentException] {
      bkm.setK(0)
    }
    intercept[IllegalArgumentException] {
      bkm.setMaxIterations(0)
    }
    intercept[IllegalArgumentException] {
      bkm.setMinDivisibleClusterSize(0.0)
    }
  }

  test("1D data") {
    val points = Vectors.sparse(1, Array.empty, Array.empty) +:
      (1 until 8).map(i => Vectors.dense(i))
    val data = sc.parallelize(points, 2)
    val bkm = new BisectingKMeans()
      .setK(4)
      .setMaxIterations(1)
      .setSeed(1L)
    // The clusters should be
    // (0, 1, 2, 3, 4, 5, 6, 7)
    //   - (0, 1, 2, 3)
    //     - (0, 1)
    //     - (2, 3)
    //   - (4, 5, 6, 7)
    //     - (4, 5)
    //     - (6, 7)
    val model = bkm.run(data)
    assert(model.k === 4)
    // The total cost should be 8 * 0.5 * 0.5 = 2.0.
    assert(model.computeCost(data) ~== 2.0 relTol 1e-12)
    val predictions = data.map(v => (v(0), model.predict(v))).collectAsMap()
    Range(0, 8, 2).foreach { i =>
      assert(predictions(i) === predictions(i + 1),
        s"$i and ${i + 1} should belong to the same cluster.")
    }
    val root = model.root
    assert(root.center(0) ~== 3.5 relTol 1e-12)
    assert(root.height ~== 2.0 relTol 1e-12)
    assert(root.children.length === 2)
    assert(root.children(0).height ~== 1.0 relTol 1e-12)
    assert(root.children(1).height ~== 1.0 relTol 1e-12)
  }

  test("points are the same") {
    val data = sc.parallelize(Seq.fill(8)(Vectors.dense(1.0, 1.0)), 2)
    val bkm = new BisectingKMeans()
      .setK(2)
      .setMaxIterations(1)
      .setSeed(1L)
    val model = bkm.run(data)
    assert(model.k === 1)
  }

  test("more desired clusters than points") {
    val data = sc.parallelize(Seq.tabulate(4)(i => Vectors.dense(i)), 2)
    val bkm = new BisectingKMeans()
      .setK(8)
      .setMaxIterations(2)
      .setSeed(1L)
    val model = bkm.run(data)
    assert(model.k === 4)
  }

  test("min divisible cluster") {
    val data = sc.parallelize(
      Seq.tabulate(16)(i => Vectors.dense(i)) ++ Seq.tabulate(4)(i => Vectors.dense(-100.0 - i)),
      2)
    val bkm = new BisectingKMeans()
      .setK(4)
      .setMinDivisibleClusterSize(10)
      .setMaxIterations(1)
      .setSeed(1L)
    val model = bkm.run(data)
    assert(model.k === 3)
    assert(model.predict(Vectors.dense(-100)) === model.predict(Vectors.dense(-97)))
    assert(model.predict(Vectors.dense(7)) !== model.predict(Vectors.dense(8)))

    bkm.setMinDivisibleClusterSize(0.5)
    val sameModel = bkm.run(data)
    assert(sameModel.k === 3)
  }

  test("larger clusters get selected first") {
    val data = sc.parallelize(
      Seq.tabulate(16)(i => Vectors.dense(i)) ++ Seq.tabulate(4)(i => Vectors.dense(-100.0 - i)),
      2)
    val bkm = new BisectingKMeans()
      .setK(3)
      .setMaxIterations(1)
      .setSeed(1L)
    val model = bkm.run(data)
    assert(model.k === 3)
    assert(model.predict(Vectors.dense(-100)) === model.predict(Vectors.dense(-97)))
    assert(model.predict(Vectors.dense(7)) !== model.predict(Vectors.dense(8)))
  }

  test("2D data") {
    val points = Seq(
      (11, 10), (9, 10), (10, 9), (10, 11),
      (11, -10), (9, -10), (10, -9), (10, -11),
      (0, 1), (0, -1)
    ).map { case (x, y) =>
      if (x == 0) {
        Vectors.sparse(2, Array(1), Array(y))
      } else {
        Vectors.dense(x, y)
      }
    }
    val data = sc.parallelize(points, 2)
    val bkm = new BisectingKMeans()
      .setK(3)
      .setMaxIterations(4)
      .setSeed(1L)
    val model = bkm.run(data)
    assert(model.k === 3)
    assert(model.root.center ~== Vectors.dense(8, 0) relTol 1e-12)
    model.root.leafNodes.foreach { node =>
      if (node.center(0) < 5) {
        assert(node.size === 2)
        assert(node.center ~== Vectors.dense(0, 0) relTol 1e-12)
      } else if (node.center(1) > 0) {
        assert(node.size === 4)
        assert(node.center ~== Vectors.dense(10, 10) relTol 1e-12)
      } else {
        assert(node.size === 4)
        assert(node.center ~== Vectors.dense(10, -10) relTol 1e-12)
      }
    }
  }

  test("BisectingKMeans model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val points = (1 until 8).map(i => Vectors.dense(i))
    val data = sc.parallelize(points, 2)
    val model = new BisectingKMeans().run(data)
    try {
      model.save(sc, path)
      val sameModel = BisectingKMeansModel.load(sc, path)
      assert(model.k === sameModel.k)
      model.clusterCenters.zip(sameModel.clusterCenters).foreach(c => c._1 === c._2)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
