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

import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

class KMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}

  private val seed = 42

  test("single cluster") {
    val data = sc.parallelize(Seq(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    ))

    val center = Vectors.dense(1.0, 3.0, 4.0)

    // No matter how many iterations we use, we should get one cluster,
    // centered at the mean of the points

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(
      data, k = 1, maxIterations = 1, initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)
  }

  test("fewer distinct points than clusters") {
    val data = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 2.0, 3.0)),
      2)

    var model = KMeans.train(data, k = 2, maxIterations = 1, initializationMode = "random")
    assert(model.clusterCenters.length === 1)

    model = KMeans.train(data, k = 2, maxIterations = 1, initializationMode = "k-means||")
    assert(model.clusterCenters.length === 1)
  }

  test("unique cluster centers") {
    val rng = new Random(seed)
    val numDistinctPoints = 10
    val points =
      (0 until numDistinctPoints).map(i => Vectors.dense(Array.fill(3)(rng.nextDouble())))
    val data = sc.parallelize(points.flatMap(Array.fill(1 + rng.nextInt(3))(_)), 2)
    val normedData = data.map(new VectorWithNorm(_))

    // less centers than k
    val km = new KMeans().setK(50)
      .setMaxIterations(5)
      .setInitializationMode("k-means||")
      .setInitializationSteps(10)
      .setSeed(seed)

    val distanceMeasureInstance = new EuclideanDistanceMeasure
    val initialCenters = km.initKMeansParallel(normedData, distanceMeasureInstance).map(_.vector)
    assert(initialCenters.length === initialCenters.distinct.length)
    assert(initialCenters.length <= numDistinctPoints)

    val model = km.run(data)
    val finalCenters = model.clusterCenters
    assert(finalCenters.length === finalCenters.distinct.length)

    // run local k-means
    val k = 10
    val km2 = new KMeans().setK(k)
      .setMaxIterations(5)
      .setInitializationMode("k-means||")
      .setInitializationSteps(10)
      .setSeed(seed)
    val initialCenters2 = km2.initKMeansParallel(normedData, distanceMeasureInstance).map(_.vector)
    assert(initialCenters2.length === initialCenters2.distinct.length)
    assert(initialCenters2.length === k)

    val model2 = km2.run(data)
    val finalCenters2 = model2.clusterCenters
    assert(finalCenters2.length === finalCenters2.distinct.length)

    val km3 = new KMeans().setK(k)
      .setMaxIterations(5)
      .setInitializationMode("random")
      .setSeed(seed)
    val model3 = km3.run(data)
    val finalCenters3 = model3.clusterCenters
    assert(finalCenters3.length === finalCenters3.distinct.length)
  }

  test("deterministic initialization") {
    // Create a large-ish set of points for clustering
    val points = List.tabulate(1000)(n => Vectors.dense(n, n))
    val rdd = sc.parallelize(points, 3)

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {
      // Create three deterministic models and compare cluster means
      val model1 = KMeans.train(rdd, k = 10, maxIterations = 2,
        initializationMode = initMode, seed = seed)
      val centers1 = model1.clusterCenters

      val model2 = KMeans.train(rdd, k = 10, maxIterations = 2,
        initializationMode = initMode, seed = seed)
      val centers2 = model2.clusterCenters

      centers1.zip(centers2).foreach { case (c1, c2) =>
        assert(c1 ~== c2 absTol 1E-14)
      }
    }
  }

  test("single cluster with big dataset") {
    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)

    // No matter how many iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.dense(1.0, 3.0, 4.0)

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.length === 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)
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

    // No matter how many iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0)))

    var model = KMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = KMeans.train(data, k = 1, maxIterations = 1, initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    data.unpersist()
  }

  test("k-means|| initialization") {

    case class VectorWithCompare(x: Vector) extends Ordered[VectorWithCompare] {
      override def compare(that: VectorWithCompare): Int = {
        if (this.x.toArray.foldLeft[Double](0.0)((acc, x) => acc + x * x) >
          that.x.toArray.foldLeft[Double](0.0)((acc, x) => acc + x * x)) {
          -1
        } else {
          1
        }
      }
    }

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

    var model = KMeans.train(rdd, k = 5, maxIterations = 1)

    assert(model.clusterCenters.sortBy(VectorWithCompare(_))
      .zip(points.sortBy(VectorWithCompare(_))).forall(x => x._1 ~== (x._2) absTol 1E-5))

    // Iterations of Lloyd's should not change the answer either
    model = KMeans.train(rdd, k = 5, maxIterations = 10)
    assert(model.clusterCenters.sortBy(VectorWithCompare(_))
      .zip(points.sortBy(VectorWithCompare(_))).forall(x => x._1 ~== (x._2) absTol 1E-5))
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
      val model = KMeans.train(rdd, k = 2, maxIterations = 2, initMode)

      val predicts = model.predict(rdd).collect()

      assert(predicts(0) === predicts(1))
      assert(predicts(0) === predicts(2))
      assert(predicts(3) === predicts(4))
      assert(predicts(3) === predicts(5))
      assert(predicts(0) != predicts(3))
    }
  }

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(true, false).foreach { case selector =>
      val model = KMeansSuite.createModel(10, 3, selector)
      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = KMeansModel.load(sc, path)
        KMeansSuite.checkEqual(model, sameModel)
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }

  test("Initialize using given cluster centers") {
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(1.0, 0.0),
      Vectors.dense(0.0, 1.0),
      Vectors.dense(1.0, 1.0)
    )
    val rdd = sc.parallelize(points, 3)
    // creating an initial model
    val initialModel = new KMeansModel(Array(points(0), points(2)))

    val returnModel = new KMeans()
      .setK(2)
      .setMaxIterations(0)
      .setInitialModel(initialModel)
      .run(rdd)
   // comparing the returned model and the initial model
    assert(returnModel.clusterCenters(0) === initialModel.clusterCenters(0))
    assert(returnModel.clusterCenters(1) === initialModel.clusterCenters(1))
  }

  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val vec1 = new VectorWithNorm(Vectors.dense(Array(1.0, 2.0)))
    val vec2 = new VectorWithNorm(Vectors.sparse(10, Array(5, 8), Array(1.0, 2.0)))

    Seq(vec1, vec2).foreach { v =>
      val v2 = ser.deserialize[VectorWithNorm](ser.serialize(v))
      assert(v2.norm === v.norm)
      assert(v2.vector === v.vector)
    }
  }
}

object KMeansSuite extends SparkFunSuite {
  def createModel(dim: Int, k: Int, isSparse: Boolean): KMeansModel = {
    val singlePoint = if (isSparse) {
      Vectors.sparse(dim, Array.empty[Int], Array.empty[Double])
    } else {
      Vectors.dense(Array.fill[Double](dim)(0.0))
    }
    new KMeansModel(Array.fill[Vector](k)(singlePoint))
  }

  def checkEqual(a: KMeansModel, b: KMeansModel): Unit = {
    assert(a.k === b.k)
    a.clusterCenters.zip(b.clusterCenters).foreach {
      case (ca: SparseVector, cb: SparseVector) =>
        assert(ca === cb)
      case (ca: DenseVector, cb: DenseVector) =>
        assert(ca === cb)
      case _ =>
        fail("checkEqual failed since the two clusters were not identical.\n")
    }
  }
}

class KMeansClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => Vectors.dense(Array.fill(n)(random.nextDouble())))
    }.cache()
    for (initMode <- Seq(KMeans.RANDOM, KMeans.K_MEANS_PARALLEL)) {
      // If we serialize data directly in the task closure, the size of the serialized task would be
      // greater than 1MB and hence Spark would throw an error.
      val model = KMeans.train(points, 2, 2, initMode)
      val predictions = model.predict(points).collect()
      val cost = model.computeCost(points)
    }
  }
}
