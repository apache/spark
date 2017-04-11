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
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class GaussianMixtureSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("gmm fails on high dimensional data") {
    val rdd = sc.parallelize(Seq(
      Vectors.sparse(GaussianMixture.MAX_NUM_FEATURES + 1, Array(0, 4), Array(3.0, 8.0)),
      Vectors.sparse(GaussianMixture.MAX_NUM_FEATURES + 1, Array(1, 5), Array(4.0, 9.0))))
    val gm = new GaussianMixture()
    withClue(s"GMM should restrict the maximum number of features to be < " +
      s"${GaussianMixture.MAX_NUM_FEATURES}") {
      intercept[IllegalArgumentException] {
        gm.run(rdd)
      }
    }
  }

  test("single cluster") {
    val data = sc.parallelize(Array(
      Vectors.dense(6.0, 9.0),
      Vectors.dense(5.0, 10.0),
      Vectors.dense(4.0, 11.0)
    ))

    // expectations
    val Ew = 1.0
    val Emu = Vectors.dense(5.0, 10.0)
    val Esigma = Matrices.dense(2, 2, Array(2.0 / 3.0, -2.0 / 3.0, -2.0 / 3.0, 2.0 / 3.0))

    val seeds = Array(314589, 29032897, 50181, 494821, 4660)
    seeds.foreach { seed =>
      val gmm = new GaussianMixture().setK(1).setSeed(seed).run(data)
      assert(gmm.weights(0) ~== Ew absTol 1E-5)
      assert(gmm.gaussians(0).mu ~== Emu absTol 1E-5)
      assert(gmm.gaussians(0).sigma ~== Esigma absTol 1E-5)
    }

  }

  test("two clusters") {
    val data = sc.parallelize(GaussianTestData.data)

    // we set an initial gaussian to induce expected results
    val initialGmm = new GaussianMixtureModel(
      Array(0.5, 0.5),
      Array(
        new MultivariateGaussian(Vectors.dense(-1.0), Matrices.dense(1, 1, Array(1.0))),
        new MultivariateGaussian(Vectors.dense(1.0), Matrices.dense(1, 1, Array(1.0)))
      )
    )

    val Ew = Array(1.0 / 3.0, 2.0 / 3.0)
    val Emu = Array(Vectors.dense(-4.3673), Vectors.dense(5.1604))
    val Esigma = Array(Matrices.dense(1, 1, Array(1.1098)), Matrices.dense(1, 1, Array(0.86644)))

    val gmm = new GaussianMixture()
      .setK(2)
      .setInitialModel(initialGmm)
      .run(data)

    assert(gmm.weights(0) ~== Ew(0) absTol 1E-3)
    assert(gmm.weights(1) ~== Ew(1) absTol 1E-3)
    assert(gmm.gaussians(0).mu ~== Emu(0) absTol 1E-3)
    assert(gmm.gaussians(1).mu ~== Emu(1) absTol 1E-3)
    assert(gmm.gaussians(0).sigma ~== Esigma(0) absTol 1E-3)
    assert(gmm.gaussians(1).sigma ~== Esigma(1) absTol 1E-3)
  }

  test("two clusters with distributed decompositions") {
    val data = sc.parallelize(GaussianTestData.data2, 2)

    val k = 5
    val d = data.first().size
    assert(GaussianMixture.shouldDistributeGaussians(k, d))

    val gmm = new GaussianMixture()
      .setK(k)
      .run(data)

    assert(gmm.k === k)
  }

  test("single cluster with sparse data") {
    val data = sc.parallelize(Array(
      Vectors.sparse(3, Array(0, 2), Array(4.0, 2.0)),
      Vectors.sparse(3, Array(0, 2), Array(2.0, 4.0)),
      Vectors.sparse(3, Array(1), Array(6.0))
      ))

    val Ew = 1.0
    val Emu = Vectors.dense(2.0, 2.0, 2.0)
    val Esigma = Matrices.dense(3, 3,
      Array(8.0 / 3.0, -4.0, 4.0 / 3.0, -4.0, 8.0, -4.0, 4.0 / 3.0, -4.0, 8.0 / 3.0)
      )

    val seeds = Array(42, 1994, 27, 11, 0)
    seeds.foreach { seed =>
      val gmm = new GaussianMixture().setK(1).setSeed(seed).run(data)
      assert(gmm.weights(0) ~== Ew absTol 1E-5)
      assert(gmm.gaussians(0).mu ~== Emu absTol 1E-5)
      assert(gmm.gaussians(0).sigma ~== Esigma absTol 1E-5)
    }
  }

  test("two clusters with sparse data") {
    val data = sc.parallelize(GaussianTestData.data)
    val sparseData = data.map(point => Vectors.sparse(1, Array(0), point.toArray))
    // we set an initial gaussian to induce expected results
    val initialGmm = new GaussianMixtureModel(
      Array(0.5, 0.5),
      Array(
        new MultivariateGaussian(Vectors.dense(-1.0), Matrices.dense(1, 1, Array(1.0))),
        new MultivariateGaussian(Vectors.dense(1.0), Matrices.dense(1, 1, Array(1.0)))
      )
    )
    val Ew = Array(1.0 / 3.0, 2.0 / 3.0)
    val Emu = Array(Vectors.dense(-4.3673), Vectors.dense(5.1604))
    val Esigma = Array(Matrices.dense(1, 1, Array(1.1098)), Matrices.dense(1, 1, Array(0.86644)))

    val sparseGMM = new GaussianMixture()
      .setK(2)
      .setInitialModel(initialGmm)
      .run(sparseData)

    assert(sparseGMM.weights(0) ~== Ew(0) absTol 1E-3)
    assert(sparseGMM.weights(1) ~== Ew(1) absTol 1E-3)
    assert(sparseGMM.gaussians(0).mu ~== Emu(0) absTol 1E-3)
    assert(sparseGMM.gaussians(1).mu ~== Emu(1) absTol 1E-3)
    assert(sparseGMM.gaussians(0).sigma ~== Esigma(0) absTol 1E-3)
    assert(sparseGMM.gaussians(1).sigma ~== Esigma(1) absTol 1E-3)
  }

  test("model save / load") {
    val data = sc.parallelize(GaussianTestData.data)

    val gmm = new GaussianMixture().setK(2).setSeed(0).run(data)
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    try {
      gmm.save(sc, path)

      // TODO: GaussianMixtureModel should implement equals/hashcode directly.
      val sameModel = GaussianMixtureModel.load(sc, path)
      assert(sameModel.k === gmm.k)
      (0 until sameModel.k).foreach { i =>
        assert(sameModel.gaussians(i).mu === gmm.gaussians(i).mu)
        assert(sameModel.gaussians(i).sigma === gmm.gaussians(i).sigma)
      }
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("model prediction, parallel and local") {
    val data = sc.parallelize(GaussianTestData.data)
    val gmm = new GaussianMixture().setK(2).setSeed(0).run(data)

    val batchPredictions = gmm.predict(data)
    batchPredictions.zip(data).collect().foreach { case (batchPred, datum) =>
      assert(batchPred === gmm.predict(datum))
    }
  }

  object GaussianTestData {

    val data = Array(
      Vectors.dense(-5.1971), Vectors.dense(-2.5359), Vectors.dense(-3.8220),
      Vectors.dense(-5.2211), Vectors.dense(-5.0602), Vectors.dense( 4.7118),
      Vectors.dense( 6.8989), Vectors.dense( 3.4592), Vectors.dense( 4.6322),
      Vectors.dense( 5.7048), Vectors.dense( 4.6567), Vectors.dense( 5.5026),
      Vectors.dense( 4.5605), Vectors.dense( 5.2043), Vectors.dense( 6.2734)
    )

    val data2: Array[Vector] = Array.tabulate(25) { i: Int =>
      Vectors.dense(Array.tabulate(50)(i + _.toDouble))
    }

  }
}
