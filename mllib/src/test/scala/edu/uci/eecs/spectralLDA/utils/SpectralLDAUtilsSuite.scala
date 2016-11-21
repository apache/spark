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

package edu.uci.eecs.spectralLDA.utils

import scala.language.postfixOps

import breeze.linalg._
import breeze.linalg.qr.QR
import breeze.stats.distributions.{Gaussian, RandBasis, ThreadLocalRandomGenerator, Uniform}
import org.apache.commons.math3.random.MersenneTwister
import scalaxy.loops._

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext


class SpectralLDAUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {
  def expectedRankOneTensor3d(x: DenseVector[Double],
                              y: DenseVector[Double],
                              z: DenseVector[Double]
                             ): DenseMatrix[Double] = {
    val d1 = x.length
    val d2 = y.length
    val d3 = z.length

    val result = DenseMatrix.zeros[Double](d1, d2 * d3)

    for (i <- 0 until d1 optimized) {
      for (j <- 0 until d2 optimized) {
        for (k <- 0 until d3 optimized) {
          result(i, k * d2 + j) = x(i) * y(j) * z(k)
        }
      }
    }

    result
  }

  test("3rd-order vector outer product") {
    val x = DenseVector.rand[Double](50)
    val y = DenseVector.rand[Double](50)
    val z = DenseVector.rand[Double](50)

    val tensor = TensorOps.makeRankOneTensor3d(x, y, z)
    val expected = expectedRankOneTensor3d(x, y, z)

    val diff = tensor - expected
    assert(norm(norm(diff(::, *)).t.toDenseVector) <= 1e-8)
  }

  test("M2 multiplied by random Gaussian matrix") {
    val a1 = SparseVector(DenseVector.rand[Double](100).toArray)
    val a2 = SparseVector(DenseVector.rand[Double](100).toArray)
    val a3 = SparseVector(DenseVector.rand[Double](100).toArray)

    val docs = Seq((1000L, a1), (1001L, a2), (1002L, a3))
    val docsRDD = sc.parallelize(docs)

    // Random Gaussian matrix
    val g = DenseMatrix.rand[Double](100, 50, Gaussian(mu = 0.0, sigma = 1.0))

    val result = DenseMatrix.zeros[Double](100, 50)
    docsRDD
      .flatMap {
        case (id: Long, w: SparseVector[Double]) => RandNLA.accumulate_M_mul_S(g, w, sum(w))
      }
      .reduceByKey(_ + _)
      .collect
      .foreach {
        case (r: Int, a: DenseVector[Double]) => result(r, ::) := a.t
      }

    val m2 = docsRDD
      .map {
        case (id: Long, w: SparseVector[Double]) =>
          val l = sum(w)
          (w * w.t - diag(w)) / (l * (l - 1.0))
      }
      .reduce(_ + _)
    val expectedResult = m2 * g

    val diff: DenseMatrix[Double] = result - expectedResult
    val normDiff: Double = norm(norm(diff(::, *)).t.toDenseVector)
    assert(normDiff <= 1e-8)
  }

  test("Randomised SVD") {
    implicit val randBasis: RandBasis =
      new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(234787)))

    val n = 100
    val k = 5

    val alpha: DenseVector[Double] = DenseVector[Double](25.0, 20.0, 15.0, 10.0, 5.0)
    val beta: DenseMatrix[Double] = DenseMatrix.rand(n, k, Uniform(0.0, 1.0))

    val norms = norm(beta(::, *)).t.toDenseVector
    for (j <- 0 until k) {
      beta(::, j) /= norms(j)
    }

    val a: DenseMatrix[Double] = beta * diag(alpha) * beta.t
    val sigma: DenseMatrix[Double] = DenseMatrix.rand(n, k, Gaussian(mu = 0.0, sigma = 1.0))
    val y = a * sigma
    val QR(q: DenseMatrix[Double], _) = qr.reduced(y)

    val (s: DenseVector[Double], u: DenseMatrix[Double]) = RandNLA.decomp2(a * q, q)

    val diff_a = u * diag(s) * u.t - a
    val norm_diff_a = norm(norm(diff_a(::, *)).t.toDenseVector)

    assert(norm_diff_a <= 1e-8)
  }

  test("Simple non-negative adjustment to vectors") {
    val inputVectors = Seq(
      DenseVector[Double](0.5, 0.5, 0.5),
      DenseVector[Double](0.5, 0.5, -0.5),
      DenseVector[Double](0.3, 0.3, -0.3, -0.3, 0.1, 0.1),
      DenseVector[Double](0.3, 0.3, 0.3, 0.3, -0.3, -0.3, -0.3, -0.3, 0.1, 0.1, 0.1, 0.1),
      DenseVector[Double](0.1, 0.1, 0.1, 0.1, 0.3, 0.3, 0.3, 0.3, -0.3, -0.3, -0.3, -0.3),
      DenseVector[Double](10, 10, 10, 10, 30, 30, 30, 30, -30, -30, -30, -30)
    )

    val expectedAdjustedVectors = Seq(
      DenseVector(0.33333333333333337, 0.33333333333333337, 0.33333333333333337),
      DenseVector(0.5, 0.5, 0.0),
      DenseVector(0.35, 0.35, 0.0, 0.0, 0.15000000000000002, 0.15000000000000002),
      DenseVector(0.22499999999999995, 0.22499999999999995,
        0.22499999999999995, 0.22499999999999995, 0.0, 0.0, 0.0, 0.0,
        0.024999999999999967, 0.024999999999999967,
        0.024999999999999967, 0.024999999999999967),
      DenseVector(0.024999999999999967, 0.024999999999999967,
        0.024999999999999967, 0.024999999999999967,
        0.22499999999999995, 0.22499999999999995,
        0.22499999999999995, 0.22499999999999995,
        0.0, 0.0, 0.0, 0.0),
      DenseVector(0.0, 0.0, 0.0, 0.0, 0.25, 0.25, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0)
    )

    for ((v1, v2) <- inputVectors.zip(expectedAdjustedVectors)) {
      assert(norm(NonNegativeAdjustment.simplexProj(v1)._1 - v2) <= 1e-8)
    }
  }
}

