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

package org.apache.spark.ml.optim

import org.apache.spark.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

/**
 * Model fitted by [[WeightedLeastSquares]].
 * @param coefficients model coefficients
 * @param intercept model intercept
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1
 */
private[ml] class WeightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double,
    val diagInvAtWA: DenseVector) extends Serializable

/**
 * Weighted least squares solver via normal equation.
 * Given weighted observations (w,,i,,, a,,i,,, b,,i,,), we use the following weighted least squares
 * formulation:
 *
 * min,,x,z,, 1/2 sum,,i,, w,,i,, (a,,i,,^T^ x + z - b,,i,,)^2^ / sum,,i,, w_i
 *   + 1/2 lambda / delta sum,,j,, (sigma,,j,, x,,j,,)^2^,
 *
 * where lambda is the regularization parameter, and delta and sigma,,j,, are controlled by
 * [[standardizeLabel]] and [[standardizeFeatures]], respectively.
 *
 * Set [[regParam]] to 0.0 and turn off both [[standardizeFeatures]] and [[standardizeLabel]] to
 * match R's `lm`.
 * Turn on [[standardizeLabel]] to match R's `glmnet`.
 *
 * @param fitIntercept whether to fit intercept. If false, z is 0.0.
 * @param regParam L2 regularization parameter (lambda)
 * @param standardizeFeatures whether to standardize features. If true, sigma_,,j,, is the
 *                            population standard deviation of the j-th column of A. Otherwise,
 *                            sigma,,j,, is 1.0.
 * @param standardizeLabel whether to standardize label. If true, delta is the population standard
 *                         deviation of the label column b. Otherwise, delta is 1.0.
 */
private[ml] class WeightedLeastSquares(
    val fitIntercept: Boolean,
    val regParam: Double,
    val standardizeFeatures: Boolean,
    val standardizeLabel: Boolean) extends Logging with Serializable {
  import WeightedLeastSquares._

  require(regParam >= 0.0, s"regParam cannot be negative: $regParam")
  if (regParam == 0.0) {
    logWarning("regParam is zero, which might cause numerical instability and overfitting.")
  }

  /**
   * Creates a [[WeightedLeastSquaresModel]] from an RDD of [[Instance]]s.
   */
  def fit(instances: RDD[Instance]): WeightedLeastSquaresModel = {
    val summary = instances.treeAggregate(new Aggregator)(_.add(_), _.merge(_))
    summary.validate()
    logInfo(s"Number of instances: ${summary.count}.")
    val k = if (fitIntercept) summary.k + 1 else summary.k
    val triK = summary.triK
    val wSum = summary.wSum
    val bBar = summary.bBar
    val bStd = summary.bStd
    val aBar = summary.aBar
    val aVar = summary.aVar
    val abBar = summary.abBar
    val aaBar = summary.aaBar
    val aaValues = aaBar.values

    // add regularization to diagonals
    var i = 0
    var j = 2
    while (i < triK) {
      var lambda = regParam
      if (standardizeFeatures) {
        lambda *= aVar(j - 2)
      }
      if (standardizeLabel) {
        // TODO: handle the case when bStd = 0
        lambda /= bStd
      }
      aaValues(i) += lambda
      i += j
      j += 1
    }

    val aa = if (fitIntercept) {
      Array.concat(aaBar.values, aBar.values, Array(1.0))
    } else {
      aaBar.values
    }
    val ab = if (fitIntercept) {
      Array.concat(abBar.values, Array(bBar))
    } else {
      abBar.values
    }

    val x = CholeskyDecomposition.solve(aa, ab)

    val aaInv = CholeskyDecomposition.inverse(aa, k)

    // aaInv is a packed upper triangular matrix, here we get all elements on diagonal
    val diagInvAtWA = new DenseVector((1 to k).map { i =>
      aaInv(i + (i - 1) * i / 2 - 1) / wSum }.toArray)

    val (coefficients, intercept) = if (fitIntercept) {
      (new DenseVector(x.slice(0, x.length - 1)), x.last)
    } else {
      (new DenseVector(x), 0.0)
    }

    new WeightedLeastSquaresModel(coefficients, intercept, diagInvAtWA)
  }
}

private[ml] object WeightedLeastSquares {

  /**
   * Aggregator to provide necessary summary statistics for solving [[WeightedLeastSquares]].
   */
  // TODO: consolidate aggregates for summary statistics
  private class Aggregator extends Serializable {
    var initialized: Boolean = false
    var k: Int = _
    var count: Long = _
    var triK: Int = _
    var wSum: Double = _
    private var wwSum: Double = _
    private var bSum: Double = _
    private var bbSum: Double = _
    private var aSum: DenseVector = _
    private var abSum: DenseVector = _
    private var aaSum: DenseVector = _

    private def init(k: Int): Unit = {
      require(k <= 4096, "In order to take the normal equation approach efficiently, " +
        s"we set the max number of features to 4096 but got $k.")
      this.k = k
      triK = k * (k + 1) / 2
      count = 0L
      wSum = 0.0
      wwSum = 0.0
      bSum = 0.0
      bbSum = 0.0
      aSum = new DenseVector(Array.ofDim(k))
      abSum = new DenseVector(Array.ofDim(k))
      aaSum = new DenseVector(Array.ofDim(triK))
      initialized = true
    }

    /**
     * Adds an instance.
     */
    def add(instance: Instance): this.type = {
      val Instance(l, w, f) = instance
      val ak = f.size
      if (!initialized) {
        init(ak)
      }
      assert(ak == k, s"Dimension mismatch. Expect vectors of size $k but got $ak.")
      count += 1L
      wSum += w
      wwSum += w * w
      bSum += w * l
      bbSum += w * l * l
      BLAS.axpy(w, f, aSum)
      BLAS.axpy(w * l, f, abSum)
      BLAS.spr(w, f, aaSum)
      this
    }

    /**
     * Merges another [[Aggregator]].
     */
    def merge(other: Aggregator): this.type = {
      if (!other.initialized) {
        this
      } else {
        if (!initialized) {
          init(other.k)
        }
        assert(k == other.k, s"dimension mismatch: this.k = $k but other.k = ${other.k}")
        count += other.count
        wSum += other.wSum
        wwSum += other.wwSum
        bSum += other.bSum
        bbSum += other.bbSum
        BLAS.axpy(1.0, other.aSum, aSum)
        BLAS.axpy(1.0, other.abSum, abSum)
        BLAS.axpy(1.0, other.aaSum, aaSum)
        this
      }
    }

    /**
     * Validates that we have seen observations.
     */
    def validate(): Unit = {
      assert(initialized, "Training dataset is empty.")
      assert(wSum > 0.0, "Sum of weights cannot be zero.")
    }

    /**
     * Weighted mean of features.
     */
    def aBar: DenseVector = {
      val output = aSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    /**
     * Weighted mean of labels.
     */
    def bBar: Double = bSum / wSum

    /**
     * Weighted population standard deviation of labels.
     */
    def bStd: Double = math.sqrt(bbSum / wSum - bBar * bBar)

    /**
     * Weighted mean of (label * features).
     */
    def abBar: DenseVector = {
      val output = abSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    /**
     * Weighted mean of (features * features^T^).
     */
    def aaBar: DenseVector = {
      val output = aaSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    /**
     * Weighted population variance of features.
     */
    def aVar: DenseVector = {
      val variance = Array.ofDim[Double](k)
      var i = 0
      var j = 2
      val aaValues = aaSum.values
      while (i < triK) {
        val l = j - 2
        val aw = aSum(l) / wSum
        variance(l) = aaValues(i) / wSum - aw * aw
        i += j
        j += 1
      }
      new DenseVector(variance)
    }
  }
}
