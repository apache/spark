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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.rdd.RDD

/**
 * Model fitted by [[WeightedLeastSquares]].
 *
 * @param coefficients model coefficients
 * @param intercept model intercept
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private[ml] class WeightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double,
    val diagInvAtWA: DenseVector,
    val objectiveHistory: Array[Double]) extends Serializable {

  def predict(features: Vector): Double = {
    BLAS.dot(coefficients, features) + intercept
  }
}

/**
 * Weighted least squares solver via normal equation.
 * Given weighted observations (w,,i,,, a,,i,,, b,,i,,), we use the following weighted least squares
 * formulation:
 *
 * min,,x,z,, 1/2 sum,,i,, w,,i,, (a,,i,,^T^ x + z - b,,i,,)^2^ / sum,,i,, w_i
 *   + lambda / delta (1/2 (1 - alpha) sum,,j,, (sigma,,j,, x,,j,,)^2^
 *   + alpha sum,,j,, abs(sigma,,j,, x,,j,,)),
 *
 * where lambda is the regularization parameter, alpha is the ElasticNet mixing parameter,
 * and delta and sigma,,j,, are controlled by [[standardizeLabel]] and [[standardizeFeatures]],
 * respectively.
 *
 * Set [[regParam]] to 0.0 and turn off both [[standardizeFeatures]] and [[standardizeLabel]] to
 * match R's `lm`.
 * Turn on [[standardizeLabel]] to match R's `glmnet`.
 *
 * @param fitIntercept whether to fit intercept. If false, z is 0.0.
 * @param regParam L2 regularization parameter (lambda).
 * @param elasticNetParam the ElasticNet mixing parameter.
 * @param standardizeFeatures whether to standardize features. If true, sigma_,,j,, is the
 *                            population standard deviation of the j-th column of A. Otherwise,
 *                            sigma,,j,, is 1.0.
 * @param standardizeLabel whether to standardize label. If true, delta is the population standard
 *                         deviation of the label column b. Otherwise, delta is 1.0.
 * @param solverType the type of solver to use for optimization.
 * @param maxIter maximum number of iterations when stochastic optimization is used.
 * @param tol the convergence tolerance of the iterations when stochastic optimization is used.
 */
private[ml] class WeightedLeastSquares(
    val fitIntercept: Boolean,
    val regParam: Double,
    val elasticNetParam: Double,
    val standardizeFeatures: Boolean,
    val standardizeLabel: Boolean,
    val solverType: WeightedLeastSquares.Solver = WeightedLeastSquares.Auto,
    val maxIter: Int = 100,
    val tol: Double = 1e-6) extends Logging with Serializable {
  import WeightedLeastSquares._

  require(regParam >= 0.0, s"regParam cannot be negative: $regParam")
  if (regParam == 0.0) {
    logWarning("regParam is zero, which might cause numerical instability and overfitting.")
  }
  require(elasticNetParam >= 0.0 && elasticNetParam <= 1.0,
    s"elasticNetParam must be in [0, 1]: $elasticNetParam")
  require(maxIter >= 0, s"maxIter must be a positive integer: $maxIter")
  require(tol > 0, s"tol must be greater than zero: $tol")

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
    val bbBar = summary.bbBar
    val aBar = summary.aBar
    val aStd = summary.aStd
    val abBar = summary.abBar
    val aaBar = summary.aaBar
    val aaBarValues = aaBar.values
    val numFeatures = abBar.size
    val rawBStd = summary.bStd
    // if b is constant (rawBStd is zero), then b cannot be scaled. In this case
    // setting bStd=abs(bBar) ensures that b is not scaled anymore in l-bfgs algorithm.
    val bStd = if (rawBStd == 0.0) math.abs(bBar) else rawBStd

    if (rawBStd == 0) {
      if (fitIntercept || bBar == 0.0) {
        if (bBar == 0.0) {
          logWarning(s"Mean and standard deviation of the label are zero, so the coefficients " +
            s"and the intercept will all be zero; as a result, training is not needed.")
        } else {
          logWarning(s"The standard deviation of the label is zero, so the coefficients will be " +
            s"zeros and the intercept will be the mean of the label; as a result, " +
            s"training is not needed.")
        }
        val coefficients = new DenseVector(Array.ofDim(numFeatures))
        val intercept = bBar
        val diagInvAtWA = new DenseVector(Array(0D))
        return new WeightedLeastSquaresModel(coefficients, intercept, diagInvAtWA, Array(0D))
      } else {
        require(!(regParam > 0.0 && standardizeLabel), "The standard deviation of the label is " +
          "zero. Model cannot be regularized with standardization=true")
        logWarning(s"The standard deviation of the label is zero. Consider setting " +
          s"fitIntercept=true.")
      }
    }

    val aBarStd = new Array[Double](numFeatures)
    var j = 0
    while (j < numFeatures) {
      if (aStd(j) == 0.0) {
        aBarStd(j) = 0.0
      } else {
        aBarStd(j) = aBar(j) / aStd(j)
      }
      j += 1
    }

    val abBarStd = new Array[Double](numFeatures)
    j = 0
    while (j < numFeatures) {
      if (aStd(j) == 0.0) {
        abBarStd(j) = 0.0
      } else {
        abBarStd(j) = abBar(j) / (aStd(j) * bStd)
      }
      j += 1
    }

    val aaBarStd = new Array[Double](triK)
    j = 0
    var kk = 0
    while (j < numFeatures) {
      val aStdJ = aStd(j)
      var i = 0
      while (i <= j) {
        val aStdI = aStd(i)
        if (aStdJ == 0.0 || aStdI == 0.0) {
          aaBarStd(kk) = 0.0
        } else {
          aaBarStd(kk) = aaBarValues(kk) / (aStdI * aStdJ)
        }
        kk += 1
        i += 1
      }
      j += 1
    }

    val bBarStd = bBar / bStd
    val bbBarStd = bbBar / (bStd * bStd)

    val effectiveRegParam = regParam / bStd
    val effectiveL1RegParam = elasticNetParam * effectiveRegParam
    val effectiveL2RegParam = (1.0 - elasticNetParam) * effectiveRegParam

    // add regularization to diagonals
    var i = 0
    j = 2
    while (i < triK) {
      var lambda = effectiveL2RegParam
      if (!standardizeFeatures) {
        val std = aStd(j - 2)
        if (std != 0.0) {
          lambda /= (std * std)
        } else {
          lambda = 0.0
        }
      }
      if (!standardizeLabel) {
        lambda *= bStd
      }
      aaBarStd(i) += lambda
      i += j
      j += 1
    }
    val aa = getAtA(aaBarStd, aBarStd)
    val ab = getAtB(abBarStd, bBarStd)

    val solver = if ((solverType == WeightedLeastSquares.Auto && elasticNetParam != 0.0) ||
      (solverType == WeightedLeastSquares.QuasiNewton)) {
      val effectiveL1RegFun: Option[(Int) => Double] = if (effectiveL1RegParam != 0.0) {
        Some((index: Int) => {
            if (fitIntercept && index == numFeatures) {
              0.0
            } else {
              if (standardizeFeatures) {
                effectiveL1RegParam
              } else {
                if (aStd(index) != 0.0) effectiveL1RegParam / aStd(index) else 0.0
              }
            }
          })
      } else {
        None
      }
      new QuasiNewtonSolver(fitIntercept, maxIter, tol, effectiveL1RegFun)
    } else {
      new CholeskySolver(fitIntercept)
    }

    val solution = solver match {
      case cholesky: CholeskySolver =>
        try {
          cholesky.solve(bBarStd, bbBarStd, ab, aa, new DenseVector(aBarStd))
        } catch {
          // if Auto solver is used and Cholesky fails due to singular AtA, then fall back to
          // quasi-newton solver
          case _: SingularMatrixException if solverType == WeightedLeastSquares.Auto =>
            logWarning("Cholesky solver failed due to singular covariance matrix. " +
              "Retrying with Quasi-Newton solver.")
            // ab and aa were modified in place, so reconstruct them
            val _aa = getAtA(aaBarStd, aBarStd)
            val _ab = getAtB(abBarStd, bBarStd)
            val newSolver = new QuasiNewtonSolver(fitIntercept, maxIter, tol, None)
            newSolver.solve(bBarStd, bbBarStd, _ab, _aa, new DenseVector(aBarStd))
        }
      case qn: QuasiNewtonSolver =>
        qn.solve(bBarStd, bbBarStd, ab, aa, new DenseVector(aBarStd))
    }
    val intercept = solution.intercept * bStd
    val coefficients = solution.coefficients

    // convert the coefficients from the scaled space to the original space
    var ii = 0
    val coefficientArray = coefficients.toArray
    val len = coefficientArray.length
    while (ii < len) {
      coefficientArray(ii) *= { if (aStd(ii) != 0.0) bStd / aStd(ii) else 0.0 }
      ii += 1
    }

    // aaInv is a packed upper triangular matrix, here we get all elements on diagonal
    val diagInvAtWA = solution.aaInv.map { inv =>
      val values = inv.values
      new DenseVector((1 to k).map { i =>
        val multiplier = if (i == k && fitIntercept) 1.0 else aStd(i - 1) * aStd(i - 1)
        values(i + (i - 1) * i / 2 - 1) / (wSum * multiplier)
      }.toArray)
    }.getOrElse(new DenseVector(Array(0D)))

    new WeightedLeastSquaresModel(coefficients, intercept, diagInvAtWA,
      solution.objectiveHistory.getOrElse(Array(0D)))
  }

  /** Construct A^T^ A from summary statistics. */
  private def getAtA(aaBar: Array[Double], aBar: Array[Double]): DenseVector = {
    if (fitIntercept) {
      new DenseVector(Array.concat(aaBar, aBar, Array(1.0)))
    } else {
      new DenseVector(aaBar.clone())
    }
  }

  /** Construct A^T^ B from summary statistics. */
  private def getAtB(abBar: Array[Double], bBar: Double): DenseVector = {
    if (fitIntercept) {
      new DenseVector(Array.concat(abBar, Array(bBar)))
    } else {
      new DenseVector(abBar.clone())
    }
  }
}

private[ml] object WeightedLeastSquares {

  /**
   * In order to take the normal equation approach efficiently, [[WeightedLeastSquares]]
   * only supports the number of features is no more than 4096.
   */
  val MAX_NUM_FEATURES: Int = 4096

  sealed trait Solver
  case object Auto extends Solver
  case object Cholesky extends Solver
  case object QuasiNewton extends Solver

  val supportedSolvers = Array(Auto, Cholesky, QuasiNewton)

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
      require(k <= MAX_NUM_FEATURES, "In order to take the normal equation approach efficiently, " +
        s"we set the max number of features to $MAX_NUM_FEATURES but got $k.")
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
     * Weighted mean of squared labels.
     */
    def bbBar: Double = bbSum / wSum

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
     * Weighted population standard deviation of features.
     */
    def aStd: DenseVector = {
      val std = Array.ofDim[Double](k)
      var i = 0
      var j = 2
      val aaValues = aaSum.values
      while (i < triK) {
        val l = j - 2
        val aw = aSum(l) / wSum
        std(l) = math.sqrt(aaValues(i) / wSum - aw * aw)
        i += j
        j += 1
      }
      new DenseVector(std)
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
