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

import org.apache.spark.internal.LogKeys.COUNT
import org.apache.spark.internal.MDC
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.OptionalInstrumentation
import org.apache.spark.rdd.RDD
import org.apache.spark.util.MavenUtils.LogStringContext

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
 * min,,x,z,, 1/2 sum,,i,, w,,i,, (a,,i,,^T^ x + z - b,,i,,)^2^ / sum,,i,, w,,i,,
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
 * @note The coefficients and intercept are always trained in the scaled space, but are returned
 *       on the original scale. [[standardizeFeatures]] and [[standardizeLabel]] can be used to
 *       control whether regularization is applied in the original space or the scaled space.
 * @param fitIntercept whether to fit intercept. If false, z is 0.0.
 * @param regParam Regularization parameter (lambda).
 * @param elasticNetParam the ElasticNet mixing parameter (alpha).
 * @param standardizeFeatures whether to standardize features. If true, sigma,,j,, is the
 *                            population standard deviation of the j-th column of A. Otherwise,
 *                            sigma,,j,, is 1.0.
 * @param standardizeLabel whether to standardize label. If true, delta is the population standard
 *                         deviation of the label column b. Otherwise, delta is 1.0.
 * @param solverType the type of solver to use for optimization.
 * @param maxIter maximum number of iterations. Only for QuasiNewton solverType.
 * @param tol the convergence tolerance of the iterations. Only for QuasiNewton solverType.
 */
private[ml] class WeightedLeastSquares(
    val fitIntercept: Boolean,
    val regParam: Double,
    val elasticNetParam: Double,
    val standardizeFeatures: Boolean,
    val standardizeLabel: Boolean,
    val solverType: WeightedLeastSquares.Solver = WeightedLeastSquares.Auto,
    val maxIter: Int = 100,
    val tol: Double = 1e-6
  ) extends Serializable {
  import WeightedLeastSquares._

  require(regParam >= 0.0, s"regParam cannot be negative: $regParam")
  require(elasticNetParam >= 0.0 && elasticNetParam <= 1.0,
    s"elasticNetParam must be in [0, 1]: $elasticNetParam")
  require(maxIter > 0, s"maxIter must be a positive integer: $maxIter")
  require(tol >= 0.0, s"tol must be >= 0, but was set to $tol")

  /**
   * Creates a [[WeightedLeastSquaresModel]] from an RDD of [[Instance]]s.
   */
  def fit(
      instances: RDD[Instance],
      instr: OptionalInstrumentation = OptionalInstrumentation.create(
        classOf[WeightedLeastSquares]),
      depth: Int = 2
    ): WeightedLeastSquaresModel = {
    if (regParam == 0.0) {
      instr.logWarning("regParam is zero, which might cause numerical instability and overfitting.")
    }

    val summary = instances.treeAggregate(new Aggregator)(_.add(_), _.merge(_), depth)
    summary.validate()
    instr.logInfo(log"Number of instances: ${MDC(COUNT, summary.count)}.")
    val k = if (fitIntercept) summary.k + 1 else summary.k
    val numFeatures = summary.k
    val triK = summary.triK
    val wSum = summary.wSum

    val rawBStd = summary.bStd
    val rawBBar = summary.bBar
    // if b is constant (rawBStd is zero), then b cannot be scaled. In this case
    // setting bStd=abs(rawBBar) ensures that b is not scaled anymore in l-bfgs algorithm.
    val bStd = if (rawBStd == 0.0) math.abs(rawBBar) else rawBStd

    if (rawBStd == 0) {
      if (fitIntercept || rawBBar == 0.0) {
        if (rawBBar == 0.0) {
          instr.logWarning(s"Mean and standard deviation of the label are zero, so the " +
            s"coefficients and the intercept will all be zero; as a result, training is not " +
            s"needed.")
        } else {
          instr.logWarning(s"The standard deviation of the label is zero, so the coefficients " +
            s"will be zeros and the intercept will be the mean of the label; as a result, " +
            s"training is not needed.")
        }
        val coefficients = new DenseVector(Array.ofDim(numFeatures))
        val intercept = rawBBar
        val diagInvAtWA = new DenseVector(Array(0D))
        return new WeightedLeastSquaresModel(coefficients, intercept, diagInvAtWA, Array(0D))
      } else {
        require(!(regParam > 0.0 && standardizeLabel), "The standard deviation of the label is " +
          "zero. Model cannot be regularized when labels are standardized.")
        instr.logWarning(s"The standard deviation of the label is zero. Consider setting " +
          s"fitIntercept=true.")
      }
    }

    val bBar = summary.bBar / bStd
    val bbBar = summary.bbBar / (bStd * bStd)

    val aStd = summary.aStd
    val aStdValues = aStd.values

    val aBar = {
      val _aBar = summary.aBar
      val _aBarValues = _aBar.values
      var i = 0
      // scale aBar to standardized space in-place
      while (i < numFeatures) {
        if (aStdValues(i) == 0.0) {
          _aBarValues(i) = 0.0
        } else {
          _aBarValues(i) /= aStdValues(i)
        }
        i += 1
      }
      _aBar
    }
    val aBarValues = aBar.values

    val abBar = {
      val _abBar = summary.abBar
      val _abBarValues = _abBar.values
      var i = 0
      // scale abBar to standardized space in-place
      while (i < numFeatures) {
        if (aStdValues(i) == 0.0) {
          _abBarValues(i) = 0.0
        } else {
          _abBarValues(i) /= (aStdValues(i) * bStd)
        }
        i += 1
      }
      _abBar
    }
    val abBarValues = abBar.values

    val aaBar = {
      val _aaBar = summary.aaBar
      val _aaBarValues = _aaBar.values
      var j = 0
      var p = 0
      // scale aaBar to standardized space in-place
      while (j < numFeatures) {
        val aStdJ = aStdValues(j)
        var i = 0
        while (i <= j) {
          val aStdI = aStdValues(i)
          if (aStdJ == 0.0 || aStdI == 0.0) {
            _aaBarValues(p) = 0.0
          } else {
            _aaBarValues(p) /= (aStdI * aStdJ)
          }
          p += 1
          i += 1
        }
        j += 1
      }
      _aaBar
    }
    val aaBarValues = aaBar.values

    val effectiveRegParam = regParam / bStd
    val effectiveL1RegParam = elasticNetParam * effectiveRegParam
    val effectiveL2RegParam = (1.0 - elasticNetParam) * effectiveRegParam

    // add L2 regularization to diagonals
    var i = 0
    var j = 2
    while (i < triK) {
      var lambda = effectiveL2RegParam
      if (!standardizeFeatures) {
        val std = aStdValues(j - 2)
        if (std != 0.0) {
          lambda /= (std * std)
        } else {
          lambda = 0.0
        }
      }
      if (!standardizeLabel) {
        lambda *= bStd
      }
      aaBarValues(i) += lambda
      i += j
      j += 1
    }

    val aa = getAtA(aaBarValues, aBarValues)
    val ab = getAtB(abBarValues, bBar)

    val solver = if ((solverType == WeightedLeastSquares.Auto && elasticNetParam != 0.0 &&
      regParam != 0.0) || (solverType == WeightedLeastSquares.QuasiNewton)) {
      val effectiveL1RegFun: Option[(Int) => Double] = if (effectiveL1RegParam != 0.0) {
        Some((index: Int) => {
            if (fitIntercept && index == numFeatures) {
              0.0
            } else {
              if (standardizeFeatures) {
                effectiveL1RegParam
              } else {
                if (aStdValues(index) != 0.0) effectiveL1RegParam / aStdValues(index) else 0.0
              }
            }
          })
      } else {
        None
      }
      new QuasiNewtonSolver(fitIntercept, maxIter, tol, effectiveL1RegFun)
    } else {
      new CholeskySolver
    }

    val solution = solver match {
      case cholesky: CholeskySolver =>
        try {
          cholesky.solve(bBar, bbBar, ab, aa, aBar)
        } catch {
          // if Auto solver is used and Cholesky fails due to singular AtA, then fall back to
          // Quasi-Newton solver.
          case _: SingularMatrixException if solverType == WeightedLeastSquares.Auto =>
            instr.logWarning("Cholesky solver failed due to singular covariance matrix. " +
              "Retrying with Quasi-Newton solver.")
            // ab and aa were modified in place, so reconstruct them
            val _aa = getAtA(aaBarValues, aBarValues)
            val _ab = getAtB(abBarValues, bBar)
            val newSolver = new QuasiNewtonSolver(fitIntercept, maxIter, tol, None)
            newSolver.solve(bBar, bbBar, _ab, _aa, aBar)
        }
      case qn: QuasiNewtonSolver =>
        qn.solve(bBar, bbBar, ab, aa, aBar)
    }

    val (coefficientArray, intercept) = if (fitIntercept) {
      (solution.coefficients.slice(0, solution.coefficients.length - 1),
        solution.coefficients.last * bStd)
    } else {
      (solution.coefficients, 0.0)
    }

    // convert the coefficients from the scaled space to the original space
    var q = 0
    val len = coefficientArray.length
    while (q < len) {
      coefficientArray(q) *= { if (aStdValues(q) != 0.0) bStd / aStdValues(q) else 0.0 }
      q += 1
    }

    // aaInv is a packed upper triangular matrix, here we get all elements on diagonal
    val diagInvAtWA = solution.aaInv.map { inv =>
      new DenseVector((1 to k).map { i =>
        val multiplier = if (i == k && fitIntercept) {
          1.0
        } else {
          aStdValues(i - 1) * aStdValues(i - 1)
        }
        inv(i + (i - 1) * i / 2 - 1) / (wSum * multiplier)
      }.toArray)
    }.getOrElse(new DenseVector(Array(0D)))

    new WeightedLeastSquaresModel(new DenseVector(coefficientArray), intercept, diagInvAtWA,
      solution.objectiveHistory.getOrElse(Array(0D)))
  }

  /** Construct A^T^ A (append bias if necessary). */
  private def getAtA(aaBar: Array[Double], aBar: Array[Double]): DenseVector = {
    if (fitIntercept) {
      new DenseVector(Array.concat(aaBar, aBar, Array(1.0)))
    } else {
      new DenseVector(aaBar.clone())
    }
  }

  /** Construct A^T^ b (append bias if necessary). */
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
    def bStd: Double = {
      // We prevent variance from negative value caused by numerical error.
      val variance = math.max(bbSum / wSum - bBar * bBar, 0.0)
      math.sqrt(variance)
    }

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
        // We prevent variance from negative value caused by numerical error.
        std(l) = math.sqrt(math.max(aaValues(i) / wSum - aw * aw, 0.0))
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
        // We prevent variance from negative value caused by numerical error.
        variance(l) = math.max(aaValues(i) / wSum - aw * aw, 0.0)
        i += j
        j += 1
      }
      new DenseVector(variance)
    }
  }
}
