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

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

private[ml] class WeightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double) extends Serializable

private[ml] class WeightedLeastSquares(
    val fitIntercept: Boolean,
    val regParam: Double,
    val standardization: Boolean) extends Logging with Serializable {
  import WeightedLeastSquares._

  require(regParam >= 0.0, s"regParam cannot be negative: $regParam")
  if (regParam == 0.0) {
    logWarning("regParam is zero, which might cause numerical instability and overfit.")
  }

  /**
   * Creates a [[WeightedLeastSquaresModel]] from an RDD of [[Instance]]s.
   */
  def fit(instances: RDD[Instance]): WeightedLeastSquaresModel = {
    val summary = instances.treeAggregate(new Aggregator)(_.add(_), _.merge(_))
    assert(summary.initialized, "Training dataset is empty.")
    // assert(summary.wSum > 0.0, "Sum of weights cannot be zero.")
    // assert(summary.count > 1.0, "Must have more than one instances.")
    val triK = summary.triK
    val bBar = summary.bBar
    val bVar = summary.bVar
    val aBar = summary.aBar
    val aVar = summary.aVar
    val abBar = summary.abBar
    val aaBar = summary.aaBar
    val aaValues = aaBar.values

    if (fitIntercept) {
      RowMatrix.dspr(-1.0, aBar, aaValues)
      BLAS.axpy(-bBar, aBar, abBar)
    }

    // add regularization to diagonals
    var i = 0
    var j = 2
    while (i < triK) {
      val scale = if (standardization) {
        aVar(j - 2) / math.sqrt(bVar)
      } else {
        1.0 / math.sqrt(bVar)
      }
      aaValues(i) += scale * regParam
      i += j
      j += 1
    }

    val x = choleskySolve(aaBar.values, abBar)

    // compute intercept
    val intercept = if (fitIntercept) {
      bBar - BLAS.dot(aBar, x)
    } else {
      0.0
    }

    new WeightedLeastSquaresModel(x, intercept)
  }

  private def choleskySolve(A: Array[Double], bx: DenseVector): DenseVector = {
    val k = bx.size
    val info = new intW(0)
    lapack.dppsv("U", k, 1, A, bx.values, k, info)
    val code = info.`val`
    assert(code == 0, s"lapack.dpotrs returned $code.")
    bx
  }
}

private[ml] object WeightedLeastSquares {

  case class Instance(w: Double, a: Vector, b: Double) {
    require(w >= 0.0, s"Weight cannot be negative: $w.")
  }

  private class Aggregator extends Serializable {
    var initialized: Boolean = false
    var k: Int = _
    var count: Long = _
    var triK: Int = _
    var wSum: Double = _
    var wwSum: Double = _
    var bSum: Double = _
    var bbSum: Double = _
    var aSum: DenseVector = _
    var abSum: DenseVector = _
    var aaSum: DenseVector = _

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
      aSum = DenseVector.zeros(k)
      abSum = DenseVector.zeros(k)
      aaSum = DenseVector.zeros(triK)
      initialized = true
    }

    def add(instance: Instance): this.type = {
      val Instance(w, a, b) = instance
      val ak = a.size
      if (!initialized) {
        init(ak)
        initialized = true
      }
      assert(ak == k, s"Dimension mismatch. Expect vectors of size $k but got $ak.")
      count += 1L
      wSum += w
      wwSum += w * w
      bSum += w * b
      bbSum += w * b * b
      BLAS.axpy(w, a, aSum)
      BLAS.axpy(w * b, a, abSum)
      RowMatrix.dspr(w, a, aaSum.values)
      this
    }

    def merge(other: Aggregator): this.type = {
      if (!other.initialized) {
        this
      } else {
        if (!initialized) {
          init(other.k)
        }
        assert(k == other.k)
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

    def aBar: DenseVector = {
      val output = aSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    def bBar: Double = bSum / wSum

    def bVar: Double = bbSum / wSum - bBar * bBar

    def abBar: DenseVector = {
      val output = abSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    def aaBar: DenseVector = {
      val output = aaSum.copy
      BLAS.scal(1.0 / wSum, output)
      output
    }

    def aVar: DenseVector = {
      val variance = Array.ofDim[Double](k)
      var i = 0
      var j = 2
      val aaValues = aaSum.values
      while (i < triK) {
        val l = j - 2
        variance(l) = wSum * aaValues(i) - aSum(l) * aSum(l)
        i += j
        j += 1
      }
      val output = new DenseVector(variance)
      // correct bias
      BLAS.scal(1.0 / (wSum * wSum), output)
      output
    }
  }
}
