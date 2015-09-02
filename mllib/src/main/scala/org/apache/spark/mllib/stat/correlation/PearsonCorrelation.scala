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

package org.apache.spark.mllib.stat.correlation

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * Compute Pearson correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
private[stat] object PearsonCorrelation extends Correlation with Logging {

  /**
   * Compute the Pearson correlation for two datasets. NaN if either vector has 0 variance.
   */
  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {
    computeCorrelationWithMatrixImpl(x, y)
  }

  /**
   * Compute the Pearson correlation matrix S, for the input matrix, where S(i, j) is the
   * correlation between column i and j. 0 covariance results in a correlation value of Double.NaN.
   */
  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val rowMatrix = new RowMatrix(X)
    val cov = rowMatrix.computeCovariance()
    computeCorrelationMatrixFromCovariance(cov)
  }

  /**
   * Compute the Pearson correlation matrix from the covariance matrix.
   * 0 covariance results in a correlation value of Double.NaN.
   */
  def computeCorrelationMatrixFromCovariance(covarianceMatrix: Matrix): Matrix = {
    val cov = covarianceMatrix.toBreeze.asInstanceOf[BDM[Double]]
    val n = cov.cols

    // Compute the standard deviation on the diagonals first
    var i = 0
    while (i < n) {
      // TODO remove once covariance numerical issue resolved.
      cov(i, i) = if (closeToZero(cov(i, i))) 0.0 else math.sqrt(cov(i, i))
      i +=1
    }

    // Loop through columns since cov is column major
    var j = 0
    var sigma = 0.0
    var containNaN = false
    while (j < n) {
      sigma = cov(j, j)
      i = 0
      while (i < j) {
        val corr = if (sigma == 0.0 || cov(i, i) == 0.0) {
          containNaN = true
          Double.NaN
        } else {
          cov(i, j) / (sigma * cov(i, i))
        }
        cov(i, j) = corr
        cov(j, i) = corr
        i += 1
      }
      j += 1
    }

    // put 1.0 on the diagonals
    i = 0
    while (i < n) {
      cov(i, i) = 1.0
      i +=1
    }

    if (containNaN) {
      logWarning("Pearson correlation matrix contains NaN values.")
    }

    Matrices.fromBreeze(cov)
  }

  private def closeToZero(value: Double, threshold: Double = 1e-12): Boolean = {
    math.abs(value) <= threshold
  }
}
