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

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.{DenseMatrix => BDM}
import breeze.numerics.{sqrt => brzSqrt}

class PearsonCorrelation extends Correlation {

  def computeCorrelationMatrix(x: RDD[Double], y: RDD[Double]): Double = {

    return 0.0
  }

  def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val rowMatrix = new RowMatrix(X)
    val cov = rowMatrix.computeCovariance().toBreeze.asInstanceOf[BDM[Double]]
    val n = cov.cols

    // Compute the standard deviation on the diagonals first
    var i = 0
    while (i < n) {
      cov(i, i) = brzSqrt(cov(i, i))
      i +=1
    }

    // we could use blas.dspr instead to compute the correlation matrix if the covariance matrix
    // is upper triangular.
    i = 0
    var j = 0
    var sigma = 0.0
    while (i < n) {
      sigma = cov(i, i)
      while (j < n) {
        if (i != j) { // we need to keep the stddev values on the diagonals throughout the update
          cov(i, j) = cov(i, j) / (sigma * cov(j, j))
        }
      }
    }

    // put 1.0 on the diagonals
    i = 0
    while (i < n) {
      cov(i, i) = 1.0
      i +=1
    }

    Matrices.fromBreeze(cov)
  }
}