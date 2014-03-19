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

package org.apache.spark.mllib.linalg

import org.apache.spark.rdd.RDD


import org.jblas.DoubleMatrix


/**
 * Class used to obtain principal components
 */
class PCA {
  private var k = 1

  /**
   * Set the number of top-k principle components to return
   */
  def setK(k: Int): PCA = {
    this.k = k
    this
  }

  /**
   * Compute PCA using the current set parameters
   */
  def compute(matrix: TallSkinnyDenseMatrix): Array[Array[Double]] = {
    computePCA(matrix)
  }

  /**
   * Compute PCA using the parameters currently set
   * See computePCA() for more details
   */
  def compute(matrix: RDD[Array[Double]]): Array[Array[Double]] = {
    computePCA(matrix)
  }

  /**
   * Computes the top k principal component coefficients for the m-by-n data matrix X.
   * Rows of X correspond to observations and columns correspond to variables. 
   * The coefficient matrix is n-by-k. Each column of coeff contains coefficients
   * for one principal component, and the columns are in descending 
   * order of component variance.
   * This function centers the data and uses the 
   * singular value decomposition (SVD) algorithm. 
   *
   * @param matrix dense matrix to perform PCA on
   * @return An nxk matrix with principal components in columns. Columns are inner arrays
   */
  private def computePCA(matrix: TallSkinnyDenseMatrix): Array[Array[Double]] = {
    val m = matrix.m
    val n = matrix.n

    if (m <= 0 || n <= 0) {
      throw new IllegalArgumentException("Expecting a well-formed matrix: m=$m n=$n")
    }

    computePCA(matrix.rows.map(_.data))
  }

  /**
   * Computes the top k principal component coefficients for the m-by-n data matrix X.
   * Rows of X correspond to observations and columns correspond to variables. 
   * The coefficient matrix is n-by-k. Each column of coeff contains coefficients
   * for one principal component, and the columns are in descending 
   * order of component variance.
   * This function centers the data and uses the 
   * singular value decomposition (SVD) algorithm. 
   *
   * @param matrix dense matrix to perform pca on
   * @return An nxk matrix of principal components
   */
  private def computePCA(matrix: RDD[Array[Double]]): Array[Array[Double]] = {
    val n = matrix.first.size

    // compute column sums and normalize matrix
    val colSumsTemp = matrix.map((_, 1)).fold((Array.ofDim[Double](n), 0)) {
      (a, b) =>
        val am = new DoubleMatrix(a._1)
        val bm = new DoubleMatrix(b._1)
        am.addi(bm)
        (a._1, a._2 + b._2)
    }

    val m = colSumsTemp._2
    val colSums = colSumsTemp._1.map(x => x / m)

    val data = matrix.map {
      x =>
        val row = Array.ofDim[Double](n)
        var i = 0
        while (i < n) {
          row(i) = x(i) - colSums(i)
          i += 1
        }
        row
    }

    val (u, s, v) = new SVD().setK(k).compute(data)
    v
  }
}

