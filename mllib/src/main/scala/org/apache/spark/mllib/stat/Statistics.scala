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

package org.apache.spark.mllib.stat

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.correlation.Correlations
import org.apache.spark.rdd.RDD

/**
 * API for statistical functions in MLlib
 */
@Experimental
object Statistics {

  /**
   * Compute the Pearson correlation matrix for the input RDD of Vectors.
   * Returns NaN if either vector has 0 variance.
   *
   * @param X an RDD[Vector] for which the correlation matrix is to be computed.
   * @return Pearson correlation matrix comparing columns in X.
   */
  def corr(X: RDD[Vector]): Matrix = Correlations.corrMatrix(X)

  /**
   * Compute the correlation matrix for the input RDD of Vectors using the specified method.
   * Methods currently supported: `pearson` (default), `spearman`
   *
   * Note that for Spearman, a rank correlation, we need to create an RDD[Double] for each column
   * and sort it in order to retrieve the ranks and then join the columns back into an RDD[Vector],
   * which is fairly costly. Cache the input RDD before calling corr with `method = "spearman"` to
   * avoid recomputing the common lineage.
   *
   * @param X an RDD[Vector] for which the correlation matrix is to be computed.
   * @param method String specifying the method to use for computing correlation.
   *               Supported: `pearson` (default), `spearman`
   * @return Correlation matrix comparing columns in X.
   */
  def corr(X: RDD[Vector], method: String): Matrix = Correlations.corrMatrix(X, method)

  /**
   * Compute the Pearson correlation for the input RDDs.
   * Columns with 0 covariance produce NaN entries in the correlation matrix.
   *
   * @param x RDD[Double] of the same cardinality as y
   * @param y RDD[Double] of the same cardinality as x
   * @return A Double containing the Pearson correlation between the two input RDD[Double]s
   */
  def corr(x: RDD[Double], y: RDD[Double]): Double = Correlations.corr(x, y)

  /**
   * Compute the correlation for the input RDDs using the specified method.
   * Methods currently supported: pearson (default), spearman
   *
   * @param x RDD[Double] of the same cardinality as y
   * @param y RDD[Double] of the same cardinality as x
   * @param method String specifying the method to use for computing correlation.
   *               Supported: `pearson` (default), `spearman`
   *@return A Double containing the correlation between the two input RDD[Double]s using the
   *         specified method.
   */
  def corr(x: RDD[Double], y: RDD[Double], method: String): Double = Correlations.corr(x, y, method)
}
