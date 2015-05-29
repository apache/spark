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
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.correlation.Correlations
import org.apache.spark.mllib.stat.test.{ChiSqTest, ChiSqTestResult}
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * API for statistical functions in MLlib.
 */
@Experimental
object Statistics {

  /**
   * Computes column-wise summary statistics for the input RDD[Vector].
   *
   * @param X an RDD[Vector] for which column-wise summary statistics are to be computed.
   * @return [[MultivariateStatisticalSummary]] object containing column-wise summary statistics.
   */
  def colStats(X: RDD[Vector]): MultivariateStatisticalSummary = {
    new RowMatrix(X).computeColumnSummaryStatistics()
  }

  /**
   * Compute the Pearson correlation matrix for the input RDD of Vectors.
   * Columns with 0 covariance produce NaN entries in the correlation matrix.
   *
   * @param X an RDD[Vector] for which the correlation matrix is to be computed.
   * @return Pearson correlation matrix comparing columns in X.
   */
  def corr(X: RDD[Vector]): Matrix = Correlations.corrMatrix(X)

  /**
   * Compute the correlation matrix for the input RDD of Vectors using the specified method.
   * Methods currently supported: `pearson` (default), `spearman`.
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
   * Returns NaN if either vector has 0 variance.
   *
   * Note: the two input RDDs need to have the same number of partitions and the same number of
   * elements in each partition.
   *
   * @param x RDD[Double] of the same cardinality as y.
   * @param y RDD[Double] of the same cardinality as x.
   * @return A Double containing the Pearson correlation between the two input RDD[Double]s
   */
  def corr(x: RDD[Double], y: RDD[Double]): Double = Correlations.corr(x, y)

  /**
   * Compute the correlation for the input RDDs using the specified method.
   * Methods currently supported: `pearson` (default), `spearman`.
   *
   * Note: the two input RDDs need to have the same number of partitions and the same number of
   * elements in each partition.
   *
   * @param x RDD[Double] of the same cardinality as y.
   * @param y RDD[Double] of the same cardinality as x.
   * @param method String specifying the method to use for computing correlation.
   *               Supported: `pearson` (default), `spearman`
   * @return A Double containing the correlation between the two input RDD[Double]s using the
   *         specified method.
   */
  def corr(x: RDD[Double], y: RDD[Double], method: String): Double = Correlations.corr(x, y, method)

  /**
   * Conduct Pearson's chi-squared goodness of fit test of the observed data against the
   * expected distribution.
   *
   * Note: the two input Vectors need to have the same size.
   *       `observed` cannot contain negative values.
   *       `expected` cannot contain nonpositive values.
   *
   * @param observed Vector containing the observed categorical counts/relative frequencies.
   * @param expected Vector containing the expected categorical counts/relative frequencies.
   *                 `expected` is rescaled if the `expected` sum differs from the `observed` sum.
   * @return ChiSquaredTest object containing the test statistic, degrees of freedom, p-value,
   *         the method used, and the null hypothesis.
   */
  def chiSqTest(observed: Vector, expected: Vector): ChiSqTestResult = {
    ChiSqTest.chiSquared(observed, expected)
  }

  /**
   * Conduct Pearson's chi-squared goodness of fit test of the observed data against the uniform
   * distribution, with each category having an expected frequency of `1 / observed.size`.
   *
   * Note: `observed` cannot contain negative values.
   *
   * @param observed Vector containing the observed categorical counts/relative frequencies.
   * @return ChiSquaredTest object containing the test statistic, degrees of freedom, p-value,
   *         the method used, and the null hypothesis.
   */
  def chiSqTest(observed: Vector): ChiSqTestResult = ChiSqTest.chiSquared(observed)

  /**
   * Conduct Pearson's independence test on the input contingency matrix, which cannot contain
   * negative entries or columns or rows that sum up to 0.
   *
   * @param observed The contingency matrix (containing either counts or relative frequencies).
   * @return ChiSquaredTest object containing the test statistic, degrees of freedom, p-value,
   *         the method used, and the null hypothesis.
   */
  def chiSqTest(observed: Matrix): ChiSqTestResult = ChiSqTest.chiSquaredMatrix(observed)

  /**
   * Conduct Pearson's independence test for every feature against the label across the input RDD.
   * For each feature, the (feature, label) pairs are converted into a contingency matrix for which
   * the chi-squared statistic is computed. All label and feature values must be categorical.
   *
   * @param data an `RDD[LabeledPoint]` containing the labeled dataset with categorical features.
   *             Real-valued features will be treated as categorical for each distinct value.
   * @return an array containing the ChiSquaredTestResult for every feature against the label.
   *         The order of the elements in the returned array reflects the order of input features.
   */
  def chiSqTest(data: RDD[LabeledPoint]): Array[ChiSqTestResult] = {
    ChiSqTest.chiSquaredFeatures(data)
  }
}
