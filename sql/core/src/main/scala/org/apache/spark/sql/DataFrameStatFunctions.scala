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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.stat._

/**
 * :: Experimental ::
 * Statistic functions for [[DataFrame]]s.
 */
@Experimental
final class DataFrameStatFunctions private[sql](df: DataFrame) {

  /**
   * Calculates the correlation of two columns of a DataFrame. Currently only supports the Pearson
   * Correlation Coefficient. For Spearman Correlation, consider using RDD methods found in 
   * MLlib's Statistics.
   *
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   */
  def corr(col1: String, col2: String, method: String): Double = {
    require(method == "pearson", "Currently only the calculation of the Pearson Correlation " +
      "coefficient is supported.")
    StatFunctions.pearsonCorrelation(df, Seq(col1, col2))
  }

  /**
   * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
   *
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   */
  def corr(col1: String, col2: String): Double = {
    corr(col1, col2, "pearson")
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * The `support` should be greater than 1e-4.
   *
   * @param cols the names of the columns to search frequent items in.
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def freqItems(cols: Array[String], support: Double): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * Uses a `default` support of 1%.
   *
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def freqItems(cols: Array[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }

  /**
   * Python friendly implementation for `freqItems`
   */
  def freqItems(cols: Seq[String], support: Double): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /**
   * Python friendly implementation for `freqItems` with a default `support` of 1%.
   */
  def freqItems(cols: Seq[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }

  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
   * @param col1 the name of the first column
   * @param col2 the name of the second column
   * @return the covariance of the two columns.
   */
  def cov(col1: String, col2: String): Double = {
    StatFunctions.calculateCov(df, Seq(col1, col2))
  }
}
