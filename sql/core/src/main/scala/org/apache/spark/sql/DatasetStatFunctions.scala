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

import org.apache.spark.sql.execution.stat.StatFunctions

/**
 * :: Experimental ::
 * Statistic functions for [[Dataset]]s.
 *
 * @since 2.0.0
 */
class DatasetStatFunctions[T: Encoder] private[sql](ds: Dataset[T]) {
  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
   *
   * @param col1 the name of the first column
   * @param col2 the name of the second column
   * @return the covariance of the two columns.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.cov("rand1", "rand2")
   *    res1: Double = 0.065...
   * }}}
   * @since 2.0.0
   */
  def cov(col1: String, col2: String): Double = {
    StatFunctions.calculateCov(ds, Seq(col1, col2))
  }

  /**
   * Calculates the correlation of two columns of a DataFrame. Currently only supports the Pearson
   * Correlation Coefficient. For Spearman Correlation, consider using RDD methods found in
   * MLlib's Statistics.
   *
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2")
   *    res1: Double = 0.613...
   * }}}
   *
   * @since 2.0.0
   */
  def corr(col1: String, col2: String, method: String): Double = {
    require(method == "pearson", "Currently only the calculation of the Pearson Correlation " +
      "coefficient is supported.")
    StatFunctions.pearsonCorrelation(ds, Seq(col1, col2))
  }

  /**
   * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
   *
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2", "pearson")
   *    res1: Double = 0.613...
   * }}}
   *
   * @since 1.4.0
   */
  def corr(col1: String, col2: String): Double = {
    corr(col1, col2, "pearson")
  }
}
