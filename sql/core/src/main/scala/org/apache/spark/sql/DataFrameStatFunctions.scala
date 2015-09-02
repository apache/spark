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

import java.{util => ju, lang => jl}

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.stat._

/**
 * :: Experimental ::
 * Statistic functions for [[DataFrame]]s.
 *
 * @since 1.4.0
 */
@Experimental
final class DataFrameStatFunctions private[sql](df: DataFrame) {

  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
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
   *
   * @since 1.4.0
   */
  def cov(col1: String, col2: String): Double = {
    StatFunctions.calculateCov(df, Seq(col1, col2))
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
   * @since 1.4.0
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

  /**
   * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
   * The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero
   * pair frequencies will be returned.
   * The first column of each row will be the distinct values of `col1` and the column names will
   * be the distinct values of `col2`. The name of the first column will be `$col1_$col2`. Counts
   * will be returned as `Long`s. Pairs that have no occurrences will have zero as their counts.
   * Null elements will be replaced by "null", and back ticks will be dropped from elements if they
   * exist.
   *
   *
   * @param col1 The name of the first column. Distinct items will make the first item of
   *             each row.
   * @param col2 The name of the second column. Distinct items will make the column names
   *             of the DataFrame.
   * @return A DataFrame containing for the contingency table.
   *
   * {{{
   *    val df = sqlContext.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
   *      (3, 3))).toDF("key", "value")
   *    val ct = df.stat.crosstab("key", "value")
   *    ct.show()
   *    +---------+---+---+---+
   *    |key_value|  1|  2|  3|
   *    +---------+---+---+---+
   *    |        2|  2|  0|  1|
   *    |        1|  1|  1|  0|
   *    |        3|  0|  1|  1|
   *    +---------+---+---+---+
   * }}}
   *
   * @since 1.4.0
   */
  def crosstab(col1: String, col2: String): DataFrame = {
    StatFunctions.crossTabulate(df, col1, col2)
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * The `support` should be greater than 1e-4.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]].
   *
   * @param cols the names of the columns to search frequent items in.
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   *
   * {{{
   *    val rows = Seq.tabulate(100) { i =>
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = sqlContext.createDataFrame(rows).toDF("a", "b")
   *    // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
   *    // "a" and "b"
   *    val freqSingles = df.stat.freqItems(Array("a", "b"), 0.4)
   *    freqSingles.show()
   *    +-----------+-------------+
   *    |a_freqItems|  b_freqItems|
   *    +-----------+-------------+
   *    |    [1, 99]|[-1.0, -99.0]|
   *    +-----------+-------------+
   *    // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
   *    val pairDf = df.select(struct("a", "b").as("a-b"))
   *    val freqPairs = pairDf.stat.freqItems(Array("a-b"), 0.1)
   *    freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
   *    +----------+
   *    |   freq_ab|
   *    +----------+
   *    |  [1,-1.0]|
   *    |   ...    |
   *    +----------+
   * }}}
   *
   * @since 1.4.0
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
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]].
   *
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   *
   * @since 1.4.0
   */
  def freqItems(cols: Array[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }

  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]].
   *
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   *
   * {{{
   *    val rows = Seq.tabulate(100) { i =>
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = sqlContext.createDataFrame(rows).toDF("a", "b")
   *    // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
   *    // "a" and "b"
   *    val freqSingles = df.stat.freqItems(Seq("a", "b"), 0.4)
   *    freqSingles.show()
   *    +-----------+-------------+
   *    |a_freqItems|  b_freqItems|
   *    +-----------+-------------+
   *    |    [1, 99]|[-1.0, -99.0]|
   *    +-----------+-------------+
   *    // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
   *    val pairDf = df.select(struct("a", "b").as("a-b"))
   *    val freqPairs = pairDf.stat.freqItems(Seq("a-b"), 0.1)
   *    freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
   *    +----------+
   *    |   freq_ab|
   *    +----------+
   *    |  [1,-1.0]|
   *    |   ...    |
   *    +----------+
   * }}}
   *
   * @since 1.4.0
   */
  def freqItems(cols: Seq[String], support: Double): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * Uses a `default` support of 1%.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]].
   *
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   *
   * @since 1.4.0
   */
  def freqItems(cols: Seq[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
   *                  its fraction as zero.
   * @param seed random seed
   * @tparam T stratum type
   * @return a new [[DataFrame]] that represents the stratified sample
   *
   * {{{
   *    val df = sqlContext.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
   *      (3, 3))).toDF("key", "value")
   *    val fractions = Map(1 -> 1.0, 3 -> 0.5)
   *    df.stat.sampleBy("key", fractions, 36L).show()
   *    +---+-----+
   *    |key|value|
   *    +---+-----+
   *    |  1|    1|
   *    |  1|    2|
   *    |  3|    2|
   *    +---+-----+
   * }}}
   *
   * @since 1.5.0
   */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame = {
    require(fractions.values.forall(p => p >= 0.0 && p <= 1.0),
      s"Fractions must be in [0, 1], but got $fractions.")
    import org.apache.spark.sql.functions.{rand, udf}
    val c = Column(col)
    val r = rand(seed)
    val f = udf { (stratum: Any, x: Double) =>
      x < fractions.getOrElse(stratum.asInstanceOf[T], 0.0)
    }
    df.filter(f(c, r))
  }

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
   *                  its fraction as zero.
   * @param seed random seed
   * @tparam T stratum type
   * @return a new [[DataFrame]] that represents the stratified sample
   *
   * @since 1.5.0
   */
  def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame = {
    sampleBy(col, fractions.asScala.toMap.asInstanceOf[Map[T, Double]], seed)
  }
}
