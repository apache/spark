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

import java.{lang => jl, util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.BinaryEncoder
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.functions.{count_min_sketch, lit}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps
import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}

/**
 * Statistic functions for `DataFrame`s.
 *
 * @since 1.4.0
 */
@Stable
abstract class DataFrameStatFunctions {
  protected def df: DataFrame

  /**
   * Calculates the approximate quantiles of a numerical column of a DataFrame.
   *
   * The result of this algorithm has the following deterministic bound: If the DataFrame has N
   * elements and if we request the quantile at probability `p` up to error `err`, then the
   * algorithm will return a sample `x` from the DataFrame so that the *exact* rank of `x` is
   * close to (p * N). More precisely,
   *
   * {{{
   *   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)
   * }}}
   *
   * This method implements a variation of the Greenwald-Khanna algorithm (with some speed
   * optimizations). The algorithm was first present in <a
   * href="https://doi.org/10.1145/375663.375670"> Space-efficient Online Computation of Quantile
   * Summaries</a> by Greenwald and Khanna.
   *
   * @param col
   *   the name of the numerical column
   * @param probabilities
   *   a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the
   *   minimum, 0.5 is the median, 1 is the maximum.
   * @param relativeError
   *   The relative target precision to achieve (greater than or equal to 0). If set to zero, the
   *   exact quantiles are computed, which could be very expensive. Note that values greater than
   *   1 are accepted but give the same result as 1.
   * @return
   *   the approximate quantiles at the given probabilities
   *
   * @note
   *   null and NaN values will be removed from the numerical column before calculation. If the
   *   dataframe is empty or the column only contains null or NaN, an empty array is returned.
   *
   * @since 2.0.0
   */
  def approxQuantile(
      col: String,
      probabilities: Array[Double],
      relativeError: Double): Array[Double] = withOrigin {
    approxQuantile(Array(col), probabilities, relativeError).head
  }

  /**
   * Calculates the approximate quantiles of numerical columns of a DataFrame.
   * @see
   *   `approxQuantile(col:Str* approxQuantile)` for detailed description.
   *
   * @param cols
   *   the names of the numerical columns
   * @param probabilities
   *   a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the
   *   minimum, 0.5 is the median, 1 is the maximum.
   * @param relativeError
   *   The relative target precision to achieve (greater than or equal to 0). If set to zero, the
   *   exact quantiles are computed, which could be very expensive. Note that values greater than
   *   1 are accepted but give the same result as 1.
   * @return
   *   the approximate quantiles at the given probabilities of each column
   *
   * @note
   *   null and NaN values will be ignored in numerical columns before calculation. For columns
   *   only containing null or NaN values, an empty array is returned.
   *
   * @since 2.2.0
   */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double): Array[Array[Double]]

  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
   *
   * @param col1
   *   the name of the first column
   * @param col2
   *   the name of the second column
   * @return
   *   the covariance of the two columns.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.cov("rand1", "rand2")
   *    res1: Double = 0.065...
   * }}}
   * @since 1.4.0
   */
  def cov(col1: String, col2: String): Double

  /**
   * Calculates the correlation of two columns of a DataFrame. Currently only supports the Pearson
   * Correlation Coefficient. For Spearman Correlation, consider using RDD methods found in
   * MLlib's Statistics.
   *
   * @param col1
   *   the name of the column
   * @param col2
   *   the name of the column to calculate the correlation against
   * @return
   *   The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2")
   *    res1: Double = 0.613...
   * }}}
   * @since 1.4.0
   */
  def corr(col1: String, col2: String, method: String): Double

  /**
   * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
   *
   * @param col1
   *   the name of the column
   * @param col2
   *   the name of the column to calculate the correlation against
   * @return
   *   The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2", "pearson")
   *    res1: Double = 0.613...
   * }}}
   * @since 1.4.0
   */
  def corr(col1: String, col2: String): Double = {
    corr(col1, col2, "pearson")
  }

  /**
   * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
   * The first column of each row will be the distinct values of `col1` and the column names will
   * be the distinct values of `col2`. The name of the first column will be `col1_col2`. Counts
   * will be returned as `Long`s. Pairs that have no occurrences will have zero as their counts.
   * Null elements will be replaced by "null", and back ticks will be dropped from elements if
   * they exist.
   *
   * @param col1
   *   The name of the first column. Distinct items will make the first item of each row.
   * @param col2
   *   The name of the second column. Distinct items will make the column names of the DataFrame.
   * @return
   *   A DataFrame containing for the contingency table.
   *
   * {{{
   *    val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)))
   *      .toDF("key", "value")
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
  def crosstab(col1: String, col2: String): DataFrame

  /**
   * Finding frequent items for columns, possibly with false positives. Using the frequent element
   * count algorithm described in <a href="https://doi.org/10.1145/762471.762473">here</a>,
   * proposed by Karp, Schenker, and Papadimitriou. The `support` should be greater than 1e-4.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @param support
   *   The minimum frequency for an item to be considered `frequent`. Should be greater than 1e-4.
   * @return
   *   A Local DataFrame with the Array of frequent items for each column.
   *
   * {{{
   *    val rows = Seq.tabulate(100) { i =>
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = spark.createDataFrame(rows).toDF("a", "b")
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
   * @since 1.4.0
   */
  def freqItems(cols: Array[String], support: Double): DataFrame =
    freqItems(cols.toImmutableArraySeq, support)

  /**
   * Finding frequent items for columns, possibly with false positives. Using the frequent element
   * count algorithm described in <a href="https://doi.org/10.1145/762471.762473">here</a>,
   * proposed by Karp, Schenker, and Papadimitriou. Uses a `default` support of 1%.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @return
   *   A Local DataFrame with the Array of frequent items for each column.
   * @since 1.4.0
   */
  def freqItems(cols: Array[String]): DataFrame = freqItems(cols, 0.01)

  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in <a
   * href="https://doi.org/10.1145/762471.762473">here</a>, proposed by Karp, Schenker, and
   * Papadimitriou.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @return
   *   A Local DataFrame with the Array of frequent items for each column.
   *
   * {{{
   *    val rows = Seq.tabulate(100) { i =>
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = spark.createDataFrame(rows).toDF("a", "b")
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
  def freqItems(cols: Seq[String], support: Double): DataFrame

  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in <a
   * href="https://doi.org/10.1145/762471.762473">here</a>, proposed by Karp, Schenker, and
   * Papadimitriou. Uses a `default` support of 1%.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @return
   *   A Local DataFrame with the Array of frequent items for each column.
   * @since 1.4.0
   */
  def freqItems(cols: Seq[String]): DataFrame = freqItems(cols, 0.01)

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not specified, we treat its fraction as
   *   zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   *
   * {{{
   *    val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
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
    sampleBy(Column(col), fractions, seed)
  }

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not specified, we treat its fraction as
   *   zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   *
   * @since 1.5.0
   */
  def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame = {
    sampleBy(col, fractions.asScala.toMap.asInstanceOf[Map[T, Double]], seed)
  }

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not specified, we treat its fraction as
   *   zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   *
   * The stratified sample can be performed over multiple columns:
   * {{{
   *    import org.apache.spark.sql.Row
   *    import org.apache.spark.sql.functions.struct
   *
   *    val df = spark.createDataFrame(Seq(("Bob", 17), ("Alice", 10), ("Nico", 8), ("Bob", 17),
   *      ("Alice", 10))).toDF("name", "age")
   *    val fractions = Map(Row("Alice", 10) -> 0.3, Row("Nico", 8) -> 1.0)
   *    df.stat.sampleBy(struct($"name", $"age"), fractions, 36L).show()
   *    +-----+---+
   *    | name|age|
   *    +-----+---+
   *    | Nico|  8|
   *    |Alice| 10|
   *    +-----+---+
   * }}}
   *
   * @since 3.0.0
   */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame

  /**
   * (Java-specific) Returns a stratified sample without replacement based on the fraction given
   * on each stratum.
   *
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not specified, we treat its fraction as
   *   zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   * @since 3.0.0
   */
  def sampleBy[T](col: Column, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame = {
    sampleBy(col, fractions.asScala.toMap.asInstanceOf[Map[T, Double]], seed)
  }

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param colName
   *   name of the column over which the sketch is built
   * @param depth
   *   depth of the sketch
   * @param width
   *   width of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(colName: String, depth: Int, width: Int, seed: Int): CountMinSketch = {
    countMinSketch(Column(colName), depth, width, seed)
  }

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param colName
   *   name of the column over which the sketch is built
   * @param eps
   *   relative error of the sketch
   * @param confidence
   *   confidence of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(
      colName: String,
      eps: Double,
      confidence: Double,
      seed: Int): CountMinSketch = {
    countMinSketch(Column(colName), eps, confidence, seed)
  }

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param col
   *   the column over which the sketch is built
   * @param depth
   *   depth of the sketch
   * @param width
   *   width of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(col: Column, depth: Int, width: Int, seed: Int): CountMinSketch = {
    val eps = 2.0 / width
    val confidence = 1 - 1 / Math.pow(2, depth)
    countMinSketch(col, eps, confidence, seed)
  }

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param col
   *   the column over which the sketch is built
   * @param eps
   *   relative error of the sketch
   * @param confidence
   *   confidence of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(col: Column, eps: Double, confidence: Double, seed: Int): CountMinSketch =
    withOrigin {
      val cms = count_min_sketch(col, lit(eps), lit(confidence), lit(seed))
      val bytes: Array[Byte] = df.select(cms).as(BinaryEncoder).head()
      CountMinSketch.readFrom(bytes)
    }

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param colName
   *   name of the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param fpp
   *   expected false positive probability of the filter.
   * @since 2.0.0
   */
  def bloomFilter(colName: String, expectedNumItems: Long, fpp: Double): BloomFilter = {
    bloomFilter(Column(colName), expectedNumItems, fpp)
  }

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param col
   *   the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param fpp
   *   expected false positive probability of the filter.
   * @since 2.0.0
   */
  def bloomFilter(col: Column, expectedNumItems: Long, fpp: Double): BloomFilter = {
    val numBits = BloomFilter.optimalNumOfBits(expectedNumItems, fpp)
    bloomFilter(col, expectedNumItems, numBits)
  }

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param colName
   *   name of the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param numBits
   *   expected number of bits of the filter.
   * @since 2.0.0
   */
  def bloomFilter(colName: String, expectedNumItems: Long, numBits: Long): BloomFilter = {
    bloomFilter(Column(colName), expectedNumItems, numBits)
  }

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param col
   *   the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param numBits
   *   expected number of bits of the filter.
   * @since 2.0.0
   */
  def bloomFilter(col: Column, expectedNumItems: Long, numBits: Long): BloomFilter = withOrigin {
    val bf = Column.internalFn("bloom_filter_agg", col, lit(expectedNumItems), lit(numBits))
    val bytes: Array[Byte] = df.select(bf).as(BinaryEncoder).head()
    BloomFilter.readFrom(bytes)
  }
}
