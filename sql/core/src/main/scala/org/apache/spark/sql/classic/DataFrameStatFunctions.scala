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

package org.apache.spark.sql.classic

import java.{lang => jl, util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.stat._
import org.apache.spark.sql.functions.col
import org.apache.spark.util.ArrayImplicits._

/**
 * Statistic functions for `DataFrame`s.
 *
 * @since 1.4.0
 */
@Stable
final class DataFrameStatFunctions private[sql](protected val df: DataFrame)
  extends sql.DataFrameStatFunctions {

  /** @inheritdoc */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double): Array[Array[Double]] = withOrigin {
    StatFunctions.multipleApproxQuantiles(
      df.select(cols.map(col).toImmutableArraySeq: _*),
      cols.toImmutableArraySeq,
      probabilities.toImmutableArraySeq,
      relativeError).map(_.toArray).toArray
  }

  /**
   * Python-friendly version of [[approxQuantile()]]
   */
  private[spark] def approxQuantile(
      cols: List[String],
      probabilities: List[Double],
      relativeError: Double): java.util.List[java.util.List[Double]] = {
    approxQuantile(cols.toArray, probabilities.toArray, relativeError)
      .map(_.toList.asJava).toList.asJava
  }

  /** @inheritdoc */
  def cov(col1: String, col2: String): Double = withOrigin {
    StatFunctions.calculateCov(df, Seq(col1, col2))
  }

  /** @inheritdoc */
  def corr(col1: String, col2: String, method: String): Double = withOrigin {
    require(method == "pearson", "Currently only the calculation of the Pearson Correlation " +
      "coefficient is supported.")
    StatFunctions.pearsonCorrelation(df, Seq(col1, col2))
  }

  /** @inheritdoc */
  def crosstab(col1: String, col2: String): DataFrame = withOrigin {
    StatFunctions.crossTabulate(df, col1, col2)
  }

  /** @inheritdoc */
  def freqItems(cols: Seq[String], support: Double): DataFrame = withOrigin {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /** @inheritdoc */
  override def freqItems(cols: Array[String], support: Double): DataFrame =
    super.freqItems(cols, support)

  /** @inheritdoc */
  override def freqItems(cols: Array[String]): DataFrame = super.freqItems(cols)

  /** @inheritdoc */
  override def freqItems(cols: Seq[String]): DataFrame = super.freqItems(cols)

  /** @inheritdoc */
  override def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame = {
    super.sampleBy(col, fractions, seed)
  }

  /** @inheritdoc */
  override def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame = {
    super.sampleBy(col, fractions, seed)
  }

  /** @inheritdoc */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame = withOrigin {
    require(fractions.values.forall(p => p >= 0.0 && p <= 1.0),
      s"Fractions must be in [0, 1], but got $fractions.")
    import org.apache.spark.sql.functions.{rand, udf}
    val r = rand(seed)
    val f = udf { (stratum: Any, x: Double) =>
      x < fractions.getOrElse(stratum.asInstanceOf[T], 0.0)
    }
    df.filter(f(col, r))
  }

  /** @inheritdoc */
  override def sampleBy[T](col: Column, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame = {
    super.sampleBy(col, fractions, seed)
  }
}
