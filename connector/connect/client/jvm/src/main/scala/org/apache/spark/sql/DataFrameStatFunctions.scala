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

import org.apache.spark.connect.proto.{Relation, StatSampleBy}
import org.apache.spark.sql.DataFrameStatFunctions.approxQuantileResultEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, BinaryEncoder, PrimitiveDoubleEncoder}
import org.apache.spark.sql.functions.lit

/**
 * Statistic functions for `DataFrame`s.
 *
 * @since 3.4.0
 */
final class DataFrameStatFunctions private[sql] (protected val df: DataFrame)
  extends api.DataFrameStatFunctions[DataFrame] {
  private def root: Relation = df.plan.getRoot
  private val sparkSession: SparkSession = df.sparkSession

  /** @inheritdoc */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double): Array[Array[Double]] = {
    require(
      probabilities.forall(p => p >= 0.0 && p <= 1.0),
      "percentile should be in the range [0.0, 1.0]")
    require(relativeError >= 0, s"Relative Error must be non-negative but got $relativeError")
    sparkSession
      .newDataset(approxQuantileResultEncoder) { builder =>
        val approxQuantileBuilder = builder.getApproxQuantileBuilder
          .setInput(root)
          .setRelativeError(relativeError)
        cols.foreach(approxQuantileBuilder.addCols)
        probabilities.foreach(approxQuantileBuilder.addProbabilities)
      }
      .head()
  }

  /** @inheritdoc */
  def cov(col1: String, col2: String): Double = {
    sparkSession
      .newDataset(PrimitiveDoubleEncoder) { builder =>
        builder.getCovBuilder.setInput(root).setCol1(col1).setCol2(col2)
      }
      .head()
  }

  /** @inheritdoc */
  def corr(col1: String, col2: String, method: String): Double = {
    require(
      method == "pearson",
      "Currently only the calculation of the Pearson Correlation " +
        "coefficient is supported.")
    sparkSession
      .newDataset(PrimitiveDoubleEncoder) { builder =>
        builder.getCorrBuilder.setInput(root).setCol1(col1).setCol2(col2)
      }
      .head()
  }

  /** @inheritdoc */
  def crosstab(col1: String, col2: String): DataFrame = {
    sparkSession.newDataFrame { builder =>
      builder.getCrosstabBuilder.setInput(root).setCol1(col1).setCol2(col2)
    }
  }

  /** @inheritdoc */
  override def freqItems(cols: Array[String], support: Double): DataFrame =
    super.freqItems(cols, support)

  /** @inheritdoc */
  override def freqItems(cols: Array[String]): DataFrame = super.freqItems(cols)

  /** @inheritdoc */
  override def freqItems(cols: Seq[String]): DataFrame = super.freqItems(cols)

  /** @inheritdoc */
  def freqItems(cols: Seq[String], support: Double): DataFrame = {
    df.sparkSession.newDataFrame { builder =>
      val freqItemsBuilder = builder.getFreqItemsBuilder
        .setInput(df.plan.getRoot)
        .setSupport(support)
      cols.foreach(freqItemsBuilder.addCols)
    }
  }

  /** @inheritdoc */
  override def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame =
    super.sampleBy(col, fractions, seed)

  /** @inheritdoc */
  override def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame =
    super.sampleBy(col, fractions, seed)

  /** @inheritdoc */
  override def sampleBy[T](col: Column, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame =
    super.sampleBy(col, fractions, seed)

  /** @inheritdoc */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame = {
    import sparkSession.RichColumn
    require(
      fractions.values.forall(p => p >= 0.0 && p <= 1.0),
      s"Fractions must be in [0, 1], but got $fractions.")
    sparkSession.newDataFrame { builder =>
      val sampleByBuilder = builder.getSampleByBuilder
        .setInput(root)
        .setCol(col.expr)
        .setSeed(seed)
      fractions.foreach { case (k, v) =>
        sampleByBuilder.addFractions(
          StatSampleBy.Fraction
            .newBuilder()
            .setStratum(lit(k).expr.getLiteral)
            .setFraction(v))
      }
    }
  }

  override protected def executeAgg(c: Column): Array[Byte] = df.select(c).as(BinaryEncoder).head()
}

private object DataFrameStatFunctions {
  private val approxQuantileResultEncoder: ArrayEncoder[Array[Double]] =
    ArrayEncoder(ArrayEncoder(PrimitiveDoubleEncoder, containsNull = false), containsNull = false)
}
