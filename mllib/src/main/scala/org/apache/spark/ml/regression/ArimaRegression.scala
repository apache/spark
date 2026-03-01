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
package org.apache.spark.ml.regression

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, inv => breezeInv}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Shared parameters are defined in [[ArimaParams]] (p, d, q).
 */
private[regression] case class ArimaCoefficients(
    phi: Array[Double],
    intercept: Double)

/**
 * ARIMA(p, d, q) estimator for univariate time series.
 *
 * Current Scala implementation supports q = 0 (i.e., ARIMA(p, d, 0)).
 * The MA component will be added in a follow-up change.
 *
 * Input column:  "y" (DoubleType)
 * Output column: "prediction" (DoubleType)
 */
class ArimaRegression(override val uid: String)
  extends Estimator[ArimaRegressionModel]
    with ArimaParams
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("arimaReg"))

  def setP(value: Int): this.type = set(p, value)
  def setD(value: Int): this.type = set(d, value)
  def setQ(value: Int): this.type = set(q, value)

  override def fit(dataset: Dataset[_]): ArimaRegressionModel = {
    val schema = dataset.schema

    require(schema.fieldNames.contains("y"),
      "Input dataset must contain a 'y' column of DoubleType representing the time series values.")
    require(schema("y").dataType == DoubleType,
      "Column 'y' must be of type DoubleType.")

    // Collect univariate series on the driver.
    // NOTE: This fits one global series; multi-series scalability
    //       can be added later via groupByKey/mapGroups.
    val series: Array[Double] = dataset
      .select(col("y").cast(DoubleType))
      .collect()
      .map(_.getDouble(0))

    val pValue = getP
    val dValue = getD
    val qValue = getQ

    val coeffs = ArimaRegression.fitSingleSeries(series, pValue, dValue, qValue)

    new ArimaRegressionModel(uid, coeffs.phi, coeffs.intercept)
      .setParent(this)
      .set(p, pValue)
      .set(d, dValue)
      .set(q, qValue)
  }

  override def copy(extra: ParamMap): ArimaRegression = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains("y"),
      "Input schema must contain 'y' column of DoubleType.")
    require(schema("y").dataType == DoubleType,
      "Column 'y' must be of type DoubleType.")

    if (schema.fieldNames.contains("prediction")) schema
    else schema.add(StructField("prediction", DoubleType, nullable = true))
  }
}

object ArimaRegression extends DefaultParamsReadable[ArimaRegression] {

  /**
   * Apply d-th order differencing to a series. Each differencing reduces the length by 1.
   */
  private[ml] def difference(series: Array[Double], d: Int): Array[Double] = {
    if (d <= 0) {
      series
    } else {
      var result = series
      var order = 0
      while (order < d) {
        val next = new Array[Double](result.length - 1)
        var i = 1
        while (i < result.length) {
          next(i - 1) = result(i) - result(i - 1)
          i += 1
        }
        result = next
        order += 1
      }
      result
    }
  }

  /**
   * Fit ARIMA(p, d, q) for a single univariate series.
   *
   * Current implementation supports q = 0 (ARIMA(p, d, 0)).
   * For q > 0, an UnsupportedOperationException is thrown.
   */
  private[ml] def fitSingleSeries(
      original: Array[Double],
      p: Int,
      d: Int,
      q: Int): ArimaCoefficients = {

    require(p >= 0 && d >= 0 && q >= 0,
      s"ARIMA(p,d,q) parameters must be non-negative, got ($p,$d,$q).")

    if (original.length <= math.max(2, p + d + q)) {
      throw new IllegalArgumentException(
        s"Time series too short for ARIMA($p,$d,$q): length = ${original.length}")
    }

    if (q > 0) {
      // First Scala version: focus on ARIMA(p, d, 0).
      throw new UnsupportedOperationException(
        "Current Scala implementation supports q = 0 only. " +
          "Moving-average terms will be added in a follow-up patch.")
    }

    // Differencing
    val yDiff = difference(original, d)
    val n = yDiff.length

    // Sample mean of differenced series
    var sum = 0.0
    var i = 0
    while (i < n) {
      sum += yDiff(i)
      i += 1
    }
    val mean = sum / n

    if (p == 0) {
      // White-noise with mean; no AR coefficients.
      ArimaCoefficients(phi = Array.empty[Double], intercept = mean)
    } else {
      // Yule–Walker equations for AR(p)
      val maxLag = p
      val gamma = new Array[Double](maxLag + 1)

      var lag = 0
      while (lag <= maxLag) {
        var s = 0.0
        var t = lag
        while (t < n) {
          s += (yDiff(t) - mean) * (yDiff(t - lag) - mean)
          t += 1
        }
        gamma(lag) = s / n
        lag += 1
      }

      // Toeplitz covariance matrix and RHS
      val G = BDM.tabulate[Double](p, p) { (row, col) =>
        gamma(math.abs(row - col))
      }
      val g = BDV(gamma.slice(1, p + 1))

      val phiVec = breezeInv(G) * g
      val phi = phiVec.toArray

      // Intercept chosen so that model's unconditional mean matches sample mean:
      // E[y_t] = c / (1 - sum(phi))  =>  c = mean * (1 - sum(phi))
      val sumPhi = phi.sum
      val intercept = mean * (1.0 - sumPhi)

      ArimaCoefficients(phi = phi, intercept = intercept)
    }
  }

  override def load(path: String): ArimaRegression = super.load(path)
}
