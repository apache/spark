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

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
 * Model for ARIMA(p, d, q).
 *
 * Current implementation is ARIMA(p, d, 0) in Scala.
 *
 * Stores:
 *   - phi: AR coefficients (length p)
 *   - intercept: constant term
 * The differencing order d and p/q are read from [[ArimaParams]].
 */
class ArimaRegressionModel(
    override val uid: String,
    val phi: Array[Double],
    val intercept: Double)
  extends Model[ArimaRegressionModel]
    with ArimaParams
    with MLWritable {

  override def copy(extra: ParamMap): ArimaRegressionModel = {
    val copied = new ArimaRegressionModel(uid, phi.clone(), intercept)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val schema = dataset.schema

    require(schema.fieldNames.contains("y"),
      "Input dataset must contain 'y' column of DoubleType.")
    require(schema("y").dataType == DoubleType,
      "Column 'y' must be of type DoubleType.")

    // Collect full series on the driver.
    // NOTE: This matches the Estimator's single-series assumption.
    val rows: Array[Row] = dataset.collect()
    val series: Array[Double] = rows.map(_.getAs[Double]("y"))

    val preds: Array[Double] = ArimaRegressionModel.forecastInSample(
      series,
      p = getP,
      d = getD,
      phi = phi,
      intercept = intercept)

    val spark = dataset.sparkSession
    val newSchema = transformSchema(schema)

    val withPred: Array[Row] = rows.zip(preds).map { case (row, pred) =>
      Row.fromSeq(row.toSeq :+ pred.asInstanceOf[Any])
    }

    val rdd = spark.sparkContext.parallelize(withPred, dataset.rdd.getNumPartitions)
    spark.createDataFrame(rdd, newSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains("prediction")) schema
    else schema.add(StructField("prediction", DoubleType, nullable = true))
  }

  override def write: MLWriter = new DefaultParamsWriter(this) {}
}

object ArimaRegressionModel extends DefaultParamsReadable[ArimaRegressionModel] {

  /**
   * In-sample one-step-ahead forecasts for the given series.
   *
   * - original: level series (before differencing)
   * - p, d: AR and differencing orders
   * - phi: AR coefficients (length p)
   * - intercept: constant term
   *
   * For d > 0, we:
   *   1. Difference the series.
   *   2. Fit AR model in the differenced domain.
   *   3. Approximate level forecasts by cumulatively summing predicted differences.
   */
  private[ml] def forecastInSample(
      original: Array[Double],
      p: Int,
      d: Int,
      phi: Array[Double],
      intercept: Double): Array[Double] = {

    val yDiff = ArimaRegression.difference(original, d)
    val n = yDiff.length

    val predsDiff = new Array[Double](n)

    if (p == 0) {
      var t = 0
      while (t < n) {
        predsDiff(t) = intercept
        t += 1
      }
    } else {
      var t = 0
      while (t < n) {
        var arPart = 0.0
        var lag = 1
        while (lag <= p) {
          val idx = t - lag
          if (idx >= 0) {
            arPart += phi(lag - 1) * yDiff(idx)
          }
          lag += 1
        }
        val yHat = intercept + arPart
        predsDiff(t) = yHat
        t += 1
      }
    }

    val preds = new Array[Double](original.length)

    if (d == 0) {
      System.arraycopy(predsDiff, 0, preds, 0, math.min(predsDiff.length, preds.length))
    } else {
      preds(0) = original(0)
      var i = 1
      while (i < original.length) {
        val idx = math.min(i - 1, predsDiff.length - 1)
        preds(i) = preds(i - 1) + predsDiff(idx)
        i += 1
      }
    }

    preds
  }
}
