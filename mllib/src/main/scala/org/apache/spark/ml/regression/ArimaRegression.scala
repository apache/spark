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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * ARIMA (AutoRegressive Integrated Moving Average) model implementation
 * for univariate time series forecasting.
 *
 * This implementation leverages PySpark Pandas UDF with statsmodels to
 * fit ARIMA(p, d, q) models in a distributed fashion.
 *
 * Input column: "y" (DoubleType)
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

  /**
   * Fits an ARIMA model using Python statsmodels via Pandas UDF.
   * The UDF runs ARIMA(p,d,q) on each time series partition or entire dataset.
   */
  override def fit(dataset: Dataset[_]): ArimaRegressionModel = {
    val spark = dataset.sparkSession
    import spark.implicits._

    require(dataset.columns.contains("y"),
      "Input dataset must contain a 'y' column of DoubleType representing the time series values.")

    // Define the ARIMA Pandas UDF (Python side using statsmodels)
    val udfScript =
      s"""
      from pyspark.sql.functions import pandas_udf
      from pyspark.sql.types import DoubleType
      import pandas as pd
      from statsmodels.tsa.arima.model import ARIMA

      @pandas_udf("double")
      def arima_forecast_udf(y: pd.Series) -> pd.Series:
          try:
              model = ARIMA(y, order=(${getOrDefault(p)}, ${getOrDefault(d)}, ${getOrDefault(q)}))
              fitted = model.fit()
              forecast = fitted.forecast(steps=1)
              return pd.Series([forecast.iloc[0]] * len(y))
          except Exception:
              return pd.Series([float('nan')] * len(y))
      """

    // Register the UDF dynamically
    spark.udf.registerPython("arima_forecast_udf", udfScript)

    // Apply the ARIMA forecast UDF
    val predicted = dataset.withColumn("prediction", call_udf("arima_forecast_udf", col("y")))

    // Create the model instance
    val model = new ArimaRegressionModel(uid)
      .setParent(this)
      .setP($(p))
      .setD($(d))
      .setQ($(q))
      .setFittedData(predicted)

    model
  }

  override def copy(extra: ParamMap): ArimaRegression = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains("y"),
      "Input schema must contain 'y' column of DoubleType.")
    StructType(schema.fields :+ StructField("prediction", DoubleType, nullable = true))
  }
}

object ArimaRegression extends DefaultParamsReadable[ArimaRegression] {
  override def load(path: String): ArimaRegression = super.load(path)
}
