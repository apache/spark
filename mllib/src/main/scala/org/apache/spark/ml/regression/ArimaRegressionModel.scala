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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class ArimaRegressionModel(override val uid: String)
  extends Model[ArimaRegressionModel]
    with ArimaParams
    with MLWritable {

  private var fittedData: DataFrame = _
  def setFittedData(df: DataFrame): this.type = { this.fittedData = df; this }

  override def copy(extra: ParamMap): ArimaRegressionModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    require(fittedData != null, "ARIMA model not fitted.")
    fittedData
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add("prediction", org.apache.spark.sql.types.DoubleType, nullable = true)
  }

  override def write: MLWriter = new DefaultParamsWriter(this)
}

object ArimaRegressionModel extends DefaultParamsReadable[ArimaRegressionModel]
