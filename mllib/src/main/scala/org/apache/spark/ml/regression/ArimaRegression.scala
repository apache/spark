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

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ArimaRegression(override val uid: String)
  extends Estimator[ArimaRegressionModel]
    with ArimaParams
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("arimaReg"))

  def setP(value: Int): this.type = set(p, value)
  def setD(value: Int): this.type = set(d, value)
  def setQ(value: Int): this.type = set(q, value)

  override def fit(dataset: Dataset[_]): ArimaRegressionModel = {
    // Dummy: assumes data is ordered with one feature column "y"
    val ts = dataset.select("y").rdd.map(_.getDouble(0)).collect()

    // [TO DO]: Replace with actual ARIMA fitting logic
    val model = new ArimaRegressionModel(uid)
      .setParent(this)
    model
  }

  override def copy(extra: ParamMap): ArimaRegression = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains("y"), "Dataset must contain 'y' column.")
    schema.add(StructField("prediction", DoubleType, false))
  }
}

object ArimaRegression extends DefaultParamsReadable[ArimaRegression] {
  override def load(path: String): ArimaRegression = super.load(path)
}
