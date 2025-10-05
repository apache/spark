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

import org.apache.spark.ml._
import org.apache.spark.ml.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ArimaRegressionModel(override val uid: String)
  extends Model[ArimaRegressionModel]
    with ArimaParams
    with MLWritable {

  override def copy(extra: ParamMap): ArimaRegressionModel = {
    val copied = new ArimaRegressionModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Dummy prediction logic — just copy y as prediction
    dataset.withColumn("prediction", col("y"))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField("prediction", DoubleType, false))
  }
}

object ArimaRegressionModel extends MLReadable[ArimaRegressionModel] {
  override def read: MLReader[ArimaRegressionModel] = new DefaultParamsReader[ArimaRegressionModel]
  override def load(path: String): ArimaRegressionModel = super.load(path)
}
