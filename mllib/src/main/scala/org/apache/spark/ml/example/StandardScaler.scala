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

package org.apache.spark.ml.example

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Row

class StandardScaler extends Estimator[StandardScalerModel] with HasInputCol {

  def setInputCol(value: String): this.type = { set(inputCol, value); this }

  override val modelParams: StandardScalerModelParams = new StandardScalerModelParams {}

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): StandardScalerModel = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    import map.implicitMapping
    val input = dataset.select((inputCol: String).attr)
      .map { case Row(v: Vector) =>
        v
      }
    val scaler = new feature.StandardScaler().fit(input)
    val model = new StandardScalerModel(scaler)
    Params.copyValues(modelParams, model)
    if (!model.paramMap.contains(model.inputCol)) {
      model.setInputCol(inputCol)
    }
    model
  }
}

trait StandardScalerModelParams extends Params with HasInputCol with HasOutputCol {
  def setInputCol(value: String): this.type = { set(inputCol, value); this }
  def setOutputCol(value: String): this.type = { set(outputCol, value); this }
}

class StandardScalerModel private[ml] (
    scaler: feature.StandardScalerModel) extends Model with StandardScalerModelParams {

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    import map.implicitMapping
    val scale: (Vector) => Vector = (v) => {
      scaler.transform(v)
    }
    dataset.select(Star(None), scale.call((inputCol: String).attr) as outputCol)
  }
}
