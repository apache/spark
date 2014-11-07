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
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * Logistic regression (example).
 */
class LogisticRegression extends Estimator[LogisticRegressionModel]
    with HasRegParam with HasMaxIter with HasLabelCol with HasFeaturesCol {

  setRegParam(0.1)
  setMaxIter(100)

  def setRegParam(value: Double): this.type = { set(regParam, value); this }
  def setMaxIter(value: Int): this.type = { set(maxIter, value); this }
  def setLabelCol(value: String): this.type = { set(labelCol, value); this }
  def setFeaturesCol(value: String): this.type = { set(featuresCol, value); this }

  override final val modelParams: LogisticRegressionModelParams =
    new LogisticRegressionModelParams {}

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): LogisticRegressionModel = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    import map.implicitMapping
    val instances = dataset.select((labelCol: String).attr, (featuresCol: String).attr)
      .map { case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
      }.cache()
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(regParam)
      .setNumIterations(maxIter)
    val lrm = new LogisticRegressionModel(lr.run(instances).weights)
    instances.unpersist()
    // copy model params
    Params.copyValues(modelParams, lrm)
    if (!lrm.isSet(lrm.featuresCol) && map.contains(lrm.featuresCol)) {
      lrm.setFeaturesCol(featuresCol)
    }
    lrm
  }

  /**
   * Validates parameters specified by the input parameter map.
   * Raises an exception if any parameter belongs to this object is invalid.
   */
  override def validate(paramMap: ParamMap): Unit = {
    super.validate(paramMap)
  }
}

trait LogisticRegressionModelParams extends Params with HasThreshold with HasFeaturesCol
    with HasScoreCol {
  def setThreshold(value: Double): this.type = { set(threshold, value); this }
  def setFeaturesCol(value: String): this.type = { set(featuresCol, value); this }
  def setScoreCol(value: String): this.type = { set(scoreCol, value); this }
}

class LogisticRegressionModel(
    val weights: Vector)
    extends Model with LogisticRegressionModelParams {

  setThreshold(0.5)

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    println(s"transform called with $map")
    import map.implicitMapping
    val score: Vector => Double = (v) => {
      val margin = BLAS.dot(v, weights)
      1.0 / (1.0 + math.exp(-margin))
    }
    val thres: Double = threshold
    val predict: Vector => Double = (v) => {
      if (score(v) > thres) 1.0 else 0.0
    }
    dataset.select(
      Star(None),
      score.call((featuresCol: String).attr) as scoreCol,
      predict.call((featuresCol: String).attr) as 'prediction)
  }
}
