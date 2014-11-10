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

package org.apache.spark.ml.classification

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{BLAS, Vector, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends Params
  with HasRegParam with HasMaxIter with HasLabelCol with HasThreshold with HasFeaturesCol
  with HasScoreCol with HasPredictionCol

/**
 * Logistic regression.
 */
class LogisticRegression extends Estimator[LogisticRegressionModel] with LogisticRegressionParams {

  setRegParam(0.1)
  setMaxIter(100)
  setThreshold(0.5)

  def setRegParam(value: Double): this.type = { set(regParam, value); this }
  def setMaxIter(value: Int): this.type = { set(maxIter, value); this }
  def setLabelCol(value: String): this.type = { set(labelCol, value); this }
  def setThreshold(value: Double): this.type = { set(threshold, value); this }
  def setFeaturesCol(value: String): this.type = { set(featuresCol, value); this }
  def setScoreCol(value: String): this.type = { set(scoreCol, value); this }
  def setPredictionCol(value: String): this.type = { set(predictionCol, value); this }

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): LogisticRegressionModel = {
    transform(dataset.schema, paramMap, logging = true)
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val instances = dataset.select(map(labelCol).attr, map(featuresCol).attr)
      .map { case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
      }.persist(StorageLevel.MEMORY_AND_DISK)
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(map(regParam))
      .setNumIterations(map(maxIter))
    val lrm = new LogisticRegressionModel(this, map, lr.run(instances).weights)
    instances.unpersist()
    // copy model params
    Params.copyValues(this, lrm)
    lrm
  }

  override def transform(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val featuresType = schema(map(featuresCol)).dataType
    // TODO: Support casting Array[Double] and Array[Float] to Vector.
    require(featuresType.isInstanceOf[VectorUDT],
      s"Features column ${map(featuresCol)} must be a vector column but got $featuresType.")
    val labelType = schema(map(labelCol)).dataType
    require(labelType == DoubleType,
      s"Cannot convert label column ${map(labelCol)} of type $labelType to a double column.")
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(map(scoreCol)), s"Score column ${map(scoreCol)} already exists.")
    require(!fieldNames.contains(map(predictionCol)),
      s"Prediction column ${map(predictionCol)} already exists.")
    val outputFields = schema.fields ++ Seq(
      StructField(map(scoreCol), DoubleType, false),
      StructField(map(predictionCol), DoubleType, false))
    StructType(outputFields)
  }
}

/**
 * Model produced by [[LogisticRegression]].
 */
class LogisticRegressionModel private[ml] (
    override val parent: LogisticRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector) extends Model with LogisticRegressionParams {

  def setThreshold(value: Double): this.type = { set(threshold, value); this }
  def setFeaturesCol(value: String): this.type = { set(featuresCol, value); this }
  def setScoreCol(value: String): this.type = { set(scoreCol, value); this }
  def setPredictionCol(value: String): this.type = { set(predictionCol, value); this }

  override def transform(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val featuresType = schema(map(featuresCol)).dataType
    // TODO: Support casting Array[Double] and Array[Float] to Vector.
    require(featuresType.isInstanceOf[VectorUDT],
      s"Features column ${map(featuresCol)} must be a vector column but got $featuresType.")
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(map(scoreCol)), s"Score column ${map(scoreCol)} already exists.")
    require(!fieldNames.contains(map(predictionCol)),
      s"Prediction column ${map(predictionCol)} already exists.")
    val outputFields = schema.fields ++ Seq(
      StructField(map(scoreCol), DoubleType, false),
      StructField(map(predictionCol), DoubleType, false))
    StructType(outputFields)
  }

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    transform(dataset.schema, paramMap, logging = true)
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val score: Vector => Double = (v) => {
      val margin = BLAS.dot(v, weights)
      1.0 / (1.0 + math.exp(-margin))
    }
    val t = map(threshold)
    val predict: Vector => Double = (v) => {
      if (score(v) > t) 1.0 else 0.0
    }
    dataset.select(
      Star(None),
      score.call(map(featuresCol).attr) as map(scoreCol),
      predict.call(map(featuresCol).attr) as map(predictionCol))
  }
}
