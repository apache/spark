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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.impl.estimator.ProbabilisticClassificationModel
import org.apache.spark.ml.param._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{Vectors, BLAS, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Dsl._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ClassifierParams
  with HasRegParam with HasMaxIter with HasThreshold with HasScoreCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      paramMap: ParamMap,
      fitting: Boolean): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, paramMap, fitting)
    val map = this.paramMap ++ paramMap
    val fieldNames = parentSchema.fieldNames
    require(!fieldNames.contains(map(scoreCol)), s"Score column ${map(scoreCol)} already exists.")
    val outputFields = parentSchema.fields ++ Seq(
      StructField(map(scoreCol), DoubleType, nullable = false))
    StructType(outputFields)
  }
}


/**
 * :: AlphaComponent ::
 * Logistic regression.
 * Currently, this class only supports binary classification.
 */
@AlphaComponent
class LogisticRegression extends Classifier[LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams {

  setRegParam(0.1)
  setMaxIter(100)
  setThreshold(0.5)

  def setRegParam(value: Double): this.type = set(regParam, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  def setThreshold(value: Double): this.type = set(threshold, value)
  def setScoreCol(value: String): this.type = set(scoreCol, value)

  /**
   * Same as [[fit()]], but using strong types.
   *
   * @param dataset  Training data.  WARNING: This does not yet handle instance weights.
   * @param paramMap  Parameters for training.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   */
  def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): LogisticRegressionModel = {
    val oldDataset = dataset.map { case LabeledPoint(label: Double, features: Vector, weight) =>
      org.apache.spark.mllib.regression.LabeledPoint(label, features)
    }
    // If dataset is persisted, do not persist oldDataset.
    val handlePersistence = dataset.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(paramMap(regParam))
      .setNumIterations(paramMap(maxIter))
    val model = lr.run(oldDataset)
    val lrm = new LogisticRegressionModel(this, paramMap, model.weights, model.intercept)
    if (handlePersistence) {
      oldDataset.unpersist()
    }
    lrm.setThreshold(paramMap(threshold))
    lrm
  }
}


/**
 * :: AlphaComponent ::
 * Model produced by [[LogisticRegression]].
 */
@AlphaComponent
class LogisticRegressionModel private[ml] (
    override val parent: LogisticRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends ClassificationModel[LogisticRegressionModel]
  with ProbabilisticClassificationModel
  with LogisticRegressionParams {

  setThreshold(0.5)

  def setThreshold(value: Double): this.type = {
    this.threshold_internal = value
    set(threshold, value)
  }
  def setScoreCol(value: String): this.type = set(scoreCol, value)

  /**
   * Store for faster test-time prediction.
   * Initialized to threshold in fittingParamMap if exists, else default threshold.
   */
  private var threshold_internal: Double = fittingParamMap.get(threshold).getOrElse(getThreshold)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, weights) + intercept
  }

  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val scoreFunction = udf { v: Vector =>
      val margin = BLAS.dot(v, weights)
      1.0 / (1.0 + math.exp(-margin))
    val t = threshold_internal
    val predictFunction: Double => Double = (score) => { if (score > t) 1.0 else 0.0 }
    dataset
      .select($"*", scoreFunction(col(map(featuresCol))).as(map(scoreCol)))
      .select($"*", predictFunction(col(map(scoreCol))).as(map(predictionCol)))
  }

  override val numClasses: Int = 2

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[threshold]].
   */
  override def predict(features: Vector): Double = {
    if (score(features) > threshold_internal) 1 else 0
  }

  override def predictProbabilities(features: Vector): Vector = {
    val s = score(features)
    Vectors.dense(Array(1.0 - s, s))
  }

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(Array(-m, m))
  }
}
