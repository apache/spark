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
import org.apache.spark.ml.param._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel


/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasMaxIter with HasThreshold


/**
 * :: AlphaComponent ::
 *
 * Logistic regression.
 * Currently, this class only supports binary classification.
 */
@AlphaComponent
class LogisticRegression
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams {

  setRegParam(0.1)
  setMaxIter(100)
  setThreshold(0.5)

  def setRegParam(value: Double): this.type = set(regParam, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  def setThreshold(value: Double): this.type = set(threshold, value)

  override protected def train(dataset: DataFrame, paramMap: ParamMap): LogisticRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val oldDataset = extractLabeledPoints(dataset, paramMap)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // Train model
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(paramMap(regParam))
      .setNumIterations(paramMap(maxIter))
    val oldModel = lr.run(oldDataset)
    val lrm = new LogisticRegressionModel(this, paramMap, oldModel.weights, oldModel.intercept)

    if (handlePersistence) {
      oldDataset.unpersist()
    }
    lrm
  }
}


/**
 * :: AlphaComponent ::
 *
 * Model produced by [[LogisticRegression]].
 */
@AlphaComponent
class LogisticRegressionModel private[ml] (
    override val parent: LogisticRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
  with LogisticRegressionParams {

  setThreshold(0.5)

  def setThreshold(value: Double): this.type = set(threshold, value)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, weights) + intercept
  }

  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override val numClasses: Int = 2

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[threshold]].
   */
  override protected def predict(features: Vector): Double = {
    if (score(features) > paramMap(threshold)) 1 else 0
  }

  override protected def predictProbabilities(features: Vector): Vector = {
    val s = score(features)
    Vectors.dense(1.0 - s, s)
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(0.0, m)
  }

  override protected def copy(): LogisticRegressionModel = {
    val m = new LogisticRegressionModel(parent, fittingParamMap, weights, intercept)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
