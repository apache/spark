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
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasMaxIter with HasFitIntercept with HasThreshold {

  setDefault(regParam -> 0.1, maxIter -> 100, threshold -> 0.5)
}

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

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  override protected def train(dataset: DataFrame): LogisticRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val oldDataset = extractLabeledPoints(dataset)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // Train model
    val lr = new LogisticRegressionWithLBFGS()
      .setIntercept($(fitIntercept))
    lr.optimizer
      .setRegParam($(regParam))
      .setNumIterations($(maxIter))
    val oldModel = lr.run(oldDataset)
    val lrm = new LogisticRegressionModel(this, oldModel.weights, oldModel.intercept)

    if (handlePersistence) {
      oldDataset.unpersist()
    }
    copyValues(lrm)
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
    val weights: Vector,
    val intercept: Double)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
  with LogisticRegressionParams {

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, weights) + intercept
  }

  /** Score (probability) for class label 1.  For binary classification only. */
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
    if (score(features) > getThreshold) 1 else 0
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        while (i < dv.size) {
          dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override def copy(extra: ParamMap): LogisticRegressionModel = {
    copyValues(new LogisticRegressionModel(parent, weights, intercept), extra)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    val t = getThreshold
    val rawThreshold = if (t == 0.0) {
      Double.NegativeInfinity
    } else if (t == 1.0) {
      Double.PositiveInfinity
    } else {
      Math.log(t / (1.0 - t))
    }
    if (rawPrediction(1) > rawThreshold) 1 else 0
  }

  override protected def probability2prediction(probability: Vector): Double = {
    if (probability(1) > getThreshold) 1 else 0
  }
}
