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
import org.apache.spark.mllib.linalg.{BLAS, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, weights) + intercept
  }

  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // This is overridden (a) to be more efficient (avoiding re-computing values when creating
    // multiple output columns) and (b) to handle threshold, which the abstractions do not use.
    // TODO: We should abstract away the steps defined by UDFs below so that the abstractions
    // can call whichever UDFs are needed to create the output columns.

    // Check schema
    transformSchema(dataset.schema, logging = true)

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    //   rawPrediction (-margin, margin)
    //   probability (1.0-score, score)
    //   prediction (max margin)
    var tmpData = dataset
    var numColsOutput = 0
    if ($(rawPredictionCol) != "") {
      val features2raw: Vector => Vector = (features) => predictRaw(features)
      tmpData = tmpData.withColumn($(rawPredictionCol),
        callUDF(features2raw, new VectorUDT, col($(featuresCol))))
      numColsOutput += 1
    }
    if ($(probabilityCol) != "") {
      if ($(rawPredictionCol) != "") {
        val raw2prob = udf { (rawPreds: Vector) =>
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          Vectors.dense(1.0 - prob1, prob1): Vector
        }
        tmpData = tmpData.withColumn($(probabilityCol), raw2prob(col($(rawPredictionCol))))
      } else {
        val features2prob = udf { (features: Vector) => predictProbabilities(features) : Vector }
        tmpData = tmpData.withColumn($(probabilityCol), features2prob(col($(featuresCol))))
      }
      numColsOutput += 1
    }
    if ($(predictionCol) != "") {
      val t = $(threshold)
      if ($(probabilityCol) != "") {
        val predict = udf { probs: Vector =>
          if (probs(1) > t) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn($(predictionCol), predict(col($(probabilityCol))))
      } else if ($(rawPredictionCol) != "") {
        val predict = udf { rawPreds: Vector =>
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          if (prob1 > t) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn($(predictionCol), predict(col($(rawPredictionCol))))
      } else {
        val predict = udf { features: Vector => this.predict(features) }
        tmpData = tmpData.withColumn($(predictionCol), predict(col($(featuresCol))))
      }
      numColsOutput += 1
    }
    if (numColsOutput == 0) {
      this.logWarning(s"$uid: LogisticRegressionModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    tmpData
  }

  override val numClasses: Int = 2

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[threshold]].
   */
  override protected def predict(features: Vector): Double = {
    if (score(features) > getThreshold) 1 else 0
  }

  override protected def predictProbabilities(features: Vector): Vector = {
    val s = score(features)
    Vectors.dense(1.0 - s, s)
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(0.0, m)
  }

  override def copy(extra: ParamMap): LogisticRegressionModel = {
    copyValues(new LogisticRegressionModel(parent, weights, intercept), extra)
  }
}
