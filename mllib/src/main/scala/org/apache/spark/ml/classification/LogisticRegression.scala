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
import org.apache.spark.mllib.linalg.{VectorUDT, BLAS, Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dsl._
import org.apache.spark.sql.types.DoubleType
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

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // This is overridden (a) to be more efficient (avoiding re-computing values when creating
    // multiple output columns) and (b) to handle threshold, which the abstractions do not use.
    // TODO: We should abstract away the steps defined by UDFs below so that the abstractions
    // can call whichever UDFs are needed to create the output columns.

    // Check schema
    transformSchema(dataset.schema, paramMap, logging = true)

    val map = this.paramMap ++ paramMap

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    //   rawPrediction (-margin, margin)
    //   probability (1.0-score, score)
    //   prediction (max margin)
    var tmpData = dataset
    var numColsOutput = 0
    if (map(rawPredictionCol) != "") {
      val features2raw: Vector => Vector = (features) => predictRaw(features)
      tmpData = tmpData.select($"*",
        callUDF(features2raw, new VectorUDT, col(map(featuresCol))).as(map(rawPredictionCol)))
      numColsOutput += 1
    }
    if (map(probabilityCol) != "") {
      if (map(rawPredictionCol) != "") {
        val raw2prob: Vector => Vector = { (rawPreds: Vector) =>
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          Vectors.dense(1.0 - prob1, prob1)
        }
        tmpData = tmpData.select($"*",
          callUDF(raw2prob, new VectorUDT, col(map(rawPredictionCol))).as(map(probabilityCol)))
      } else {
        val features2prob: Vector => Vector = (features: Vector) => predictProbabilities(features)
        tmpData = tmpData.select($"*",
          callUDF(features2prob, new VectorUDT, col(map(featuresCol))).as(map(probabilityCol)))
      }
      numColsOutput += 1
    }
    if (map(predictionCol) != "") {
      val t = map(threshold)
      if (map(probabilityCol) != "") {
        val predict: Vector => Double = { probs: Vector =>
          if (probs(1) > t) 1.0 else 0.0
        }
        tmpData = tmpData.select($"*",
          callUDF(predict, DoubleType, col(map(probabilityCol))).as(map(predictionCol)))
      } else if (map(rawPredictionCol) != "") {
        val predict: Vector => Double = { rawPreds: Vector =>
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          if (prob1 > t) 1.0 else 0.0
        }
        tmpData = tmpData.select($"*",
          callUDF(predict, DoubleType, col(map(rawPredictionCol))).as(map(predictionCol)))
      } else {
        val predict: Vector => Double = (features: Vector) => this.predict(features)
        tmpData = tmpData.select($"*",
          callUDF(predict, DoubleType, col(map(featuresCol))).as(map(predictionCol)))
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
    println(s"LR.predict with threshold: ${paramMap(threshold)}")
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
