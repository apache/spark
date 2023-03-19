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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap

/**
 * Single-label binary or multiclass classification. Classes are indexed {0, 1, ..., numClasses -
 * 1}.
 *
 * @tparam FeaturesType
 *   Type of input features. E.g., `Vector`
 * @tparam E
 *   Concrete Estimator type
 * @tparam M
 *   Concrete Model type
 */
abstract class Classifier[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
    extends Predictor[FeaturesType, E, M]
    with ClassifierParams {

  @Since("3.5.0")
  def setRawPredictionCol(value: String): E = set(rawPredictionCol, value).asInstanceOf[E]

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * Model produced by a [[Classifier]]. Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType
 *   Type of input features. E.g., `Vector`
 * @tparam M
 *   Concrete Model type
 */
abstract class ClassificationModel[FeaturesType, M <: ClassificationModel[FeaturesType, M]]
    extends PredictionModel[FeaturesType, M]
    with ClassifierParams {

  /** @group setParam */
  @Since("3.5.0")
  def setRawPredictionCol(value: String): M = set(rawPredictionCol, value).asInstanceOf[M]

  /** Number of classes (values which the label can take). */
  @Since("3.5.0")
  def numClasses: Int = getModelAttr("numClasses").asInstanceOf[Int]

  /**
   * Predict label for the given features. This method is used to implement `transform()` and
   * output [[predictionCol]].
   *
   * This default implementation for classification predicts the index of the maximum value from
   * `predictRaw()`.
   */
  @Since("3.5.0")
  override def predict(features: FeaturesType): Double = {
    // TODO: should send the vector to the server,
    //  then invoke the 'predict' method of the remote model

    // Note: Subclass may need to override this, since the result
    // maybe adjusted by param like `thresholds`.
    throw new NotImplementedError
  }

  /**
   * Raw prediction for each possible label. The meaning of a "raw" prediction may vary between
   * algorithms, but it intuitively gives a measure of confidence in each possible label (where
   * larger = more confident). This internal method is used to implement `transform()` and output
   * [[rawPredictionCol]].
   *
   * @return
   *   vector where element i is the raw prediction for label i. This raw prediction may be any
   *   real number, where a larger value indicates greater confidence for that label.
   */
  @Since("3.5.0")
  def predictRaw(features: FeaturesType): Vector = {
    // TODO: should send the vector to the server,
    //  then invoke the 'predictRaw' method of the remote model
    throw new NotImplementedError
  }

  /**
   * If the rawPrediction and prediction columns are set, this method returns the current model,
   * otherwise it generates new columns for them and sets them as columns on a new copy of
   * the current model
   */
  private[classification] def findSummaryModel():
  (ClassificationModel[FeaturesType, M], String, String) = {
    val model = if ($(rawPredictionCol).isEmpty && $(predictionCol).isEmpty) {
      copy(ParamMap.empty)
        .setRawPredictionCol("rawPrediction_" + java.util.UUID.randomUUID.toString)
        .setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else if ($(rawPredictionCol).isEmpty) {
      copy(ParamMap.empty).setRawPredictionCol("rawPrediction_" +
        java.util.UUID.randomUUID.toString)
    } else if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else {
      this
    }
    (model, model.getRawPredictionCol, model.getPredictionCol)
  }
}
