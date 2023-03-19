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
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

/**
 * Single-label binary or multiclass classifier which can output class conditional probabilities.
 *
 * @tparam FeaturesType
 *   Type of input features. E.g., `Vector`
 * @tparam E
 *   Concrete Estimator type
 * @tparam M
 *   Concrete Model type
 */
abstract class ProbabilisticClassifier[
    FeaturesType,
    E <: ProbabilisticClassifier[FeaturesType, E, M],
    M <: ProbabilisticClassificationModel[FeaturesType, M]]
    extends Classifier[FeaturesType, E, M]
    with ProbabilisticClassifierParams {

  /** @group setParam */
  @Since("3.5.0")
  def setProbabilityCol(value: String): E = set(probabilityCol, value).asInstanceOf[E]

  /** @group setParam */
  @Since("3.5.0")
  def setThresholds(value: Array[Double]): E = set(thresholds, value).asInstanceOf[E]
}

/**
 * Model produced by a [[ProbabilisticClassifier]]. Classes are indexed {0, 1, ..., numClasses -
 * 1}.
 *
 * @tparam FeaturesType
 *   Type of input features. E.g., `Vector`
 * @tparam M
 *   Concrete Model type
 */
abstract class ProbabilisticClassificationModel[
    FeaturesType,
    M <: ProbabilisticClassificationModel[FeaturesType, M]]
    extends ClassificationModel[FeaturesType, M]
    with ProbabilisticClassifierParams {

  /** @group setParam */
  @Since("3.5.0")
  def setProbabilityCol(value: String): M = set(probabilityCol, value).asInstanceOf[M]

  /** @group setParam */
  @Since("3.5.0")
  def setThresholds(value: Array[Double]): M = {
    require(
      value.length == numClasses,
      this.getClass.getSimpleName +
        ".setThresholds() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${value.length}")
    set(thresholds, value).asInstanceOf[M]
  }

  /**
   * Predict the probability of each class given the features. These predictions are also called
   * class conditional probabilities.
   *
   * This internal method is used to implement `transform()` and output [[probabilityCol]].
   *
   * @return
   *   Estimated class conditional probabilities
   */
  @Since("3.5.0")
  def predictProbability(features: FeaturesType): Vector = {
    // TODO: should send the vector to the server,
    //  then invoke the 'predictProbability' method of the remote model
    throw new NotImplementedError
  }

  /**
   *If the probability and prediction columns are set, this method returns the current model,
   * otherwise it generates new columns for them and sets them as columns on a new copy of
   * the current model
   */
  override private[classification] def findSummaryModel():
  (ProbabilisticClassificationModel[FeaturesType, M], String, String) = {
    val model = if ($(probabilityCol).isEmpty && $(predictionCol).isEmpty) {
      copy(ParamMap.empty)
        .setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
        .setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else if ($(probabilityCol).isEmpty) {
      copy(ParamMap.empty).setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
    } else if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else {
      this
    }
    (model, model.getProbabilityCol, model.getPredictionCol)
  }
}
