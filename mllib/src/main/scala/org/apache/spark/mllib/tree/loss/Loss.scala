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

package org.apache.spark.mllib.tree.loss

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.TreeEnsembleModel
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for adding "pluggable" loss functions for the gradient boosting algorithm.
 */
@Since("1.2.0")
@DeveloperApi
trait Loss extends Serializable {

  /**
   * Method to calculate the gradients for the gradient boosting calculation.
   * @param prediction Predicted feature
   * @param label true label.
   * @return Loss gradient.
   */
  @Since("1.2.0")
  def gradient(prediction: Double, label: Double): Double

  /**
   * Method to calculate error of the base learner for the gradient boosting calculation.
   *
   * @param model Model of the weak learner.
   * @param data Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return Measure of model error on data
   *
   * @note This method is not used by the gradient boosting algorithm but is useful for debugging
   * purposes.
   */
  @Since("1.2.0")
  def computeError(model: TreeEnsembleModel, data: RDD[LabeledPoint]): Double = {
    data.map(point => computeError(model.predict(point.features), point.label)).mean()
  }

  /**
   * Method to calculate loss when the predictions are already known.
   *
   * @param prediction Predicted label.
   * @param label True label.
   * @return Measure of model error on datapoint.
   *
   * @note This method is used in the method evaluateEachIteration to avoid recomputing the
   * predicted values from previously fit trees.
   */
  private[spark] def computeError(prediction: Double, label: Double): Double
}

private[spark] trait ClassificationLoss extends Loss {
  /**
   * Computes the class probability given the margin.
   */
  private[spark] def computeProbability(margin: Double): Double
}
