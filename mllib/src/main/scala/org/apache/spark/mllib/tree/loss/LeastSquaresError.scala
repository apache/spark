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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel

/**
 * Class for least squares error loss calculation.
 */
object LeastSquaresError extends Loss {

  /**
   * Method to calculate the loss gradients for the gradient boosting calculation
   * @param model Model of the weak learner
   * @param point Instance of the training dataset
   * @param learningRate Learning rate parameter for regularization
   * @return Loss gradient
   */
  @DeveloperApi
  override def lossGradient(
    model: DecisionTreeModel,
    point: LabeledPoint,
    learningRate: Double): Double = {
    point.label - model.predict(point.features) * learningRate
  }

}