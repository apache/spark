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
import org.apache.spark.mllib.util.MLUtils


/**
 * :: DeveloperApi ::
 * Class for log loss calculation (for classification).
 * This uses twice the binomial negative log likelihood, called "deviance" in Friedman (1999).
 *
 * The log loss is defined as:
 *   2 log(1 + exp(-2 y F(x)))
 * where y is a label in {-1, 1} and F(x) is the model prediction for features x.
 */
@Since("1.2.0")
@DeveloperApi
object LogLoss extends Loss {

  /**
   * Method to calculate the loss gradients for the gradient boosting calculation for binary
   * classification
   * The gradient with respect to F(x) is: - 4 y / (1 + exp(2 y F(x)))
   * @param prediction Predicted label.
   * @param label True label.
   * @return Loss gradient
   */
  @Since("1.2.0")
  override def gradient(prediction: Double, label: Double): Double = {
    - 4.0 * label / (1.0 + math.exp(2.0 * label * prediction))
  }

  override private[mllib] def computeError(prediction: Double, label: Double): Double = {
    val margin = 2.0 * label * prediction
    // The following is equivalent to 2.0 * log(1 + exp(-margin)) but more numerically stable.
    2.0 * MLUtils.log1pExp(-margin)
  }
}
