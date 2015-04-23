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
import org.apache.spark.mllib.tree.model.TreeEnsembleModel


/**
 * :: DeveloperApi ::
 * Class for absolute error loss calculation (for regression).
 *
 * The absolute (L1) error is defined as:
 *  |y - F(x)|
 * where y is the label and F(x) is the model prediction for features x.
 */
@DeveloperApi
object AbsoluteError extends Loss {

  /**
   * Method to calculate the gradients for the gradient boosting calculation for least
   * absolute error calculation.
   * The gradient with respect to F(x) is: sign(F(x) - y)
   * @param prediction Predicted label.
   * @param label True label.
   * @return Loss gradient
   */
  override def gradient(prediction: Double, label: Double): Double = {
    if (label - prediction < 0) 1.0 else -1.0
  }

  override private[mllib] def computeError(prediction: Double, label: Double): Double = {
    val err = label - prediction
    math.abs(err)
  }
}
