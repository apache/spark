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

import org.apache.spark.annotation.Since


/**
 * Class for squared error loss calculation.
 *
 * The squared (L2) error is defined as:
 *   (y - F(x))**2
 * where y is the label and F(x) is the model prediction for features x.
 */
@Since("1.2.0")
object SquaredError extends Loss {

  /**
   * Method to calculate the gradients for the gradient boosting calculation for least
   * squares error calculation.
   * The gradient with respect to F(x) is: - 2 (y - F(x))
   * @param prediction Predicted label.
   * @param label True label.
   * @return Loss gradient
   */
  @Since("1.2.0")
  override def gradient(prediction: Double, label: Double): Double = {
    - 2.0 * (label - prediction)
  }

  override private[spark] def computeError(prediction: Double, label: Double): Double = {
    val err = label - prediction
    err * err
  }
}
