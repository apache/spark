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

package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.api.java.JavaRDD

/**
 * Represents a classification model that predicts to which of a set of categories an example
 * belongs. The categories are represented by double values: 0.0, 1.0
 */
class BinaryClassificationModel (
     override val weights: Vector,
     override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable {

  protected var threshold: Double = 0.0

  // this is only used to ensure prior behaviour of deprecated `predict``
  protected var useThreshold: Boolean = true

  /**
   * Setter and getter for the threshold. The threshold separates positive predictions from
   * negative predictions. An example with prediction score greater than or equal to this
   * threshold is identified as an positive, and negative otherwise. The default value is 0.5.
   */
  def setThreshold(threshold: Double): this.type = {
    this.useThreshold = true
    this.threshold = threshold
    this
  }

  def getThreshold = threshold

  private def compareWithThreshold(value: Double): Double =
    if (value < threshold) 0.0 else 1.0

  def predictClass(testData: RDD[Vector]): RDD[Double] = {
    predictScore(testData).map(compareWithThreshold)
  }

  def predictClass(testData: Vector): Double = {
    compareWithThreshold(predictScore(testData))
  }

  /**
   * DEPRECATED: Use predictScore(...) or predictClass(...) instead
   * Clears the threshold so that `predict` will output raw prediction scores.
   */
  @Deprecated
  def clearThreshold(): this.type = {
    this.useThreshold = false
    this
  }

  /**
   * DEPRECATED: Use predictScore(...) or predictClass(...) instead
   */
  @Deprecated
  override protected def predictPoint(
                                       dataMatrix: Vector,
                                       weightMatrix: Vector,
                                       intercept: Double) = {
    if (useThreshold) predictClass(dataMatrix)
    else predictScore(dataMatrix)
  }

  /**
   * DEPRECATED: Use predictScore(...) or predictClass(...) instead
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Double] where each entry contains the corresponding prediction
   */
  @Deprecated
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    if (useThreshold) predictClass(testData)
    else predictScore(testData)
  }

  /**
   * DEPRECATED: Use predictScore(...) or predictClass(...) instead
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted category from the trained model
   */
  @Deprecated
  override def predict(testData: Vector): Double = {
    if (useThreshold) predictClass(testData)
    else predictScore(testData)
  }
}
