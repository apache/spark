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

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Represents a probabilistic classification model that provides a probability
 * distribution over a set of classes, rather than only predicting a class.
 */
@Experimental
trait ProbabilisticClassificationModel extends ClassificationModel {
  /**
   * Return probability for the prediction of the given data set using the model trained.
   *
   * @param testData RDD representing data points to be classified
   * @return an RDD[Double] where each entry contains the corresponding prediction
   */
  def predictProbability(testData: RDD[Vector]): RDD[Double]

  /**
   * Return probability for a single data point prediction using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted category from the trained model
   */
  def predictProbability(testData: Vector): Double
}
