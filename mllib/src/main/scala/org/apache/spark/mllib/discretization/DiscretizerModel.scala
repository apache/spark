/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.mllib.discretization

import org.apache.spark.rdd.RDD

trait DiscretizerModel extends Serializable {
  /**
   * Return the thresholds used to discretized the given feature
   *
   * @param feature The number of the feature to discretize
   */
  def getThresholdsForFeature(feature: Int): Seq[Double]

  /**
   * Return the thresholds used to discretized the given features
   *
   * @param features The number of the feature to discretize
   */
  def getThresholdsForFeature(features: Seq[Int]): Map[Int, Seq[Double]]

  /**
   * Return the thresholds used to discretized the continuous features
   */
  def getThresholdsForContinuousFeatures: Map[Int, Seq[Double]]

  /**
   * Discretizes an RDD
   */
  def discretize: RDD[_]

}
