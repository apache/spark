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

package org.apache.spark.mllib.rdd

/**
 * :: Experimental ::
 * A class for holding dataset metadata.
 * @param numClasses  Number of classes for classification.  Values of 0 or 1 indicate regression.
 * @param numFeatures Number of features.
 * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
 *                                number of discrete values they take. For example, an entry (n ->
 *                                k) implies the feature n is categorical with k categories 0,
 *                                1, 2, ... , k-1. It's important to note that features are
 *                                zero-indexed.
 */
class DatasetInfo (
    val numClasses: Int,
    val numFeatures: Int,
    val categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int]())
  extends Serializable {

  /**
   * Indicates if this dataset's label is real-valued (numClasses < 2).
   */
  def isRegression: Boolean = {
    numClasses < 2
  }

  /**
   * Indicates if this dataset's label is categorical (numClasses >= 2).
   */
  def isClassification: Boolean = {
    numClasses >= 2
  }

  /**
   * Indicates if this dataset's label is categorical with >2 categories.
   */
  def isMulticlass: Boolean = {
    numClasses > 2
  }

  /**
   * Indicates if this dataset's label is categorical with >2 categories,
   * and there is at least one categorical feature.
   */
  def isMulticlassWithCategoricalFeatures: Boolean = {
    isMulticlass && categoricalFeaturesInfo.nonEmpty
  }

}
