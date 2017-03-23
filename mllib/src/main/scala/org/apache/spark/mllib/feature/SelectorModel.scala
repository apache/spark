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

package org.apache.spark.mllib.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Generic selector model.
 *
 * @param selectedFeatures list of indices to select (filter). Must be ordered asc
 */
@Experimental
class SelectorModel (val selectedFeatures: Array[Int]) extends VectorTransformer {

  require(isSorted(selectedFeatures), "Array has to be sorted asc")

  protected def isSorted(array: Array[Int]): Boolean = {
    var i = 1
    while (i < array.length) {
      if (array(i) < array(i-1)) return false
      i += 1
    }
    true
  }

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  override def transform(vector: Vector): Vector = {
    FeatureUtils.compress(vector, selectedFeatures)
  }


}
