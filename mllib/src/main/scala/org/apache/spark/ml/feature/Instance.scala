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

package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector

/**
 * Class that represents an instance of weighted data point with label and features.
 *
 * @param label Label for this data point.
 * @param weight The weight of this instance.
 * @param features The vector of features for this data point.
 */
private[ml] case class Instance(label: Double, weight: Double, features: Vector)

/**
 * Case class that represents an instance of data point with
 * label, weight, offset and features.
 * This is mainly used in GeneralizedLinearRegression currently.
 *
 * @param label Label for this data point.
 * @param weight The weight of this instance.
 * @param offset The offset used for this data point.
 * @param features The vector of features for this data point.
 */
private[ml] case class OffsetInstance(
    label: Double,
    weight: Double,
    offset: Double,
    features: Vector) {

  /** Converts to an [[Instance]] object by leaving out the offset. */
  def toInstance: Instance = Instance(label, weight, features)

}
