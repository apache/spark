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

package org.apache.spark.ml.source.libsvm

/**
 * Options for the LibSVM data source.
 */
private[libsvm] class LibSVMOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable {

  /**
   * If unspecified or nonpositive, the number of features will be determined automatically at the
   * cost of one additional pass.
   * This is also useful when the dataset is already split into multiple files and you want to load
   * them separately, because some features may not present in certain files, which leads to
   * inconsistent feature dimensions.
   */
  val numFeatures = {
    val numFeaturesValue = parameters("numFeatures").toInt
    assert(numFeaturesValue > 0)
    numFeaturesValue
  }

  /**
   * Feature vector type, "sparse" (default) or "dense".
   */
  val sparse = parameters.getOrElse("vectorType", "sparse") == "sparse"
}
