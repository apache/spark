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

package org.apache.spark.mllib.tree.configuration

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.configuration.DTParams

/**
 * :: Experimental ::
 * Stores all the configuration options for DecisionTreeRegressor construction
 * @param maxDepth maximum depth of the tree
 * @param maxBins maximum number of bins used for splitting features
 * @param quantileStrategy algorithm for calculating quantiles
 * @param maxMemoryInMB maximum memory in MB allocated to histogram aggregation. Default value is
 *                      128 MB.
 * @param impurity criterion used for information gain calculation (e.g., "variance")
 */
@Experimental
class DTRegressorParams (
    maxDepth: Int = 5,
    maxBins: Int = 100,
    quantileStrategy: String = "sort",
    maxMemoryInMB: Int = 128,
    val impurity: String = "variance")
  extends DTParams(maxDepth, maxBins, quantileStrategy, maxMemoryInMB) {

  /*
  if (!List("variance").contains(impurity)) {
      throw new IllegalArgumentException(s"Bad impurity parameter for regression: $impurity")
  }
  */

}
