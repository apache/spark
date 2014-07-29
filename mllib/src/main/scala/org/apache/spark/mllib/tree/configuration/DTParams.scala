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

import scala.beans.BeanProperty

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Stores configuration options for DecisionTree construction.
 * @param maxDepth Maximum depth of the tree.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
 * @param maxBins maximum number of bins used for splitting features
 * @param quantileStrategy algorithm for calculating quantiles
 * @param maxMemoryInMB maximum memory in MB allocated to histogram aggregation. Default value is
 *                      128 MB.
 */
@Experimental
private[mllib] abstract class DTParams (
    @BeanProperty var maxDepth: Int,
    @BeanProperty var maxBins: Int,
    var quantileStrategy: String,
    @BeanProperty var maxMemoryInMB: Int) extends Serializable {

  def getQuantileStrategy: String = this.quantileStrategy

  def setQuantileStrategy(quantileStrategy: String) = {
    if (!QuantileStrategies.nameToStrategyMap.contains(quantileStrategy)) {
      throw new IllegalArgumentException(s"Bad quantileStrategy parameter: $quantileStrategy."
        + s"  Supported values: ${DTParams.supportedQuantileStrategies.mkString(", ")}.")
    }
    this.quantileStrategy = quantileStrategy
  }

}


object DTParams {

  /**
   * Get list of supported quantileStrategy options.
   */
  def supportedQuantileStrategies: List[String] = QuantileStrategies.nameToStrategyMap.keys.toList

}
