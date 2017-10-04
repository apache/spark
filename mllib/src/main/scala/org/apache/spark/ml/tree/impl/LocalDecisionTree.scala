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

package org.apache.spark.ml.tree.impl

import org.apache.spark.ml.tree._
import org.apache.spark.mllib.tree.model.ImpurityStats

/** Object exposing methods for local training of decision trees */
private[ml] object LocalDecisionTree {

  /**
   * Iterate over feature values and labels for a specific (node, feature), updating stats
   * aggregator for the current node.
   */
  private[impl] def updateAggregator(
      statsAggregator: DTStatsAggregator,
      col: FeatureVector,
      labels: Array[Double],
      from: Int,
      to: Int,
      featureIndexIdx: Int,
      splits: Array[Array[Split]]): Unit = {
    val metadata = statsAggregator.metadata
    if (metadata.isUnordered(col.featureIndex)) {
      from.until(to).foreach { idx =>
        val rowIndex = col.indices(idx)
        AggUpdateUtils.updateUnorderedFeature(statsAggregator, col.values(idx), labels(rowIndex),
          featureIndex = col.featureIndex, featureIndexIdx, splits)
      }
    } else {
      from.until(to).foreach { idx =>
        val rowIndex = col.indices(idx)
        AggUpdateUtils.updateOrderedFeature(statsAggregator, col.values(idx), labels(rowIndex),
          featureIndex = col.featureIndex, featureIndexIdx)
      }
    }
  }

}
