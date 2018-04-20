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

package org.apache.spark.ml.clustering

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, Row}

/**
 * :: Experimental ::
 * Summary of clustering algorithms.
 *
 * @param predictions  `DataFrame` produced by model.transform().
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 */
@Experimental
class ClusteringSummary private[clustering] (
    @transient val predictions: DataFrame,
    val predictionCol: String,
    val featuresCol: String,
    val k: Int) extends Serializable {

  /**
   * Cluster centers of the transformed data.
   */
  @transient lazy val cluster: DataFrame = predictions.select(predictionCol)

  /**
   * Size of (number of data points in) each cluster.
   */
  lazy val clusterSizes: Array[Long] = {
    val sizes = Array.fill[Long](k)(0)
    cluster.groupBy(predictionCol).count().select(predictionCol, "count").collect().foreach {
      case Row(cluster: Int, count: Long) => sizes(cluster) = count
    }
    sizes
  }
}
