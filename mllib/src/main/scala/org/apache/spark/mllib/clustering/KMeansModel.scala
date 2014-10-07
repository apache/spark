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

package org.apache.spark.mllib.clustering

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector

/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */

class KMeansModel(specific: GeneralizedKMeansModel[_, _]) {

  val k: Int = specific.k

  /** Returns the cluster index that a given point belongs to. */
  def predict(point: Vector): Int = specific.predict(point)

  /**
   * Maps given points to their cluster indices.
   */
  def predict(points: RDD[Vector]): RDD[Int] = specific.predict(points)

  /**
   * Maps given points to their cluster indices.
   * @param points input points
   * @return the predicted cluster index for each input point
   */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] = specific.predict(points)

  /**
   * Get the K-means cost for this model on the given data.
   * @param data data for which cost is to be computed
   * @return  the K-means cost for this model on the given data
   */
  def computeCost(data: RDD[Vector]): Double = specific.computeCost(data)

  /**
   * Get the array of cluster centers
   * @return  the array of cluster centers
   */
  def clusterCenters: Array[Vector] = specific.clusterCenters
}
