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

import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Clustering model produced by [[BisectingKMeans]].
 * The prediction is done level-by-level from the root node to a leaf node, and at each node among
 * its children the closest to the input point is selected.
 *
 * @param root the root node of the clustering tree
 */
@Since("1.6.0")
@Experimental
class BisectingKMeansModel private[clustering] (
    private[clustering] val root: ClusteringTreeNode
  ) extends Serializable with Logging {

  /**
   * Leaf cluster centers.
   */
  @Since("1.6.0")
  def clusterCenters: Array[Vector] = root.leafNodes.map(_.center)

  /**
   * Number of leaf clusters.
   */
  lazy val k: Int = clusterCenters.length

  /**
   * Predicts the index of the cluster that the input point belongs to.
   */
  @Since("1.6.0")
  def predict(point: Vector): Int = {
    root.predict(point)
  }

  /**
   * Predicts the indices of the clusters that the input points belong to.
   */
  @Since("1.6.0")
  def predict(points: RDD[Vector]): RDD[Int] = {
    points.map { p => root.predict(p) }
  }

  /**
   * Java-friendly version of [[predict()]].
   */
  @Since("1.6.0")
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Computes the squared distance between the input point and the cluster center it belongs to.
   */
  @Since("1.6.0")
  def computeCost(point: Vector): Double = {
    root.computeCost(point)
  }

  /**
   * Computes the sum of squared distances between the input points and their corresponding cluster
   * centers.
   */
  @Since("1.6.0")
  def computeCost(data: RDD[Vector]): Double = {
    data.map(root.computeCost).sum()
  }

  /**
   * Java-friendly version of [[computeCost()]].
   */
  @Since("1.6.0")
  def computeCost(data: JavaRDD[Vector]): Double = this.computeCost(data.rdd)
}
