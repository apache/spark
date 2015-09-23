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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * A clustering model for DP-means. Each point belongs to the cluster with the closest center.
 * @param clusterCenters An array of cluster centers
 * @param lambdaValue lambda value used to find the clusters
 */
class DpMeansModel(
    val clusterCenters: Array[Vector],
    val lambdaValue: Double) extends Serializable {

  /** A Java-friendly constructor that takes an Iterable of Vectors. */
  def this(centers: java.lang.Iterable[Vector], lambdaValue: Double) =
    this(centers.asScala.toArray, lambdaValue)

  /** Total number of clusters obtained. */
  def k: Int = clusterCenters.length

  /** Returns the cluster index that a given point belongs to. */
  def predict(point: Vector): Int = {
    val centersWithNorm = clusterCentersWithNorm
    DpMeans.assignCluster(centersWithNorm.to[mutable.ArrayBuffer], new VectorWithNorm(point))._1
  }

  /** Maps the points in the given RDD to their closest cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = points.context.broadcast(centersWithNorm)
    points.map { p =>
      DpMeans.assignCluster(
        bcCentersWithNorm.value.to[mutable.ArrayBuffer], new VectorWithNorm(p)
      )._1
    }
  }

  /**
   * Return the cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  def computeCost(data: RDD[Vector]): Double = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = data.context.broadcast(centersWithNorm)
    data.map(p => DpMeans.assignCluster(bcCentersWithNorm.value.to[mutable.ArrayBuffer],
        new VectorWithNorm(p))._2).sum()
  }

  private def clusterCentersWithNorm: Iterable[VectorWithNorm] =
    clusterCenters.map(new VectorWithNorm(_))

}
