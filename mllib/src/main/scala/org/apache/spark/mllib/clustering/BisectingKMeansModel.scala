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

import breeze.linalg.{Vector => BV, norm => breezeNorm}

import org.apache.spark.Logging
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * This class is used for the model of the bisecting kmeans
 *
 * @param node a cluster as a tree node
 */
@Since("1.6.0")
class BisectingKMeansModel @Since("1.6.0") (
    @Since("1.6.0") val node: BisectingClusterNode
  ) extends Serializable with Logging {

  @Since("1.6.0")
  def getClusters: Array[BisectingClusterNode] = this.node.getLeavesNodes

  @Since("1.6.0")
  def getCenters: Array[Vector] = this.getClusters.map(_.center)

  /**
   * Predicts the closest cluster by one point
   */
  @Since("1.6.0")
  def predict(vector: Vector): Int = {
    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val closestLeafNode = this.node.findClosestLeaf(vector, metric)

    val closestCenter = closestLeafNode.center
    val centers = this.getCenters.map(_.toBreeze)
    BisectingKMeans.findClosestCenter(metric)(centers)(closestCenter.toBreeze)
  }

  /**
   * Predicts the closest cluster by RDD of the points
   */
  @Since("1.6.0")
  def predict(data: RDD[Vector]): RDD[Int] = {
    val sc = data.sparkContext
    data.map { p => predict(p) }
  }

  /**
   * Predicts the closest cluster by RDD of the points for Java
   */
  @Since("1.6.0")
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Computes Within Set Sum of Squared Error(WSSSE)
   */
  @Since("1.6.0")
  def WSSSE(data: RDD[Vector]): Double = {
    val bvCenters = this.getCenters.map(_.toBreeze)
    data.context.broadcast(bvCenters)
    val distances = data.map {point =>
      val bvPoint = point.toBreeze
      val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
      val idx = BisectingKMeans.findClosestCenter(metric)(bvCenters)(bvPoint)
      val closestCenter = bvCenters(idx)
      val distance = metric(bvPoint, closestCenter)
      distance
    }
    distances.sum()
  }

  @Since("1.6.0")
  def WSSSE(data: JavaRDD[Vector]): Double = this.WSSSE(data.rdd)

}

