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

import org.apache.spark.SparkContext._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.base.{ PointOps, FP }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */
private[mllib] class GeneralizedKMeansModel[P <: FP, C <: FP](
  val pointOps: PointOps[P, C],
  val centers: Array[C])
  extends Serializable {

  val k: Int = clusterCenters.length

  def clusterCenters: Array[Vector] = centers.map { c => pointOps.centerToVector(c) }

  /** Returns the cluster index that a given point belongs to. */
  def predict(point: Vector): Int =
    pointOps.findClosest(centers, pointOps.vectorToPoint(point))._1

  /** Maps given points to their cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] =
    points.map(p => pointOps.findClosest(centers, pointOps.vectorToPoint(p))._1)

  /** Maps given points to their cluster indices. */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  def computeCost(data: RDD[Vector]): Double =
    data.map(p => pointOps.findClosest(centers, pointOps.vectorToPoint(p))._2).sum()

}
