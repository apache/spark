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

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * this class is used for the model of the hierarchical clustering
 *
 * @param clusterTree a cluster as a tree node
 * @param trainTime the milliseconds for executing a training
 * @param predictTime the milliseconds for executing a prediction
 * @param isTrained if the model has been trained, the flag is true
 */
class HierarchicalClusteringModel private (
  val clusterTree: ClusterTree,
  private[mllib] var trainTime: Int,
  private[mllib] var predictTime: Int,
  private[mllib] var isTrained: Boolean) extends Serializable {

  def this(clusterTree: ClusterTree) = this(clusterTree, 0, 0, false)

  def getClusters(): Array[ClusterTree] = clusterTree.getClusters().toArray

  def getCenters(): Array[Vector] = getClusters().map(_.center)

  /**
   * Predicts the closest cluster of each point
   */
  def predict(vector: Vector): Int = {
    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    this.clusterTree.assignClusterIndex(metric)(vector)
  }

  /**
   * Predicts the closest cluster of each point
   */
  def predict(data: RDD[Vector]): RDD[(Int, Vector)] = {
    val startTime = System.currentTimeMillis() // to measure the execution time

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val centers = getClusters().map(_.center.toBreeze)
    val treeRoot = this.clusterTree
    val closestClusterIndexFinder = treeRoot.assignClusterIndex(metric) _
    data.sparkContext.broadcast(closestClusterIndexFinder)
    val predicted = data.map(point => (closestClusterIndexFinder(point), point))
    this.predictTime = (System.currentTimeMillis() - startTime).toInt
    predicted
  }

  /** Maps given points to their cluster indices. */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).map(_._1).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Computes the sum of total variance of all cluster
   */
  def computeCost(): Double =  this.getClusters().map(_.getVariance().get).sum
}
