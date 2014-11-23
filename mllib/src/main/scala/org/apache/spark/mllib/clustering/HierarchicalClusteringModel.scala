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
import org.apache.spark.Logging
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * this class is used for the model of the hierarchical clustering
 *
 * @param clusterTree a cluster as a tree node
 * @param isTrained if the model has been trained, the flag is true
 */
class HierarchicalClusteringModel private (
  val clusterTree: ClusterTree,
  private[mllib] var isTrained: Boolean) extends Serializable with Logging with Cloneable {

  def this(clusterTree: ClusterTree) = this(clusterTree, false)

  override def clone(): HierarchicalClusteringModel = {
    new HierarchicalClusteringModel(this.clusterTree.clone(), true)
  }

  /**
   * Cuts a cluster tree by given threshold of dendrogram height
   *
   * @param height a threshold to cut a cluster tree
   * @return a hierarchical clustering model
   */
  def cut(height: Double): HierarchicalClusteringModel = {
    val cloned = this.clone()
    cloned.clusterTree.cut(height)
    cloned
  }

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
    val sc = data.sparkContext

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    val treeRoot = this.clusterTree
    sc.broadcast(metric)
    sc.broadcast(treeRoot)
    val predicted = data.map(point => (treeRoot.assignClusterIndex(metric)(point), point))

    val predictTime = System.currentTimeMillis() - startTime
    logInfo(s"Predicting Time: ${predictTime.toDouble / 1000} [sec]")

    predicted
  }

  /** Maps given points to their cluster indices. */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).map(_._1).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Computes the sum of total variance of all cluster
   */
  def getSumOfVariance(): Double = this.getClusters().map(_.getVariance().get).sum

  def getClusters(): Array[ClusterTree] = clusterTree.getClusters().toArray

  def getCenters(): Array[Vector] = getClusters().map(_.center)

  /**
   * Converts a clustering merging list
   * Returned data format is fit for scipy's dendrogram function
   * SEE ALSO: scipy.cluster.hierarchy.dendrogram
   *
   * @return List[(node1, node2, distance, tree size)]
   */
  def toMergeList(): List[(Int, Int, Double, Int)] = {
    val seq = this.clusterTree.toSeq().sortWith{ case (a, b) => a.getHeight() < b.getHeight()}
    val leaves = seq.filter(_.isLeaf())
    val nodes = seq.filter(!_.isLeaf()).filter(_.children.size > 1)
    val clusters = leaves ++ nodes
    val treeMap = clusters.zipWithIndex.map { case (tree, idx) => (tree -> idx)}.toMap

    // If a node only has one-child, the child is regarded as the cluster of the child.
    // Cluster A has cluster B and Cluster B. B is a leaf. C only has cluster D.
    // ==> A merge list is (B, D), not (B, C).
    def getIndex(map: Map[ClusterTree, Int], tree: ClusterTree): Int = {
      tree.children.size match {
        case 1 => getIndex(map, tree.children(0))
        case _ => map(tree)
      }
    }
    clusters.filter(tree => !tree.isLeaf()).toList.map { tree =>
      (getIndex(treeMap, tree.children(0)),
          getIndex(treeMap, tree.children(1)),
          tree.getHeight(),
          tree.getTreeSize())
    }
  }
}
