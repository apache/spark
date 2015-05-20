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

import java.io.File

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

/**
 * This class is used for the model of the hierarchical clustering
 *
 * @param tree a cluster as a tree node
 */
class HierarchicalClusteringModel(val tree: ClusterTree)
    extends Serializable with Saveable with Logging {

  /** Current version of model save/load format. */
  override protected def formatVersion: String = "1.0"

  override def save(sc: SparkContext, path: String): Unit = this.save(path)

  def save(sc: JavaSparkContext, path: String): Unit = this.save(path)

  private def save(path: String): Unit = {
    val pathObj = new File(HierarchicalClusteringModel.getModelFilePath(path)).getParentFile
    if (pathObj.exists()) {
      throw new IllegalArgumentException("You should save your model in another directory. " +
          "the directory already exists: " + path)
    }

    pathObj.mkdir();
    val modelFilePath = HierarchicalClusteringModel.getModelFilePath(path)
    val oos = new java.io.ObjectOutputStream(new java.io.FileOutputStream(modelFilePath))
    try {
      oos.writeObject(this)
    } finally {
      oos.close()
    }
  }

  def getClusters: Array[ClusterTree] = this.tree.getLeavesNodes

  def getCenters: Array[Vector] = this.getClusters.map(_.center)

  /**
   * Predicts the closest cluster by one point
   */
  def predict(vector: Vector): Int = {
    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)

    val centers = this.getCenters.map(_.toBreeze)
    HierarchicalClustering.findClosestCenter(metric)(centers)(vector.toBreeze)
  }

  /**
   * Predicts the closest cluster by RDD of the points
   */
  def predict(data: RDD[Vector]): RDD[Int] = {
    val sc = data.sparkContext

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)
    val centers = this.getCenters.map(_.toBreeze)
    sc.broadcast(centers)

    data.map{point =>
      HierarchicalClustering.findClosestCenter(metric)(centers)(point.toBreeze)
    }
  }

  /**
   * Predicts the closest cluster by RDD of the points for Java
   */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Computes Within Set Sum of Squeared Error(WSSSE)
   */
  def WSSSE(data: RDD[Vector]): Double = {
    val bvCenters = this.getCenters.map(_.toBreeze)
    data.context.broadcast(bvCenters)
    val distances = data.map {point =>
      val bvPoint = point.toBreeze
      val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
      val idx = HierarchicalClustering.findClosestCenter(metric)(bvCenters)(bvPoint)
      val closestCenter = bvCenters(idx)
      val distance = metric(bvPoint, closestCenter)
      distance
    }
    distances.sum()
  }

  def WSSSE(data: JavaRDD[Vector]): Double = this.WSSSE(data.rdd)

  def toAdjacencyList(): Array[(Int, Int, Double)] = this.tree.toAdjacencyList()

  /** Since Java doesn't support tuple, we must support the data structure for java and py4j. */
  def toJavaAdjacencyList(): java.util.ArrayList[java.util.ArrayList[java.lang.Double]] = {
    var javaList = new java.util.ArrayList[java.util.ArrayList[java.lang.Double]]();
    this.tree.toAdjacencyList().foreach { x =>
      val edge = new java.util.ArrayList[java.lang.Double]()
      edge.add(x._1)
      edge.add(x._2)
      edge.add(x._3)
      javaList.add(edge)
    }
    javaList
  }

  def toLinkageMatrix(): Array[(Int, Int, Double, Int)] = this.tree.toLinkageMatrix()

  /** Since Java doesn't support tuple, we must support the data structure for java and py4j. */
  def toJavaLinkageMatrix(): java.util.ArrayList[java.util.ArrayList[java.lang.Double]] = {
    val javaList = new java.util.ArrayList[java.util.ArrayList[java.lang.Double]]()
    this.tree.toLinkageMatrix().foreach {x =>
      val row = new java.util.ArrayList[java.lang.Double]()
      row.add(x._1)
      row.add(x._2)
      row.add(x._3)
      row.add(x._4)
      javaList.add(row)
    }
    javaList
  }
}


object HierarchicalClusteringModel extends Loader[HierarchicalClusteringModel] {

  override def load(sc: SparkContext, path: String): HierarchicalClusteringModel = {
    this.load(path)
  }

  def load(sc: JavaSparkContext, path: String): HierarchicalClusteringModel = {
    this.load(path)
  }

  def load(path: String): HierarchicalClusteringModel = {
    val modelFilePath = getModelFilePath(path)
    val stream = new java.io.ObjectInputStream(new java.io.FileInputStream(modelFilePath))
    try {
      stream.readObject().asInstanceOf[HierarchicalClusteringModel]
    } finally {
      stream.close()
    }
  }

  private[clustering]
  def getModelFilePath(path: String): String = FilenameUtils.concat(path, "model")
}
