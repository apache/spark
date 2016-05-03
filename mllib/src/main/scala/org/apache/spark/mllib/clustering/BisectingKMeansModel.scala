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

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

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
  ) extends Serializable with Saveable with Logging {

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

  @Since("2.0.0")
  override def save(sc: SparkContext, path: String): Unit = {
    BisectingKMeansModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

@Since("2.0.0")
object BisectingKMeansModel extends Loader[BisectingKMeansModel] {

  @Since("2.0.0")
  override def load(sc: SparkContext, path: String): BisectingKMeansModel = {
    val (loadedClassName, formatVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val rootId = (metadata \ "rootId").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, formatVersion) match {
      case (classNameV1_0, "1.0") =>
        val model = SaveLoadV1_0.load(sc, path, rootId)
        model
      case _ => throw new Exception(
        s"BisectingKMeansModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $formatVersion).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }
  }

  private case class Data(index: Int, size: Long, center: Vector, norm: Double, cost: Double,
     height: Double, children: Seq[Int])

  private object Data {
    def apply(r: Row): Data = Data(r.getInt(0), r.getLong(1), r.getAs[Vector](2), r.getDouble(3),
      r.getDouble(4), r.getDouble(5), r.getSeq[Int](6))
  }

  private[clustering] object SaveLoadV1_0 {
    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.BisectingKMeansModel"

    def save(sc: SparkContext, model: BisectingKMeansModel, path: String): Unit = {
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
          ~ ("rootId" -> model.root.index)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val data = getNodes(model.root).map(node => Data(node.index, node.size,
        node.centerWithNorm.vector, node.centerWithNorm.norm, node.cost, node.height,
        node.children.map(_.index)))
      val dataRDD = sc.parallelize(data).toDF()
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    private def getNodes(node: ClusteringTreeNode): Array[ClusteringTreeNode] = {
      if (node.children.isEmpty) {
        Array(node)
      } else {
        node.children.flatMap(getNodes(_)) ++ Array(node)
      }
    }

    def load(sc: SparkContext, path: String, rootId: Int): BisectingKMeansModel = {
      val sqlContext = SQLContext.getOrCreate(sc)
      val rows = sqlContext.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Data](rows.schema)
      val data = rows.select("index", "size", "center", "norm", "cost", "height", "children")
      val nodes = data.rdd.map(Data.apply).collect().map(d => (d.index, d)).toMap
      val rootNode = buildTree(rootId, nodes)
      new BisectingKMeansModel(rootNode)
    }

    private def buildTree(rootId: Int, nodes: Map[Int, Data]): ClusteringTreeNode = {
      val root = nodes.get(rootId).get
      if (root.children.isEmpty) {
        new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
          root.cost, root.height, new Array[ClusteringTreeNode](0))
      } else {
        val children = root.children.map(c => buildTree(c, nodes))
        new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
          root.cost, root.height, children.toArray)
      }
    }
  }
}
