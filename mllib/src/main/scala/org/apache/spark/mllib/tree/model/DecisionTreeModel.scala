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

package org.apache.spark.mllib.tree.model

import scala.collection.mutable

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.util.Utils

/**
 * Decision tree model for classification or regression.
 * This model stores the decision tree structure and parameters.
 * @param topNode root node
 * @param algo algorithm type -- classification or regression
 */
@Since("1.0.0")
class DecisionTreeModel @Since("1.0.0") (
    @Since("1.0.0") val topNode: Node,
    @Since("1.0.0") val algo: Algo) extends Serializable with Saveable {

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return Double prediction from the trained model
   */
  @Since("1.0.0")
  def predict(features: Vector): Double = {
    topNode.predict(features)
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD of predictions for each of the given data points
   */
  @Since("1.0.0")
  def predict(features: RDD[Vector]): RDD[Double] = {
    features.map(x => predict(x))
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features JavaRDD representing data points to be predicted
   * @return JavaRDD of predictions for each of the given data points
   */
  @Since("1.2.0")
  def predict(features: JavaRDD[Vector]): JavaRDD[java.lang.Double] = {
    predict(features.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Double]]
  }

  /**
   * Get number of nodes in tree, including leaf nodes.
   */
  @Since("1.1.0")
  def numNodes: Int = {
    1 + topNode.numDescendants
  }

  /**
   * Get depth of tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
  @Since("1.1.0")
  def depth: Int = {
    topNode.subtreeDepth
  }

  /**
   * Print a summary of the model.
   */
  override def toString: String = algo match {
    case Classification =>
      s"DecisionTreeModel classifier of depth $depth with $numNodes nodes"
    case Regression =>
      s"DecisionTreeModel regressor of depth $depth with $numNodes nodes"
    case _ => throw new IllegalArgumentException(
      s"DecisionTreeModel given unknown algo parameter: $algo.")
  }

  /**
   * Print the full model to a string.
   */
  @Since("1.2.0")
  def toDebugString: String = {
    val header = toString + "\n"
    header + topNode.subtreeToString(2)
  }

  /**
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    DecisionTreeModel.SaveLoadV1_0.save(sc, path, this)
  }

  override protected def formatVersion: String = DecisionTreeModel.formatVersion
}

@Since("1.3.0")
object DecisionTreeModel extends Loader[DecisionTreeModel] with Logging {

  private[spark] def formatVersion: String = "1.0"

  private[tree] object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    // Hard-code class name string in case it changes in the future
    def thisClassName: String = "org.apache.spark.mllib.tree.DecisionTreeModel"

    case class PredictData(predict: Double, prob: Double) {
      def toPredict: Predict = new Predict(predict, prob)
    }

    object PredictData {
      def apply(p: Predict): PredictData = PredictData(p.predict, p.prob)

      def apply(r: Row): PredictData = PredictData(r.getDouble(0), r.getDouble(1))
    }

    case class SplitData(
        feature: Int,
        threshold: Double,
        featureType: Int,
        categories: Seq[Double]) {
      def toSplit: Split = {
        new Split(feature, threshold, FeatureType(featureType), categories.toList)
      }
    }

    object SplitData {
      def apply(s: Split): SplitData = {
        SplitData(s.feature, s.threshold, s.featureType.id, s.categories)
      }

      def apply(r: Row): SplitData = {
        SplitData(r.getInt(0), r.getDouble(1), r.getInt(2), r.getAs[Seq[Double]](3))
      }
    }

    /** Model data for model import/export */
    case class NodeData(
        treeId: Int,
        nodeId: Int,
        predict: PredictData,
        impurity: Double,
        isLeaf: Boolean,
        split: Option[SplitData],
        leftNodeId: Option[Int],
        rightNodeId: Option[Int],
        infoGain: Option[Double])

    object NodeData {
      def apply(treeId: Int, n: Node): NodeData = {
        NodeData(treeId, n.id, PredictData(n.predict), n.impurity, n.isLeaf,
          n.split.map(SplitData.apply), n.leftNode.map(_.id), n.rightNode.map(_.id),
          n.stats.map(_.gain))
      }

      def apply(r: Row): NodeData = {
        val split = if (r.isNullAt(5)) None else Some(SplitData(r.getStruct(5)))
        val leftNodeId = if (r.isNullAt(6)) None else Some(r.getInt(6))
        val rightNodeId = if (r.isNullAt(7)) None else Some(r.getInt(7))
        val infoGain = if (r.isNullAt(8)) None else Some(r.getDouble(8))
        NodeData(r.getInt(0), r.getInt(1), PredictData(r.getStruct(2)), r.getDouble(3),
          r.getBoolean(4), split, leftNodeId, rightNodeId, infoGain)
      }
    }

    def save(sc: SparkContext, path: String, model: DecisionTreeModel): Unit = {
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._

      // SPARK-6120: We do a hacky check here so users understand why save() is failing
      //             when they run the ML guide example.
      // TODO: Fix this issue for real.
      val memThreshold = 768
      if (sc.isLocal) {
        val driverMemory = sc.getConf.getOption("spark.driver.memory")
          .orElse(Option(System.getenv("SPARK_DRIVER_MEMORY")))
          .map(Utils.memoryStringToMb)
          .getOrElse(Utils.DEFAULT_DRIVER_MEM_MB)
        if (driverMemory <= memThreshold) {
          logWarning(s"$thisClassName.save() was called, but it may fail because of too little" +
            s" driver memory (${driverMemory}m)." +
            s"  If failure occurs, try setting driver-memory ${memThreshold}m (or larger).")
        }
      } else {
        if (sc.executorMemory <= memThreshold) {
          logWarning(s"$thisClassName.save() was called, but it may fail because of too little" +
            s" executor memory (${sc.executorMemory}m)." +
            s"  If failure occurs try setting executor-memory ${memThreshold}m (or larger).")
        }
      }

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("algo" -> model.algo.toString) ~ ("numNodes" -> model.numNodes)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val nodes = model.topNode.subtreeIterator.toSeq
      val dataRDD: DataFrame = sc.parallelize(nodes)
        .map(NodeData.apply(0, _))
        .toDF()
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String, algo: String, numNodes: Int): DecisionTreeModel = {
      val datapath = Loader.dataPath(path)
      val sqlContext = SQLContext.getOrCreate(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.read.parquet(datapath)
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[NodeData](dataRDD.schema)
      val nodes = dataRDD.rdd.map(NodeData.apply)
      // Build node data into a tree.
      val trees = constructTrees(nodes)
      assert(trees.length == 1,
        "Decision tree should contain exactly one tree but got ${trees.size} trees.")
      val model = new DecisionTreeModel(trees(0), Algo.fromString(algo))
      assert(model.numNodes == numNodes, s"Unable to load DecisionTreeModel data from: $datapath." +
        s" Expected $numNodes nodes but found ${model.numNodes}")
      model
    }

    def constructTrees(nodes: RDD[NodeData]): Array[Node] = {
      val trees = nodes
        .groupBy(_.treeId)
        .mapValues(_.toArray)
        .collect()
        .map { case (treeId, data) =>
          (treeId, constructTree(data))
        }.sortBy(_._1)
      val numTrees = trees.length
      val treeIndices = trees.map(_._1).toSeq
      assert(treeIndices == (0 until numTrees),
        s"Tree indices must start from 0 and increment by 1, but we found $treeIndices.")
      trees.map(_._2)
    }

    /**
     * Given a list of nodes from a tree, construct the tree.
     * @param data array of all node data in a tree.
     */
    def constructTree(data: Array[NodeData]): Node = {
      val dataMap: Map[Int, NodeData] = data.map(n => n.nodeId -> n).toMap
      assert(dataMap.contains(1),
        s"DecisionTree missing root node (id = 1).")
      constructNode(1, dataMap, mutable.Map.empty)
    }

    /**
     * Builds a node from the node data map and adds new nodes to the input nodes map.
     */
    private def constructNode(
      id: Int,
      dataMap: Map[Int, NodeData],
      nodes: mutable.Map[Int, Node]): Node = {
      if (nodes.contains(id)) {
        return nodes(id)
      }
      val data = dataMap(id)
      val node =
        if (data.isLeaf) {
          Node(data.nodeId, data.predict.toPredict, data.impurity, data.isLeaf)
        } else {
          val leftNode = constructNode(data.leftNodeId.get, dataMap, nodes)
          val rightNode = constructNode(data.rightNodeId.get, dataMap, nodes)
          val stats = new InformationGainStats(data.infoGain.get, data.impurity, leftNode.impurity,
            rightNode.impurity, leftNode.predict, rightNode.predict)
          new Node(data.nodeId, data.predict.toPredict, data.impurity, data.isLeaf,
            data.split.map(_.toSplit), Some(leftNode), Some(rightNode), Some(stats))
        }
      nodes += node.id -> node
      node
    }
  }

  /**
   *
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  @Since("1.3.0")
  override def load(sc: SparkContext, path: String): DecisionTreeModel = {
    implicit val formats = DefaultFormats
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    val algo = (metadata \ "algo").extract[String]
    val numNodes = (metadata \ "numNodes").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        SaveLoadV1_0.load(sc, path, algo, numNodes)
      case _ => throw new Exception(
        s"DecisionTreeModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}
