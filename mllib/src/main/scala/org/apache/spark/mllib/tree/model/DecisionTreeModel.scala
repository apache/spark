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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * :: Experimental ::
 * Decision tree model for classification or regression.
 * This model stores the decision tree structure and parameters.
 * @param topNode root node
 * @param algo algorithm type -- classification or regression
 */
@Experimental
class DecisionTreeModel(val topNode: Node, val algo: Algo) extends Serializable with Saveable {

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return Double prediction from the trained model
   */
  def predict(features: Vector): Double = {
    topNode.predict(features)
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD of predictions for each of the given data points
   */
  def predict(features: RDD[Vector]): RDD[Double] = {
    features.map(x => predict(x))
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features JavaRDD representing data points to be predicted
   * @return JavaRDD of predictions for each of the given data points
   */
  def predict(features: JavaRDD[Vector]): JavaRDD[Double] = {
    predict(features.rdd)
  }

  /**
   * Get number of nodes in tree, including leaf nodes.
   */
  def numNodes: Int = {
    1 + topNode.numDescendants
  }

  /**
   * Get depth of tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
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
  def toDebugString: String = {
    val header = toString + "\n"
    header + topNode.subtreeToString(2)
  }

  override def save(sc: SparkContext, path: String): Unit = {
    DecisionTreeModel.SaveLoadV1_0.save(sc, path, this)
  }

  override protected def formatVersion: String = "1.0"
}

object DecisionTreeModel extends Loader[DecisionTreeModel] {

  /**
   * Iterator which does a DFS traversal (left to right) of a decision tree.
   *
   * Note: This is private[ml] to permit unit tests.
   */
  private[mllib] class NodeIterator(model: DecisionTreeModel) extends Iterator[Node] {

    /**
     * FILO stack of Nodes during our DFS.
     * The top Node is returned by next().
     * Any Node on the queue is either a leaf or has children whom we have not yet visited.
     * This is empty once all Nodes have been traversed.
     */
    val nodeTrace: mutable.Stack[Node] = new mutable.Stack[Node]()

    nodeTrace.push(model.topNode)

    override def hasNext: Boolean = nodeTrace.nonEmpty

    /**
     * Produces the next element of this iterator.
     * If [[hasNext]] is false, then this throws an exception.
     */
    override def next(): Node = {
      if (nodeTrace.isEmpty) {
        throw new Exception(
          "DecisionTreeModel.NodeIterator.next() was called, but no more elements remain.")
      }
      val n = nodeTrace.pop()
      if (!n.isLeaf) {
        // n is a parent
        nodeTrace.push(n.rightNode.get, n.leftNode.get)
      }
      n
    }
  }

  private object SaveLoadV1_0 {

    def thisFormatVersion = "1.0"

    def thisClassName = "org.apache.spark.mllib.tree.DecisionTreeModel"

    private case class PredictData(predict: Double, prob: Double)

    private object PredictData {
      def apply(p: Predict): PredictData = PredictData(p.predict, p.prob)
    }

    private case class InformationGainStatsData(
        gain: Double,
        impurity: Double,
        leftImpurity: Double,
        rightImpurity: Double,
        leftPredict: PredictData,
        rightPredict: PredictData)

    private object InformationGainStatsData {
      def apply(i: InformationGainStats): InformationGainStatsData = {
        InformationGainStatsData(i.gain, i.impurity, i.leftImpurity, i.rightImpurity,
          PredictData(i.leftPredict), PredictData(i.rightPredict))
      }
    }

    private case class SplitData(
        feature: Int,
        threshold: Double,
        featureType: Int,
        categories: Seq[Double]) // TODO: Change to List once SPARK-3365 is fixed

    private object SplitData {
      def apply(s: Split): SplitData = {
        SplitData(s.feature, s.threshold, s.featureType.id, s.categories)
      }
    }

    /** Model data for model import/export */
    private case class NodeData(
        id: Int,
        predict: PredictData,
        impurity: Double,
        isLeaf: Boolean,
        split: Option[SplitData],
        leftNodeId: Option[Int],
        rightNodeId: Option[Int],
        stats: Option[InformationGainStatsData])

    private object NodeData {
      def apply(n: Node): NodeData = {
        NodeData(n.id, PredictData(n.predict), n.impurity, n.isLeaf, n.split.map(SplitData.apply),
          n.leftNode.map(_.id), n.rightNode.map(_.id), n.stats.map(InformationGainStatsData.apply))
      }
    }

    def save(sc: SparkContext, path: String, model: DecisionTreeModel): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadataRDD =
        sc.parallelize(Seq((thisClassName, thisFormatVersion, model.algo.toString, model.numNodes)))
          .toDataFrame("class", "version", "algo", "numNodes")
      metadataRDD.toJSON.repartition(1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val nodeIterator = new DecisionTreeModel.NodeIterator(model)
      val dataRDD: DataFrame = sc.parallelize(nodeIterator.toSeq).map(NodeData.apply)
      dataRDD.saveAsParquetFile(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String, algo: String, numNodes: Int): DecisionTreeModel = {
      val datapath = Loader.dataPath(path)
      val sqlContext = new SQLContext(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.parquetFile(datapath)
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[NodeData](dataRDD.schema)
      // TODO: Extract save/load for 1 tree so that it can be reused for ensembles?
      val splitsRDD: RDD[Option[Split]] =
        dataRDD.select("split.feature", "split.threshold", "split.featureType", "split.categories")
          .map { row: Row =>
          if (row.isNullAt(0)) {
            None
          } else {
            row match {
              case Row(feature: Int, threshold: Double, featureType: Int, categories: Seq[_]) =>
                // Note: The type cast for categories is safe since we checked the schema.
                Some(Split(feature, threshold, FeatureType(featureType),
                  categories.asInstanceOf[Seq[Double]].toList))
            }
          }
        }
      val lrChildNodesRDD: RDD[Option[(Int, Int)]] =
        dataRDD.select("leftNodeId", "rightNodeId").map { row: Row =>
          if (row.isNullAt(0)) {
            None
          } else {
            row match {
              case Row(leftNodeId: Int, rightNodeId: Int) =>
                Some((leftNodeId, rightNodeId))
            }
          }
        }
      val gainStatsRDD: RDD[Option[InformationGainStats]] = dataRDD.select(
        "stats.gain", "stats.impurity", "stats.leftImpurity", "stats.rightImpurity",
        "stats.leftPredict.predict", "stats.leftPredict.prob",
        "stats.rightPredict.predict", "stats.rightPredict.prob").map { row: Row =>
        if (row.isNullAt(0)) {
          None
        } else {
          row match {
            case Row(gain: Double, impurity: Double, leftImpurity: Double, rightImpurity: Double,
                leftPredictPredict: Double, leftPredictProb: Double,
                rightPredictPredict: Double, rightPredictProb: Double) =>
              Some(new InformationGainStats(gain, impurity, leftImpurity, rightImpurity,
                new Predict(leftPredictPredict, leftPredictProb),
                new Predict(rightPredictPredict, rightPredictProb)))
          }
        }
      }
      // nodesRDD stores (Node, leftChildId, rightChildId) where the child ids are only relevant if
      //   Node.isLeaf == false
      val nodesRDD: RDD[(Node, Int, Int)] =
        dataRDD.select("id", "predict.predict", "predict.prob", "impurity", "isLeaf").rdd
          .zip(splitsRDD).zip(lrChildNodesRDD).zip(gainStatsRDD).map {
          case (((Row(id: Int, predictPredict: Double, predictProb: Double,
              impurity: Double, isLeaf: Boolean),
              split: Option[Split]), lrChildNodes: Option[(Int, Int)]),
              gainStats: Option[InformationGainStats]) =>
            val (leftChildId, rightChildId) = lrChildNodes.getOrElse((-1, -1))
            (new Node(id, new Predict(predictPredict, predictProb), impurity, isLeaf,
              split, None, None, gainStats),
              leftChildId, rightChildId)
        }
      // Collect tree nodes, and build them into a tree.
      //  nodesMap: node id -> (node, leftChild, rightChild)
      val nodesMap: Map[Int, (Node, Int, Int)] = nodesRDD.collect().map(n => n._1.id -> n).toMap
      assert(nodesMap.contains(1),
        s"DecisionTree missing root node (id = 1) after loading from: $datapath")
      val topNode = nodesMap(1)
      linkSubtree(topNode._1, topNode._2, topNode._3, nodesMap)
      assert(nodesMap.size == numNodes,
        s"Unable to load DecisionTreeModel data from: $datapath." +
        s"  Expected $numNodes nodes but found ${nodesMap.size}")
      new DecisionTreeModel(topNode._1, Algo.fromString(algo))
    }
  }

  /**
   * Link the given node to its children (if any), and recurse down the subtree.
   * @param node  Node to link.  Node.leftNode and Node.rightNode will be set if there are children.
   * @param leftChildId  Id of left child.  Ignored if node is a leaf.
   * @param rightChildId  Id of right child.  Ignored if node is a leaf.
   * @param nodesMap  Map storing all nodes as a map: node id -> (Node, leftChildId, rightChildId).
   */
  private def linkSubtree(
      node: Node,
      leftChildId: Int,
      rightChildId: Int,
      nodesMap: Map[Int, (Node, Int, Int)]): Unit = {
    if (node.isLeaf) return
    assert(nodesMap.contains(leftChildId),
      s"DecisionTreeModel.load could not find child (id=$leftChildId) of node ${node.id}.")
    assert(nodesMap.contains(rightChildId),
      s"DecisionTreeModel.load could not find child (id=$rightChildId) of node ${node.id}.")
    val leftChild = nodesMap(leftChildId)
    val rightChild = nodesMap(rightChildId)
    node.leftNode = Some(leftChild._1)
    node.rightNode = Some(rightChild._1)
    linkSubtree(leftChild._1, leftChild._2, leftChild._3, nodesMap)
    linkSubtree(rightChild._1, rightChild._2, rightChild._3, nodesMap)
  }

  override def load(sc: SparkContext, path: String): DecisionTreeModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    val (algo: String, numNodes: Int) = try {
      val algo_numNodes = metadata.select("algo", "numNodes").collect()
      assert(algo_numNodes.length == 1)
      algo_numNodes(0) match {
        case Row(a: String, n: Int) => (a, n)
      }
    } catch {
      // Catch both Error and Exception since the checks above can throw either.
      case e: Throwable =>
        throw new Exception(
          s"Unable to load DecisionTreeModel metadata from: ${Loader.metadataPath(path)}."
          + s"  Error message: ${e.getMessage}")
    }
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
