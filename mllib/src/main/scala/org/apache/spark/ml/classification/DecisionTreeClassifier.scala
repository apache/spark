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

package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path
import org.json4s.JsonAST.JValue

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] learning algorithm
 * for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
@Experimental
final class DecisionTreeClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, DecisionTreeClassifier, DecisionTreeClassificationModel]
  with DecisionTreeParams with TreeClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtc"))

  // Override parameter setters from parent trait for Java API compatibility.

  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = super.setMaxDepth(value)

  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = super.setMaxBins(value)

  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type =
    super.setMinInstancesPerNode(value)

  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = super.setMinInfoGain(value)

  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = super.setMaxMemoryInMB(value)

  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = super.setCacheNodeIds(value)

  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = super.setCheckpointInterval(value)

  @Since("1.4.0")
  override def setImpurity(value: String): this.type = super.setImpurity(value)

  @Since("1.6.0")
  override def setSeed(value: Long): this.type = super.setSeed(value)

  override protected def train(dataset: DataFrame): DecisionTreeClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None => throw new IllegalArgumentException("DecisionTreeClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
        // TODO: Automatically index labels: SPARK-7126
    }
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy = getOldStrategy(categoricalFeatures, numClasses)
    val trees = RandomForest.run(oldDataset, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), parentUID = Some(uid))
    trees.head.asInstanceOf[DecisionTreeClassificationModel]
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity,
      subsamplingRate = 1.0)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}

@Since("1.4.0")
@Experimental
object DecisionTreeClassifier extends DefaultParamsReadable[DecisionTreeClassifier] {
  /** Accessor for supported impurities: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  @Since("1.6.0")
  override def load(path: String): DecisionTreeClassifier = super.load(path)
}

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
@Experimental
final class DecisionTreeClassificationModel private[ml] (
    @Since("1.4.0")override val uid: String,
    @Since("1.4.0")override val rootNode: Node,
    @Since("1.6.0")override val numFeatures: Int,
    @Since("1.5.0")override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, DecisionTreeClassificationModel]
  with DecisionTreeModel with MLWritable with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  /**
   * Construct a decision tree classification model.
   * @param rootNode  Root node of tree, with other nodes attached.
   */
  private[ml] def this(rootNode: Node, numFeatures: Int, numClasses: Int) =
    this(Identifiable.randomUID("dtc"), rootNode, numFeatures, numClasses)

  override protected def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }

  override protected def predictRaw(features: Vector): Vector = {
    Vectors.dense(rootNode.predictImpl(features).impurityStats.stats.clone())
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in DecisionTreeClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeClassificationModel = {
    copyValues(new DecisionTreeClassificationModel(uid, rootNode, numFeatures, numClasses), extra)
      .setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"DecisionTreeClassificationModel (uid=$uid) of depth $depth with $numNodes nodes"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * This feature importance is calculated as follows:
   *   - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *     where gain is scaled by the number of instances passing through node
   *   - Normalize importances for tree to sum to 1.
   *
   * Note: Feature importance for single decision trees can have high variance due to
   *       correlated predictor variables. Consider using a [[RandomForestClassifier]]
   *       to determine feature importance instead.
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector = RandomForest.featureImportances(this, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Classification)
  }

  @Since("1.6.0")
  override def write: MLWriter =
    new DecisionTreeClassificationModel.DecisionTreeClassificationModelWriter(this)
}

@Since("1.6.0")
object DecisionTreeClassificationModel extends MLReadable[DecisionTreeClassificationModel] {

  @Since("1.6.0")
  override def read: MLReader[DecisionTreeClassificationModel] =
    new DecisionTreeClassificationModelReader

  @Since("1.6.0")
  override def load(path: String): DecisionTreeClassificationModel = super.load(path)

  private[DecisionTreeClassificationModel]
  class DecisionTreeClassificationModelWriter(instance: DecisionTreeClassificationModel)
    extends MLWriter {

    import org.json4s.jackson.JsonMethods._
    import org.json4s.JsonDSL._

    /**
     * Info for a [[org.apache.spark.ml.tree.Split]]
     * @param featureIndex  Index of feature split on
     * @param leftCategoriesOrThreshold  For categorical feature, set of leftCategories.
     *                                   For continuous feature, threshold.
     * @param numCategories  For categorical feature, number of categories.
     *                       For continuous feature, -1.
     */
    private[ml] case class SplitData(
      featureIndex: Int,
      leftCategoriesOrThreshold: Array[Double],
      numCategories: Int)

    private[ml] object SplitData {
      def apply(split: Split): SplitData = split match {
        case s: CategoricalSplit =>
          SplitData(s.featureIndex, s.leftCategories, s.numCategories)
        case s: ContinuousSplit =>
          SplitData(s.featureIndex, Array(s.threshold), -1)
      }
    }

    /**
     * Info for a [[Node]]
     * @param id  Index used for tree reconstruction.  Indices follow an in-order traversal.
     * @param impurityStats  Stats array.  Impurity type is stored in metadata.
     * @param gain  Gain, or arbitrary value if leaf node.
     * @param leftChild  Left child index, or arbitrary value if leaf node.
     * @param rightChild  Right child index, or arbitrary value if leaf node.
     * @param split  Split info, or arbitrary value if leaf node.
     */
    private[ml] case class NodeData(
        id: Int,
        prediction: Double,
        impurity: Double,
        impurityStats: Array[Double],
        gain: Double,
        leftChild: Int,
        rightChild: Int,
        split: SplitData)

    private[ml] object NodeData {
      /**
       * Create [[NodeData]] instances for this node and all children.
       * @param id  Current ID.  IDs are assigned via an in-order traversal.
       * @return (sequence of nodes in preorder traversal order, largest ID in subtree)
       *         The nodes are returned in preorder traversal (root first) so that it is easy to
       *         get the ID of the subtree's root node.
       */
      def build(node: Node, id: Int): (Seq[NodeData], Int) = node match {
        case n: InternalNode =>
          val (leftNodeData, leftIdx) = build(n.leftChild, id)
          val (rightNodeData, rightIdx) = build(n.rightChild, leftIdx + 2)
          val thisNodeData = NodeData(leftIdx + 1, n.prediction, n.impurity, n.impurityStats.stats,
            n.gain, leftNodeData.head.id, rightNodeData.head.id, SplitData(n.split))
          (thisNodeData +: (leftNodeData ++ rightNodeData), rightIdx)
        case _: LeafNode =>
          (Seq(NodeData(id, node.prediction, node.impurity, node.impurityStats.stats,
            -1.0, -1, -1, SplitData(-1, Array.empty[Double], -1))),
            id)
      }

      def reconstruct(df: DataFrame): Node = {
        val nodeDatas = df.select("id", "prediction", "impurity", "impurityStats", "gain",
          "leftChild", "rightChild",
          "split.featureIndex", "split.leftCategoriesOrThreshold", "split.numCategories")
          .map {
            case Row(id: Int, prediction: Double, impurity: Double, impurityStats: Seq[Double],
                gain: Double, leftChild: Int, rightChild: Int, splitFeatureIndex: Int,
                splitLeftCategoriesOrThreshold: Seq[Double], splitNumCategories: Int) =>
              
          }
      }
    }

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JValue = render(Map(
        "numFeatures" -> instance.numFeatures,
        "numClasses" -> instance.numClasses))
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val (nodeData, _) = NodeData.build(instance.rootNode, 0)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(nodeData).write.parquet(dataPath)
    }
  }

  private class DecisionTreeClassificationModelReader
    extends MLReader[DecisionTreeClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): DecisionTreeClassificationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.format("parquet").load(dataPath)

    }
  }

  /** (private[ml]) Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldDecisionTreeModel,
      parent: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1): DecisionTreeClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification DecisionTreeModel (old API) to" +
        s" DecisionTreeClassificationModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("dtc")
    // Can't infer number of features from old model, so default to -1
    new DecisionTreeClassificationModel(uid, rootNode, numFeatures, -1)
  }
}
