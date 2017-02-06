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
import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.{DecisionTreeModel, Node, TreeClassifierParams}
import org.apache.spark.ml.tree.DecisionTreeModelReadWrite._
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

/**
 * Decision tree learning algorithm (http://en.wikipedia.org/wiki/Decision_tree_learning)
 * for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
class DecisionTreeClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, DecisionTreeClassifier, DecisionTreeClassificationModel]
  with DecisionTreeClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtc"))

  // Override parameter setters from parent trait for Java API compatibility.

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("2.2.0")
  def setMinWeightFractionPerNode(value: Double): this.type = set(minWeightFractionPerNode, value)

  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = set(impurity, value)

  /** @group setParam */
  @Since("1.6.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  override protected def train(dataset: Dataset[_]): DecisionTreeClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = getNumClasses(dataset)

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }
    require(numClasses > 0, s"DecisionTreeClassifier (in extractLabeledPoints) found numClasses =" +
      s" $numClasses, but requires numClasses > 0.")
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          require(label % 1 == 0 && label >= 0 && label < numClasses, s"Classifier was given" +
            s" dataset with invalid label $label.  Labels must be integers in range" +
            s" [0, $numClasses).")
          Instance(label, weight, features)
      }
    val strategy = getOldStrategy(categoricalFeatures, numClasses)

    val instr = Instrumentation.create(this, instances)
    instr.logParams(params: _*)

    val trees = RandomForest.run(instances, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeClassificationModel]
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Train a decision tree on an RDD */
  private[ml] def train(data: RDD[LabeledPoint],
      oldStrategy: OldStrategy): DecisionTreeClassificationModel = {

    val instances = data.map(_.toInstance(1.0))
    val instr = Instrumentation.create(this, instances)
    instr.logParams(params: _*)
    val trees = RandomForest.run(instances, oldStrategy, numTrees = 1,
      featureSubsetStrategy = "all", seed = 0L, instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeClassificationModel]
    instr.logSuccess(m)
    m
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
object DecisionTreeClassifier extends DefaultParamsReadable[DecisionTreeClassifier] {
  /** Accessor for supported impurities: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  @Since("2.0.0")
  override def load(path: String): DecisionTreeClassifier = super.load(path)
}

/**
 * Decision tree model (http://en.wikipedia.org/wiki/Decision_tree_learning) for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
class DecisionTreeClassificationModel private[ml] (
    @Since("1.4.0")override val uid: String,
    @Since("1.4.0")override val rootNode: Node,
    @Since("1.6.0")override val numFeatures: Int,
    @Since("1.5.0")override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, DecisionTreeClassificationModel]
  with DecisionTreeModel with DecisionTreeClassifierParams with MLWritable with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  /**
   * Construct a decision tree classification model.
   *
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
   * @note Feature importance for single decision trees can have high variance due to
   * correlated predictor variables. Consider using a [[RandomForestClassifier]]
   * to determine feature importance instead.
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(this, numFeatures)

  /** Convert to spark.mllib DecisionTreeModel (losing some information) */
  override private[spark] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Classification)
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new DecisionTreeClassificationModel.DecisionTreeClassificationModelWriter(this)
}

@Since("2.0.0")
object DecisionTreeClassificationModel extends MLReadable[DecisionTreeClassificationModel] {

  @Since("2.0.0")
  override def read: MLReader[DecisionTreeClassificationModel] =
    new DecisionTreeClassificationModelReader

  @Since("2.0.0")
  override def load(path: String): DecisionTreeClassificationModel = super.load(path)

  private[DecisionTreeClassificationModel]
  class DecisionTreeClassificationModelWriter(instance: DecisionTreeClassificationModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numClasses" -> instance.numClasses)
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val (nodeData, _) = NodeData.build(instance.rootNode, 0)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(nodeData).write.parquet(dataPath)
    }
  }

  private class DecisionTreeClassificationModelReader
    extends MLReader[DecisionTreeClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): DecisionTreeClassificationModel = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val root = loadTreeNodes(path, metadata, sparkSession)
      val model = new DecisionTreeClassificationModel(metadata.uid, root, numFeatures, numClasses)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
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
