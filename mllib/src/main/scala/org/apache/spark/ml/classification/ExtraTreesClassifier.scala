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

import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.{TreeClassifierParams, TreeEnsembleModel}
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType


/**
 * <a href="https://en.wikipedia.org/wiki/Random_forest#ExtraTrees">Extra Trees</a> learning
 * algorithm for classification. It is an ensemble of individual
 * <a href="https://orbi.uliege.be/bitstream/2268/9357/1/geurts-mlj-advance.pdf">extremely
 * randomized trees</a>.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("3.0.0")
class ExtraTreesClassifier (override val uid: String)
  extends ProbabilisticClassifier[Vector, ExtraTreesClassifier, ExtraTreesClassificationModel]
  with ExtraTreesClassifierParams with DefaultParamsWritable {

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("etc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  /** @group setParam */
  @Since("3.0.0")
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMinWeightFractionPerNode(value: Double): this.type = set(minWeightFractionPerNode, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("3.0.0")
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("3.0.0")
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("3.0.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("3.0.0")
  def setImpurity(value: String): this.type = set(impurity, value)

  // Parameters from TreeEnsembleParams:

  /** @group setParam */
  @Since("3.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  // Parameters from RandomForestParams:

  /** @group setParam */
  @Since("3.0.0")
  def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group setParam */
  @Since("3.0.0")
  def setBootstrap(value: Boolean): this.type = set(bootstrap, value)

  /** @group setParam */
  @Since("3.0.0")
  def setFeatureSubsetStrategy(value: String): this.type =
    set(featureSubsetStrategy, value)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * By default the weightCol is not set, so all instances have weight 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  // Parameters from ExtraTreesParams:

  /** @group expertSetParam */
  @Since("3.0.0")
  def setNumRandomSplitsPerFeature(value: Int): this.type =
    set(numRandomSplitsPerFeature, value)

  override protected def train(
      dataset: Dataset[_]): ExtraTreesClassificationModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = getNumClasses(dataset)

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val instances = extractInstances(dataset, numClasses)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)
    strategy.bootstrap = $(bootstrap)
    strategy.numRandomSplitsPerFeature = $(numRandomSplitsPerFeature)

    instr.logParams(this, labelCol, featuresCol, weightCol, predictionCol, probabilityCol,
      rawPredictionCol, leafCol, impurity, numTrees, featureSubsetStrategy, maxDepth, maxBins,
      maxMemoryInMB, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed,
      subsamplingRate, thresholds, cacheNodeIds, checkpointInterval, bootstrap,
      numRandomSplitsPerFeature)

    val trees = RandomForest
      .run(instances, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeClassificationModel])
    trees.foreach(copyValues(_))

    val numFeatures = trees.head.numFeatures
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)
    new ExtraTreesClassificationModel(uid, trees, numFeatures, numClasses)
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): ExtraTreesClassifier = defaultCopy(extra)
}

@Since("3.0.0")
object ExtraTreesClassifier extends DefaultParamsReadable[ExtraTreesClassifier] {
  /** Accessor for supported impurity settings: entropy, gini */
  @Since("3.0.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("3.0.0")
  final val supportedFeatureSubsetStrategies: Array[String] =
    TreeEnsembleParams.supportedFeatureSubsetStrategies

  @Since("3.0.0")
  override def load(path: String): ExtraTreesClassifier = super.load(path)
}

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 *
 * @param _trees  Decision trees in the ensemble.
 *                Warning: These have null parents.
 */
@Since("3.0.0")
class ExtraTreesClassificationModel private[ml] (
    override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel],
    override val numFeatures: Int,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, ExtraTreesClassificationModel]
  with ExtraTreesClassifierParams with TreeEnsembleModel[DecisionTreeClassificationModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "ExtraTreesClassificationModel requires at least 1 tree.")

  /**
   * Construct a extra trees classification model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int) =
    this(Identifiable.randomUID("etc"), trees, numFeatures, numClasses)

  @Since("3.0.0")
  override def trees: Array[DecisionTreeClassificationModel] = _trees

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](_trees.length)(1.0)

  @Since("3.0.0")
  override def treeWeights: Array[Double] = _treeWeights

  @Since("3.0.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = super.transformSchema(schema)
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.updateField(outputSchema, getLeafField($(leafCol)))
    }
    outputSchema
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val outputData = super.transform(dataset)
    if ($(leafCol).nonEmpty) {
      val leafUDF = udf { features: Vector => predictLeaf(features) }
      outputData.withColumn($(leafCol), leafUDF(col($(featuresCol))),
        outputSchema($(leafCol)).metadata)
    } else {
      outputData
    }
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = {
    // TODO: When we add a generic Bagging class, handle transform there: SPARK-7128
    // Classifies using majority votes.
    // Ignore the tree weights since all are 1.0 for now.
    val votes = Array.ofDim[Double](numClasses)
    _trees.view.foreach { tree =>
      val classCounts = tree.rootNode.predictImpl(features).impurityStats.stats
      val total = classCounts.sum
      if (total != 0) {
        var i = 0
        while (i < numClasses) {
          votes(i) += classCounts(i) / total
          i += 1
        }
      }
    }
    Vectors.dense(votes)
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in ExtraTreesClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): ExtraTreesClassificationModel = {
    copyValues(new ExtraTreesClassificationModel(uid, _trees, numFeatures, numClasses), extra)
      .setParent(parent)
  }

  @Since("3.0.0")
  override def toString: String = {
    s"ExtraTreesClassificationModel: uid=$uid, numTrees=$getNumTrees, numClasses=$numClasses, " +
      s"numFeatures=$numFeatures"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * @see `DecisionTreeClassificationModel.featureImportances`
   */
  @Since("3.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  @Since("3.0.0")
  override def write: MLWriter =
    new ExtraTreesClassificationModel.ExtraTreesClassificationModelWriter(this)
}

@Since("3.0.0")
object ExtraTreesClassificationModel extends MLReadable[ExtraTreesClassificationModel] {

  @Since("3.0.0")
  override def read: MLReader[ExtraTreesClassificationModel] =
    new ExtraTreesClassificationModelReader

  @Since("3.0.0")
  override def load(path: String): ExtraTreesClassificationModel = super.load(path)

  private[ExtraTreesClassificationModel]
  class ExtraTreesClassificationModelWriter(instance: ExtraTreesClassificationModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Note: numTrees is not currently used, but could be nice to store for fast querying.
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numClasses" -> instance.numClasses,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class ExtraTreesClassificationModelReader
    extends MLReader[ExtraTreesClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ExtraTreesClassificationModel].getName
    private val treeClassName = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): ExtraTreesClassificationModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], _) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeClassificationModel] = treesData.map {
        case (treeMetadata, root) =>
          val tree =
            new DecisionTreeClassificationModel(treeMetadata.uid, root, numFeatures, numClasses)
          treeMetadata.getAndSetParams(tree)
          tree
      }
      require(numTrees == trees.length, s"ExtraTreesClassificationModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new ExtraTreesClassificationModel(metadata.uid, trees, numFeatures, numClasses)
      metadata.getAndSetParams(model)
      model
    }
  }
}
