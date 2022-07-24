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
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.{TreeClassifierParams, TreeEnsembleModel}
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> learning algorithm for
 * classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
class RandomForestClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, RandomForestClassifier, RandomForestClassificationModel]
  with RandomForestClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("rfc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  /** @group setParam */
  @Since("1.4.0")
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMinWeightFractionPerNode(value: Double): this.type = set(minWeightFractionPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
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
  @Since("1.4.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.4.0")
  def setImpurity(value: String): this.type = set(impurity, value)

  // Parameters from TreeEnsembleParams:

  /** @group setParam */
  @Since("1.4.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group setParam */
  @Since("1.4.0")
  def setSeed(value: Long): this.type = set(seed, value)

  // Parameters from RandomForestParams:

  /** @group setParam */
  @Since("1.4.0")
  def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group setParam */
  @Since("3.0.0")
  def setBootstrap(value: Boolean): this.type = set(bootstrap, value)

  /** @group setParam */
  @Since("1.4.0")
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

  override protected def train(
      dataset: Dataset[_]): RandomForestClassificationModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses = getNumClasses(dataset, $(labelCol))

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val instances = dataset.select(
      checkClassificationLabels($(labelCol), Some(numClasses)),
      checkNonNegativeWeights(get(weightCol)),
      checkNonNanVectors($(featuresCol))
    ).rdd.map { case Row(l: Double, w: Double, v: Vector) => Instance(l, w, v)
    }.setName("training instances")

    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)
    strategy.bootstrap = $(bootstrap)

    instr.logParams(this, labelCol, featuresCol, weightCol, predictionCol, probabilityCol,
      rawPredictionCol, leafCol, impurity, numTrees, featureSubsetStrategy, maxDepth, maxBins,
      maxMemoryInMB, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed,
      subsamplingRate, thresholds, cacheNodeIds, checkpointInterval, bootstrap)

    val trees = RandomForest
      .run(instances, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeClassificationModel])
    trees.foreach(copyValues(_))

    val numFeatures = trees.head.numFeatures
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)
    createModel(dataset, trees, numFeatures, numClasses)
  }

  private def createModel(
      dataset: Dataset[_],
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int): RandomForestClassificationModel = {
    val model = copyValues(new RandomForestClassificationModel(uid, trees, numFeatures, numClasses))
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)

    val (summaryModel, probabilityColName, predictionColName) = model.findSummaryModel()
    val rfSummary = if (numClasses <= 2) {
      new BinaryRandomForestClassificationTrainingSummaryImpl(
        summaryModel.transform(dataset),
        probabilityColName,
        predictionColName,
        $(labelCol),
        weightColName,
        Array(0.0))
    } else {
      new RandomForestClassificationTrainingSummaryImpl(
        summaryModel.transform(dataset),
        predictionColName,
        $(labelCol),
        weightColName,
        Array(0.0))
    }
    model.setSummary(Some(rfSummary))
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): RandomForestClassifier = defaultCopy(extra)
}

@Since("1.4.0")
object RandomForestClassifier extends DefaultParamsReadable[RandomForestClassifier] {
  /** Accessor for supported impurity settings: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("1.4.0")
  final val supportedFeatureSubsetStrategies: Array[String] =
    TreeEnsembleParams.supportedFeatureSubsetStrategies

  @Since("2.0.0")
  override def load(path: String): RandomForestClassifier = super.load(path)
}

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 *
 * @param _trees  Decision trees in the ensemble.
 *                Warning: These have null parents.
 */
@Since("1.4.0")
class RandomForestClassificationModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel],
    @Since("1.6.0") override val numFeatures: Int,
    @Since("1.5.0") override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, RandomForestClassificationModel]
  with RandomForestClassifierParams with TreeEnsembleModel[DecisionTreeClassificationModel]
  with MLWritable with Serializable
  with HasTrainingSummary[RandomForestClassificationTrainingSummary] {

  require(_trees.nonEmpty, "RandomForestClassificationModel requires at least 1 tree.")

  /**
   * Construct a random forest classification model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int) =
    this(Identifiable.randomUID("rfc"), trees, numFeatures, numClasses)

  @Since("1.4.0")
  override def trees: Array[DecisionTreeClassificationModel] = _trees

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](_trees.length)(1.0)

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false.
   */
  @Since("3.1.0")
  override def summary: RandomForestClassificationTrainingSummary = super.summary

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false or it is a multiclass model.
   */
  @Since("3.1.0")
  def binarySummary: BinaryRandomForestClassificationTrainingSummary = summary match {
    case b: BinaryRandomForestClassificationTrainingSummary => b
    case _ =>
      throw new RuntimeException("Cannot create a binary summary for a non-binary model" +
        s"(numClasses=${numClasses}), use summary instead.")
  }

  /**
   * Evaluates the model on a test dataset.
   *
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("3.1.0")
  def evaluate(dataset: Dataset[_]): RandomForestClassificationSummary = {
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)
    // Handle possible missing or invalid prediction columns
    val (summaryModel, probabilityColName, predictionColName) = findSummaryModel()
    if (numClasses > 2) {
      new RandomForestClassificationSummaryImpl(summaryModel.transform(dataset),
        predictionColName, $(labelCol), weightColName)
    } else {
      new BinaryRandomForestClassificationSummaryImpl(summaryModel.transform(dataset),
        probabilityColName, predictionColName, $(labelCol), weightColName)
    }
  }

  @Since("1.4.0")
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
    _trees.foreach { tree =>
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
        throw new RuntimeException("Unexpected error in RandomForestClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): RandomForestClassificationModel = {
    copyValues(new RandomForestClassificationModel(uid, _trees, numFeatures, numClasses), extra)
      .setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"RandomForestClassificationModel: uid=$uid, numTrees=$getNumTrees, numClasses=$numClasses, " +
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
  @Since("1.5.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Classification, _trees.map(_.toOld))
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new RandomForestClassificationModel.RandomForestClassificationModelWriter(this)
}

@Since("2.0.0")
object RandomForestClassificationModel extends MLReadable[RandomForestClassificationModel] {

  @Since("2.0.0")
  override def read: MLReader[RandomForestClassificationModel] =
    new RandomForestClassificationModelReader

  @Since("2.0.0")
  override def load(path: String): RandomForestClassificationModel = super.load(path)

  private[RandomForestClassificationModel]
  class RandomForestClassificationModelWriter(instance: RandomForestClassificationModel)
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

  private class RandomForestClassificationModelReader
    extends MLReader[RandomForestClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[RandomForestClassificationModel].getName
    private val treeClassName = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): RandomForestClassificationModel = {
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
      require(numTrees == trees.length, s"RandomForestClassificationModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new RandomForestClassificationModel(metadata.uid, trees, numFeatures, numClasses)
      metadata.getAndSetParams(model)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldRandomForestModel,
      parent: RandomForestClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      numFeatures: Int = -1): RandomForestClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeClassificationModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("rfc")
    new RandomForestClassificationModel(uid, newTrees, numFeatures, numClasses)
  }
}

/**
 * Abstraction for multiclass RandomForestClassification results for a given model.
 */
sealed trait RandomForestClassificationSummary extends ClassificationSummary {
  /**
   * Convenient method for casting to BinaryRandomForestClassificationSummary.
   * This method will throw an Exception if the summary is not a binary summary.
   */
  @Since("3.1.0")
  def asBinary: BinaryRandomForestClassificationSummary = this match {
    case b: BinaryRandomForestClassificationSummary => b
    case _ =>
      throw new RuntimeException("Cannot cast to a binary summary.")
  }
}

/**
 * Abstraction for multiclass RandomForestClassification training results.
 */
sealed trait RandomForestClassificationTrainingSummary extends RandomForestClassificationSummary
  with TrainingSummary

/**
 * Abstraction for BinaryRandomForestClassification results for a given model.
 */
sealed trait BinaryRandomForestClassificationSummary extends BinaryClassificationSummary

/**
 * Abstraction for BinaryRandomForestClassification training results.
 */
sealed trait BinaryRandomForestClassificationTrainingSummary extends
  BinaryRandomForestClassificationSummary with RandomForestClassificationTrainingSummary

/**
 * Multiclass RandomForestClassification training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class RandomForestClassificationTrainingSummaryImpl(
    predictions: DataFrame,
    predictionCol: String,
    labelCol: String,
    weightCol: String,
    override val objectiveHistory: Array[Double])
  extends RandomForestClassificationSummaryImpl(
    predictions, predictionCol, labelCol, weightCol)
    with RandomForestClassificationTrainingSummary

/**
 * Multiclass RandomForestClassification results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 */
private class RandomForestClassificationSummaryImpl(
    @transient override val predictions: DataFrame,
    override val predictionCol: String,
    override val labelCol: String,
    override val weightCol: String)
  extends RandomForestClassificationSummary

/**
 * Binary RandomForestClassification training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the probability of each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class BinaryRandomForestClassificationTrainingSummaryImpl(
    predictions: DataFrame,
    scoreCol: String,
    predictionCol: String,
    labelCol: String,
    weightCol: String,
    override val objectiveHistory: Array[Double])
  extends BinaryRandomForestClassificationSummaryImpl(
    predictions, scoreCol, predictionCol, labelCol, weightCol)
    with BinaryRandomForestClassificationTrainingSummary

/**
 * Binary RandomForestClassification for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the prediction of
 *                 each class as a vector.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 */
private class BinaryRandomForestClassificationSummaryImpl(
    predictions: DataFrame,
    override val scoreCol: String,
    predictionCol: String,
    labelCol: String,
    weightCol: String)
  extends RandomForestClassificationSummaryImpl(
    predictions, predictionCol, labelCol, weightCol)
    with BinaryRandomForestClassificationSummary
