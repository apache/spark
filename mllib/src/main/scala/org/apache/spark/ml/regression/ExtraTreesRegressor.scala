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

package org.apache.spark.ml.regression

import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

/**
 * <a href="https://en.wikipedia.org/wiki/Random_forest#ExtraTrees">Extra Trees</a> learning
 * algorithm for regression. It is an ensemble of individual
 * <a href="https://orbi.uliege.be/bitstream/2268/9357/1/geurts-mlj-advance.pdf">extremely
 * randomized trees</a>.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("3.0.0")
class ExtraTreesRegressor (override val uid: String)
  extends Regressor[Vector, ExtraTreesRegressor, ExtraTreesRegressionModel]
  with ExtraTreesRegressorParams with DefaultParamsWritable {

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("etr"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

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
      dataset: Dataset[_]): ExtraTreesRegressionModel = instrumented { instr =>
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))

    val instances = extractInstances(dataset)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity)
    strategy.bootstrap = $(bootstrap)
    strategy.numRandomSplitsPerFeature = $(numRandomSplitsPerFeature)

    instr.logPipelineStage(this)
    instr.logDataset(instances)
    instr.logParams(this, labelCol, featuresCol, weightCol, predictionCol, leafCol, impurity,
      numTrees, featureSubsetStrategy, maxDepth, maxBins, maxMemoryInMB, minInfoGain,
      minInstancesPerNode, minWeightFractionPerNode, seed, subsamplingRate, cacheNodeIds,
      checkpointInterval, bootstrap, numRandomSplitsPerFeature)

    val trees = RandomForest
      .run(instances, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeRegressionModel])
    trees.foreach(copyValues(_))

    val numFeatures = trees.head.numFeatures
    instr.logNamedValue(Instrumentation.loggerTags.numFeatures, numFeatures)
    new ExtraTreesRegressionModel(uid, trees, numFeatures)
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): ExtraTreesRegressor = defaultCopy(extra)
}

@Since("3.0.0")
object ExtraTreesRegressor extends DefaultParamsReadable[ExtraTreesRegressor]{
  /** Accessor for supported impurity settings: variance */
  @Since("3.0.0")
  final val supportedImpurities: Array[String] = HasVarianceImpurity.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("3.0.0")
  final val supportedFeatureSubsetStrategies: Array[String] =
    TreeEnsembleParams.supportedFeatureSubsetStrategies

  @Since("3.0.0")
  override def load(path: String): ExtraTreesRegressor = super.load(path)

}

/**
 * <a href="https://en.wikipedia.org/wiki/Random_forest#ExtraTrees">Extra Trees</a> learning
 * algorithm for regression. It is an ensemble of individual
 * <a href="https://orbi.uliege.be/bitstream/2268/9357/1/geurts-mlj-advance.pdf">extremely
 * randomized trees</a>.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 * @param _trees  Decision trees in the ensemble.
 * @param numFeatures  Number of features used by this model
 */
@Since("3.0.0")
class ExtraTreesRegressionModel private[ml] (
    override val uid: String,
    private val _trees: Array[DecisionTreeRegressionModel],
    override val numFeatures: Int)
  extends RegressionModel[Vector, ExtraTreesRegressionModel]
  with ExtraTreesRegressorParams with TreeEnsembleModel[DecisionTreeRegressionModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "ExtraTreesRegressionModel requires at least 1 tree.")

  /**
   * Construct a random forest regression model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(trees: Array[DecisionTreeRegressionModel], numFeatures: Int) =
    this(Identifiable.randomUID("etr"), trees, numFeatures)

  @Since("3.0.0")
  override def trees: Array[DecisionTreeRegressionModel] = _trees

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

    var predictionColNames = Seq.empty[String]
    var predictionColumns = Seq.empty[Column]

    val bcastModel = dataset.sparkSession.sparkContext.broadcast(this)

    if ($(predictionCol).nonEmpty) {
      val predictUDF = udf { features: Vector => bcastModel.value.predict(features) }
      predictionColNames :+= $(predictionCol)
      predictionColumns :+= predictUDF(col($(featuresCol)))
        .as($(predictionCol), outputSchema($(predictionCol)).metadata)
    }

    if ($(leafCol).nonEmpty) {
      val leafUDF = udf { features: Vector => bcastModel.value.predictLeaf(features) }
      predictionColNames :+= $(leafCol)
      predictionColumns :+= leafUDF(col($(featuresCol)))
        .as($(leafCol), outputSchema($(leafCol)).metadata)
    }

    if (predictionColNames.nonEmpty) {
      dataset.withColumns(predictionColNames, predictionColumns)
    } else {
      this.logWarning(s"$uid: ExtraTreesRegressionModel.transform() does nothing" +
        " because no output columns were set.")
      dataset.toDF()
    }
  }

  override def predict(features: Vector): Double = {
    // TODO: When we add a generic Bagging class, handle transform there.  SPARK-7128
    // Predict average of tree predictions.
    // Ignore the weights since all are 1.0 for now.
    _trees.map(_.rootNode.predictImpl(features).prediction).sum / getNumTrees
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): ExtraTreesRegressionModel = {
    copyValues(new ExtraTreesRegressionModel(uid, _trees, numFeatures), extra).setParent(parent)
  }

  @Since("3.0.0")
  override def toString: String = {
    s"ExtraTreesRegressionModel: uid=$uid, numTrees=$getNumTrees, numFeatures=$numFeatures"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * @see `DecisionTreeRegressionModel.featureImportances`
   */
  @Since("3.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  @Since("3.0.0")
  override def write: MLWriter =
    new ExtraTreesRegressionModel.ExtraTreesRegressionModelWriter(this)
}

@Since("3.0.0")
object ExtraTreesRegressionModel extends MLReadable[ExtraTreesRegressionModel] {

  @Since("3.0.0")
  override def read: MLReader[ExtraTreesRegressionModel] = new ExtraTreesRegressionModelReader

  @Since("3.0.0")
  override def load(path: String): ExtraTreesRegressionModel = super.load(path)

  private[ExtraTreesRegressionModel]
  class ExtraTreesRegressionModelWriter(instance: ExtraTreesRegressionModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class ExtraTreesRegressionModelReader extends MLReader[ExtraTreesRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ExtraTreesRegressionModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): ExtraTreesRegressionModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeRegressionModel] = treesData.map { case (treeMetadata, root) =>
        val tree =
          new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
        treeMetadata.getAndSetParams(tree)
        tree
      }
      require(numTrees == trees.length, s"ExtraTreesRegressionModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new ExtraTreesRegressionModel(metadata.uid, trees, numFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }
}
