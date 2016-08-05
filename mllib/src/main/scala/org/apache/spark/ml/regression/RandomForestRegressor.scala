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
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@Since("1.4.0")
class RandomForestRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Predictor[Vector, RandomForestRegressor, RandomForestRegressionModel]
  with RandomForestRegressorParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("rfr"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:
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

  // Parameters from TreeEnsembleParams:
  @Since("1.4.0")
  override def setSubsamplingRate(value: Double): this.type = super.setSubsamplingRate(value)

  @Since("1.4.0")
  override def setSeed(value: Long): this.type = super.setSeed(value)

  // Parameters from RandomForestParams:
  @Since("1.4.0")
  override def setNumTrees(value: Int): this.type = super.setNumTrees(value)

  @Since("1.4.0")
  override def setFeatureSubsetStrategy(value: String): this.type =
    super.setFeatureSubsetStrategy(value)

  override protected def train(dataset: Dataset[_]): RandomForestRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity)

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(params: _*)

    val trees = RandomForest
      .run(oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeRegressionModel])

    val numFeatures = oldDataset.first().features.size
    val m = new RandomForestRegressionModel(trees, numFeatures)
    instr.logSuccess(m)
    m
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): RandomForestRegressor = defaultCopy(extra)
}

@Since("1.4.0")
object RandomForestRegressor extends DefaultParamsReadable[RandomForestRegressor]{
  /** Accessor for supported impurity settings: variance */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("1.4.0")
  final val supportedFeatureSubsetStrategies: Array[String] =
    RandomForestParams.supportedFeatureSubsetStrategies

  @Since("2.0.0")
  override def load(path: String): RandomForestRegressor = super.load(path)

}

/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for regression.
 * It supports both continuous and categorical features.
 *
 * @param _trees  Decision trees in the ensemble.
 * @param numFeatures  Number of features used by this model
 */
@Since("1.4.0")
class RandomForestRegressionModel private[ml] (
    override val uid: String,
    private val _trees: Array[DecisionTreeRegressionModel],
    override val numFeatures: Int)
  extends PredictionModel[Vector, RandomForestRegressionModel]
  with RandomForestRegressionModelParams with TreeEnsembleModel[DecisionTreeRegressionModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "RandomForestRegressionModel requires at least 1 tree.")

  /**
   * Construct a random forest regression model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(trees: Array[DecisionTreeRegressionModel], numFeatures: Int) =
    this(Identifiable.randomUID("rfr"), trees, numFeatures)

  @Since("1.4.0")
  override def trees: Array[DecisionTreeRegressionModel] = _trees

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](_trees.length)(1.0)

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: Dataset[_]): DataFrame = {
    val bcastModel = dataset.sparkSession.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override protected def predict(features: Vector): Double = {
    // TODO: When we add a generic Bagging class, handle transform there.  SPARK-7128
    // Predict average of tree predictions.
    // Ignore the weights since all are 1.0 for now.
    _trees.map(_.rootNode.predictImpl(features).prediction).sum / getNumTrees
  }

  /**
   * Number of trees in ensemble
   * @deprecated  Use [[getNumTrees]] instead.  This method will be removed in 2.1.0
   */
  // TODO: Once this is removed, then this class can inherit from RandomForestRegressorParams
  @deprecated("Use getNumTrees instead.  This method will be removed in 2.1.0.", "2.0.0")
  val numTrees: Int = trees.length

  @Since("1.4.0")
  override def copy(extra: ParamMap): RandomForestRegressionModel = {
    copyValues(new RandomForestRegressionModel(uid, _trees, numFeatures), extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"RandomForestRegressionModel (uid=$uid) with $getNumTrees trees"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * @see [[DecisionTreeRegressionModel.featureImportances]]
   */
  @Since("1.5.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Regression, _trees.map(_.toOld))
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new RandomForestRegressionModel.RandomForestRegressionModelWriter(this)
}

@Since("2.0.0")
object RandomForestRegressionModel extends MLReadable[RandomForestRegressionModel] {

  @Since("2.0.0")
  override def read: MLReader[RandomForestRegressionModel] = new RandomForestRegressionModelReader

  @Since("2.0.0")
  override def load(path: String): RandomForestRegressionModel = super.load(path)

  private[RandomForestRegressionModel]
  class RandomForestRegressionModelWriter(instance: RandomForestRegressionModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class RandomForestRegressionModelReader extends MLReader[RandomForestRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[RandomForestRegressionModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): RandomForestRegressionModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeRegressionModel] = treesData.map { case (treeMetadata, root) =>
        val tree =
          new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
        DefaultParamsReader.getAndSetParams(tree, treeMetadata)
        tree
      }
      require(numTrees == trees.length, s"RandomForestRegressionModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new RandomForestRegressionModel(metadata.uid, trees, numFeatures)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldRandomForestModel,
      parent: RandomForestRegressor,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1): RandomForestRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestRegressionModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("rfr")
    new RandomForestRegressionModel(uid, newTrees, numFeatures)
  }
}
