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

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.{DecisionTreeModel, RandomForestParams, TreeClassifierParams, TreeEnsembleModel}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for
 * classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Experimental
final class RandomForestClassifier(override val uid: String)
  extends Predictor[Vector, RandomForestClassifier, RandomForestClassificationModel]
  with RandomForestParams with TreeClassifierParams {

  def this() = this(Identifiable.randomUID("rfc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  override def setMaxDepth(value: Int): this.type = super.setMaxDepth(value)

  override def setMaxBins(value: Int): this.type = super.setMaxBins(value)

  override def setMinInstancesPerNode(value: Int): this.type =
    super.setMinInstancesPerNode(value)

  override def setMinInfoGain(value: Double): this.type = super.setMinInfoGain(value)

  override def setMaxMemoryInMB(value: Int): this.type = super.setMaxMemoryInMB(value)

  override def setCacheNodeIds(value: Boolean): this.type = super.setCacheNodeIds(value)

  override def setCheckpointInterval(value: Int): this.type = super.setCheckpointInterval(value)

  override def setImpurity(value: String): this.type = super.setImpurity(value)

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(value: Double): this.type = super.setSubsamplingRate(value)

  override def setSeed(value: Long): this.type = super.setSeed(value)

  // Parameters from RandomForestParams:

  override def setNumTrees(value: Int): this.type = super.setNumTrees(value)

  override def setFeatureSubsetStrategy(value: String): this.type =
    super.setFeatureSubsetStrategy(value)

  override protected def train(dataset: DataFrame): RandomForestClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None => throw new IllegalArgumentException("RandomForestClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
      // TODO: Automatically index labels: SPARK-7126
    }
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)
    val oldModel = OldRandomForest.trainClassifier(
      oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed.toInt)
    RandomForestClassificationModel.fromOld(oldModel, this, categoricalFeatures)
  }

  override def copy(extra: ParamMap): RandomForestClassifier = defaultCopy(extra)
}

@Experimental
object RandomForestClassifier {
  /** Accessor for supported impurity settings: entropy, gini */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  final val supportedFeatureSubsetStrategies: Array[String] =
    RandomForestParams.supportedFeatureSubsetStrategies
}

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 * @param _trees  Decision trees in the ensemble.
 *               Warning: These have null parents.
 */
@Experimental
final class RandomForestClassificationModel private[ml] (
    override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel])
  extends PredictionModel[Vector, RandomForestClassificationModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "RandomForestClassificationModel requires at least 1 tree.")

  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  override def treeWeights: Array[Double] = _treeWeights

  override protected def predict(features: Vector): Double = {
    // TODO: Override transform() to broadcast model.  SPARK-7127
    // TODO: When we add a generic Bagging class, handle transform there: SPARK-7128
    // Classifies using majority votes.
    // Ignore the weights since all are 1.0 for now.
    val votes = mutable.Map.empty[Int, Double]
    _trees.view.foreach { tree =>
      val prediction = tree.rootNode.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + 1.0 // 1.0 = weight
    }
    votes.maxBy(_._2)._1
  }

  override def copy(extra: ParamMap): RandomForestClassificationModel = {
    copyValues(new RandomForestClassificationModel(uid, _trees), extra)
  }

  override def toString: String = {
    s"RandomForestClassificationModel with $numTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Classification, _trees.map(_.toOld))
  }
}

private[ml] object RandomForestClassificationModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldRandomForestModel,
      parent: RandomForestClassifier,
      categoricalFeatures: Map[Int, Int]): RandomForestClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeClassificationModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("rfc")
    new RandomForestClassificationModel(uid, newTrees)
  }
}
