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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.{DecisionTreeModel, RandomForestParams, TreeClassifierParams, TreeEnsembleModel}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for
 * classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
@Experimental
final class RandomForestClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, RandomForestClassifier, RandomForestClassificationModel]
  with RandomForestParams with TreeClassifierParams {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("rfc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

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
    val trees =
      RandomForest.run(oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed)
        .map(_.asInstanceOf[DecisionTreeClassificationModel])
    val numFeatures = oldDataset.first().features.size
    new RandomForestClassificationModel(trees, numFeatures, numClasses)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): RandomForestClassifier = defaultCopy(extra)
}

@Since("1.4.0")
@Experimental
object RandomForestClassifier {
  /** Accessor for supported impurity settings: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("1.4.0")
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
@Since("1.4.0")
@Experimental
final class RandomForestClassificationModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel],
    @Since("1.6.0") override val numFeatures: Int,
    @Since("1.5.0") override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, RandomForestClassificationModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "RandomForestClassificationModel requires at least 1 tree.")

  /**
   * Construct a random forest classification model, with all trees weighted equally.
   * @param trees  Component trees
   */
  private[ml] def this(
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int) =
    this(Identifiable.randomUID("rfc"), trees, numFeatures, numClasses)

  @Since("1.4.0")
  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: DataFrame): DataFrame = {
    val bcastModel = dataset.sqlContext.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override protected def predictRaw(features: Vector): Vector = {
    // TODO: When we add a generic Bagging class, handle transform there: SPARK-7128
    // Classifies using majority votes.
    // Ignore the tree weights since all are 1.0 for now.
    val votes = Array.fill[Double](numClasses)(0.0)
    _trees.view.foreach { tree =>
      val classCounts: Array[Double] = tree.rootNode.predictImpl(features).impurityStats.stats
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
    s"RandomForestClassificationModel (uid=$uid) with $numTrees trees"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * This feature importance is calculated as follows:
   *  - Average over trees:
   *     - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *       where gain is scaled by the number of instances passing through node
   *     - Normalize importances for tree based on total number of training instances used
   *       to build tree.
   *  - Normalize feature importance vector to sum to 1.
   */
  lazy val featureImportances: Vector = RandomForest.featureImportances(trees, numFeatures)

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
