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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.{DecisionTreeModel, DecisionTreeParams, Node, TreeRegressorParams}
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] learning algorithm
 * for regression.
 * It supports both continuous and categorical features.
 */
@Since("1.4.0")
@Experimental
final class DecisionTreeRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Predictor[Vector, DecisionTreeRegressor, DecisionTreeRegressionModel]
  with DecisionTreeParams with TreeRegressorParams {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtr"))

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

  override def setSeed(value: Long): this.type = super.setSeed(value)

  override protected def train(dataset: DataFrame): DecisionTreeRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy = getOldStrategy(categoricalFeatures)
    val trees = RandomForest.run(oldDataset, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), parentUID = Some(uid))
    trees.head.asInstanceOf[DecisionTreeRegressionModel]
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity,
      subsamplingRate = 1.0)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeRegressor = defaultCopy(extra)
}

@Since("1.4.0")
@Experimental
object DecisionTreeRegressor {
  /** Accessor for supported impurities: variance */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities
}

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] model for regression.
 * It supports both continuous and categorical features.
 * @param rootNode  Root of the decision tree
 */
@Since("1.4.0")
@Experimental
final class DecisionTreeRegressionModel private[ml] (
    override val uid: String,
    override val rootNode: Node,
    override val numFeatures: Int)
  extends PredictionModel[Vector, DecisionTreeRegressionModel]
  with DecisionTreeModel with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  /**
   * Construct a decision tree regression model.
   * @param rootNode  Root node of tree, with other nodes attached.
   */
  private[ml] def this(rootNode: Node, numFeatures: Int) =
    this(Identifiable.randomUID("dtr"), rootNode, numFeatures)

  override protected def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeRegressionModel = {
    copyValues(new DecisionTreeRegressionModel(uid, rootNode, numFeatures), extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"DecisionTreeRegressionModel (uid=$uid) of depth $depth with $numNodes nodes"
  }

  /** Convert to a model in the old API */
  private[ml] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Regression)
  }
}

private[ml] object DecisionTreeRegressionModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldDecisionTreeModel,
      parent: DecisionTreeRegressor,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1): DecisionTreeRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression DecisionTreeModel (old API) to" +
        s" DecisionTreeRegressionModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("dtr")
    new DecisionTreeRegressionModel(uid, rootNode, numFeatures)
  }
}
