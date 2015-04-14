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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor}
import org.apache.spark.ml.impl.tree._
import org.apache.spark.ml.param.{Params, ParamMap}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] learning algorithm
 * for regression.
 * It supports both continuous and categorical features.
 */
@AlphaComponent
class DecisionTreeRegressor
  extends Predictor[Vector, DecisionTreeRegressor, DecisionTreeRegressionModel]
  with DecisionTreeParams[DecisionTreeRegressor]
  with TreeRegressorParams[DecisionTreeRegressor] {

  // Override parameter setters from parent trait for Java API compatibility.

  override def setMaxDepth(maxDepth: Int): DecisionTreeRegressor = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): DecisionTreeRegressor = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): DecisionTreeRegressor =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): DecisionTreeRegressor =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): DecisionTreeRegressor =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): DecisionTreeRegressor =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): DecisionTreeRegressor =
    super.setCheckpointInterval(checkpointInterval)

  override def setImpurity(impurity: String): DecisionTreeRegressor = super.setImpurity(impurity)

  override protected def train(
      dataset: DataFrame,
      paramMap: ParamMap): DecisionTreeRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema(paramMap(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset, paramMap)
    val strategy = getOldStrategy(categoricalFeatures)
    val oldModel = OldDecisionTree.train(oldDataset, strategy)
    DecisionTreeRegressionModel.fromOld(oldModel, this, paramMap)
  }

  /** Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 0)
    strategy.algo = OldAlgo.Regression
    strategy.setImpurity(getOldImpurity)
    strategy
  }
}

object DecisionTreeRegressor {
  /** Accessor for supported impurities */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] model for regression.
 * It supports both continuous and categorical features.
 * @param rootNode  Root of the decision tree
 */
@AlphaComponent
class DecisionTreeRegressionModel private[ml] (
    override val parent: DecisionTreeRegressor,
    override val fittingParamMap: ParamMap,
    override val rootNode: Node)
  extends PredictionModel[Vector, DecisionTreeRegressionModel]
  with DecisionTreeModel with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  override protected def predict(features: Vector): Double = {
    rootNode.predict(features)
  }

  override protected def copy(): DecisionTreeRegressionModel = {
    val m = new DecisionTreeRegressionModel(parent, fittingParamMap, rootNode)
    Params.inheritValues(this.extractParamMap(), this, m)
    m
  }

  override def toString: String = {
    s"DecisionTreeRegressionModel of depth $depth with $numNodes nodes"
  }

  /** Convert to a model in the old API */
  private[ml] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Regression)
  }
}

object DecisionTreeRegressionModel {

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldDecisionTreeModel,
      parent: DecisionTreeRegressor,
      fittingParamMap: ParamMap): DecisionTreeRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression DecisionTreeModel (old API) to" +
        s" DecisionTreeRegressionModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode)
    new DecisionTreeRegressionModel(parent, fittingParamMap, rootNode)
  }
}
