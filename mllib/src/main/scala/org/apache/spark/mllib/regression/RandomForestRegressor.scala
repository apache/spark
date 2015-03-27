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

package org.apache.spark.mllib.regression

import org.apache.spark.SparkContext
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD


/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
class RandomForestRegressor
  extends TreeRegressor[RandomForestRegressionModel]
  with RandomForestParams[RandomForestRegressor]
  with TreeRegressorParams[RandomForestRegressor] {

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

  override def setMaxDepth(maxDepth: Int): RandomForestRegressor = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): RandomForestRegressor = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): RandomForestRegressor =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): RandomForestRegressor =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): RandomForestRegressor =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): RandomForestRegressor =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): RandomForestRegressor =
    super.setCheckpointInterval(checkpointInterval)

  override def setImpurity(impurity: String): RandomForestRegressor =
    super.setImpurity(impurity)

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(subsamplingRate: Double): RandomForestRegressor =
    super.setSubsamplingRate(subsamplingRate)

  override def setSeed(seed: Long): RandomForestRegressor = super.setSeed(seed)

  // Parameters from RandomForestParams:

  override def setNumTrees(numTrees: Int): RandomForestRegressor = super.setNumTrees(numTrees)

  override def setFeaturesPerNode(featuresPerNode: String): RandomForestRegressor =
    super.setFeaturesPerNode(featuresPerNode)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int]): RandomForestRegressionModel = {
    val strategy = getOldStrategy(categoricalFeatures)
    val oldModel = OldRandomForest.trainRegressor(
      input, strategy, getNumTrees, getFeaturesPerNodeStr, getSeed.toInt)
    RandomForestRegressionModel.fromOld(oldModel)
  }

  /**
   * Create a Strategy instance to use with the old API.
   * TODO: Make this protected once we deprecate the old API.
   */
  private[mllib] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 0)
    strategy.algo = OldAlgo.Regression
    strategy.impurity = getOldImpurity
    strategy
  }
}

object RandomForestRegressor {

  /** Accessor for supported impurity settings */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  /** Accessor for supported featuresPerNode settings */
  final val supportedFeaturesPerNode: Array[String] = RandomForestParams.supportedFeaturesPerNode
}

/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for regression.
 * It supports both continuous and categorical features.
 * @param trees  Decision trees in the ensemble.
 */
class RandomForestRegressionModel(val trees: Array[DecisionTreeRegressionModel])
  extends TreeEnsembleModel with Serializable with Saveable {

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override lazy val getTreeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  override def predict(features: Vector): Double = {
    // Predict average of tree predictions.
    // Ignore the weights since all are 1.0 for now.
    trees.map(_.predict(features)).sum / numTrees
  }

  override def toString: String = {
    s"RandomForestRegressionModel with $numTrees trees"
  }

  override def save(sc: SparkContext, path: String): Unit = {
    this.toOld.save(sc, path)
  }

  override protected def formatVersion: String = OldRandomForestModel.formatVersion

  /** Convert to a model in the old API */
  private[mllib] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Regression, trees.map(_.toOld))
  }
}

object RandomForestRegressionModel extends Loader[RandomForestRegressionModel] {

  override def load(sc: SparkContext, path: String): RandomForestRegressionModel = {
    RandomForestRegressionModel.fromOld(OldRandomForestModel.load(sc, path))
  }

  private[mllib] def fromOld(oldModel: OldRandomForestModel): RandomForestRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression RandomForestModel (old API) to" +
        s" RandomForestRegressionModel (new API).  Algo is: ${oldModel.algo}")
    new RandomForestRegressionModel(oldModel.trees.map(DecisionTreeRegressionModel.fromOld))
  }
}
