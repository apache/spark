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

package org.apache.spark.mllib.classification

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD


/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for
 * classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
class RandomForestClassifier
  extends TreeClassifier[RandomForestClassificationModel]
  with RandomForestParams[RandomForestClassifier]
  with TreeClassifierParams[RandomForestClassifier] {

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  override def setMaxDepth(maxDepth: Int): RandomForestClassifier = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): RandomForestClassifier = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): RandomForestClassifier =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): RandomForestClassifier =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): RandomForestClassifier =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): RandomForestClassifier =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): RandomForestClassifier =
    super.setCheckpointInterval(checkpointInterval)

  override def setImpurity(impurity: String): RandomForestClassifier =
    super.setImpurity(impurity)

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(subsamplingRate: Double): RandomForestClassifier =
    super.setSubsamplingRate(subsamplingRate)

  override def setSeed(seed: Long): RandomForestClassifier = super.setSeed(seed)

  // Parameters from RandomForestParams:

  override def setNumTrees(numTrees: Int): RandomForestClassifier = super.setNumTrees(numTrees)

  override def setFeaturesPerNode(featuresPerNode: String): RandomForestClassifier =
    super.setFeaturesPerNode(featuresPerNode)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): RandomForestClassificationModel = {
    val strategy = getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldRandomForest.trainClassifier(
      input, strategy, getNumTrees, getFeaturesPerNodeStr, getSeed.toInt)
    RandomForestClassificationModel.fromOld(oldModel)
  }

  /**
   * Create a Strategy instance to use with the old API.
   * TODO: Make this protected once we deprecate the old API.
   */
  override private[mllib] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses)
    strategy.algo = OldAlgo.Classification
    strategy.impurity = getOldImpurity
    strategy
  }
}

object RandomForestClassifier {

  /** Accessor for supported impurity settings */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featuresPerNode settings */
  final val supportedFeaturesPerNode: Array[String] = RandomForestParams.supportedFeaturesPerNode
}

/**
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 * @param trees  Decision trees in the ensemble.
 */
class RandomForestClassificationModel(val trees: Array[DecisionTreeClassificationModel])
  extends TreeEnsembleModel with Serializable with Saveable {

  require(numTrees > 0, "RandomForestClassificationModel requires at least 1 tree.")

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override lazy val getTreeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  override def predict(features: Vector): Double = {
    // Classifies using majority votes.
    // Ignore the weights since all are 1.0 for now.
    val votes = mutable.Map.empty[Int, Double]
    trees.view.foreach { tree =>
      val prediction = tree.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + 1.0 // 1.0 = weight
    }
    votes.maxBy(_._2)._1
  }

  override def toString: String = {
    s"RandomForestClassificationModel with $numTrees trees"
  }

  override def save(sc: SparkContext, path: String): Unit = {
    this.toOld.save(sc, path)
  }

  override protected def formatVersion: String = OldRandomForestModel.formatVersion

  /** Convert to a model in the old API */
  private[mllib] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Classification, trees.map(_.toOld))
  }
}

object RandomForestClassificationModel
  extends Loader[RandomForestClassificationModel] {

  override def load(sc: SparkContext, path: String): RandomForestClassificationModel = {
    RandomForestClassificationModel.fromOld(OldRandomForestModel.load(sc, path))
  }

  private[mllib] def fromOld(oldModel: OldRandomForestModel): RandomForestClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification RandomForestModel (old API) to" +
        s" RandomForestClassificationModel (new API).  Algo is: ${oldModel.algo}")
    new RandomForestClassificationModel(oldModel.trees.map(DecisionTreeClassificationModel.fromOld))
  }
}
