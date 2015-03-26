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

import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD


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
    strategy.setSubsamplingRate(getSubsamplingRate)
    strategy
  }
}

object RandomForestClassifier {

  /** Accessor for supported impurity settings */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  /** Accessor for supported featuresPerNode settings */
  final val supportedFeaturesPerNode: Array[String] = RandomForestParams.supportedFeaturesPerNode
}

class RandomForestClassificationModel(
    val trees: Array[DecisionTreeClassificationModel],
    val treeWeights: Array[Double])
  extends TreeEnsembleModel with Serializable {

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override def getTreeWeights: Array[Double] = treeWeights

  override def predict(features: Vector): Double = {
    // Classifies using (weighted) majority votes
    val votes = mutable.Map.empty[Int, Double]
    trees.view.zip(treeWeights).foreach { case (tree, weight) =>
      val prediction = tree.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + weight
    }
    votes.maxBy(_._2)._1
  }

  override def toString: String = {
    s"RandomForestClassificationModel with $numTrees trees"
  }
}

private[mllib] object RandomForestClassificationModel {

  def fromOld(oldModel: OldRandomForestModel): RandomForestClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification RandomForestModel (old API) to" +
        s" RandomForestClassificationModel (new API).  Algo is: ${oldModel.algo}")
    new RandomForestClassificationModel(oldModel.trees.map(DecisionTreeClassificationModel.fromOld),
      Array.fill(oldModel.trees.size)(1.0))
  }
}
