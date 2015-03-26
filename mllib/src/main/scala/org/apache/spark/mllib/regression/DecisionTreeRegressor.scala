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
import org.apache.spark.mllib.impl.tree.{TreeRegressor, TreeRegressorParams}
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD


class DecisionTreeRegressor
  extends TreeRegressor[DecisionTreeRegressionModel]
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

  override def setImpurity(impurity: String): DecisionTreeRegressor =
    super.setImpurity(impurity)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int]): DecisionTreeRegressionModel = {
    val strategy = getOldStrategy(categoricalFeatures)
    val oldModel = OldDecisionTree.train(input, strategy)
    DecisionTreeRegressionModel.fromOld(oldModel)
  }

  /**
   * Create a Strategy instance to use with the old API.
   * TODO: Remove once we move implementation to new API.
   */
  private[mllib] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
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

class DecisionTreeRegressionModel private[mllib] (rootNode: Node)
  extends DecisionTreeModel(rootNode) with Serializable with Saveable {

  override def toString: String = {
    s"DecisionTreeRegressionModel of depth $depth with $numNodes nodes"
  }

  override def save(sc: SparkContext, path: String): Unit = {
    this.toOld.save(sc, path)
  }

  override protected def formatVersion: String = OldDecisionTreeModel.formatVersion

  /** Convert to a model in the old API */
  private[mllib] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Regression)
  }
}

object DecisionTreeRegressionModel extends Loader[DecisionTreeRegressionModel] {

  override def load(sc: SparkContext, path: String): DecisionTreeRegressionModel = {
    DecisionTreeRegressionModel.fromOld(OldDecisionTreeModel.load(sc, path))
  }

  private[mllib] def fromOld(oldModel: OldDecisionTreeModel): DecisionTreeRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-Regression DecisionTreeModel (old API) to" +
        s" DecisionTreeRegressionModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode)
    new DecisionTreeRegressionModel(rootNode)
  }
}
