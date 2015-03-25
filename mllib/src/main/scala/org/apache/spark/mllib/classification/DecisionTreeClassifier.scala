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

import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD


class DecisionTreeClassifier
  extends TreeClassifier[DecisionTreeClassificationModel]
  with DecisionTreeClassifierParams[DecisionTreeClassifier] {

  // Override parameter setters from parent trait for Java API compatibility.

  override def setMaxDepth(maxDepth: Int): DecisionTreeClassifier = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): DecisionTreeClassifier = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): DecisionTreeClassifier =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): DecisionTreeClassifier =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): DecisionTreeClassifier =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): DecisionTreeClassifier =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): DecisionTreeClassifier =
    super.setCheckpointInterval(checkpointInterval)

  override def setImpurity(impurity: String): DecisionTreeClassifier =
    super.setImpurity(impurity)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): DecisionTreeClassificationModel = {
    val strategy = getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldDecisionTree.train(input, strategy)
    DecisionTreeClassificationModel.fromOld(oldModel)
  }
}

object DecisionTreeClassifier {

  /** Accessor for supported impurities */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities
}

class DecisionTreeClassificationModel private[mllib] (rootNode: Node)
  extends DecisionTreeModel(rootNode) with Serializable {

  override def toString: String = {
    s"DecisionTreeClassificationModel of depth $depth with $numNodes nodes"
  }

  // TODO
  //override def save(sc: SparkContext, path: String): Unit = {
}

private[mllib] object DecisionTreeClassificationModel {

  def fromOld(oldModel: OldDecisionTreeModel): DecisionTreeClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification DecisionTreeModel (old API) to" +
        s" DecisionTreeClassificationModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode)
    new DecisionTreeClassificationModel(rootNode)
  }
}
