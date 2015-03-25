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

import org.apache.spark.mllib.impl.tree.{GBTClassifierParams, GBTParams, TreeClassifierParams, TreeClassifier}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.rdd.RDD

/*
class GBTClassifier
  extends TreeClassifier[GBTClassificationModel]
  with GBTClassifierParams[GBTClassifier] {

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  override def setMaxDepth(maxDepth: Int): GBTClassifier = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): GBTClassifier = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): GBTClassifier =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): GBTClassifier =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): GBTClassifier =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): GBTClassifier =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): GBTClassifier =
    super.setCheckpointInterval(checkpointInterval)

  override def setImpurity(impurity: String): GBTClassifier = super.setImpurity(impurity)

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(subsamplingRate: Double): GBTClassifier =
    super.setSubsamplingRate(subsamplingRate)

  override def setSeed(seed: Long): GBTClassifier = super.setSeed(seed)

  // Parameters from GBTParams:

  override def setNumIterations(numIterations: Int): GBTClassifier =
    super.setNumIterations(numIterations)

  override def setLearningRate(learningRate: Double): GBTClassifier =
    super.setLearningRate(learningRate)

  override def setValidationTol(validationTol: Double): GBTClassifier =
    super.setValidationTol(validationTol)

  // Parameters from GBTParams:

  override def setLoss(loss: String): GBTClassifier = super.setLoss(loss)

  override def run(
                    input: RDD[LabeledPoint],
                    categoricalFeatures: Map[Int, Int],
                    numClasses: Int): RandomForestClassificationModel = {
    val boostingStrategy = getOldBoostingStrategy(categoricalFeatures, numClasses)
    val oldModel = OldGBT.train(input, boostingStrategy)
    GBTClassificationModel.fromOld(oldModel)
  }

}

class GBTClassificationModel
*/
