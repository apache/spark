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

import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.classification.tree.ClassificationImpurity
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


class RandomForestClassifier
  extends TreeClassifier[RandomForestClassificationModel]
  with RandomForestParams[RandomForestClassifier]
  with TreeClassifierParams[RandomForestClassifier] {

  // Override parameter setters from parent trait for Java API compatibility.

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

  override def setImpurity(impurity: ClassificationImpurity): RandomForestClassifier =
    super.setImpurity(impurity)

  override def setNumTrees(numTrees: Int): RandomForestClassifier = super.setNumTrees(numTrees)

  override def setFeatureSubsetStrategy(
      featureSubsetStrategy: FeatureSubsetStrategy): RandomForestClassifier =
    super.setFeatureSubsetStrategy(featureSubsetStrategy)

  override def setSubsamplingRate(subsamplingRate: Double): RandomForestClassifier =
    super.setSubsamplingRate(subsamplingRate)

  override def setSeed(seed: Long): RandomForestClassifier = super.setSeed(seed)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      numClasses: Int): RandomForestClassificationModel = {
    // TODO
    new RandomForestClassificationModel
  }

}

object RandomForestClassifier {

  /** Accessor for supported FeatureSubsetStrategy options */
  final val featureSubsetStrategies = FeatureSubsetStrategy

  def supportedImpurities = ClassificationImpurity
}

class RandomForestClassificationModel extends Serializable {

}
