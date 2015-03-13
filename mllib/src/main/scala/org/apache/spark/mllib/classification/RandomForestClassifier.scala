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
import org.apache.spark.mllib.impl.tree.{FeatureSubsetStrategies, FeatureSubsetStrategy, TreeClassifierParams, RandomForestParams}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


class RandomForestClassifier
  extends RandomForestParams[RandomForestClassifier]
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

  def run(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int] = Map.empty[Int, Int],
      numClasses: Int = 2): RandomForestClassificationModel = {
    // TODO
    new RandomForestClassificationModel
  }

  def run(input: JavaRDD[LabeledPoint]): RandomForestClassificationModel = {
    run(input.rdd)
  }

  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]):
  RandomForestClassificationModel = {
    run(input, categoricalFeaturesInfo, numClasses = 2)
  }

  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): RandomForestClassificationModel = {
    run(input.rdd, categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }

}

object RandomForestClassifier {

  /** Accessor for supported FeatureSubsetStrategy options */
  final val featureSubsetStrategies = FeatureSubsetStrategies

  final val Entropy: ClassificationImpurity = ClassificationImpurity.Entropy

  final val Gini: ClassificationImpurity = ClassificationImpurity.Gini
}

class RandomForestClassificationModel extends Serializable {

}
