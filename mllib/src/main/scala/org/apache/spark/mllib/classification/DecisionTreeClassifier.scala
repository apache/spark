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
import org.apache.spark.mllib.impl.tree.{TreeClassifierParams, DecisionTreeParams}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


class DecisionTreeClassifier
  extends DecisionTreeParams[DecisionTreeClassifier]
  with TreeClassifierParams[DecisionTreeClassifier] {

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

  override def setImpurity(impurity: ClassificationImpurity): DecisionTreeClassifier =
    super.setImpurity(impurity)

  def run(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int] = Map.empty[Int, Int],
      numClasses: Int = 2): DecisionTreeClassificationModel = {
    // TODO
    new DecisionTreeClassificationModel
  }

  def run(input: JavaRDD[LabeledPoint]): DecisionTreeClassificationModel = {
    run(input.rdd)
  }

  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]):
        DecisionTreeClassificationModel = {
    run(input, categoricalFeaturesInfo, numClasses = 2)
  }

  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): DecisionTreeClassificationModel = {
    run(input.rdd, categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }

}

object DecisionTreeClassifier {

  final val Entropy: ClassificationImpurity = ClassificationImpurity.Entropy

  final val Gini: ClassificationImpurity = ClassificationImpurity.Gini
}


class DecisionTreeClassificationModel extends Serializable {

}
