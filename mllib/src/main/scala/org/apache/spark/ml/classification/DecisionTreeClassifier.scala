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

package org.apache.spark.ml.classification

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor}
import org.apache.spark.ml.impl.tree._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.{DecisionTreeModel, Node}
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
 * for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@AlphaComponent
final class DecisionTreeClassifier
  extends Predictor[Vector, DecisionTreeClassifier, DecisionTreeClassificationModel]
  with DecisionTreeParams with TreeClassifierParams {

  // Override parameter setters from parent trait for Java API compatibility.

  override def setMaxDepth(value: Int): this.type = super.setMaxDepth(value)

  override def setMaxBins(value: Int): this.type = super.setMaxBins(value)

  override def setMinInstancesPerNode(value: Int): this.type =
    super.setMinInstancesPerNode(value)

  override def setMinInfoGain(value: Double): this.type = super.setMinInfoGain(value)

  override def setMaxMemoryInMB(value: Int): this.type = super.setMaxMemoryInMB(value)

  override def setCacheNodeIds(value: Boolean): this.type = super.setCacheNodeIds(value)

  override def setCheckpointInterval(value: Int): this.type = super.setCheckpointInterval(value)

  override def setImpurity(value: String): this.type = super.setImpurity(value)

  override protected def train(dataset: DataFrame): DecisionTreeClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None => throw new IllegalArgumentException("DecisionTreeClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
        // TODO: Automatically index labels: SPARK-7126
    }
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy = getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldDecisionTree.train(oldDataset, strategy)
    DecisionTreeClassificationModel.fromOld(oldModel, this, categoricalFeatures)
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity,
      subsamplingRate = 1.0)
  }
}

object DecisionTreeClassifier {
  /** Accessor for supported impurities: entropy, gini */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@AlphaComponent
final class DecisionTreeClassificationModel private[ml] (
    override val parent: DecisionTreeClassifier,
    override val rootNode: Node)
  extends PredictionModel[Vector, DecisionTreeClassificationModel]
  with DecisionTreeModel with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  override protected def predict(features: Vector): Double = {
    rootNode.predict(features)
  }

  override def copy(extra: ParamMap): DecisionTreeClassificationModel = {
    copyValues(new DecisionTreeClassificationModel(parent, rootNode), extra)
  }

  override def toString: String = {
    s"DecisionTreeClassificationModel of depth $depth with $numNodes nodes"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Classification)
  }
}

private[ml] object DecisionTreeClassificationModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldDecisionTreeModel,
      parent: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int]): DecisionTreeClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification DecisionTreeModel (old API) to" +
        s" DecisionTreeClassificationModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    new DecisionTreeClassificationModel(parent, rootNode)
  }
}
