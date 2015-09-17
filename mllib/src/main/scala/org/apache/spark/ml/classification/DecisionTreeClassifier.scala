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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.{DecisionTreeModel, DecisionTreeParams, Node, TreeClassifierParams}
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] learning algorithm
 * for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Experimental
final class DecisionTreeClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, DecisionTreeClassifier, DecisionTreeClassificationModel]
  with DecisionTreeParams with TreeClassifierParams {

  def this() = this(Identifiable.randomUID("dtc"))

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
    val trees = RandomForest.run(oldDataset, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = 0L, parentUID = Some(uid))
    trees.head.asInstanceOf[DecisionTreeClassificationModel]
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity,
      subsamplingRate = 1.0)
  }

  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}

@Experimental
object DecisionTreeClassifier {
  /** Accessor for supported impurities: entropy, gini */
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities
}

/**
 * :: Experimental ::
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree]] model for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Experimental
final class DecisionTreeClassificationModel private[ml] (
    override val uid: String,
    override val rootNode: Node,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, DecisionTreeClassificationModel]
  with DecisionTreeModel with Serializable {

  require(rootNode != null,
    "DecisionTreeClassificationModel given null rootNode, but it requires a non-null rootNode.")

  /**
   * Construct a decision tree classification model.
   * @param rootNode  Root node of tree, with other nodes attached.
   */
  private[ml] def this(rootNode: Node, numClasses: Int) =
    this(Identifiable.randomUID("dtc"), rootNode, numClasses)

  override protected def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }

  override protected def predictRaw(features: Vector): Vector = {
    Vectors.dense(rootNode.predictImpl(features).impurityStats.stats.clone())
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in DecisionTreeClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def copy(extra: ParamMap): DecisionTreeClassificationModel = {
    copyValues(new DecisionTreeClassificationModel(uid, rootNode, numClasses), extra)
      .setParent(parent)
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
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("dtc")
    new DecisionTreeClassificationModel(uid, rootNode, -1)
  }
}
