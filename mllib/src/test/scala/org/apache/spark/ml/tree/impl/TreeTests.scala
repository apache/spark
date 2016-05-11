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

package org.apache.spark.ml.tree.impl

import scala.collection.JavaConverters._

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

private[ml] object TreeTests extends SparkFunSuite {

  /**
   * Convert the given data to a DataFrame, and set the features and label metadata.
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param categoricalFeatures  Map: categorical feature index -> number of distinct values
   * @param numClasses  Number of classes label can take.  If 0, mark as continuous.
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): DataFrame = {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("TreeTests")
      .getOrCreate()
    import spark.implicits._

    val df = data.toDF()
    val numFeatures = data.first().features.size
    val featuresAttributes = Range(0, numFeatures).map { feature =>
      if (categoricalFeatures.contains(feature)) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(categoricalFeatures(feature))
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }.toArray
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName("label")
    } else {
      NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    df.select(df("features").as("features", featuresMetadata),
      df("label").as("label", labelMetadata))
  }

  /** Java-friendly version of [[setMetadata()]] */
  def setMetadata(
      data: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): DataFrame = {
    setMetadata(data.rdd, categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }

  /**
   * Set label metadata (particularly the number of classes) on a DataFrame.
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param numClasses  Number of classes label can take. If 0, mark as continuous.
   * @param labelColName  Name of the label column on which to set the metadata.
   * @param featuresColName  Name of the features column
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: DataFrame,
      numClasses: Int,
      labelColName: String,
      featuresColName: String): DataFrame = {
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName(labelColName)
    } else {
      NominalAttribute.defaultAttr.withName(labelColName).withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    data.select(data(featuresColName), data(labelColName).as(labelColName, labelMetadata))
  }

  /**
   * Check if the two trees are exactly the same.
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   * If the trees are not equal, this prints the two trees and throws an exception.
   */
  def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      checkEqual(a.rootNode, b.rootNode)
    } catch {
      case ex: Exception =>
        throw new AssertionError("checkEqual failed since the two trees were not identical.\n" +
          "TREE A:\n" + a.toDebugString + "\n" +
          "TREE B:\n" + b.toDebugString + "\n", ex)
    }
  }

  /**
   * Return true iff the two nodes and their descendants are exactly the same.
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   */
  private def checkEqual(a: Node, b: Node): Unit = {
    assert(a.prediction === b.prediction)
    assert(a.impurity === b.impurity)
    (a, b) match {
      case (aye: InternalNode, bee: InternalNode) =>
        assert(aye.split === bee.split)
        checkEqual(aye.leftChild, bee.leftChild)
        checkEqual(aye.rightChild, bee.rightChild)
      case (aye: LeafNode, bee: LeafNode) => // do nothing
      case _ =>
        throw new AssertionError("Found mismatched nodes")
    }
  }

  /**
   * Check if the two models are exactly the same.
   * If the models are not equal, this throws an exception.
   */
  def checkEqual[M <: DecisionTreeModel](a: TreeEnsembleModel[M], b: TreeEnsembleModel[M]): Unit = {
    try {
      a.trees.zip(b.trees).foreach { case (treeA, treeB) =>
        TreeTests.checkEqual(treeA, treeB)
      }
      assert(a.treeWeights === b.treeWeights)
    } catch {
      case ex: Exception => throw new AssertionError(
        "checkEqual failed since the two tree ensembles were not identical")
    }
  }

  /**
   * Helper method for constructing a tree for testing.
   * Given left, right children, construct a parent node.
   * @param split  Split for parent node
   * @return  Parent node with children attached
   */
  def buildParentNode(left: Node, right: Node, split: Split): Node = {
    val leftImp = left.impurityStats
    val rightImp = right.impurityStats
    val parentImp = leftImp.copy.add(rightImp)
    val leftWeight = leftImp.count / parentImp.count.toDouble
    val rightWeight = rightImp.count / parentImp.count.toDouble
    val gain = parentImp.calculate() -
      (leftWeight * leftImp.calculate() + rightWeight * rightImp.calculate())
    val pred = parentImp.predict
    new InternalNode(pred, parentImp.calculate(), gain, left, right, split, parentImp)
  }

  /**
   * Create some toy data for testing feature importances.
   */
  def featureImportanceData(sc: SparkContext): RDD[LabeledPoint] = sc.parallelize(Seq(
    new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
    new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))
  ))

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   *
   * This set of Params is for all Decision Tree-based models.
   */
  val allParamSettings: Map[String, Any] = Map(
    "checkpointInterval" -> 7,
    "seed" -> 543L,
    "maxDepth" -> 2,
    "maxBins" -> 20,
    "minInstancesPerNode" -> 2,
    "minInfoGain" -> 1e-14,
    "maxMemoryInMB" -> 257,
    "cacheNodeIds" -> true
  )

  /** Data for tree read/write tests which produces a non-trivial tree. */
  def getTreeReadWriteData(sc: SparkContext): RDD[LabeledPoint] = {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 2.0)),
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0)))
    sc.parallelize(arr)
  }
}
