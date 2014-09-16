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

package org.apache.spark.mllib.tree

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impl.{BaggedPoint, TreePoint, DecisionTreeMetadata, TimeTracker}
import org.apache.spark.mllib.tree.impurity.Impurities
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * A class which implements a random forest learning algorithm for classification and regression.
 * It supports both continuous and categorical features.
 *
 * @param strategy The configuration parameters for the random forest algorithm which specify
 *                 the type of algorithm (classification, regression, etc.), feature type
 *                 (continuous, categorical), depth of the tree, quantile calculation strategy,
 *                 etc.
 * @param numTrees If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
 * @param featureSubsetStrategy Number of features to consider for splits at each node.
 *                              Supported: "auto" (default), "all", "sqrt", "log2", "onethird".
 *                              If "auto" is set, this parameter is set based on numTrees:
 *                              if numTrees == 1, then featureSubsetStrategy = "all";
 *                              if numTrees > 1, then featureSubsetStrategy = "sqrt".
 * @param seed  Random seed for bootstrapping and choosing feature subsets.
 */
@Experimental
private class RandomForest (
    private val strategy: Strategy,
    private val numTrees: Int,
    featureSubsetStrategy: String,
    private val seed: Int)
  extends Serializable with Logging {

  strategy.assertValid()
  require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
  require(Array("auto", "all", "sqrt", "log2", "onethird").contains(featureSubsetStrategy),
    s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
    s" Supported values: auto, all, sqrt, log2, onethird.")

  /**
   * Method to train a decision tree model over an RDD
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @return RandomForestModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): RandomForestModel = {

    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    val retaggedInput = input.retag(classOf[LabeledPoint])
    val metadata =
      DecisionTreeMetadata.buildMetadata(retaggedInput, strategy, numTrees, featureSubsetStrategy)
    logDebug("algo = " + strategy.algo)
    logDebug("numTrees = " + numTrees)
    logDebug("seed = " + seed)
    logDebug("maxBins = " + metadata.maxBins)
    logDebug("featureSubsetStrategy = " + featureSubsetStrategy)
    logDebug("numFeaturesPerNode = " + metadata.numFeaturesPerNode)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val (splits, bins) = DecisionTree.findSplitsBins(retaggedInput, metadata)
    timer.stop("findSplitsBins")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
        s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
      }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, bins, metadata)
    val baggedInput = if (numTrees > 1) {
      BaggedPoint.convertToBaggedRDD(treeInput, numTrees, seed)
    } else {
      BaggedPoint.convertToBaggedRDDWithoutSampling(treeInput)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")

    // Max memory usage for aggregates
    val maxMemoryUsage = strategy.maxMemoryInMB * 1024L * 1024L
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    val maxMemoryPerNode = {
      val featureSubset: Array[Int] = if (metadata.subsamplingFeatures) {
        // Find numFeaturesPerNode largest bins to get an upper bound on memory usage.
        metadata.numBins.zipWithIndex.sortBy(- _._1).take(metadata.numFeaturesPerNode).map(_._2)
      } else {
        null
      }
      RandomForest.numElementsForNode(metadata, featureSubset) * 8L
    }
    require(maxMemoryPerNode <= maxMemoryUsage,
      s"RandomForest/DecisionTree given maxMemoryInMB = ${strategy.maxMemoryInMB}," +
      " which is too small for the given features." +
      s"  Minimum value = ${maxMemoryPerNode / (1024L * 1024L)}")
    // TODO: Calculate memory usage more precisely.
    //val numElementsPerNode = DecisionTree.getElementsPerNode(metadata)
    //logDebug("numElementsPerNode = " + numElementsPerNode)
    //val arraySizePerNode = 8 * numElementsPerNode // approx. memory usage for bin aggregate array
    //val maxNumberOfNodesPerGroup = math.max((maxMemoryUsage / arraySizePerNode).toInt, 1)
    //logDebug("maxNumberOfNodesPerGroup = " + maxNumberOfNodesPerGroup)

    timer.stop("init")

    /*
     * The main idea here is to perform group-wise training of the decision tree nodes thus
     * reducing the passes over the data from (# nodes) to (# nodes / maxNumberOfNodesPerGroup).
     * Each data sample is handled by a particular node (or it reaches a leaf and is not used
     * in lower levels).
     */

    // FIFO queue of nodes to train: (treeIndex, node)
    val nodeQueue = new mutable.Queue[(Int, Node)]()

    val rng = new scala.util.Random()
    rng.setSeed(seed)

    // Allocate and queue root nodes.
    val topNodes: Array[Node] = Array.fill[Node](numTrees)(Node.emptyNode(nodeIndex = 1))
    Range(0, numTrees).foreach(treeIndex => nodeQueue.enqueue((treeIndex, topNodes(treeIndex))))

    while (nodeQueue.nonEmpty) {
      // Collect some nodes to split:
      //  nodesForGroup(treeIndex) = nodes to split
      // Track memory usage for aggregates, and stop adding nodes when too large.
      //val totalNodesInGroup = math.min(nodeQueue.size, maxNumberOfNodesPerGroup)
      val mutableNodesForGroup = new mutable.HashMap[Int, mutable.ArrayBuffer[Node]]()
      val mutableFeaturesForNodes = new mutable.HashMap[Int, mutable.HashMap[Int, Array[Int]]]()
      var memUsage: Long = 0L
      while (nodeQueue.nonEmpty && memUsage < maxMemoryUsage) {
        val (treeIndex, node) = nodeQueue.head
        // Choose subset of features for node, or null if using all.
        val featureSubset: Array[Int] = if (metadata.subsamplingFeatures) {
          // TODO: Use more efficient subsampling?
          rng.shuffle(Range(0,metadata.numFeatures).toList)
            .take(metadata.numFeaturesPerNode).toArray
        } else {
          null
        }
        val nodeMemUsage = RandomForest.numElementsForNode(metadata, featureSubset) * 8L
        if (memUsage + nodeMemUsage <= maxMemoryUsage) {
          nodeQueue.dequeue()
          mutableNodesForGroup.getOrElseUpdate(treeIndex, new mutable.ArrayBuffer[Node]()) += node
          if (metadata.subsamplingFeatures) {
            mutableFeaturesForNodes
              .getOrElseUpdate(treeIndex, new mutable.HashMap[Int, Array[Int]]())(node.id)
              = featureSubset
          }
        }
        memUsage += nodeMemUsage
      }
      val nodesForGroup: Map[Int, Array[Node]] = mutableNodesForGroup.mapValues(_.toArray).toMap
      val featuresForNodes: Map[Int, Map[Int, Array[Int]]] = if (metadata.subsamplingFeatures) {
        mutableFeaturesForNodes.mapValues(_.toMap).toMap
      } else {
        null
      }
      // Sanity check (should never occur):
      assert(nodesForGroup.size > 0,
        s"RandomForest selected empty nodesForGroup.  Error for unknown reason.")

      // Choose node splits, and enqueue new nodes as needed.
      timer.start("findBestSplits")
      DecisionTree.findBestSplits(baggedInput,
        metadata, topNodes, nodesForGroup, featuresForNodes, splits, bins, nodeQueue, timer)
      timer.stop("findBestSplits")
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    val trees = topNodes.map(topNode => new DecisionTreeModel(topNode, strategy.algo))
    RandomForestModel.build(trees)
  }

}

object RandomForest extends Serializable with Logging {

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClassesForClassification number of classes for classification.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto" (default), "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                              if numTrees == 1, then featureSubsetStrategy = "all";
   *                              if numTrees > 1, then featureSubsetStrategy = "sqrt".
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClassesForClassification: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val dtStrategy = new Strategy(Classification, impurityType, maxDepth,
      numClassesForClassification, maxBins, Sort, categoricalFeaturesInfo)
    val rf = new RandomForest(dtStrategy, numTrees, featureSubsetStrategy, seed)
    rf.train(input)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainClassifier]]
   */
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numClassesForClassification: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainClassifier(input.rdd, numClassesForClassification,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto" (default), "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                              if numTrees == 1, then featureSubsetStrategy = "all";
   *                              if numTrees > 1, then featureSubsetStrategy = "sqrt".
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "variance".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val dtStrategy = new Strategy(Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
    val rf = new RandomForest(dtStrategy, numTrees, featureSubsetStrategy, seed)
    rf.train(input)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainRegressor]]
   */
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * Get the number of values to be stored for this node in the bin aggregates.
   * @param featureSubset  Indices of features which may be split at this node.
   *                       If null, then use all features.
   */
  private[tree] def numElementsForNode(
      metadata: DecisionTreeMetadata,
      featureSubset: Array[Int]): Long = {
    val totalBins = if (featureSubset != null) {
      featureSubset.map(featureIndex => metadata.numBins(featureIndex).toLong).sum
    } else {
      metadata.numBins.map(_.toLong).sum
    }
    if (metadata.isClassification) {
      metadata.numClasses * totalBins
    } else {
      3 * totalBins
    }
  }

}
