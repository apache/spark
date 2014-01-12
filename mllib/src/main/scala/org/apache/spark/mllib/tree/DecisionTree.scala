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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.Split
import scala.util.control.Breaks._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.FeatureType._


class DecisionTree(val strategy : Strategy) extends Serializable with Logging {

  def train(input : RDD[LabeledPoint]) : DecisionTreeModel = {

    //Cache input RDD for speedup during multiple passes
    input.cache()

    val (splits, bins) = DecisionTree.find_splits_bins(input, strategy)
    logDebug("numSplits = " + bins(0).length)
    strategy.numBins = bins(0).length

    //TODO: Level-wise training of tree and obtain Decision Tree model
    val maxDepth = strategy.maxDepth

    val maxNumNodes = scala.math.pow(2,maxDepth).toInt - 1
    val filters = new Array[List[Filter]](maxNumNodes)
    filters(0) = List()
    val parentImpurities = new Array[Double](maxNumNodes)
    //Dummy value for top node (updated during first split calculation)
    //parentImpurities(0) = Double.MinValue
    val nodes = new Array[Node](maxNumNodes)


    breakable {
      for (level <- 0 until maxDepth){

        logDebug("#####################################")
        logDebug("level = " + level)
        logDebug("#####################################")

        //Find best split for all nodes at a level
        val numNodes= scala.math.pow(2,level).toInt
        val splitsStatsForLevel = DecisionTree.findBestSplits(input, parentImpurities, strategy, level, filters,splits,bins)

        for ((nodeSplitStats, index) <- splitsStatsForLevel.view.zipWithIndex){

          extractNodeInfo(nodeSplitStats, level, index, nodes)
          extractInfoForLowerLevels(level, index, maxDepth, nodeSplitStats, parentImpurities, filters)
          logDebug("final best split = " + nodeSplitStats._1)

        }
        require(scala.math.pow(2,level)==splitsStatsForLevel.length)

        val allLeaf = splitsStatsForLevel.forall(_._2.gain <= 0 )
        logDebug("all leaf = " + allLeaf)
        if (allLeaf) break

      }
    }

    val topNode = nodes(0)
    topNode.build(nodes)

    val decisionTreeModel = {
      return new DecisionTreeModel(topNode)
    }

    return decisionTreeModel
  }


  private def extractNodeInfo(nodeSplitStats: (Split, InformationGainStats), level: Int, index: Int, nodes: Array[Node]) {
    val split = nodeSplitStats._1
    val stats = nodeSplitStats._2
    val nodeIndex = scala.math.pow(2, level).toInt - 1 + index
    val predict = {
      val leftSamples = nodeSplitStats._2.leftSamples.toDouble
      val rightSamples = nodeSplitStats._2.rightSamples.toDouble
      val totalSamples = leftSamples + rightSamples
      leftSamples / totalSamples
    }
    val isLeaf = (stats.gain <= 0) || (level == strategy.maxDepth - 1)
    val node = new Node(nodeIndex, predict, isLeaf, Some(split), None, None, Some(stats))
    logDebug("Node = " + node)
    nodes(nodeIndex) = node
  }

  private def extractInfoForLowerLevels(level: Int, index: Int, maxDepth: Int, nodeSplitStats: (Split, InformationGainStats), parentImpurities: Array[Double], filters: Array[List[Filter]]) {
    for (i <- 0 to 1) {

      val nodeIndex = (scala.math.pow(2, level + 1)).toInt - 1 + 2 * index + i

      if (level < maxDepth - 1) {

        val impurity = if (i == 0) nodeSplitStats._2.leftImpurity else nodeSplitStats._2.rightImpurity
        logDebug("nodeIndex = " + nodeIndex + ", impurity = " + impurity)
        parentImpurities(nodeIndex) = impurity
        filters(nodeIndex) = new Filter(nodeSplitStats._1, if (i == 0) -1 else 1) :: filters((nodeIndex - 1) / 2)
        for (filter <- filters(nodeIndex)) {
          logDebug("Filter = " + filter)
        }

      }
    }
  }
}

object DecisionTree extends Serializable with Logging {

  /*
  Returns an Array[Split] of optimal splits for all nodes at a given level

  @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data for DecisionTree
  @param parentImpurities Impurities for all parent nodes for the current level
  @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing parameters for construction the DecisionTree
  @param level Level of the tree
  @param filters Filter for all nodes at a given level
  @param splits possible splits for all features
  @param bins possible bins for all features

  @return Array[Split] instance for best splits for all nodes at a given level.
    */
  def findBestSplits(
                      input : RDD[LabeledPoint],
                      parentImpurities : Array[Double],
                      strategy: Strategy,
                      level: Int,
                      filters : Array[List[Filter]],
                      splits : Array[Array[Split]],
                      bins : Array[Array[Bin]]) : Array[(Split, InformationGainStats)] = {

    //Common calculations for multiple nested methods
    val numNodes = scala.math.pow(2, level).toInt
    logDebug("numNodes = " + numNodes)
    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length
    logDebug("numFeatures = " + numFeatures)
    val numSplits = strategy.numBins
    logDebug("numSplits = " + numSplits)

    /*Find the filters used before reaching the current code*/
    def findParentFilters(nodeIndex: Int): List[Filter] = {
      if (level == 0) {
        List[Filter]()
      } else {
        val nodeFilterIndex = scala.math.pow(2, level).toInt - 1 + nodeIndex
        //val parentFilterIndex = nodeFilterIndex / 2
        //TODO: Check left or right filter
        filters(nodeFilterIndex)
      }
    }

    /*Find whether the sample is valid input for the current node.

    In other words, does it pass through all the filters for the current node.
    */
    def isSampleValid(parentFilters: List[Filter], labeledPoint: LabeledPoint): Boolean = {

      //Leaf
      if ((level > 0) & (parentFilters.length == 0) ){
        return false
      }

      for (filter <- parentFilters) {
        val features = labeledPoint.features
        val featureIndex = filter.split.feature
        val threshold = filter.split.threshold
        val comparison = filter.comparison
        comparison match {
          case(-1) => if (features(featureIndex) > threshold) return false
          case(0) => if (features(featureIndex) != threshold) return false
          case(1) => if (features(featureIndex) <= threshold) return false
        }
      }
      true
    }

    /*Finds the right bin for the given feature*/
    def findBin(featureIndex: Int, labeledPoint: LabeledPoint) : Int = {
      //logDebug("finding bin for labeled point " + labeledPoint.features(featureIndex))
      //TODO: Do binary search
      for (binIndex <- 0 until strategy.numBins) {
        val bin = bins(featureIndex)(binIndex)
        //TODO: Remove this requirement post basic functional
        val lowThreshold = bin.lowSplit.threshold
        val highThreshold = bin.highSplit.threshold
        val features = labeledPoint.features
        if ((lowThreshold <= features(featureIndex)) & (highThreshold > features(featureIndex))) {
          return binIndex
        }
      }
      throw new UnknownError("no bin was found.")

    }

    /*Finds bins for all nodes (and all features) at a given level
     k features, l nodes (level = log2(l))
     Storage label, b11, b12, b13, .., bk, b21, b22, .. ,bl1, bl2, .. ,blk
     Denotes invalid sample for tree by noting bin for feature 1 as -1
    */
    def findBinsForLevel(labeledPoint : LabeledPoint) : Array[Double] = {


      // calculating bin index and label per feature per node
      val arr = new Array[Double](1+(numFeatures * numNodes))
      arr(0) = labeledPoint.label
      for (index <- 0 until numNodes) {
        val parentFilters = findParentFilters(index)
        //Find out whether the sample qualifies for the particular node
        val sampleValid = isSampleValid(parentFilters, labeledPoint)
        val shift = 1 + numFeatures * index
        if (!sampleValid) {
          //Add to invalid bin index -1
          for (featureIndex <- 0 until numFeatures) {
            arr(shift+featureIndex) = -1
            //TODO: Break since marking one bin is sufficient
          }
        } else {
          for (featureIndex <- 0 until numFeatures) {
            //logDebug("shift+featureIndex =" + (shift+featureIndex))
            arr(shift + featureIndex) = findBin(featureIndex, labeledPoint)
          }
        }

      }
      arr
    }

    /*
    Performs a sequential aggregation over a partition.

    for p bins, k features, l nodes (level = log2(l)) storage is of the form:
    b111_left_count,b111_right_count, .... , bpk1_left_count, bpk1_right_count, .... , bpkl_left_count, bpkl_right_count

    @param agg Array[Double] storing aggregate calculation of size 2*numSplits*numFeatures*numNodes for classification
    @param arr Array[Double] of size 1+(numFeatures*numNodes)
    @return Array[Double] storing aggregate calculation of size 2*numSplits*numFeatures*numNodes for classification
     */
    def binSeqOp(agg : Array[Double], arr: Array[Double]) : Array[Double] = {
      //TODO: Requires logic for regressions
      for (node <- 0 until numNodes) {
         val validSignalIndex = 1+numFeatures*node
         val isSampleValidForNode = if(arr(validSignalIndex) != -1) true else false
         if(isSampleValidForNode){
           val label = arr(0)
           for (feature <- 0 until numFeatures){
               val arrShift = 1 + numFeatures*node
               val aggShift = 2*numSplits*numFeatures*node
               val arrIndex = arrShift + feature
               val aggIndex = aggShift + 2*feature*numSplits + arr(arrIndex).toInt*2
               label match {
                 case(0.0) => agg(aggIndex) =  agg(aggIndex) + 1
                 case(1.0) => agg(aggIndex+1) =  agg(aggIndex+1) + 1
               }
           }
         }
      }
      agg
    }

    //TODO: This length if different for regression
    val binAggregateLength = 2*numSplits * numFeatures * numNodes
    logDebug("binAggregageLength = " + binAggregateLength)

    /*Combines the aggregates from partitions
    @param agg1 Array containing aggregates from one or more partitions
    @param agg2 Array containing aggregates from one or more partitions

    @return Combined aggregate from agg1 and agg2
     */
    def binCombOp(agg1 : Array[Double], agg2: Array[Double]) : Array[Double] = {
      val combinedAggregate = new Array[Double](binAggregateLength)
      for (index <- 0 until binAggregateLength){
        combinedAggregate(index) = agg1(index) + agg2(index)
      }
      combinedAggregate
    }

    logDebug("input = " + input.count)
    val binMappedRDD = input.map(x => findBinsForLevel(x))
    logDebug("binMappedRDD.count = " + binMappedRDD.count)
    //calculate bin aggregates

    val binAggregates = binMappedRDD.aggregate(Array.fill[Double](2*numSplits*numFeatures*numNodes)(0))(binSeqOp,binCombOp)
    logDebug("binAggregates.length = " + binAggregates.length)
    //binAggregates.foreach(x => logDebug(x))


    def calculateGainForSplit(leftNodeAgg: Array[Array[Double]],
                              featureIndex: Int,
                              index: Int,
                              rightNodeAgg: Array[Array[Double]],
                              topImpurity: Double) : InformationGainStats = {

      val left0Count = leftNodeAgg(featureIndex)(2 * index)
      val left1Count = leftNodeAgg(featureIndex)(2 * index + 1)
      val leftCount = left0Count + left1Count

      val right0Count = rightNodeAgg(featureIndex)(2 * index)
      val right1Count = rightNodeAgg(featureIndex)(2 * index + 1)
      val rightCount = right0Count + right1Count

      val impurity = if (level > 0) topImpurity else strategy.impurity.calculate(left0Count + right0Count, left1Count + right1Count)

      if (leftCount == 0) return new InformationGainStats(0,topImpurity,Double.MinValue,0,topImpurity,rightCount.toLong)
      if (rightCount == 0) return new InformationGainStats(0,topImpurity,topImpurity,leftCount.toLong,Double.MinValue,0)

      val leftImpurity = strategy.impurity.calculate(left0Count, left1Count)
      val rightImpurity = strategy.impurity.calculate(right0Count, right1Count)

      val leftWeight = leftCount.toDouble / (leftCount + rightCount)
      val rightWeight = rightCount.toDouble / (leftCount + rightCount)

      val gain = {
        if (level > 0) {
          impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
        } else {
          impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
        }
      }

      new InformationGainStats(gain,impurity,leftImpurity,leftCount.toLong,rightImpurity,rightCount.toLong)

    }

    /*
    Extracts left and right split aggregates

    @param binData Array[Double] of size 2*numFeatures*numSplits
    @return (leftNodeAgg, rightNodeAgg) tuple of type (Array[Double], Array[Double]) where
    each array is of size(numFeature,2*(numSplits-1))
     */
    def extractLeftRightNodeAggregates(binData: Array[Double]): (Array[Array[Double]], Array[Array[Double]]) = {
      val leftNodeAgg = Array.ofDim[Double](numFeatures, 2 * (numSplits - 1))
      val rightNodeAgg = Array.ofDim[Double](numFeatures, 2 * (numSplits - 1))
      for (featureIndex <- 0 until numFeatures) {
        val shift = 2*featureIndex*numSplits
        leftNodeAgg(featureIndex)(0) = binData(shift + 0)
        leftNodeAgg(featureIndex)(1) = binData(shift + 1)
        rightNodeAgg(featureIndex)(2 * (numSplits - 2)) = binData(shift + (2 * (numSplits - 1)))
        rightNodeAgg(featureIndex)(2 * (numSplits - 2) + 1) = binData(shift + (2 * (numSplits - 1)) + 1)
        for (splitIndex <- 1 until numSplits - 1) {
          leftNodeAgg(featureIndex)(2 * splitIndex)
            = binData(shift + 2*splitIndex) + leftNodeAgg(featureIndex)(2 * splitIndex - 2)
          leftNodeAgg(featureIndex)(2 * splitIndex + 1)
            = binData(shift + 2*splitIndex + 1) + leftNodeAgg(featureIndex)(2 * splitIndex - 2 + 1)
          rightNodeAgg(featureIndex)(2 * (numSplits - 2 - splitIndex))
            = binData(shift + (2 * (numSplits - 1 - splitIndex))) + rightNodeAgg(featureIndex)(2 * (numSplits - 1 - splitIndex))
          rightNodeAgg(featureIndex)(2 * (numSplits - 2 - splitIndex) + 1)
            = binData(shift + (2 * (numSplits - 1 - splitIndex) + 1)) + rightNodeAgg(featureIndex)(2 * (numSplits - 1 - splitIndex) + 1)
        }
      }
      (leftNodeAgg, rightNodeAgg)
    }

    def calculateGainsForAllNodeSplits(leftNodeAgg: Array[Array[Double]], rightNodeAgg: Array[Array[Double]], nodeImpurity: Double)
    : Array[Array[InformationGainStats]] = {

      val gains = Array.ofDim[InformationGainStats](numFeatures, numSplits - 1)

      for (featureIndex <- 0 until numFeatures) {
        for (index <- 0 until numSplits -1) {
          //logDebug("splitIndex = " + index)
          gains(featureIndex)(index) = calculateGainForSplit(leftNodeAgg, featureIndex, index, rightNodeAgg, nodeImpurity)
        }
      }
      gains
    }

    /*
        Find the best split for a node given bin aggregate data

        @param binData Array[Double] of size 2*numSplits*numFeatures
        */
    def binsToBestSplit(binData : Array[Double], nodeImpurity : Double) : (Split, InformationGainStats) = {
      logDebug("node impurity = " + nodeImpurity)
      val (leftNodeAgg, rightNodeAgg) = extractLeftRightNodeAggregates(binData)
      val gains = calculateGainsForAllNodeSplits(leftNodeAgg, rightNodeAgg, nodeImpurity)

      val (bestFeatureIndex,bestSplitIndex, gainStats) = {
        var bestFeatureIndex = 0
        var bestSplitIndex = 0
        //Initialization with infeasible values
        var bestGainStats = new InformationGainStats(Double.MinValue,-1.0,-1.0,0,-1.0,0)
        for (featureIndex <- 0 until numFeatures) {
          for (splitIndex <- 0 until numSplits - 1){
            val gainStats =  gains(featureIndex)(splitIndex)
            if(gainStats.gain > bestGainStats.gain) {
              bestGainStats = gainStats
              bestFeatureIndex = featureIndex
              bestSplitIndex = splitIndex
            }
          }
        }
        (bestFeatureIndex,bestSplitIndex,bestGainStats)
      }

      (splits(bestFeatureIndex)(bestSplitIndex),gainStats)
    }

    //Calculate best splits for all nodes at a given level
    val bestSplits = new Array[(Split, InformationGainStats)](numNodes)
    for (node <- 0 until numNodes){
      val nodeImpurityIndex = scala.math.pow(2, level).toInt - 1 + node
      val shift = 2*node*numSplits*numFeatures
      val binsForNode = binAggregates.slice(shift,shift+2*numSplits*numFeatures)
      logDebug("nodeImpurityIndex = " + nodeImpurityIndex)
      val parentNodeImpurity = parentImpurities(nodeImpurityIndex)
      logDebug("node impurity = " + parentNodeImpurity)
      bestSplits(node) = binsToBestSplit(binsForNode, parentNodeImpurity)
    }

    bestSplits
  }

  /*
  Returns split and bins for decision tree calculation.

  @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data for DecisionTree
  @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing parameters for construction the DecisionTree
  @return a tuple of (splits,bins) where Split is an Array[Array[Split]] of size (numFeatures,numSplits-1) and bins is an
   Array[Array[Bin]] of size (numFeatures,numSplits1)
   */
  def find_splits_bins(input : RDD[LabeledPoint], strategy : Strategy) : (Array[Array[Split]], Array[Array[Bin]]) = {

    val count = input.count()

    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length

    val maxBins = strategy.maxBins
    val numBins = if (maxBins <= count) maxBins else count.toInt

    logDebug("maxBins = " + numBins)
    //Calculate the number of sample for approximate quantile calculation
    //TODO: Justify this calculation
    val requiredSamples = numBins*numBins
    val fraction = if (requiredSamples < count) requiredSamples.toDouble / count else 1.0
    logDebug("fraction of data used for calculating quantiles = " + fraction)
    //sampled input for RDD calculation
    val sampledInput = input.sample(false, fraction, 42).collect()
    val numSamples = sampledInput.length

    val stride : Double = numSamples.toDouble/numBins
    logDebug("stride = " + stride)

    strategy.quantileCalculationStrategy match {
      case Sort => {
        val splits =  Array.ofDim[Split](numFeatures,numBins-1)
        val bins = Array.ofDim[Bin](numFeatures,numBins)

        //Find all splits
        for (featureIndex <- 0 until numFeatures){
          val isFeatureContinous = strategy.categoricalFeaturesInfo.get(featureIndex).isEmpty
          if (isFeatureContinous) {
            val featureSamples  = sampledInput.map(lp => lp.features(featureIndex)).sorted

            val stride : Double = numSamples.toDouble/numBins
            logDebug("stride = " + stride)
            for (index <- 0 until numBins-1) {
              val sampleIndex = (index+1)*stride.toInt
              val split = new Split(featureIndex,featureSamples(sampleIndex),Continuous)
              splits(featureIndex)(index) = split
            }
          } else {
            val maxFeatureValue = strategy.categoricalFeaturesInfo(featureIndex)
            for (index <- 0 until maxFeatureValue){
              //TODO: Sort by centriod
              val split = new Split(featureIndex,index,Categorical)
              splits(featureIndex)(index) = split
            }
          }
        }

        //Find all bins
        for (featureIndex <- 0 until numFeatures){
          bins(featureIndex)(0)
            = new Bin(new DummyLowSplit(Continuous),splits(featureIndex)(0),Continuous)
          for (index <- 1 until numBins - 1){
            val bin = new Bin(splits(featureIndex)(index-1),splits(featureIndex)(index),Continuous)
            bins(featureIndex)(index) = bin
          }
          bins(featureIndex)(numBins-1)
            = new Bin(splits(featureIndex)(numBins-3),new DummyHighSplit(Continuous),Continuous)
        }

        (splits,bins)
      }
      case MinMax => {
        throw new UnsupportedOperationException("minmax not supported yet.")
      }
      case ApproxHist => {
        throw new UnsupportedOperationException("approximate histogram not supported yet.")
      }

    }
  }



}