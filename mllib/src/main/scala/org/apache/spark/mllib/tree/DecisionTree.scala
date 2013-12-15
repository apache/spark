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
import org.apache.spark.mllib.tree.impurity.Gini


class DecisionTree(val strategy : Strategy) {

  def train(input : RDD[LabeledPoint]) : DecisionTreeModel = {

    //Cache input RDD for speedup during multiple passes
    input.cache()

    //TODO: Find all splits and bins using quantiles including support for categorical features, single-pass
    //TODO: Think about broadcasting this
    val (splits, bins) = DecisionTree.find_splits_bins(input, strategy)

    //TODO: Level-wise training of tree and obtain Decision Tree model
    val maxDepth = strategy.maxDepth

    val maxNumNodes = scala.math.pow(2,maxDepth).toInt - 1
    val filters = new Array[List[Filter]](maxNumNodes)

    for (level <- 0 until maxDepth){
      //Find best split for all nodes at a level
      val numNodes= scala.math.pow(2,level).toInt
      //TODO: Change the input parent impurities values
      val splits_stats_for_level = DecisionTree.findBestSplits(input, Array(2.0), strategy, level, filters,splits,bins)
      for (tmp <- splits_stats_for_level){
        println("final best split = " + tmp._1)
      }
      //TODO: update filters and decision tree model
      require(scala.math.pow(2,level)==splits_stats_for_level.length)

    }

    return new DecisionTreeModel()
  }

}

object DecisionTree extends Serializable {

  /*
  Returns an Array[Split] of optimal splits for all nodes at a given level

  @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data for DecisionTree
  @param parentImpurities Impurities for all parent nodes for the current level
  @param strategy [[org.apache.spark.mllib.tree.Strategy]] instance containing parameters for construction the DecisionTree
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
                      bins : Array[Array[Bin]]) : Array[(Split, Double, Long, Long)] = {

    //Common calculations for multiple nested methods
    val numNodes = scala.math.pow(2, level).toInt
    println("numNodes = " + numNodes)
    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length
    println("numFeatures = " + numFeatures)
    val numSplits = strategy.numSplits
    println("numSplits = " + numSplits)

    /*Find the filters used before reaching the current code*/
    def findParentFilters(nodeIndex: Int): List[Filter] = {
      if (level == 0) {
        List[Filter]()
      } else {
        val nodeFilterIndex = scala.math.pow(2, level).toInt + nodeIndex
        //val parentFilterIndex = nodeFilterIndex / 2
        //TODO: Check left or right filter
        filters(nodeFilterIndex)
      }
    }

    /*Find whether the sample is valid input for the current node.

    In other words, does it pass through all the filters for the current node.
    */
    def isSampleValid(parentFilters: List[Filter], labeledPoint: LabeledPoint): Boolean = {

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
      //println("finding bin for labeled point " + labeledPoint.features(featureIndex))
      //TODO: Do binary search
      for (binIndex <- 0 until strategy.numSplits) {
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
      for (nodeIndex <- 0 until numNodes) {
        val parentFilters = findParentFilters(nodeIndex)
        //Find out whether the sample qualifies for the particular node
        val sampleValid = isSampleValid(parentFilters, labeledPoint)
        val shift = 1 + numFeatures * nodeIndex
        if (!sampleValid) {
          //Add to invalid bin index -1
          for (featureIndex <- 0 until numFeatures) {
            arr(shift+featureIndex) = -1
            //TODO: Break since marking one bin is sufficient
          }
        } else {
          for (featureIndex <- 0 until numFeatures) {
            //println("shift+featureIndex =" + (shift+featureIndex))
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
    println("binAggregageLength = " + binAggregateLength)

    /*Combines the aggregates from partitions
    @param agg1 Array containing aggregates from one or more partitions
    @param agg2 Array contianing aggregates from one or more partitions

    @return Combined aggregate from agg1 and agg2
     */
    def binCombOp(agg1 : Array[Double], agg2: Array[Double]) : Array[Double] = {
      val combinedAggregate = new Array[Double](binAggregateLength)
      for (index <- 0 until binAggregateLength){
        combinedAggregate(index) = agg1(index) + agg2(index)
      }
      combinedAggregate
    }

    println("input = " + input.count)
    val binMappedRDD = input.map(x => findBinsForLevel(x))
    println("binMappedRDD.count = " + binMappedRDD.count)
    //calculate bin aggregates

    val binAggregates = binMappedRDD.aggregate(Array.fill[Double](2*numSplits*numFeatures*numNodes)(0))(binSeqOp,binCombOp)
    println("binAggregates.length = " + binAggregates.length)
    //binAggregates.foreach(x => println(x))


    def calculateGainForSplit(leftNodeAgg: Array[Array[Double]],
                              featureIndex: Int,
                              index: Int,
                              rightNodeAgg: Array[Array[Double]],
                              topImpurity: Double) : (Double, Long, Long) = {

      val left0Count = leftNodeAgg(featureIndex)(2 * index)
      val left1Count = leftNodeAgg(featureIndex)(2 * index + 1)
      val leftCount = left0Count + left1Count

      val right0Count = rightNodeAgg(featureIndex)(2 * index)
      val right1Count = rightNodeAgg(featureIndex)(2 * index + 1)
      val rightCount = right0Count + right1Count

      if (leftCount == 0) return (0, leftCount.toLong, rightCount.toLong)

      //println("left0count = " + left0Count + ", left1count = " + left1Count + ", leftCount = " + leftCount)
      val leftImpurity = strategy.impurity.calculate(left0Count, left1Count)

      if (rightCount == 0) return (0, leftCount.toLong, rightCount.toLong)

      //println("right0count = " + right0Count + ", right1count = " + right1Count + ", rightCount = " + rightCount)
      val rightImpurity = strategy.impurity.calculate(right0Count, right1Count)

      val leftWeight = leftCount.toDouble / (leftCount + rightCount)
      val rightWeight = rightCount.toDouble / (leftCount + rightCount)

      (topImpurity - leftWeight * leftImpurity - rightWeight * rightImpurity, leftCount.toLong, rightCount.toLong)

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
      //println("binData.length = " + binData.length)
      //println("binData.sum = " + binData.sum)
      for (featureIndex <- 0 until numFeatures) {
        //println("featureIndex = " + featureIndex)
        val shift = 2*featureIndex*numSplits
        leftNodeAgg(featureIndex)(0) = binData(shift + 0)
        //println("binData(shift + 0) = " + binData(shift + 0))
        leftNodeAgg(featureIndex)(1) = binData(shift + 1)
        //println("binData(shift + 1) = " + binData(shift + 1))
        rightNodeAgg(featureIndex)(2 * (numSplits - 2)) = binData(shift + (2 * (numSplits - 1)))
        //println(binData(shift + (2 * (numSplits - 1))))
        rightNodeAgg(featureIndex)(2 * (numSplits - 2) + 1) = binData(shift + (2 * (numSplits - 1)) + 1)
        //println(binData(shift + (2 * (numSplits - 1)) + 1))
        for (splitIndex <- 1 until numSplits - 1) {
          //println("splitIndex = " + splitIndex)
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
    : Array[Array[(Double,Long,Long)]] = {

      val gains = Array.ofDim[(Double,Long,Long)](numFeatures, numSplits - 1)

      for (featureIndex <- 0 until numFeatures) {
        for (index <- 0 until numSplits -1) {
          //println("splitIndex = " + index)
          gains(featureIndex)(index) = calculateGainForSplit(leftNodeAgg, featureIndex, index, rightNodeAgg, nodeImpurity)
        }
      }
      gains
    }

    /*
        Find the best split for a node given bin aggregate data

        @param binData Array[Double] of size 2*numSplits*numFeatures
        */
    def binsToBestSplit(binData : Array[Double], nodeImpurity : Double) : (Split, Double, Long, Long) = {
      println("node impurity = " + nodeImpurity)
      val (leftNodeAgg, rightNodeAgg) = extractLeftRightNodeAggregates(binData)
      val gains = calculateGainsForAllNodeSplits(leftNodeAgg, rightNodeAgg, nodeImpurity)

      //println("gains.size = " + gains.size)
      //println("gains(0).size = " + gains(0).size)

      val (bestFeatureIndex,bestSplitIndex, gain, leftCount, rightCount) = {
        var bestFeatureIndex = 0
        var bestSplitIndex = 0
        var maxGain = Double.MinValue
        var leftSamples = Long.MinValue
        var rightSamples = Long.MinValue
        for (featureIndex <- 0 until numFeatures) {
          for (splitIndex <- 0 until numSplits - 1){
            val gain =  gains(featureIndex)(splitIndex)
            //println("featureIndex =  " + featureIndex + ", splitIndex =  " + splitIndex + ", gain = " + gain)
            if(gain._1 > maxGain) {
              maxGain = gain._1
              leftSamples = gain._2
              rightSamples = gain._3
              bestFeatureIndex = featureIndex
              bestSplitIndex = splitIndex
              println("bestFeatureIndex =  " + bestFeatureIndex + ", bestSplitIndex =  " + bestSplitIndex
                + ", maxGain = " + maxGain + ", leftSamples = " + leftSamples + ",rightSamples = " + rightSamples)
            }
          }
        }
        (bestFeatureIndex,bestSplitIndex,maxGain,leftSamples,rightSamples)
      }

      (splits(bestFeatureIndex)(bestSplitIndex),gain,leftCount,rightCount)
      //TODO: Return array of node stats with split and impurity information
    }

    //Calculate best splits for all nodes at a given level
    val bestSplits = new Array[(Split, Double, Long, Long)](numNodes)
    for (node <- 0 until numNodes){
      val shift = 2*node*numSplits*numFeatures
      val binsForNode = binAggregates.slice(shift,shift+2*numSplits*numFeatures)
      val parentNodeImpurity = parentImpurities(node/2)
      bestSplits(node) = binsToBestSplit(binsForNode, parentNodeImpurity)
    }

    bestSplits
  }

  /*
  Returns split and bins for decision tree calculation.

  @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data for DecisionTree
  @param strategy [[org.apache.spark.mllib.tree.Strategy]] instance containing parameters for construction the DecisionTree
  @return a tuple of (splits,bins) where Split is an Array[Array[Split]] of size (numFeatures,numSplits-1) and bins is an
   Array[Array[Bin]] of size (numFeatures,numSplits1)
   */
  def find_splits_bins(input : RDD[LabeledPoint], strategy : Strategy) : (Array[Array[Split]], Array[Array[Bin]]) = {

    val numSplits = strategy.numSplits
    println("numSplits = " + numSplits)

    //Calculate the number of sample for approximate quantile calculation
    //TODO: Justify this calculation
    val requiredSamples = numSplits*numSplits
    val count = input.count()
    val fraction = if (requiredSamples < count) requiredSamples.toDouble / count else 1.0
    println("fraction of data used for calculating quantiles = " + fraction)

    //sampled input for RDD calculation
    val sampledInput = input.sample(false, fraction, 42).collect()
    val numSamples = sampledInput.length

    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length

    strategy.quantileCalculationStrategy match {
      case "sort" => {
        val splits =  Array.ofDim[Split](numFeatures,numSplits-1)
        val bins = Array.ofDim[Bin](numFeatures,numSplits)

        //Find all splits
        for (featureIndex <- 0 until numFeatures){
          val featureSamples  = sampledInput.map(lp => lp.features(featureIndex)).sorted

          if (numSamples < numSplits) {
            //TODO: Test this
            println("numSamples = " + numSamples + ", less than numSplits = " + numSplits)
            for (index <- 0 until numSplits-1) {
              val split = new Split(featureIndex,featureSamples(index),"continuous")
              splits(featureIndex)(index) = split
            }
          } else {
            val stride : Double = numSamples.toDouble/numSplits
            println("stride = " + stride)
            for (index <- 0 until numSplits-1) {
              val sampleIndex = (index+1)*stride.toInt
              val split = new Split(featureIndex,featureSamples(sampleIndex),"continuous")
              splits(featureIndex)(index) = split
            }
          }
        }

        //Find all bins
        for (featureIndex <- 0 until numFeatures){
          bins(featureIndex)(0)
            = new Bin(new DummyLowSplit("continuous"),splits(featureIndex)(0),"continuous")
          for (index <- 1 until numSplits - 1){
            val bin = new Bin(splits(featureIndex)(index-1),splits(featureIndex)(index),"continuous")
            bins(featureIndex)(index) = bin
          }
          bins(featureIndex)(numSplits-1)
            = new Bin(splits(featureIndex)(numSplits-3),new DummyHighSplit("continuous"),"continuous")
        }

        (splits,bins)
      }
      case "minMax" => {
        (Array.ofDim[Split](numFeatures,numSplits),Array.ofDim[Bin](numFeatures,numSplits+2))
      }
      case "approximateHistogram" => {
        throw new UnsupportedOperationException("approximate histogram not supported yet.")
      }

    }
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "DecisionTree")
    val data = loadLabeledData(sc, args(1))

    val strategy = new Strategy(kind = "classification", impurity = Gini, maxDepth = 2, numSplits = 569)
    val model = new DecisionTree(strategy).train(data)

    sc.stop()
  }

  /**
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.trim().split(",")
      val label = parts(0).toDouble
      val features = parts.slice(1,parts.length).map(_.toDouble)
      LabeledPoint(label, features)
    }
  }



}