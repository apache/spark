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
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.Split


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
      val bestSplits = DecisionTree.findBestSplits(input, strategy, level, filters,splits,bins)
      //TODO: update filters and decision tree model
    }

    return new DecisionTreeModel()
  }

}

object DecisionTree extends Serializable {

  /*
  Returns an Array[Split] of optimal splits for all nodes at a given level

  @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data for DecisionTree
  @param strategy [[org.apache.spark.mllib.tree.Strategy]] instance containing parameters for construction the DecisionTree
  @param level Level of the tree
  @param filters Filter for all nodes at a given level
  @param splits possible splits for all features
  @param bins possible bins for all features

  @return Array[Split] instance for best splits for all nodes at a given level.
    */
  def findBestSplits(
                      input : RDD[LabeledPoint],
                      strategy: Strategy,
                      level: Int,
                      filters : Array[List[Filter]],
                      splits : Array[Array[Split]],
                      bins : Array[Array[Bin]]) : Array[Split] = {

    //TODO: Move these calculations outside
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
        val parentFilterIndex = nodeFilterIndex / 2
        filters(parentFilterIndex)
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
     k features, l nodes
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
    Performs a sequential aggreation over a partition

    @param agg Array[Double] storing aggregate calculation of size numSplits*numFeatures*numNodes for classification
    and 3*numSplits*numFeatures*numNodes for regression
    @param arr Array[Double] of size 1+(numFeatures*numNodes)
    @return Array[Double] storing aggregate calculation of size numSplits*numFeatures*numNodes for classification
    and 3*numSplits*numFeatures*numNodes for regression
     */
    def binSeqOp(agg : Array[Double], arr: Array[Double]) : Array[Double] = {
      for (node <- 0 until numNodes) {
         val validSignalIndex = 1+numFeatures*node
         val isSampleValidForNode = if(arr(validSignalIndex) != -1) true else false
         if(isSampleValidForNode) {
           for (feature <- 0 until numFeatures){
               val arrShift = 1 + numFeatures*node
               val aggShift = numSplits*numFeatures*node
               val arrIndex = arrShift + feature
               val aggIndex = aggShift + feature*numSplits + arr(arrIndex).toInt
               agg(aggIndex) =  agg(aggIndex) + 1
           }
         }
      }
      agg
    }

    def binCombOp(par1 : Array[Double], par2: Array[Double]) : Array[Double] = {
      par1
    }

    println("input = " + input.count)
    val binMappedRDD = input.map(x => findBinsForLevel(x))
    println("binMappedRDD.count = " + binMappedRDD.count)
    //calculate bin aggregates

    val binAggregates = binMappedRDD.aggregate(Array.fill[Double](numSplits*numFeatures*numNodes)(0))(binSeqOp,binCombOp)

    //find best split
    println("binAggregates.length = " + binAggregates.length)


    val bestSplits = new Array[Split](numNodes)
    for (node <- 0 until numNodes){
       val binsForNode = binAggregates.slice(node,numSplits*node)
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

    //TODO: Remove this requirement
    require(numSamples > numSplits, "length of input samples should be greater than numSplits")

    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length

    strategy.quantileCalculationStrategy match {
      case "sort" => {
        val splits =  Array.ofDim[Split](numFeatures,numSplits-1)
        val bins = Array.ofDim[Bin](numFeatures,numSplits)

        //Find all splits
        for (featureIndex <- 0 until numFeatures){
          val featureSamples  = sampledInput.map(lp => lp.features(featureIndex)).sorted
          val stride : Double = numSamples.toDouble/numSplits
          for (index <- 0 until numSplits-1) {
            val sampleIndex = (index+1)*stride.toInt
            val split = new Split(featureIndex,featureSamples(sampleIndex),"continuous")
            splits(featureIndex)(index) = split
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

}